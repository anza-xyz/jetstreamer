from dataclasses import replace
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import guard_historical_performance_canaries as guard


def sample(**overrides: object) -> guard.UnitSample:
    base = guard.UnitSample(
        unit="horizon-perf-epoch202@singleton-t16.service",
        load_state="loaded",
        active_state="active",
        sub_state="running",
        result="success",
        main_pid=123,
        invocation_id="invocation",
        restarts=0,
        control_group="/system.slice/test.service",
        worker_pid=124,
        worker_vmas=500_000,
    )
    return replace(base, **overrides)


class HistoricalPerformanceGuardTest(unittest.TestCase):
    def test_parses_systemctl_properties_without_splitting_values(self) -> None:
        self.assertEqual(
            guard.parse_systemctl_show("LoadState=loaded\nControlGroup=/a=b\n"),
            {"LoadState": "loaded", "ControlGroup": "/a=b"},
        )

    def test_healthy_cohort_has_no_trip_reason(self) -> None:
        self.assertEqual(guard.trip_reasons(200, 100, 900_000, [sample()]), [])

    def test_disk_vma_restart_and_failure_trip_independently(self) -> None:
        reasons = guard.trip_reasons(
            99,
            100,
            900_000,
            [
                sample(
                    active_state="failed",
                    result="exit-code",
                    restarts=1,
                    worker_vmas=900_001,
                )
            ],
        )
        self.assertEqual(len(reasons), 4)
        self.assertTrue(any("below floor" in reason for reason in reasons))
        self.assertTrue(any("terminal failure" in reason for reason in reasons))
        self.assertTrue(any("restarted" in reason for reason in reasons))
        self.assertTrue(any("VMAs" in reason for reason in reasons))

    def test_successfully_finished_peer_does_not_trip_remaining_canaries(self) -> None:
        finished = sample(
            active_state="inactive",
            sub_state="dead",
            result="success",
            main_pid=0,
            worker_pid=None,
            worker_vmas=None,
        )
        self.assertEqual(guard.trip_reasons(200, 100, 900_000, [finished]), [])
        self.assertFalse(guard.cohort_running([finished]))
        self.assertTrue(guard.cohort_running([finished, sample()]))

    @patch("scripts.guard_historical_performance_canaries.subprocess.run")
    def test_stops_all_active_units_in_one_systemd_transaction(self, run: object) -> None:
        run.return_value = SimpleNamespace(returncode=0, stderr="")
        active_two = replace(
            sample(), unit="horizon-perf-epoch202@waves-t16.service"
        )
        finished = sample(active_state="inactive", main_pid=0)
        outcomes = guard.stop_active_units([sample(), active_two, finished])
        run.assert_called_once_with(
            [
                "systemctl",
                "stop",
                "horizon-perf-epoch202@singleton-t16.service",
                "horizon-perf-epoch202@waves-t16.service",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(outcomes[0]["return_code"], 0)

    def test_trip_receipt_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trip.json"
            guard.write_json_noclobber(path, {"reason": "test"})
            self.assertEqual(json.loads(path.read_text()), {"reason": "test"})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                guard.write_json_noclobber(path, {})

    def test_trip_receipt_requires_owner_only_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o755)
            with self.assertRaisesRegex(ValueError, "not owner-only"):
                guard.write_json_noclobber(Path(directory) / "trip.json", {})


if __name__ == "__main__":
    unittest.main()
