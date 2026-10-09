from dataclasses import replace
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import guard_historical_replay_reserve as guard


INVOCATION = "a" * 32


def sample(**overrides: object) -> guard.UnitSample:
    value = guard.UnitSample(
        unit="horizon-qualify-204-v15-retry3.service",
        load_state="loaded",
        active_state="active",
        sub_state="running",
        result="success",
        main_pid=123,
        invocation_id=INVOCATION,
        restarts=0,
    )
    return replace(value, **overrides)


class HistoricalReplayReserveGuardTest(unittest.TestCase):
    def test_healthy_sample_does_not_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            with (
                patch.object(guard, "available_bytes", return_value=200),
                patch.object(guard, "sample_unit", return_value=sample()),
                patch.object(guard, "run_systemctl") as systemctl,
            ):
                with redirect_stdout(io.StringIO()):
                    result = guard.main(
                        [
                            "--unit=horizon-qualify-204-v15-retry3.service",
                            f"--expected-invocation-id={INVOCATION}",
                            f"--filesystem={root}",
                            "--minimum-free-bytes=100",
                            "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                            f"--intent-receipt={root / 'intent.json'}",
                            f"--completion-receipt={root / 'complete.json'}",
                        ]
                    )
            self.assertEqual(result, 0)
            systemctl.assert_not_called()
            self.assertFalse((root / "intent.json").exists())

    def test_trip_writes_intent_before_stop_and_completion_after(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            stopped = sample(active_state="inactive", sub_state="dead", main_pid=0)
            events: list[str] = []

            def systemctl(arguments: list[str]) -> SimpleNamespace:
                events.append("systemctl:" + " ".join(arguments))
                return SimpleNamespace(returncode=0, stderr="")

            original_write = guard.write_json_noclobber
            with (
                patch.object(guard, "available_bytes", side_effect=[99, 99]),
                patch.object(guard, "sample_unit", side_effect=[sample(), stopped]),
                patch.object(guard, "run_systemctl", side_effect=systemctl),
                patch.object(guard, "write_json_noclobber") as patched_write,
            ):
                patched_write.side_effect = lambda path, payload: (
                    events.append(f"write:{path.name}"),
                    original_write(path, payload),
                )[-1]
                with redirect_stdout(io.StringIO()):
                    result = guard.main(
                        [
                            "--unit=horizon-qualify-204-v15-retry3.service",
                            f"--expected-invocation-id={INVOCATION}",
                            f"--filesystem={root}",
                            "--minimum-free-bytes=100",
                            "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                            f"--intent-receipt={root / 'intent.json'}",
                            f"--completion-receipt={root / 'complete.json'}",
                        ]
                    )
            self.assertEqual(result, 1)
            self.assertEqual(
                events,
                [
                    "write:intent.json",
                    "systemctl:stop horizon-qualify-204-v15-retry3.service",
                    "write:complete.json",
                    "systemctl:stop horizon-monitor-recurring-204-v15-retry3.timer",
                ],
            )
            self.assertEqual(
                json.loads((root / "intent.json").read_text())["schema"],
                guard.INTENT_SCHEMA,
            )
            completion = json.loads((root / "complete.json").read_text())
            self.assertEqual(completion["schema"], guard.COMPLETION_SCHEMA)
            self.assertFalse(completion["scratch_deleted"])
            self.assertEqual(stat.S_IMODE((root / "intent.json").stat().st_mode), 0o600)

    def test_mismatching_invocation_refuses_to_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            with (
                patch.object(guard, "available_bytes", return_value=1),
                patch.object(
                    guard,
                    "sample_unit",
                    return_value=sample(invocation_id="b" * 32),
                ),
                patch.object(guard, "run_systemctl") as systemctl,
                self.assertRaisesRegex(guard.GuardError, "invocation changed"),
            ):
                guard.main(
                    [
                        "--unit=horizon-qualify-204-v15-retry3.service",
                        f"--expected-invocation-id={INVOCATION}",
                        f"--filesystem={root}",
                        "--minimum-free-bytes=100",
                        "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                        f"--intent-receipt={root / 'intent.json'}",
                        f"--completion-receipt={root / 'complete.json'}",
                    ]
                )
            systemctl.assert_not_called()

    def test_naturally_finished_unit_does_not_trip_below_floor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            finished = sample(active_state="inactive", sub_state="dead", main_pid=0)
            with (
                patch.object(guard, "available_bytes", return_value=1),
                patch.object(guard, "sample_unit", return_value=finished),
                patch.object(guard, "run_systemctl") as systemctl,
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(
                    [
                        "--unit=horizon-qualify-204-v15-retry3.service",
                        f"--expected-invocation-id={INVOCATION}",
                        f"--filesystem={root}",
                        "--minimum-free-bytes=100",
                        "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                        f"--intent-receipt={root / 'intent.json'}",
                        f"--completion-receipt={root / 'complete.json'}",
                    ]
                )
            self.assertEqual(result, 0)
            systemctl.assert_not_called()
            self.assertFalse((root / "intent.json").exists())

    def test_intent_resume_stops_even_if_free_space_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            intent_path = root / "intent.json"
            guard.write_json_noclobber(
                intent_path,
                {
                    "schema": guard.INTENT_SCHEMA,
                    "unit": "horizon-qualify-204-v15-retry3.service",
                    "expected_invocation_id": INVOCATION,
                    "filesystem": str(root),
                    "minimum_free_bytes": 100,
                },
            )
            stopped = sample(active_state="inactive", sub_state="dead", main_pid=0)
            with (
                patch.object(guard, "available_bytes", side_effect=[200, 200]),
                patch.object(guard, "sample_unit", side_effect=[sample(), stopped]),
                patch.object(
                    guard,
                    "run_systemctl",
                    return_value=SimpleNamespace(returncode=0, stderr=""),
                ) as systemctl,
            ):
                with redirect_stdout(io.StringIO()):
                    result = guard.main(
                        [
                            "--unit=horizon-qualify-204-v15-retry3.service",
                            f"--expected-invocation-id={INVOCATION}",
                            f"--filesystem={root}",
                            "--minimum-free-bytes=100",
                            "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                            f"--intent-receipt={intent_path}",
                            f"--completion-receipt={root / 'complete.json'}",
                        ]
                    )
                self.assertEqual(result, 1)
            self.assertEqual(systemctl.call_count, 2)
            self.assertTrue((root / "complete.json").exists())

    def test_existing_unbound_completion_refuses_to_stop_timer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            completion_path = root / "complete.json"
            guard.write_json_noclobber(
                completion_path,
                {
                    "schema": guard.COMPLETION_SCHEMA,
                    "unit": "horizon-qualify-999.service",
                },
            )
            with (
                patch.object(guard, "run_systemctl") as systemctl,
                self.assertRaisesRegex(guard.GuardError, "does not match"),
            ):
                guard.main(
                    [
                        "--unit=horizon-qualify-204-v15-retry3.service",
                        f"--expected-invocation-id={INVOCATION}",
                        f"--filesystem={root}",
                        "--minimum-free-bytes=100",
                        "--monitor-timer=horizon-monitor-recurring-204-v15-retry3.timer",
                        f"--intent-receipt={root / 'intent.json'}",
                        f"--completion-receipt={completion_path}",
                    ]
                )
            systemctl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
