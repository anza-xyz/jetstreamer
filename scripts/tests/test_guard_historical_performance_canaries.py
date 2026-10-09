from dataclasses import replace
import json
import os
from pathlib import Path
import signal
import stat
import tempfile
import time
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
        worker_count=1,
        worker_pid=124,
        worker_vmas=500_000,
    )
    return replace(base, **overrides)


class HistoricalPerformanceGuardTest(unittest.TestCase):
    def test_accepts_qualification_unit_and_recurring_timer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = guard.parse_args(
                [
                    f"--filesystem={root}",
                    "--minimum-free-bytes=1",
                    "--maximum-worker-vmas=1",
                    f"--receipt-directory={root}",
                    "--timer-unit=horizon-monitor-recurring-214-215-contiguous-v1.timer",
                    "horizon-qualify-214-215-contiguous-v1.service",
                ]
            )
            self.assertEqual(
                args.units, ["horizon-qualify-214-215-contiguous-v1.service"]
            )

    def test_accepts_dedicated_followup_guard_timer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = guard.parse_args(
                [
                    f"--filesystem={root}",
                    "--minimum-free-bytes=1",
                    "--maximum-worker-vmas=1",
                    f"--receipt-directory={root}",
                    "--timer-unit=horizon-perf-epoch202-guard-store8.timer",
                    "horizon-perf-epoch202@waves-control-t16.service",
                ]
            )
            self.assertEqual(
                args.timer_unit, "horizon-perf-epoch202-guard-store8.timer"
            )

    def test_parses_systemctl_properties_without_splitting_values(self) -> None:
        self.assertEqual(
            guard.parse_systemctl_show("LoadState=loaded\nControlGroup=/a=b\n"),
            {"LoadState": "loaded", "ControlGroup": "/a=b"},
        )

    def test_healthy_cohort_has_no_trip_reason(self) -> None:
        self.assertEqual(guard.trip_reasons(200, 100, 900_000, [sample()]), [])

    def test_exact_invocation_binding_trips_only_on_mismatch(self) -> None:
        expected = {sample().unit: "a" * 32}
        self.assertEqual(
            guard.trip_reasons(
                200,
                100,
                900_000,
                [sample(invocation_id="a" * 32)],
                expected,
            ),
            [],
        )
        reasons = guard.trip_reasons(200, 100, 900_000, [sample()], expected)
        self.assertTrue(any("does not match" in reason for reason in reasons))

    def test_expected_invocation_cli_must_bind_every_guarded_unit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = guard.parse_args(
                [
                    f"--filesystem={root}",
                    "--minimum-free-bytes=1",
                    "--maximum-worker-vmas=1",
                    f"--receipt-directory={root}",
                    "--timer-unit=horizon-monitor-recurring-test.timer",
                    "--expected-invocation=horizon-qualify-a.service=" + "a" * 32,
                    "horizon-qualify-a.service",
                ]
            )
            self.assertEqual(
                args.expected_invocations,
                {"horizon-qualify-a.service": "a" * 32},
            )
            with self.assertRaises(SystemExit):
                guard.parse_args(
                    [
                        f"--filesystem={root}",
                        "--minimum-free-bytes=1",
                        "--maximum-worker-vmas=1",
                        f"--receipt-directory={root}",
                        "--timer-unit=horizon-monitor-recurring-test.timer",
                        "--expected-invocation=horizon-qualify-a.service=" + "a" * 32,
                        "horizon-qualify-a.service",
                        "horizon-qualify-b.service",
                    ]
                )

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
            worker_count=0,
            worker_pid=None,
            worker_vmas=None,
        )
        self.assertEqual(guard.trip_reasons(200, 100, 900_000, [finished]), [])
        self.assertFalse(guard.cohort_running([finished]))
        self.assertTrue(guard.cohort_running([finished, sample()]))

    def test_active_unit_without_exactly_one_measurable_worker_trips(self) -> None:
        for count, vmas in ((0, None), (1, None), (2, None)):
            with self.subTest(count=count, vmas=vmas):
                reasons = guard.trip_reasons(
                    200,
                    100,
                    900_000,
                    [sample(worker_count=count, worker_pid=None, worker_vmas=vmas)],
                )
                self.assertTrue(any("identifiable historical workers" in item for item in reasons))

    def test_valid_target_stop_handoff_allows_only_workerless_drain(self) -> None:
        draining = sample(worker_count=0, worker_pid=None, worker_vmas=None)
        self.assertEqual(
            guard.trip_reasons(
                200,
                100,
                900_000,
                [draining],
                workerless_drain_units={draining.unit},
            ),
            [],
        )
        two_workers = replace(draining, worker_count=2)
        reasons = guard.trip_reasons(
            200,
            100,
            900_000,
            [two_workers],
            workerless_drain_units={two_workers.unit},
        )
        self.assertTrue(any("2 identifiable" in reason for reason in reasons))

    def test_loads_fresh_invocation_bound_target_stop_intent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "target-stop.json"
            now = time.time()
            payload = {
                "schema": guard.TARGET_STOP_INTENT_SCHEMA,
                "systemd_invocation_id": "a" * 32,
                "target_slot": 100,
                "observed_stop_slot": 101,
                "child_pid": 123,
                "controlled_stop_signal": signal.SIGINT,
                "scratch_snapshot_captured": True,
                "external_signal": None,
                "written_unix_seconds": now - 1,
            }
            path.write_text(json.dumps(payload))
            os.chmod(path, 0o600)

            observed = guard.load_target_stop_intent(
                path,
                required_uid=os.getuid(),
                expected_invocation="a" * 32,
                expected_target_slot=100,
                observed_unix_seconds=now,
                maximum_age_seconds=60,
            )
            self.assertEqual(observed, payload)

            payload["scratch_snapshot_captured"] = False
            path.write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "does not prove bounded drainage"):
                guard.load_target_stop_intent(
                    path,
                    required_uid=os.getuid(),
                    expected_invocation="a" * 32,
                    expected_target_slot=100,
                    observed_unix_seconds=now,
                    maximum_age_seconds=60,
                )

    def test_target_stop_cli_bindings_are_all_or_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            unit = "horizon-qualify-a.service"
            args = guard.parse_args(
                [
                    f"--filesystem={root}",
                    "--minimum-free-bytes=1",
                    "--maximum-worker-vmas=1",
                    f"--receipt-directory={root}",
                    "--timer-unit=horizon-monitor-recurring-test.timer",
                    f"--expected-invocation={unit}=" + "a" * 32,
                    f"--target-stop-intent={unit}={root / 'target-stop.json'}",
                    "--target-slot=100",
                    f"--target-stop-intent-uid={os.getuid()}",
                    unit,
                ]
            )
            self.assertEqual(
                args.target_stop_intents,
                {unit: root / "target-stop.json"},
            )

            with self.assertRaises(SystemExit):
                guard.parse_args(
                    [
                        f"--filesystem={root}",
                        "--minimum-free-bytes=1",
                        "--maximum-worker-vmas=1",
                        f"--receipt-directory={root}",
                        "--timer-unit=horizon-monitor-recurring-test.timer",
                        f"--target-stop-intent={unit}={root / 'target-stop.json'}",
                        "--target-slot=100",
                        f"--target-stop-intent-uid={os.getuid()}",
                        unit,
                    ]
                )

    @patch("scripts.guard_historical_performance_canaries.stop_active_units")
    @patch("scripts.guard_historical_performance_canaries.sample_unit")
    def test_main_accepts_authenticated_workerless_post_target_drain(
        self,
        sample_unit: object,
        stop_active_units: object,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            unit = "horizon-qualify-a.service"
            invocation = "a" * 32
            path = root / "target-stop.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": guard.TARGET_STOP_INTENT_SCHEMA,
                        "systemd_invocation_id": invocation,
                        "target_slot": 100,
                        "observed_stop_slot": 101,
                        "child_pid": 123,
                        "controlled_stop_signal": signal.SIGINT,
                        "scratch_snapshot_captured": True,
                        "external_signal": None,
                        "written_unix_seconds": time.time(),
                    }
                )
            )
            os.chmod(path, 0o600)
            sample_unit.return_value = sample(
                unit=unit,
                invocation_id=invocation,
                worker_count=0,
                worker_pid=None,
                worker_vmas=None,
            )

            result = guard.main(
                [
                    f"--filesystem={root}",
                    "--minimum-free-bytes=1",
                    "--maximum-worker-vmas=900000",
                    f"--receipt-directory={root}",
                    "--timer-unit=horizon-monitor-recurring-test.timer",
                    f"--expected-invocation={unit}={invocation}",
                    f"--target-stop-intent={unit}={path}",
                    "--target-slot=100",
                    f"--target-stop-intent-uid={os.getuid()}",
                    unit,
                ]
            )

        self.assertEqual(result, 0)
        stop_active_units.assert_not_called()

    def test_recognizes_bound_worker_command_and_original_binary_name(self) -> None:
        self.assertTrue(
            guard.is_historical_worker_command(
                b"/scratch/.historical-runtime/parent/bound-worker/historical-worker\0"
            )
        )
        self.assertTrue(
            guard.is_historical_worker_command(
                b"/immutable/jetstreamer-historical-worker-v1-6-16\0--arg\0"
            )
        )
        self.assertTrue(
            guard.is_historical_worker_command(
                b"/sealed/jetstreamer-historical-worker-v1-6-17\0serve\0"
            )
        )
        self.assertFalse(
            guard.is_historical_worker_command(b"/immutable/jetstreamer-node\0")
        )

    def test_worker_vmas_reads_bound_worker_from_nested_cgroup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cgroups = root / "cgroup"
            proc = root / "proc"
            group = cgroups / "system.slice" / "test.service" / "worker"
            group.mkdir(parents=True)
            (group / "cgroup.procs").write_text("123\n124\n")
            for pid in (123, 124):
                (proc / str(pid)).mkdir(parents=True)
            (proc / "123" / "cmdline").write_bytes(b"/immutable/jetstreamer-node\0")
            (proc / "123" / "maps").write_bytes(b"node\n")
            (proc / "124" / "cmdline").write_bytes(
                b"/scratch/.historical-runtime/x/bound-worker/historical-worker\0"
            )
            (proc / "124" / "maps").write_bytes(b"a\nb\nc\n")
            self.assertEqual(
                guard.worker_vmas(
                    "/system.slice/test.service", cgroup_root=cgroups, proc_root=proc
                ),
                (1, 124, 3),
            )

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
