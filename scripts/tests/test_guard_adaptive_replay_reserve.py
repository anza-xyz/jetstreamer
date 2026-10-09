from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import guard_adaptive_replay_reserve as guard


CONTROLLER = "jetstreamer-epochs182-200-controller-v119-r2-retry.service"
HORIZON_CONTROLLER = "horizon-controller-201-cached-store8-t16-v4.service"
PRODUCER = "jetstreamer-root-sweep-historical-182-200-v117-r2-e186-186-lane-m-21.service"
TIMER = "jetstreamer-adaptive-reserve-epoch186-v1.timer"
CONTROLLER_INVOCATION = "a" * 32
PRODUCER_INVOCATION = "b" * 32


def sample(unit: str, invocation: str, **overrides: object) -> guard.UnitSample:
    value = guard.UnitSample(
        unit=unit,
        load_state="loaded",
        active_state="active",
        sub_state="running",
        result="success",
        main_pid=123,
        invocation_id=invocation,
        restarts=0,
    )
    return replace(value, **overrides)


def stopped(unit: str, invocation: str) -> guard.UnitSample:
    return sample(
        unit,
        invocation,
        active_state="inactive",
        sub_state="dead",
        main_pid=0,
    )


def arguments(root: Path) -> list[str]:
    return [
        f"--controller-unit={CONTROLLER}",
        f"--expected-controller-invocation-id={CONTROLLER_INVOCATION}",
        f"--producer-unit={PRODUCER}",
        f"--expected-producer-invocation-id={PRODUCER_INVOCATION}",
        f"--filesystem={root}",
        "--minimum-free-bytes=100",
        "--maximum-worker-vmas=3500000",
        f"--guard-timer={TIMER}",
        f"--intent-receipt={root / 'intent.json'}",
        f"--completion-receipt={root / 'complete.json'}",
    ]


class AdaptiveReplayReserveGuardTest(unittest.TestCase):
    def run_as_root(self):
        return patch.object(guard, "require_root", return_value=None)

    def test_horizon_controller_name_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            args = arguments(root)
            args[0] = f"--controller-unit={HORIZON_CONTROLLER}"
            with self.run_as_root():
                namespace = guard.parse_args(args)
            self.assertEqual(namespace.controller_unit, HORIZON_CONTROLLER)

    def test_healthy_sample_does_not_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", return_value=200),
                patch.object(
                    guard,
                    "sample_unit",
                    side_effect=[
                        sample(CONTROLLER, CONTROLLER_INVOCATION),
                        sample(
                            PRODUCER,
                            PRODUCER_INVOCATION,
                            worker_count=1,
                            worker_pid=124,
                            worker_vmas=500_000,
                        ),
                    ],
                ),
                patch.object(guard, "run_systemctl") as systemctl,
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(arguments(root))
            self.assertEqual(result, 0)
            systemctl.assert_not_called()
            self.assertFalse((root / "intent.json").exists())

    def test_vma_trip_stops_controller_before_producer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            events: list[str] = []
            samples = {
                CONTROLLER: [
                    sample(CONTROLLER, CONTROLLER_INVOCATION),
                    stopped(CONTROLLER, CONTROLLER_INVOCATION),
                ],
                PRODUCER: [
                    sample(
                        PRODUCER,
                        PRODUCER_INVOCATION,
                        worker_count=1,
                        worker_pid=124,
                        worker_vmas=3_500_001,
                    ),
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    stopped(PRODUCER, PRODUCER_INVOCATION),
                ],
            }

            def sample_next(unit: str) -> guard.UnitSample:
                events.append(f"sample:{unit}")
                return samples[unit].pop(0)

            def systemctl(command: list[str]) -> SimpleNamespace:
                events.append("systemctl:" + " ".join(command))
                return SimpleNamespace(returncode=0, stderr="")

            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", side_effect=[200, 200]),
                patch.object(guard, "sample_unit", side_effect=sample_next),
                patch.object(guard, "run_systemctl", side_effect=systemctl),
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(arguments(root))
            self.assertEqual(result, 1)
            self.assertLess(
                events.index(f"systemctl:stop {CONTROLLER}"),
                events.index(f"systemctl:stop {PRODUCER}"),
            )
            intent = json.loads((root / "intent.json").read_text())
            self.assertTrue(any("VMAs" in item for item in intent["trip_reasons"]))

    def test_zero_worker_terminal_handoff_does_not_trip_vma_guard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", return_value=200),
                patch.object(
                    guard,
                    "sample_unit",
                    side_effect=[
                        sample(CONTROLLER, CONTROLLER_INVOCATION),
                        sample(
                            PRODUCER,
                            PRODUCER_INVOCATION,
                            worker_count=0,
                            worker_pid=None,
                            worker_vmas=None,
                        ),
                    ],
                ),
                patch.object(guard, "run_systemctl") as systemctl,
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(arguments(root))
            self.assertEqual(result, 0)
            systemctl.assert_not_called()

    def test_multiple_workers_trip_vma_guard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            samples = {
                CONTROLLER: [
                    sample(CONTROLLER, CONTROLLER_INVOCATION),
                    stopped(CONTROLLER, CONTROLLER_INVOCATION),
                ],
                PRODUCER: [
                    sample(PRODUCER, PRODUCER_INVOCATION, worker_count=2),
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    stopped(PRODUCER, PRODUCER_INVOCATION),
                ],
            }
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", side_effect=[200, 200]),
                patch.object(
                    guard, "sample_unit", side_effect=lambda unit: samples[unit].pop(0)
                ),
                patch.object(
                    guard,
                    "run_systemctl",
                    return_value=SimpleNamespace(returncode=0, stderr=""),
                ),
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(arguments(root))
            self.assertEqual(result, 1)
            intent = json.loads((root / "intent.json").read_text())
            self.assertTrue(any("2 identifiable" in item for item in intent["trip_reasons"]))

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

    def test_trip_stops_controller_before_producer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            events: list[str] = []
            samples = {
                CONTROLLER: [
                    sample(CONTROLLER, CONTROLLER_INVOCATION),
                    stopped(CONTROLLER, CONTROLLER_INVOCATION),
                ],
                PRODUCER: [
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    stopped(PRODUCER, PRODUCER_INVOCATION),
                ],
            }

            def sample_next(unit: str) -> guard.UnitSample:
                events.append(f"sample:{unit}")
                return samples[unit].pop(0)

            def systemctl(command: list[str]) -> SimpleNamespace:
                events.append("systemctl:" + " ".join(command))
                return SimpleNamespace(returncode=0, stderr="")

            original_write = guard.write_json_noclobber
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", side_effect=[99, 99]),
                patch.object(guard, "sample_unit", side_effect=sample_next),
                patch.object(guard, "run_systemctl", side_effect=systemctl),
                patch.object(guard, "write_json_noclobber") as write,
                redirect_stdout(io.StringIO()),
            ):
                write.side_effect = lambda path, payload: (
                    events.append(f"write:{path.name}"),
                    original_write(path, payload),
                )[-1]
                result = guard.main(arguments(root))
            self.assertEqual(result, 1)
            self.assertLess(
                events.index(f"systemctl:stop {CONTROLLER}"),
                events.index(f"systemctl:stop {PRODUCER}"),
            )
            self.assertLess(
                events.index("write:intent.json"),
                events.index(f"systemctl:stop {CONTROLLER}"),
            )
            self.assertLess(
                events.index(f"systemctl:stop {PRODUCER}"),
                events.index("write:complete.json"),
            )
            completion = json.loads((root / "complete.json").read_text())
            self.assertEqual(completion["schema"], guard.COMPLETION_SCHEMA)
            self.assertFalse(completion["scratch_deleted"])
            self.assertEqual(stat.S_IMODE((root / "intent.json").stat().st_mode), 0o600)

    def test_invocation_mismatch_refuses_before_intent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", return_value=1),
                patch.object(
                    guard,
                    "sample_unit",
                    side_effect=[
                        sample(CONTROLLER, "c" * 32),
                        sample(PRODUCER, PRODUCER_INVOCATION),
                    ],
                ),
                patch.object(guard, "run_systemctl") as systemctl,
                self.assertRaisesRegex(guard.GuardError, "controller invocation changed"),
            ):
                guard.main(arguments(root))
            systemctl.assert_not_called()
            self.assertFalse((root / "intent.json").exists())

    def test_intent_resumes_after_space_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            args = arguments(root)
            with self.run_as_root():
                namespace = guard.parse_args(args)
            guard.write_json_noclobber(
                root / "intent.json",
                {"schema": guard.INTENT_SCHEMA, **guard.receipt_binding(namespace)},
            )
            samples = {
                CONTROLLER: [
                    sample(CONTROLLER, CONTROLLER_INVOCATION),
                    stopped(CONTROLLER, CONTROLLER_INVOCATION),
                ],
                PRODUCER: [
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    stopped(PRODUCER, PRODUCER_INVOCATION),
                ],
            }
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", side_effect=[200, 200]),
                patch.object(
                    guard, "sample_unit", side_effect=lambda unit: samples[unit].pop(0)
                ),
                patch.object(
                    guard,
                    "run_systemctl",
                    return_value=SimpleNamespace(returncode=0, stderr=""),
                ) as systemctl,
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(args)
            self.assertEqual(result, 1)
            self.assertEqual(systemctl.call_count, 3)
            self.assertTrue((root / "complete.json").exists())

    def test_collected_producer_after_controller_stop_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            collected = stopped(PRODUCER, "")
            collected = replace(collected, load_state="not-found")
            samples = {
                CONTROLLER: [
                    sample(CONTROLLER, CONTROLLER_INVOCATION),
                    stopped(CONTROLLER, CONTROLLER_INVOCATION),
                ],
                PRODUCER: [
                    sample(PRODUCER, PRODUCER_INVOCATION),
                    collected,
                    collected,
                ],
            }
            with (
                self.run_as_root(),
                patch.object(guard, "available_bytes", side_effect=[1, 1]),
                patch.object(
                    guard, "sample_unit", side_effect=lambda unit: samples[unit].pop(0)
                ),
                patch.object(
                    guard,
                    "run_systemctl",
                    return_value=SimpleNamespace(returncode=0, stderr=""),
                ) as systemctl,
                redirect_stdout(io.StringIO()),
            ):
                result = guard.main(arguments(root))
            self.assertEqual(result, 1)
            self.assertEqual(
                [call.args[0] for call in systemctl.call_args_list],
                [["stop", CONTROLLER], ["stop", TIMER]],
            )

    def test_existing_unbound_completion_refuses_to_stop_timer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            guard.write_json_noclobber(
                root / "complete.json",
                {"schema": guard.COMPLETION_SCHEMA, "controller_unit": "wrong.service"},
            )
            with (
                self.run_as_root(),
                patch.object(guard, "run_systemctl") as systemctl,
                self.assertRaisesRegex(guard.GuardError, "does not match"),
            ):
                guard.main(arguments(root))
            systemctl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
