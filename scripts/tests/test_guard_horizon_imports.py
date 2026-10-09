from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import guard_horizon_imports as guard  # noqa: E402


class GuardHorizonImportsTests(unittest.TestCase):
    def test_controller_units_discovers_versioned_controllers(self) -> None:
        units = {
            "jetstreamer-epoch130-v1417-controller-v4.service",
            "jetstreamer-epochs151-173-controller-v91-retry.service",
            "jetstreamer-horizon-import-guard-v2.service",
            "jetstreamer-root-import-example.service",
            "unrelated-controller.service",
        }

        self.assertEqual(
            guard.controller_units(units),
            (
                "jetstreamer-epoch130-v1417-controller-v4.service",
                "jetstreamer-epochs151-173-controller-v91-retry.service",
            ),
        )

    def test_controller_units_has_a_bounded_scope(self) -> None:
        units = {
            f"jetstreamer-epoch-{index}-controller.service"
            for index in range(guard.MAX_CONTROLLERS + 1)
        }

        with self.assertRaisesRegex(RuntimeError, "refusing to manage"):
            guard.controller_units(units)

    def test_stop_controllers_authenticates_batch_before_mutation(self) -> None:
        controllers = (
            "jetstreamer-a-controller.service",
            "jetstreamer-b-controller.service",
        )
        execute = mock.Mock()

        with (
            mock.patch.object(
                guard,
                "command_from_systemd",
                side_effect=[["validated-a"], RuntimeError("bad digest")],
            ),
            mock.patch.object(guard, "run", execute),
            mock.patch.object(guard, "persist_pause_state") as persist,
            self.assertRaisesRegex(RuntimeError, "bad digest"),
        ):
            guard.stop_controllers(
                "jetstreamer-root-import-test.service",
                "a" * 32,
                controllers,
            )

        execute.assert_not_called()
        persist.assert_not_called()

    def test_stop_controllers_captures_validated_late_arrivals(self) -> None:
        first = "jetstreamer-a-controller.service"
        late = "jetstreamer-b-controller-v2.service"

        with (
            mock.patch.object(
                guard,
                "command_from_systemd",
                side_effect=lambda unit: ["resume", unit],
            ) as authenticate,
            mock.patch.object(guard, "run") as execute,
            mock.patch.object(guard, "persist_pause_state") as persist,
            mock.patch.object(
                guard,
                "running_units",
                side_effect=[{late}, set()],
            ),
        ):
            commands = guard.stop_controllers(
                "jetstreamer-root-import-test.service",
                "a" * 32,
                (first,),
            )

        self.assertEqual(
            commands,
            {first: ["resume", first], late: ["resume", late]},
        )
        self.assertEqual(
            authenticate.call_args_list,
            [mock.call(first), mock.call(late)],
        )
        self.assertEqual(
            persist.call_args_list,
            [
                mock.call(
                    "jetstreamer-root-import-test.service",
                    "a" * 32,
                    {first: ["resume", first]},
                ),
                mock.call(
                    "jetstreamer-root-import-test.service",
                    "a" * 32,
                    {first: ["resume", first], late: ["resume", late]},
                ),
            ],
        )
        self.assertEqual(
            execute.call_args_list,
            [
                mock.call(["/usr/bin/systemctl", "stop", first]),
                mock.call(["/usr/bin/systemctl", "stop", late]),
            ],
        )

    def test_pause_state_round_trip_and_extension(self) -> None:
        importer = "jetstreamer-root-import-test.service"
        invocation = "a" * 32
        first = "jetstreamer-a-controller.service"
        second = "jetstreamer-b-controller.service"
        commands = {
            first: ["/usr/bin/python3", "first", "--retry-failed"],
        }
        extended = {
            **commands,
            second: ["/usr/bin/python3", "second", "--retry-failed"],
        }

        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            with (
                mock.patch.object(guard, "PAUSE_STATE_DIRECTORY", directory),
                mock.patch.object(
                    guard,
                    "validate_controller_command",
                    side_effect=lambda _unit, command: list(command),
                ),
            ):
                self.assertIsNone(guard.load_pause_state())
                guard.persist_pause_state(importer, invocation, commands)
                self.assertEqual(
                    guard.load_pause_state(),
                    {
                        "schema": guard.PAUSE_STATE_SCHEMA,
                        "importer": importer,
                        "importer_invocation_id": invocation,
                        "controllers": commands,
                    },
                )
                guard.persist_pause_state(importer, invocation, extended)
                self.assertEqual(guard.load_pause_state()["controllers"], extended)
                guard.remove_pause_state()
                self.assertIsNone(guard.load_pause_state())

    def test_pause_state_refuses_conflicting_importer(self) -> None:
        controller = "jetstreamer-a-controller.service"
        commands = {controller: ["/usr/bin/python3", "first", "--retry-failed"]}

        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            with (
                mock.patch.object(guard, "PAUSE_STATE_DIRECTORY", directory),
                mock.patch.object(
                    guard,
                    "validate_controller_command",
                    side_effect=lambda _unit, command: list(command),
                ),
            ):
                guard.persist_pause_state(
                    "jetstreamer-root-import-first.service",
                    "a" * 32,
                    commands,
                )
                with self.assertRaisesRegex(RuntimeError, "conflicts"):
                    guard.persist_pause_state(
                        "jetstreamer-root-import-second.service",
                        "b" * 32,
                        commands,
                    )

    def test_recovery_finishes_pause_before_waiting_for_importer(self) -> None:
        importer = "jetstreamer-root-import-test.service"
        invocation = "a" * 32
        stopped = "jetstreamer-a-controller.service"
        still_running = "jetstreamer-b-controller.service"
        state = {
            "importer": importer,
            "importer_invocation_id": invocation,
            "controllers": {stopped: ["resume", stopped]},
        }
        terminal = {
            "InvocationID": invocation,
            "NRestarts": "0",
            "SubState": "exited",
        }

        with (
            mock.patch.object(guard, "load_pause_state", return_value=state),
            mock.patch.object(
                guard,
                "unit_properties",
                return_value={
                    "InvocationID": invocation,
                    "NRestarts": "0",
                    "SubState": "running",
                },
            ),
            mock.patch.object(
                guard,
                "running_units",
                return_value={still_running},
            ),
            mock.patch.object(
                guard,
                "stop_controllers",
                return_value={
                    stopped: ["resume", stopped],
                    still_running: ["resume", still_running],
                },
            ) as stop,
            mock.patch.object(guard, "wait_for_importer", return_value=terminal),
            mock.patch.object(guard, "controllers_deliberately_paused", return_value=False),
            mock.patch.object(guard, "start_controller") as start,
            mock.patch.object(guard, "remove_pause_state") as remove,
        ):
            self.assertTrue(guard.recover_paused_controllers())

        stop.assert_called_once_with(
            importer,
            invocation,
            (still_running,),
            known_commands={stopped: ["resume", stopped]},
        )
        self.assertEqual(start.call_count, 2)
        remove.assert_called_once_with()

    def test_recovery_accepts_collected_bound_importer_when_no_importer_is_live(self) -> None:
        importer = "jetstreamer-root-import-test.service"
        controller = "jetstreamer-a-controller.service"
        command = ["resume", controller]
        state = {
            "importer": importer,
            "importer_invocation_id": "a" * 32,
            "controllers": {controller: command},
        }

        with (
            mock.patch.object(guard, "load_pause_state", return_value=state),
            mock.patch.object(
                guard,
                "unit_properties",
                side_effect=[
                    {
                        "LoadState": "not-found",
                        "ActiveState": "inactive",
                        "SubState": "dead",
                        "InvocationID": "",
                    },
                    {"LoadState": "not-found"},
                ],
            ),
            mock.patch.object(guard, "running_units", return_value=set()),
            mock.patch.object(guard, "controllers_deliberately_paused", return_value=False),
            mock.patch.object(guard, "start_controller") as start,
            mock.patch.object(guard, "remove_pause_state") as remove,
        ):
            self.assertTrue(guard.recover_paused_controllers())

        start.assert_called_once_with(controller, command)
        remove.assert_called_once_with()

    def test_recovery_refuses_collected_importer_when_another_importer_is_live(self) -> None:
        state = {
            "importer": "jetstreamer-root-import-test.service",
            "importer_invocation_id": "a" * 32,
            "controllers": {
                "jetstreamer-a-controller.service": ["resume"],
            },
        }

        with (
            mock.patch.object(guard, "load_pause_state", return_value=state),
            mock.patch.object(
                guard,
                "unit_properties",
                return_value={
                    "LoadState": "not-found",
                    "ActiveState": "inactive",
                    "SubState": "dead",
                    "InvocationID": "",
                },
            ),
            mock.patch.object(
                guard,
                "running_units",
                return_value={"jetstreamer-root-import-other.service"},
            ),
            mock.patch.object(guard, "start_controller") as start,
            self.assertRaisesRegex(RuntimeError, "another importer is live"),
        ):
            guard.recover_paused_controllers()

        start.assert_not_called()

    def test_wait_for_importer_waits_through_deactivation(self) -> None:
        deactivating = {
            "ActiveState": "deactivating",
            "SubState": "stop-sigterm",
        }
        terminal = {"ActiveState": "inactive", "SubState": "dead"}

        with (
            mock.patch.object(
                guard,
                "unit_properties",
                side_effect=[deactivating, terminal],
            ),
            mock.patch.object(guard.time, "sleep") as sleep,
        ):
            self.assertEqual(guard.wait_for_importer("example.service"), terminal)

        sleep.assert_called_once_with(guard.POLL_SECONDS)

    def test_command_reads_live_execstart_from_systemd(self) -> None:
        unit = "jetstreamer-example-controller-v1.service"
        script = "/usr/local/lib/jetstreamer/adaptive-root-cohort-sweep-test.py"
        digest = "a" * 64
        command = [
            "/usr/bin/python3",
            script,
            "--state-dir=/var/lib/jetstreamer-root-sweep-test",
            f"--controller-sha256={digest}",
            "--execute",
        ]
        replies = [
            mock.Mock(
                stdout=(
                    '{"type":"o","data":'
                    '["/org/freedesktop/systemd1/unit/example"]}'
                )
            ),
            mock.Mock(
                stdout=json.dumps(
                    {
                        "type": "a(sasbttttuii)",
                        "data": [
                            [
                                "/usr/bin/python3",
                                command,
                                False,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                            ]
                        ],
                    }
                )
            ),
        ]
        properties = {
            "LoadState": "loaded",
            "ActiveState": "active",
            "SubState": "running",
            "Transient": "yes",
            "FragmentPath": f"/run/systemd/transient/{unit}",
        }

        with (
            mock.patch.object(guard, "unit_properties", return_value=properties),
            mock.patch.object(guard, "run", side_effect=replies) as execute,
            mock.patch("builtins.open", mock.mock_open(read_data=b"script")),
            mock.patch.object(
                guard.hashlib,
                "file_digest",
                return_value=mock.Mock(hexdigest=lambda: digest),
            ),
        ):
            self.assertEqual(
                guard.command_from_systemd(unit),
                [*command, "--retry-failed"],
            )

        self.assertEqual(execute.call_count, 2)

    def test_command_refuses_nonrunning_controller(self) -> None:
        with (
            mock.patch.object(
                guard,
                "unit_properties",
                return_value={
                    "LoadState": "loaded",
                    "ActiveState": "activating",
                    "SubState": "auto-restart",
                    "Transient": "yes",
                    "FragmentPath": "/run/systemd/transient/example.service",
                },
            ),
            mock.patch.object(guard, "run") as execute,
            self.assertRaisesRegex(RuntimeError, "not a live transient"),
        ):
            guard.command_from_systemd("jetstreamer-example-controller.service")

        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
