from __future__ import annotations

import json
from pathlib import Path
import sys
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
            self.assertRaisesRegex(RuntimeError, "bad digest"),
        ):
            guard.stop_controllers(controllers)

        execute.assert_not_called()

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
            mock.patch.object(
                guard,
                "running_units",
                side_effect=[{late}, set()],
            ),
        ):
            commands = guard.stop_controllers((first,))

        self.assertEqual(
            commands,
            {first: ["resume", first], late: ["resume", late]},
        )
        self.assertEqual(
            authenticate.call_args_list,
            [mock.call(first), mock.call(late)],
        )
        self.assertEqual(
            execute.call_args_list,
            [
                mock.call(["/usr/bin/systemctl", "stop", first]),
                mock.call(["/usr/bin/systemctl", "stop", late]),
            ],
        )

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
