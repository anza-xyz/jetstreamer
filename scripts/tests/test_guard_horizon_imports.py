from __future__ import annotations

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
                "command_from_journal",
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
                "command_from_journal",
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


if __name__ == "__main__":
    unittest.main()
