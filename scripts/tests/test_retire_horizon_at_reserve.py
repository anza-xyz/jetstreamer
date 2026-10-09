from __future__ import annotations

from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import retire_horizon_at_reserve as retire  # noqa: E402


def fixture(root: Path) -> list[str]:
    uploader = root / "jetstreamer-r2"
    uploader.write_bytes(b"binary")
    uploader.chmod(0o755)
    horizon = root / "horizon"
    receipts = root / "receipts"
    horizon.mkdir()
    receipts.mkdir()
    os.chmod(receipts, 0o700)
    return [
        str(uploader),
        str(horizon),
        str(receipts),
        f"--filesystem={root}",
        "--minimum-free-bytes=100",
        "--first-epoch=101",
        "--last-epoch=200",
        f"--local-mutation-lock={receipts / 'local-mutation.lock'}",
    ]


def write_pair(directory: Path, epoch: int) -> None:
    (directory / f"epoch-{epoch}.jet").write_bytes(b"archive")
    (directory / f"epoch-{epoch}.jet.sha256").write_text(
        f"{'ab' * 32}  epoch-{epoch}.jet\n", encoding="ascii"
    )


class ReserveRetirementTest(unittest.TestCase):
    def test_above_floor_never_invokes_uploader(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            with (
                mock.patch.object(retire, "available_bytes", return_value=100),
                mock.patch.object(retire.subprocess, "run") as run,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(retire.main(args), 0)
            run.assert_not_called()

    def test_below_floor_retires_explicit_epochs_until_floor_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            horizon = root / "horizon"
            write_pair(horizon, 129)
            write_pair(horizon, 130)

            def invoke(command: list[str], *, check: bool) -> None:
                self.assertTrue(check)
                self.assertIn("retire-local", command)
                epoch_range = command[command.index("--epochs") + 1]
                epoch = int(epoch_range.split("-", 1)[0])
                self.assertEqual(epoch_range, f"{epoch}-{epoch}")
                (horizon / f"epoch-{epoch}.jet.sha256").unlink()
                (horizon / f"epoch-{epoch}.jet").unlink()

            with (
                mock.patch.object(
                    retire, "available_bytes", side_effect=[99, 99, 99, 200, 200]
                ),
                mock.patch.object(retire.subprocess, "run", side_effect=invoke) as run,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(retire.main(args), 0)
            self.assertEqual(run.call_count, 2)

    def test_below_floor_stops_after_first_retirement_recovers_floor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            horizon = root / "horizon"
            write_pair(horizon, 129)
            write_pair(horizon, 130)

            def invoke(command: list[str], *, check: bool) -> None:
                self.assertTrue(check)
                self.assertEqual(command[command.index("--epochs") + 1], "129-129")
                (horizon / "epoch-129.jet.sha256").unlink()
                (horizon / "epoch-129.jet").unlink()

            with (
                mock.patch.object(
                    retire, "available_bytes", side_effect=[99, 99, 101, 101]
                ),
                mock.patch.object(retire.subprocess, "run", side_effect=invoke) as run,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(retire.main(args), 0)
            run.assert_called_once()
            self.assertTrue((horizon / "epoch-130.jet").is_file())

    def test_incomplete_or_out_of_range_namespace_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            horizon = root / "horizon"
            (horizon / "epoch-129.jet").write_bytes(b"partial")
            with (
                mock.patch.object(retire, "available_bytes", side_effect=[1, 1]),
                self.assertRaisesRegex(retire.RetirementError, "incomplete local pair"),
            ):
                retire.main(args)
            (horizon / "epoch-129.jet").unlink()
            write_pair(horizon, 201)
            with (
                mock.patch.object(retire, "available_bytes", side_effect=[1, 1]),
                self.assertRaisesRegex(retire.RetirementError, "outside the authorized range"),
            ):
                retire.main(args)

    def test_below_floor_without_a_pair_fails_loudly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            with (
                mock.patch.object(retire, "available_bytes", side_effect=[1, 1]),
                self.assertRaisesRegex(
                    retire.RetirementError, "no complete local pair"
                ),
            ):
                retire.main(args)

    def test_exhausting_pairs_below_floor_fails_loudly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            args = fixture(root)
            horizon = root / "horizon"
            write_pair(horizon, 129)

            def invoke(command: list[str], *, check: bool) -> None:
                self.assertTrue(check)
                (horizon / "epoch-129.jet.sha256").unlink()
                (horizon / "epoch-129.jet").unlink()

            with (
                mock.patch.object(
                    retire, "available_bytes", side_effect=[1, 1, 1, 1]
                ),
                mock.patch.object(retire.subprocess, "run", side_effect=invoke),
                redirect_stdout(io.StringIO()),
                self.assertRaisesRegex(
                    retire.RetirementError, "filesystem remains below the reserve"
                ),
            ):
                retire.main(args)


if __name__ == "__main__":
    unittest.main()
