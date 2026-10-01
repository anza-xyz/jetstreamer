from __future__ import annotations

import hashlib
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "verify_horizon_boundaries_progressive.sh"
)


class ProgressiveBoundaryVerificationTests(unittest.TestCase):
    def write_archive(
        self, directory: Path, full_receipts: Path, epoch: int, payload: bytes
    ) -> None:
        archive = directory / f"epoch-{epoch}.jet"
        archive.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        (directory / f"epoch-{epoch}.jet.sha256").write_text(
            f"{digest}  epoch-{epoch}.jet\n", encoding="ascii"
        )
        (full_receipts / f"epoch-{epoch}.full.ok").write_text(
            f"{digest} {'1' * 64} {'2' * 64}\n", encoding="ascii"
        )

    def write_verifier(self, root: Path, exit_code: int = 0) -> Path:
        verifier = root / "archive_boundaries"
        verifier.write_text(f"#!/bin/sh\nexit {exit_code}\n", encoding="ascii")
        verifier.chmod(verifier.stat().st_mode | stat.S_IXUSR)
        return verifier

    def run_script(
        self, verifier: Path, archives: Path, full_receipts: Path, state: Path
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(SCRIPT),
                str(verifier),
                str(archives),
                str(full_receipts),
                str(state),
                "7",
                "8",
                "1",
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=10,
        )

    def test_receipt_survives_safe_local_retirement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archives = root / "archives"
            archives.mkdir()
            full_receipts = root / "full"
            full_receipts.mkdir()
            state = root / "state"
            verifier = self.write_verifier(root)
            self.write_archive(archives, full_receipts, 7, b"left")
            self.write_archive(archives, full_receipts, 8, b"right")

            first = self.run_script(verifier, archives, full_receipts, state)
            self.assertEqual(first.returncode, 0, first.stderr)
            receipt = state / "receipts" / "boundary-7-8.ok"
            fields = receipt.read_text(encoding="ascii").split()
            self.assertEqual(len(fields), 4)
            self.assertEqual(fields[0], hashlib.sha256(b"left").hexdigest())
            self.assertEqual(fields[1], hashlib.sha256(b"right").hexdigest())

            for path in archives.iterdir():
                path.unlink()
            second = self.run_script(verifier, archives, full_receipts, state)
            self.assertEqual(second.returncode, 0, second.stderr)

    def test_failed_boundary_never_writes_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archives = root / "archives"
            archives.mkdir()
            full_receipts = root / "full"
            full_receipts.mkdir()
            state = root / "state"
            verifier = self.write_verifier(root, exit_code=1)
            self.write_archive(archives, full_receipts, 7, b"left")
            self.write_archive(archives, full_receipts, 8, b"right")

            result = self.run_script(verifier, archives, full_receipts, state)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((state / "receipts" / "boundary-7-8.ok").exists())

    def test_missing_full_receipt_never_runs_boundary_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archives = root / "archives"
            archives.mkdir()
            full_receipts = root / "full"
            full_receipts.mkdir()
            state = root / "state"
            marker = root / "verifier-ran"
            verifier = root / "archive_boundaries"
            verifier.write_text(
                f"#!/bin/sh\ntouch '{marker}'\nexit 0\n", encoding="ascii"
            )
            verifier.chmod(verifier.stat().st_mode | stat.S_IXUSR)
            self.write_archive(archives, full_receipts, 7, b"left")
            self.write_archive(archives, full_receipts, 8, b"right")
            (full_receipts / "epoch-8.full.ok").unlink()

            with self.assertRaises(subprocess.TimeoutExpired):
                self.run_script(verifier, archives, full_receipts, state)
            self.assertFalse(marker.exists())
            self.assertFalse((state / "receipts" / "boundary-7-8.ok").exists())


if __name__ == "__main__":
    unittest.main()
