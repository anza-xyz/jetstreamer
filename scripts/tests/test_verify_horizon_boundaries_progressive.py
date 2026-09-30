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
    def write_archive(self, directory: Path, epoch: int, payload: bytes) -> None:
        archive = directory / f"epoch-{epoch}.jet"
        archive.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        (directory / f"epoch-{epoch}.jet.sha256").write_text(
            f"{digest}  epoch-{epoch}.jet\n", encoding="ascii"
        )

    def write_verifier(self, root: Path, exit_code: int = 0) -> Path:
        verifier = root / "verify_archive"
        verifier.write_text(f"#!/bin/sh\nexit {exit_code}\n", encoding="ascii")
        verifier.chmod(verifier.stat().st_mode | stat.S_IXUSR)
        return verifier

    def run_script(
        self, verifier: Path, archives: Path, state: Path
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(SCRIPT),
                str(verifier),
                str(archives),
                str(state),
                "7",
                "8",
                "1",
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
            state = root / "state"
            verifier = self.write_verifier(root)
            self.write_archive(archives, 7, b"left")
            self.write_archive(archives, 8, b"right")

            first = self.run_script(verifier, archives, state)
            self.assertEqual(first.returncode, 0, first.stderr)
            receipt = state / "receipts" / "boundary-7-8.ok"
            fields = receipt.read_text(encoding="ascii").split()
            self.assertEqual(len(fields), 4)
            self.assertEqual(fields[0], hashlib.sha256(b"left").hexdigest())
            self.assertEqual(fields[1], hashlib.sha256(b"right").hexdigest())

            for path in archives.iterdir():
                path.unlink()
            second = self.run_script(verifier, archives, state)
            self.assertEqual(second.returncode, 0, second.stderr)

    def test_failed_boundary_never_writes_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archives = root / "archives"
            archives.mkdir()
            state = root / "state"
            verifier = self.write_verifier(root, exit_code=1)
            self.write_archive(archives, 7, b"left")
            self.write_archive(archives, 8, b"right")

            result = self.run_script(verifier, archives, state)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((state / "receipts" / "boundary-7-8.ok").exists())


if __name__ == "__main__":
    unittest.main()
