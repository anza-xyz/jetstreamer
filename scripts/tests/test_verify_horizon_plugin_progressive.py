from __future__ import annotations

import hashlib
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parents[1]
SCRIPT = SCRIPTS / "verify_horizon_plugin_progressive.sh"
BOUNDARY_SCRIPT = SCRIPTS / "verify_horizon_boundaries_progressive.sh"


class ProgressivePluginVerificationTests(unittest.TestCase):
    def test_receipt_is_published_with_file_scoped_fsync(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archives = root / "archives"
            archives.mkdir()
            state = root / "state"
            pipeline = root / "horizon_pipeline"
            pipeline.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
            pipeline.chmod(pipeline.stat().st_mode | stat.S_IXUSR)

            archive = archives / "epoch-7.jet"
            archive.write_bytes(b"archive")
            archive_sha = hashlib.sha256(b"archive").hexdigest()
            (archives / "epoch-7.jet.sha256").write_text(
                f"{archive_sha}  epoch-7.jet\n", encoding="ascii"
            )

            result = subprocess.run(
                [
                    str(SCRIPT),
                    str(pipeline),
                    str(archives),
                    str(state),
                    "7",
                    "7",
                    "1",
                    "1",
                ],
                check=False,
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            receipt = state / "receipts" / "epoch-7.plugin.ok"
            fields = receipt.read_text(encoding="ascii").split()
            self.assertEqual(
                fields,
                [
                    archive_sha,
                    hashlib.sha256(pipeline.read_bytes()).hexdigest(),
                    hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
                ],
            )

    def test_progressive_verifiers_never_call_syncfs(self) -> None:
        for script in (SCRIPT, BOUNDARY_SCRIPT):
            source = script.read_text(encoding="utf-8")
            self.assertNotIn("sync -f", source)
            self.assertNotIn("sync --file-system", source)
            self.assertIn("os.fsync(descriptor)", source)


if __name__ == "__main__":
    unittest.main()
