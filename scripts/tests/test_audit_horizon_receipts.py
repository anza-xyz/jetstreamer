from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest


import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import audit_horizon_receipts as audit  # noqa: E402


class HorizonReceiptAuditTests(unittest.TestCase):
    def write_epoch(self, root: Path, epoch: int, digest: str) -> None:
        verifier = "cd" * 32
        script = "ef" * 32
        (root / "full" / f"epoch-{epoch}.full.ok").write_text(
            f"{digest} {verifier} {script}\n", encoding="ascii"
        )
        (root / "plugin" / f"epoch-{epoch}.plugin.ok").write_text(
            f"{digest} {verifier} {script}\n", encoding="ascii"
        )
        (root / "r2" / f"epoch-{epoch}.r2.json").write_text(
            json.dumps(
                {
                    "schema": audit.R2_SCHEMA,
                    "epoch": epoch,
                    "archive_key": f"epoch-{epoch}.jet",
                    "checksum_key": f"epoch-{epoch}.jet.sha256",
                    "archive_length": 100,
                    "archive_sha256": digest,
                    "remote_sha256_readback": True,
                    "r2_composite_sha256": None,
                }
            ),
            encoding="utf-8",
        )

    def args(self, root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            first_epoch=7,
            last_epoch=8,
            full_receipts=root / "full",
            plugin_receipts=root / "plugin",
            plugin_pipeline_sha256="cd" * 32,
            plugin_verifier_script_sha256="ef" * 32,
            boundary_receipts=root / "boundary",
            r2_receipts=root / "r2",
            require_outer_boundaries=False,
        )

    def test_complete_internal_range(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("full", "plugin", "boundary", "r2"):
                (root / name).mkdir()
            left = hashlib.sha256(b"left").hexdigest()
            right = hashlib.sha256(b"right").hexdigest()
            self.write_epoch(root, 7, left)
            self.write_epoch(root, 8, right)
            (root / "boundary" / "boundary-7-8.ok").write_text(
                f"{left} {right} {'01' * 32} {'02' * 32}\n", encoding="ascii"
            )
            self.assertEqual(audit.audit(self.args(root)), [])

    def test_digest_mismatch_and_missing_boundary_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("full", "plugin", "boundary", "r2"):
                (root / name).mkdir()
            left = hashlib.sha256(b"left").hexdigest()
            right = hashlib.sha256(b"right").hexdigest()
            self.write_epoch(root, 7, left)
            self.write_epoch(root, 8, right)
            (root / "plugin" / "epoch-7.plugin.ok").write_text(
                f"{'ab' * 32} {'cd' * 32} {'ef' * 32}\n", encoding="ascii"
            )
            errors = audit.audit(self.args(root))
            self.assertTrue(any("epoch 7: receipt digest mismatch" in e for e in errors))
            self.assertTrue(any("boundary 7-8: missing/invalid" in e for e in errors))

    def test_wrong_plugin_identity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("full", "plugin", "boundary", "r2"):
                (root / name).mkdir()
            left = hashlib.sha256(b"left").hexdigest()
            right = hashlib.sha256(b"right").hexdigest()
            self.write_epoch(root, 7, left)
            self.write_epoch(root, 8, right)
            (root / "boundary" / "boundary-7-8.ok").write_text(
                f"{left} {right} {'01' * 32} {'02' * 32}\n", encoding="ascii"
            )
            args = self.args(root)
            args.plugin_pipeline_sha256 = "03" * 32
            errors = audit.audit(args)
            self.assertTrue(
                any("epoch 7: missing/invalid plugin receipt" in e for e in errors)
            )


if __name__ == "__main__":
    unittest.main()
