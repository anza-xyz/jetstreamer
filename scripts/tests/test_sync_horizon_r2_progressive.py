from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sync_horizon_r2_progressive as progressive  # noqa: E402


class ProgressiveR2Tests(unittest.TestCase):
    def test_local_pair_requires_canonical_matching_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / "epoch-123.jet"
            archive.write_bytes(b"archive")
            digest = "ab" * 32
            (directory / "epoch-123.jet.sha256").write_text(
                f"{digest}  epoch-123.jet\n", encoding="ascii"
            )
            pair = progressive.local_pair(directory, 123)
            self.assertIsNotNone(pair)
            assert pair is not None
            self.assertEqual(pair[2], digest)

            (directory / "epoch-123.jet.sha256").write_text(
                f"{digest} *epoch-123.jet\n", encoding="ascii"
            )
            with self.assertRaisesRegex(RuntimeError, "non-canonical"):
                progressive.local_pair(directory, 123)

    def test_receipt_must_bind_current_local_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / "epoch-7.jet"
            archive.write_bytes(b"archive")
            metadata = archive.stat()
            digest = "cd" * 32
            receipt_path = directory / "epoch-7.r2.json"
            receipt = {
                "schema": "jetstreamer-horizon-r2-receipt-v1",
                "epoch": 7,
                "archive_key": "epoch-7.jet",
                "checksum_key": "epoch-7.jet.sha256",
                "archive_sha256": digest,
                "archive_length": metadata.st_size,
                "verified_unix_seconds": metadata.st_mtime + 1,
                "remote_sha256_readback": True,
            }
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertTrue(
                progressive.receipt_matches(receipt_path, 7, digest, metadata)
            )

            receipt["archive_length"] += 1
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertFalse(
                progressive.receipt_matches(receipt_path, 7, digest, metadata)
            )

    def test_incomplete_pair_is_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "epoch-9.jet").write_bytes(b"partial")
            self.assertIsNone(progressive.local_pair(directory, 9))

    def test_retirement_requires_both_digest_bound_gates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            full = root / "full"
            plugin = root / "plugin"
            boundary = root / "boundary"
            full.mkdir()
            plugin.mkdir()
            boundary.mkdir()
            digest = "ab" * 32
            verifier = "cd" * 32
            script = "ef" * 32
            (full / "epoch-7.full.ok").write_text(
                f"{digest} {verifier} {script}\n", encoding="ascii"
            )
            (plugin / "epoch-7.plugin.ok").write_text(
                f"{digest} {verifier} {script}\n", encoding="ascii"
            )
            (boundary / "boundary-6-7.ok").write_text(
                f"{'01' * 32} {digest} {verifier} {script}\n", encoding="ascii"
            )
            (boundary / "boundary-7-8.ok").write_text(
                f"{digest} {'02' * 32} {verifier} {script}\n", encoding="ascii"
            )
            args = SimpleNamespace(
                delete_local=True,
                defer_epochs=[],
                full_receipt_directory=full,
                full_verifier_sha256=verifier,
                full_verifier_script_sha256=script,
                plugin_receipt_directory=plugin,
                plugin_pipeline_sha256=verifier,
                plugin_verifier_script_sha256=script,
                boundary_receipt_directory=boundary,
                boundary_verifier_sha256=verifier,
                boundary_verifier_script_sha256=script,
            )
            self.assertTrue(progressive.retirement_allowed(args, 7, digest))
            self.assertFalse(progressive.retirement_allowed(args, 7, "01" * 32))

            args.full_verifier_sha256 = "07" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.full_verifier_sha256 = verifier
            args.full_verifier_script_sha256 = "08" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.full_verifier_script_sha256 = script

            args.plugin_pipeline_sha256 = "03" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.plugin_pipeline_sha256 = verifier
            args.plugin_verifier_script_sha256 = "04" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.plugin_verifier_script_sha256 = script

            args.boundary_verifier_sha256 = "05" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.boundary_verifier_sha256 = verifier
            args.boundary_verifier_script_sha256 = "06" * 32
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            args.boundary_verifier_script_sha256 = script

            (boundary / "boundary-7-8.ok").unlink()
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))
            (boundary / "boundary-7-8.ok").write_text(
                f"{digest} {'02' * 32} {verifier} {script}\n", encoding="ascii"
            )
            args.defer_epochs = [(7, 9)]
            self.assertFalse(progressive.retirement_allowed(args, 7, digest))

    def test_sync_delegates_local_deletion_to_rust_uploader(self) -> None:
        args = SimpleNamespace(
            uploader=Path("/uploader"),
            horizon_directory=Path("/horizon"),
            receipt_directory=Path("/receipts"),
            legacy_part_size_mib=5,
            concurrency=4,
        )
        with mock.patch.object(progressive.subprocess, "run") as run:
            progressive.sync_epoch(args, 7, delete_local=True)
        command = run.call_args.args[0]
        self.assertIn("--delete-local", command)
        run.assert_called_once_with(command, check=True)


if __name__ == "__main__":
    unittest.main()
