from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


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


if __name__ == "__main__":
    unittest.main()
