import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from scripts import validate_historical_performance_admission_receipt as validator


class HistoricalPerformanceAdmissionReceiptValidationTest(unittest.TestCase):
    def write_json(self, path: Path, payload: object) -> str:
        data = (json.dumps(payload, sort_keys=True) + "\n").encode()
        path.write_bytes(data)
        os.chmod(path, 0o600)
        return hashlib.sha256(data).hexdigest()

    def test_exact_manifest_and_receipt_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            manifest = root / "manifest.json"
            manifest_sha256 = self.write_json(
                manifest,
                {"schema": validator.ADMISSION_MANIFEST_SCHEMA},
            )
            receipt = root / "receipt.json"
            self.write_json(
                receipt,
                {
                    "schema": validator.ADMISSION_RECEIPT_SCHEMA,
                    "admitted": True,
                    "manifest": str(manifest),
                    "manifest_sha256": manifest_sha256,
                    "evidence": {},
                    "services_installed": False,
                    "services_started": False,
                    "remote_mutations": False,
                    "r2_mutations": False,
                },
            )
            os.chmod(root, 0o555)
            result = validator.validate(
                receipt,
                manifest,
                manifest_sha256,
                required_uid=os.getuid(),
            )
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["manifest_sha256"], manifest_sha256)
            os.chmod(root, 0o700)

    def test_manifest_digest_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            manifest = root / "manifest.json"
            self.write_json(manifest, {"schema": validator.ADMISSION_MANIFEST_SCHEMA})
            receipt = root / "receipt.json"
            self.write_json(receipt, {})
            with self.assertRaisesRegex(validator.ValidationError, "SHA-256"):
                validator.validate(
                    receipt,
                    manifest,
                    "0" * 64,
                    required_uid=os.getuid(),
                )

    def test_receipt_binding_and_identity_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            manifest = root / "manifest.json"
            manifest_sha256 = self.write_json(
                manifest,
                {"schema": validator.ADMISSION_MANIFEST_SCHEMA},
            )
            receipt = root / "receipt.json"
            self.write_json(
                receipt,
                {
                    "schema": validator.ADMISSION_RECEIPT_SCHEMA,
                    "admitted": True,
                    "manifest": str(manifest),
                    "manifest_sha256": "1" * 64,
                    "evidence": {},
                    "services_installed": False,
                    "services_started": False,
                    "remote_mutations": False,
                    "r2_mutations": False,
                },
            )
            with self.assertRaisesRegex(validator.ValidationError, "does not authorize"):
                validator.validate(
                    receipt,
                    manifest,
                    manifest_sha256,
                    required_uid=os.getuid(),
                )
            os.chmod(receipt, 0o644)
            with self.assertRaisesRegex(validator.ValidationError, "unsafe identity"):
                validator.validate(
                    receipt,
                    manifest,
                    manifest_sha256,
                    required_uid=os.getuid(),
                )


if __name__ == "__main__":
    unittest.main()
