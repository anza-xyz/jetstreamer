import base64
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import restore_bound_gcs_snapshot as restore


FILENAME = "snapshot-87263434-Emrm2S17KbKwEg2zmJKrJvGj9PEfZxxoYHidGGsg1QKK.tar.zst"
URI = f"gs://{restore.ALLOWED_BUCKET}/87263434/{FILENAME}#1634789740125991"


class BoundGcsSnapshotRestoreTest(unittest.TestCase):
    def test_parses_generation_pinned_root_snapshot(self) -> None:
        self.assertEqual(
            restore.parse_versioned_uri(URI),
            (FILENAME, 1634789740125991, 87263434),
        )

    def test_parses_generation_pinned_hourly_snapshot(self) -> None:
        uri = f"gs://{restore.ALLOWED_BUCKET}/87263000/hourly/{FILENAME}#9"
        self.assertEqual(
            restore.parse_versioned_uri(uri), (FILENAME, 9, 87263434)
        )

    def test_rejects_live_object_other_bucket_and_wrong_anchor(self) -> None:
        invalid = (
            URI.rsplit("#", 1)[0],
            URI.replace(restore.ALLOWED_BUCKET, "other-bucket"),
            URI.replace("/87263434/", "/87263433/"),
            URI.replace("#1634789740125991", "#0"),
        )
        for uri in invalid:
            with self.subTest(uri=uri), self.assertRaises(restore.RestoreError):
                restore.parse_versioned_uri(uri)

    def test_canonical_base64_requires_exact_decoded_size(self) -> None:
        value = base64.b64encode(b"1234").decode()
        self.assertEqual(restore.canonical_base64(value, 4, "CRC"), value)
        for invalid in ("not base64", base64.b64encode(b"123").decode(), value.rstrip("=")):
            with self.subTest(invalid=invalid), self.assertRaises(restore.RestoreError):
                restore.canonical_base64(invalid, 4, "CRC")

    def test_cloud_identity_is_one_safe_token(self) -> None:
        self.assertEqual(
            restore.require_cloud_identity("user@example.com", "account"),
            "user@example.com",
        )
        self.assertEqual(
            restore.require_cloud_identity("principal-lane-200702", "project"),
            "principal-lane-200702",
        )
        for invalid in ("", " two", "two words", "--project", "two/parts"):
            with self.subTest(invalid=invalid), self.assertRaises(restore.RestoreError):
                restore.require_cloud_identity(invalid, "identity")

    def test_download_pins_account_billing_project_and_generation(self) -> None:
        command = restore.download_command(
            Path("/usr/bin/gcloud"),
            URI,
            Path("/private/payload"),
            "user@example.com",
            "principal-lane-200702",
        )
        self.assertEqual(command[:3], ["/usr/bin/gcloud", "storage", "cp"])
        self.assertIn("--account=user@example.com", command)
        self.assertIn("--billing-project=principal-lane-200702", command)
        self.assertIn(URI, command)
        self.assertEqual(command[-2:], ["/private/payload", "--quiet"])

    @patch("scripts.restore_bound_gcs_snapshot.run_checked")
    def test_remote_description_binds_generation_identity_and_hashes(
        self, run_checked: object
    ) -> None:
        crc = base64.b64encode(b"1234").decode()
        md5 = base64.b64encode(b"1234567890123456").decode()
        object_name = f"87263434/{FILENAME}"
        run_checked.return_value = SimpleNamespace(
            stdout=json.dumps(
                {
                    "bucket": restore.ALLOWED_BUCKET,
                    "name": object_name,
                    "generation": "1634789740125991",
                    "size": "8081511792",
                    "crc32c": crc,
                    "md5Hash": md5,
                    "id": f"{restore.ALLOWED_BUCKET}/{object_name}/1634789740125991",
                }
            )
        )
        result = restore.describe_remote_snapshot(
            Path("/usr/bin/gcloud"),
            URI,
            "user@example.com",
            "principal-lane-200702",
            8081511792,
            crc,
            md5,
        )
        self.assertEqual(result["generation"], 1634789740125991)
        command = run_checked.call_args.args[0]
        self.assertEqual(
            command[:4], ["/usr/bin/gcloud", "storage", "objects", "describe"]
        )
        self.assertIn("--account=user@example.com", command)
        self.assertIn("--billing-project=principal-lane-200702", command)
        self.assertIn(URI, command)

        metadata = json.loads(run_checked.return_value.stdout)
        metadata["generation"] = "1634789740125992"
        run_checked.return_value.stdout = json.dumps(metadata)
        with self.assertRaisesRegex(restore.RestoreError, "generation mismatch"):
            restore.describe_remote_snapshot(
                Path("/usr/bin/gcloud"),
                URI,
                "user@example.com",
                "principal-lane-200702",
                8081511792,
                crc,
                md5,
            )

    def test_canonical_base64_rejects_non_strings(self) -> None:
        for invalid in (None, 1, b"MTIzNA=="):
            with self.subTest(invalid=invalid), self.assertRaises(restore.RestoreError):
                restore.canonical_base64(invalid, 4, "CRC")

    @patch("scripts.restore_bound_gcs_snapshot.available_bytes", return_value=99)
    def test_free_space_gate_fails_closed(self, _available: object) -> None:
        with self.assertRaisesRegex(restore.RestoreError, "below restore floor"):
            restore.require_free_space(Path("/filesystem"), 100)
        self.assertEqual(restore.require_free_space(Path("/filesystem"), 99), 99)

    def test_publish_is_noclobber_and_durable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / "temporary"
            destination = root / "destination"
            temporary.write_bytes(b"snapshot")
            os.chmod(temporary, 0o600)
            self.assertTrue(restore.publish_noclobber(temporary, destination))
            self.assertEqual(destination.read_bytes(), b"snapshot")
            replacement = root / "replacement"
            replacement.write_bytes(b"other")
            self.assertFalse(restore.publish_noclobber(replacement, destination))
            self.assertEqual(destination.read_bytes(), b"snapshot")

    def test_receipt_is_owner_only_noclobber_and_fsynced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            receipt = root / "receipt.json"
            restore.write_receipt_noclobber(receipt, {"ok": True})
            self.assertEqual(json.loads(receipt.read_text()), {"ok": True})
            self.assertEqual(stat.S_IMODE(receipt.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                restore.write_receipt_noclobber(receipt, {})

    @patch("scripts.restore_bound_gcs_snapshot.run_checked")
    def test_local_hashes_require_one_complete_result(self, run_checked: object) -> None:
        run_checked.return_value.stdout = json.dumps(
            [
                {
                    "crc32c_hash": base64.b64encode(b"1234").decode(),
                    "md5_hash": base64.b64encode(b"1234567890123456").decode(),
                    "digest_format": "base64",
                    "url": "/snapshot",
                }
            ]
        )
        self.assertEqual(
            restore.local_hashes(Path("/usr/bin/gcloud"), Path("/snapshot")),
            ("MTIzNA==", "MTIzNDU2Nzg5MDEyMzQ1Ng=="),
        )

    @patch("scripts.restore_bound_gcs_snapshot.local_hashes")
    def test_validate_file_checks_identity_size_and_hashes(self, hashes: object) -> None:
        crc = base64.b64encode(b"1234").decode()
        md5 = base64.b64encode(b"1234567890123456").decode()
        hashes.return_value = (crc, md5)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot"
            path.write_bytes(b"payload")
            os.chmod(path, 0o600)
            details = restore.validate_file(Path("/gcloud"), path, 7, crc, md5)
            self.assertEqual(details["size"], 7)
            with self.assertRaisesRegex(restore.RestoreError, "size mismatch"):
                restore.validate_file(Path("/gcloud"), path, 8, crc, md5)


if __name__ == "__main__":
    unittest.main()
