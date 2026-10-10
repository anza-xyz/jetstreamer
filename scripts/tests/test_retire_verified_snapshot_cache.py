import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "retire_verified_snapshot_cache.py"
SPEC = importlib.util.spec_from_file_location("retire_verified_snapshot_cache", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
import sys

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RetireVerifiedSnapshotCacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.uid = os.getuid()
        self.private = self.root / "private"
        self.private.mkdir(mode=0o700)
        self.cache_dir = self.private / "snapshots"
        self.cache_dir.mkdir(mode=0o700)
        self.cache = self.cache_dir / "snapshot-123-Hash.tar.zst"
        self.cache.write_bytes(b"verified snapshot bytes")
        self.cache.chmod(0o600)
        cache_stat = self.cache.stat()

        self.restore = self.cache_dir / "bootstrap-123-restore-receipt.json"
        self.write_json(
            self.restore,
            {
                "schema": MODULE.RESTORE_SCHEMA,
                "destination": str(self.cache),
                "versioned_uri": "gs://mainnet-beta-ledger-us-ny5/123/snapshot#456",
                "file": {
                    "device": cache_stat.st_dev,
                    "inode": cache_stat.st_ino,
                    "size": cache_stat.st_size,
                    "mtime_ns": cache_stat.st_mtime_ns,
                    "mode": cache_stat.st_mode & 0o777,
                    "uid": cache_stat.st_uid,
                },
                "remote_mutations": False,
                "r2_mutations": False,
            },
        )

        self.package = self.root / "package"
        self.package.mkdir(mode=0o700)
        self.deployment = self.package / self.cache.name
        self.deployment.write_bytes(self.cache.read_bytes())
        self.deployment.chmod(0o444)
        digest = hashlib.sha256(self.cache.read_bytes()).hexdigest()
        self.sums = self.package / "SHA256SUMS"
        self.sums.write_text(f"{digest}  {self.cache.name}\n", encoding="ascii")
        self.sums.chmod(0o444)
        self.package.chmod(0o555)

        self.receipts = self.root / "receipts"
        self.receipts.mkdir(mode=0o700)
        sums_sha = hashlib.sha256(self.sums.read_bytes()).hexdigest()
        self.capability = self.receipts / "capability.json"
        self.write_json(
            self.capability,
            {
                "schema": MODULE.CAPABILITY_SCHEMA,
                "deployment": {
                    "path": str(self.package),
                    "sha256sums_sha256": sums_sha,
                    "bootstrap_is_direct_singly_linked_member": True,
                },
                "probe_cleanup": {
                    "process_reference_scan": "zero references via separate scanner",
                    "trees_deleted": True,
                    "parent_directory_fsynced": True,
                },
                "publication_authorized": False,
                "remote_mutations": False,
                "r2_mutations": False,
            },
        )
        self.intent = self.receipts / "intent.json"
        self.completion = self.receipts / "completion.json"
        self.proc = self.root / "proc"
        self.proc.mkdir(mode=0o700)
        self.plan = self.receipts / "plan.json"
        self.plan_payload = {
            "schema": MODULE.PLAN_SCHEMA,
            "private_root": str(self.private),
            "expected_cache_uid": self.uid,
            "cache_file": str(self.cache),
            "restore_receipt": str(self.restore),
            "capability_receipt": str(self.capability),
            "deployment_file": str(self.deployment),
            "sha256sums_file": str(self.sums),
            "intent_receipt": str(self.intent),
            "completion_receipt": str(self.completion),
        }
        self.write_json(self.plan, self.plan_payload)

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def write_json(path, payload):
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.chmod(0o600)

    def validate(self):
        with mock.patch.object(MODULE.os, "geteuid", return_value=0):
            return MODULE.validate_evidence(
                self.plan,
                proc_root=self.proc,
                expected_plan_uid=self.uid,
                expected_deployment_uid=self.uid,
                expected_receipt_uid=self.uid,
                expected_capability_uid=self.uid,
            )

    def retire(self, *, check_only=False):
        with mock.patch.object(MODULE.os, "geteuid", return_value=0):
            return MODULE.retire_cache(
                self.plan,
                proc_root=self.proc,
                expected_plan_uid=self.uid,
                expected_deployment_uid=self.uid,
                expected_receipt_uid=self.uid,
                expected_capability_uid=self.uid,
                check_only=check_only,
            )

    def test_validates_independent_immutable_copy(self):
        evidence, metadata = self.validate()
        self.assertEqual(evidence["cache_sha256"], evidence["deployment_sha256"])
        self.assertNotEqual(evidence["cache_inode"], evidence["deployment_inode"])
        self.assertEqual(metadata.st_ino, self.cache.stat().st_ino)

    def test_check_only_preserves_cache_and_writes_no_receipts(self):
        result = self.retire(check_only=True)
        self.assertEqual(result["status"], "verified-not-deleted")
        self.assertTrue(self.cache.exists())
        self.assertFalse(self.intent.exists())
        self.assertFalse(self.completion.exists())

    def test_retirement_writes_intent_then_completion(self):
        result = self.retire()
        self.assertEqual(result["status"], "deleted")
        self.assertFalse(self.cache.exists())
        self.assertTrue(self.intent.exists())
        self.assertTrue(self.completion.exists())
        completion = json.loads(self.completion.read_text())
        self.assertEqual(completion["cache_sha256"], result["cache_sha256"])
        self.assertFalse(completion["r2_mutations"])

    def test_rejects_hardlinked_deployment(self):
        self.package.chmod(0o755)
        self.deployment.unlink()
        os.link(self.cache, self.deployment)
        self.deployment.chmod(0o444)
        self.package.chmod(0o555)
        with self.assertRaisesRegex(MODULE.RetirementError, "unsafe identity|independent"):
            self.validate()

    def test_rejects_deployment_hash_mismatch(self):
        self.package.chmod(0o755)
        self.deployment.chmod(0o644)
        self.deployment.write_bytes(b"wrong")
        self.deployment.chmod(0o444)
        self.package.chmod(0o555)
        with self.assertRaisesRegex(MODULE.RetirementError, "independent|size|hashes"):
            self.validate()

    def test_rejects_restore_identity_mismatch(self):
        receipt = json.loads(self.restore.read_text())
        receipt["file"]["inode"] += 1
        self.write_json(self.restore, receipt)
        with self.assertRaisesRegex(MODULE.RetirementError, "exact cache identity"):
            self.validate()

    def test_rejects_restore_receipt_with_remote_mutation(self):
        receipt = json.loads(self.restore.read_text())
        receipt["remote_mutations"] = True
        self.write_json(self.restore, receipt)
        with self.assertRaisesRegex(MODULE.RetirementError, "exact cache identity"):
            self.validate()

    def test_rejects_unbound_capability_receipt(self):
        capability = json.loads(self.capability.read_text())
        capability["deployment"]["sha256sums_sha256"] = "0" * 64
        self.write_json(self.capability, capability)
        with self.assertRaisesRegex(MODULE.RetirementError, "capability receipt"):
            self.validate()

    def test_process_reference_scan_finds_open_fd(self):
        process = self.proc / "123"
        (process / "fd").mkdir(parents=True)
        (process / "cmdline").write_bytes(b"worker\0")
        (process / "maps").write_bytes(b"")
        (process / "fd" / "7").symlink_to(self.cache)
        with self.assertRaisesRegex(MODULE.RetirementError, "live process references"):
            self.validate()

    def test_process_reference_scan_finds_command_line(self):
        process = self.proc / "123"
        (process / "fd").mkdir(parents=True)
        (process / "cmdline").write_bytes(os.fsencode(str(self.cache)) + b"\0")
        (process / "maps").write_bytes(b"")
        with self.assertRaisesRegex(MODULE.RetirementError, "command line"):
            self.validate()

    def test_sibling_prefix_is_not_a_reference(self):
        self.assertFalse(
            MODULE.encoded_path_reference(
                os.fsencode(str(self.cache) + "-other"), os.fsencode(str(self.cache))
            )
        )

    def test_rejects_existing_intent(self):
        self.intent.write_text("existing", encoding="utf-8")
        self.intent.chmod(0o600)
        with self.assertRaisesRegex(MODULE.RetirementError, "already exists"):
            self.retire()

    def test_rejects_wrong_plan_schema(self):
        plan = dict(self.plan_payload)
        plan["schema"] = "wrong"
        self.write_json(self.plan, plan)
        with self.assertRaisesRegex(MODULE.RetirementError, "schema mismatch"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
