#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import importlib.util
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_independent_qualification_validator.py"
SPEC = importlib.util.spec_from_file_location("qualification_launcher", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class QualificationValidatorLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.root.chmod(0o700)
        self.receipts = self.root / "receipts"
        self.receipts.mkdir(mode=0o700)
        self.archive = self.root / "epoch-208-through-90287519.jet"
        self.archive.write_bytes(b"sealed archive")
        self.archive.chmod(0o400)
        self.validator = self.root / "validator"
        self.validator.write_text(
            "#!/usr/bin/python3\n"
            "import json, sys\n"
            "print(json.dumps({'schema': 'test-receipt', 'arguments': sys.argv[1:]}))\n"
        )
        self.validator.chmod(0o500)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def request(self, **changes: object) -> object:
        values = {
            "validator": self.validator,
            "validator_sha256": hashlib.sha256(self.validator.read_bytes()).hexdigest(),
            "archive": self.archive,
            "receipt": self.receipts / "receipt.json",
            "epoch": 208,
            "output_start": 89856000,
            "bootstrap": 89855469,
            "terminal": 90287519,
            "runtime": "solana-v1.6.16",
            "worker_sha256": "1" * 64,
            "private_root": self.root,
            "conflict_slot": MODULE.EPOCH_208_CONFLICT_SLOT,
            "conflict_account": MODULE.EPOCH_208_CONFLICT_ACCOUNT,
            "conflict_first": MODULE.EPOCH_208_CONFLICT_FIRST,
            "conflict_second": MODULE.EPOCH_208_CONFLICT_SECOND,
        }
        values.update(changes)
        return MODULE.ValidationRequest(**values)

    def run_request(self, request: object) -> object:
        return MODULE.run_validation(request, deployment_uid=os.getuid())

    def test_publishes_valid_json_with_atomic_conflict_expectations(self) -> None:
        request = self.request()
        payload = self.run_request(request)
        self.assertEqual(payload["schema"], "test-receipt")
        arguments = payload["arguments"]
        self.assertIn("--expected-conflict-slot=89856107", arguments)
        self.assertIn(
            f"--expected-conflict-account={MODULE.EPOCH_208_CONFLICT_ACCOUNT}",
            arguments,
        )
        self.assertTrue(request.receipt.is_file())
        self.assertEqual(stat.S_IMODE(request.receipt.stat().st_mode), 0o600)
        self.assertEqual(list(self.receipts.glob("*.tmp")), [])

    def test_rejects_partial_conflict_expectations(self) -> None:
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "requires"):
            self.run_request(self.request(conflict_second=None))

    def test_epoch_208_rejects_missing_conflict_gate(self) -> None:
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "requires"):
            self.run_request(
                self.request(
                    conflict_slot=None,
                    conflict_account=None,
                    conflict_first=None,
                    conflict_second=None,
                )
            )

    def test_epoch_208_rejects_noncanonical_conflict_gate(self) -> None:
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "not canonical"):
            self.run_request(self.request(conflict_account="wrong-account"))

    def test_rejects_validator_digest_mismatch(self) -> None:
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "SHA-256 mismatch"):
            self.run_request(self.request(validator_sha256="0" * 64))

    def test_rejects_existing_receipt_without_overwrite(self) -> None:
        receipt = self.receipts / "receipt.json"
        receipt.write_text("preserve me")
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "already exists"):
            self.run_request(self.request())
        self.assertEqual(receipt.read_text(), "preserve me")

    def test_rejects_archive_symlink(self) -> None:
        target = self.root / "target.jet"
        target.write_bytes(b"archive")
        linked = self.root / "linked.jet"
        linked.symlink_to(target)
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "unsafe identity"):
            self.run_request(self.request(archive=linked))

    def test_validator_failure_leaves_no_receipt_or_temporary(self) -> None:
        self.validator.chmod(0o700)
        self.validator.write_text("#!/bin/sh\necho failed >&2\nexit 7\n")
        self.validator.chmod(0o500)
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "failed"):
            self.run_request(self.request())
        self.assertFalse((self.receipts / "receipt.json").exists())
        self.assertEqual(list(self.receipts.iterdir()), [])

    def test_invalid_json_leaves_no_receipt(self) -> None:
        self.validator.chmod(0o700)
        self.validator.write_text("#!/bin/sh\nprintf 'not json\\n'\n")
        self.validator.chmod(0o500)
        with self.assertRaisesRegex(MODULE.ValidationLaunchError, "not valid JSON"):
            self.run_request(self.request())
        self.assertFalse((self.receipts / "receipt.json").exists())


if __name__ == "__main__":
    unittest.main()
