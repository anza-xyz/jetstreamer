#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "verify_generation_bound_cache.py"
SPEC = importlib.util.spec_from_file_location("cache_verifier", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class GenerationBoundCacheVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.root.chmod(0o700)
        self.cache = self.root / "cache"
        self.cache.mkdir(mode=0o700)
        self.receipts = self.root / "receipts"
        self.receipts.mkdir(mode=0o700)
        self.stage = self.cache / "object.download"
        self.data = b"generation-bound cache bytes"
        self.stage.write_bytes(self.data)
        self.stage.chmod(0o600)
        self.invocation = "a" * 32

    def tearDown(self) -> None:
        self.temp.cleanup()

    def args(self, **changes: object) -> argparse.Namespace:
        values = {
            "stage": self.stage,
            "destination": self.cache / "object",
            "receipt": self.receipts / "receipt.json",
            "source_identity": "gs://bucket/object#123",
            "expected_size": len(self.data),
            "expected_md5_base64": base64.b64encode(
                hashlib.md5(self.data, usedforsecurity=False).digest()
            ).decode(),
            "expected_crc32c_base64": "AAAAAA==",
            "download_unit": "download.service",
            "expected_download_invocation": self.invocation,
            "expected_stage_uid": os.getuid(),
            "expected_receipt_parent_uid": os.getuid(),
        }
        values.update(changes)
        return argparse.Namespace(**values)

    def successful_unit(self, _unit: str) -> dict[str, str]:
        return {
            "LoadState": "loaded",
            "ActiveState": "inactive",
            "SubState": "dead",
            "Result": "success",
            "MainPID": "0",
            "InvocationID": self.invocation,
            "NRestarts": "0",
            "ExecMainStatus": "0",
            "ExecStart": "sealed download",
        }

    def verify(self, args: argparse.Namespace | None = None) -> dict[str, object]:
        return MODULE.verify_and_publish(
            args or self.args(),
            unit_sampler=self.successful_unit,
            final_uid=os.getuid(),
            final_gid=os.getgid(),
            receipt_uid=os.getuid(),
            receipt_gid=os.getgid(),
        )

    def test_verifies_and_publishes_cache_before_receipt(self) -> None:
        result = self.verify()
        destination = self.cache / "object"
        self.assertFalse(self.stage.exists())
        self.assertEqual(destination.read_bytes(), self.data)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o444)
        receipt = json_load(self.receipts / "receipt.json")
        self.assertEqual(receipt["destination_sha256"], hashlib.sha256(self.data).hexdigest())
        self.assertEqual(result["receipt"], str(self.receipts / "receipt.json"))

    def test_rejects_nonterminal_download(self) -> None:
        def active(_unit: str) -> dict[str, str]:
            state = self.successful_unit(_unit)
            state.update(ActiveState="active", SubState="running", MainPID="123")
            return state

        with self.assertRaisesRegex(MODULE.CacheVerificationError, "not a clean terminal"):
            MODULE.verify_and_publish(
                self.args(),
                unit_sampler=active,
                final_uid=os.getuid(),
                final_gid=os.getgid(),
                receipt_uid=os.getuid(),
                receipt_gid=os.getgid(),
            )
        self.assertTrue(self.stage.exists())

    def test_rejects_md5_mismatch_without_publishing(self) -> None:
        with self.assertRaisesRegex(MODULE.CacheVerificationError, "MD5 mismatch"):
            self.verify(self.args(expected_md5_base64=base64.b64encode(b"0" * 16).decode()))
        self.assertTrue(self.stage.exists())
        self.assertFalse((self.cache / "object").exists())

    def test_rejects_existing_destination_without_overwrite(self) -> None:
        destination = self.cache / "object"
        destination.write_text("preserve")
        with self.assertRaisesRegex(MODULE.CacheVerificationError, "already exists"):
            self.verify()
        self.assertEqual(destination.read_text(), "preserve")

    def test_rejects_symlinked_stage(self) -> None:
        target = self.cache / "target"
        self.stage.rename(target)
        self.stage.symlink_to(target)
        with self.assertRaisesRegex(MODULE.CacheVerificationError, "canonical"):
            self.verify(self.args(expected_size=target.stat().st_size))


def json_load(path: Path) -> dict[str, object]:
    import json

    return json.loads(path.read_text())


if __name__ == "__main__":
    unittest.main()
