#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_generation_bound_local_scan.py"
SPEC = importlib.util.spec_from_file_location("local_scan_launcher", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LocalScanLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.root.chmod(0o700)
        self.archive = self.root / "rocksdb.tar.bz2"
        self.archive.write_bytes(b"sealed cache")
        self.archive.chmod(0o444)
        self.scanner = self.root / "scanner"
        self.scanner.write_text("#!/bin/sh\n")
        self.scanner.chmod(0o555)
        self.sst_dump = self.root / "sst_dump"
        self.sst_dump.write_text("#!/bin/sh\n")
        self.sst_dump.chmod(0o555)
        self.work = self.root / "work"
        self.work.mkdir(mode=0o700)
        self.results = self.root / "results"
        self.results.mkdir(mode=0o700)
        self.source = "gs://bucket/object#123"
        self.invocation = "a" * 32
        self.md5 = base64.b64encode(
            hashlib.md5(self.archive.read_bytes(), usedforsecurity=False).digest()
        ).decode()
        metadata = self.archive.stat()
        self.receipt = self.root / "receipt.json"
        self.receipt.write_text(
            json.dumps(
                {
                    "schema": "jetstreamer-generation-bound-local-cache-v1",
                    "source_identity": self.source,
                    "source_size": metadata.st_size,
                    "source_md5_base64": self.md5,
                    "source_crc32c_base64": "AAAAAA==",
                    "download_unit": "download.service",
                    "download_invocation_id": self.invocation,
                    "download_unit_state": {
                        "LoadState": "loaded",
                        "ActiveState": "inactive",
                        "SubState": "dead",
                        "Result": "success",
                        "MainPID": "0",
                        "InvocationID": self.invocation,
                        "NRestarts": "0",
                        "ExecMainStatus": "0",
                    },
                    "destination": str(self.archive),
                    "destination_sha256": hashlib.sha256(self.archive.read_bytes()).hexdigest(),
                    "destination_md5_base64": self.md5,
                    "destination_identity": {
                        "device": metadata.st_dev,
                        "inode": metadata.st_ino,
                        "size": metadata.st_size,
                        "mtime_ns": metadata.st_mtime_ns,
                        "uid": metadata.st_uid,
                        "gid": metadata.st_gid,
                        "mode": metadata.st_mode & 0o777,
                        "links": metadata.st_nlink,
                    },
                    "gcloud_download_checksum_validation_required": True,
                    "publication_authorized": False,
                    "runtime_route_promotion_authorized": False,
                    "remote_mutations": False,
                    "r2_mutations": False,
                }
            )
            + "\n"
        )
        self.receipt.chmod(0o600)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def args(self, **changes: object) -> argparse.Namespace:
        values = {
            "cache_receipt": self.receipt,
            "archive_file": self.archive,
            "source_identity": self.source,
            "expected_size": self.archive.stat().st_size,
            "expected_md5_base64": self.md5,
            "expected_crc32c_base64": "AAAAAA==",
            "download_unit": "download.service",
            "expected_download_invocation": self.invocation,
            "scanner": self.scanner,
            "scanner_sha256": hashlib.sha256(self.scanner.read_bytes()).hexdigest(),
            "sst_dump": self.sst_dump,
            "sst_dump_sha256": hashlib.sha256(self.sst_dump.read_bytes()).hexdigest(),
            "run_uid": os.getuid(),
            "run_gid": os.getgid(),
            "target_prefix": ["0001", "0002"],
            "candidate_column_family": ["data_shred", "meta"],
            "work_directory": self.work,
            "result_directory": self.results,
            "max_sst_bytes": 1024,
            "max_retained_bytes": 2048,
            "max_matches": 10,
            "progress_every_ssts": 1,
        }
        values.update(changes)
        return argparse.Namespace(**values)

    def prepare(self, args: argparse.Namespace | None = None) -> tuple[list[str], dict[str, str]]:
        original_geteuid = MODULE.os.geteuid
        original_parse = MODULE.parse_receipt
        original_archive = MODULE.validate_archive
        original_hash = MODULE.hash_regular_file

        def trusted_parse(local_args: argparse.Namespace) -> tuple[dict[str, object], str]:
            return original_parse(local_args, trusted_uid=os.getuid())

        def trusted_archive(
            local_args: argparse.Namespace, receipt: dict[str, object]
        ) -> str:
            return original_archive(local_args, receipt, trusted_uid=os.getuid())

        def trusted_hash(path: Path, **kwargs: object):
            if path in (self.scanner, self.sst_dump):
                kwargs["expected_uid"] = os.getuid()
            return original_hash(path, **kwargs)

        MODULE.os.geteuid = lambda: 0
        MODULE.parse_receipt = trusted_parse
        MODULE.validate_archive = trusted_archive
        MODULE.hash_regular_file = trusted_hash
        try:
            return MODULE.validate_configuration(args or self.args())
        finally:
            MODULE.os.geteuid = original_geteuid
            MODULE.parse_receipt = original_parse
            MODULE.validate_archive = original_archive
            MODULE.hash_regular_file = original_hash

    def test_builds_exact_local_scan_command(self) -> None:
        command, evidence = self.prepare()
        self.assertEqual(command[0], str(self.scanner))
        self.assertIn("--archive-file-sha256", command)
        self.assertEqual(evidence["archive_file"], str(self.archive))
        self.assertEqual(evidence["cache_receipt"], str(self.receipt))
        self.assertEqual(command.count("--target-prefix"), 2)

    def test_rejects_changed_archive_identity(self) -> None:
        self.archive.chmod(0o600)
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "identity"):
            self.prepare()

    def test_rejects_changed_source_identity(self) -> None:
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "source_identity"):
            self.prepare(self.args(source_identity="gs://bucket/object#124"))

    def test_rejects_nonterminal_download_evidence(self) -> None:
        receipt = json.loads(self.receipt.read_text())
        receipt["download_unit_state"]["ActiveState"] = "active"
        self.receipt.write_text(json.dumps(receipt) + "\n")
        self.receipt.chmod(0o600)
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "ActiveState"):
            self.prepare()

    def test_rejects_existing_result_content(self) -> None:
        (self.results / "old-state.json").write_text("preserve")
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "must be empty"):
            self.prepare()

    def test_rejects_wrong_scanner_digest(self) -> None:
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "scanner executable"):
            self.prepare(self.args(scanner_sha256="0" * 64))

    def test_rejects_symlinked_receipt(self) -> None:
        target = self.root / "receipt-target.json"
        self.receipt.rename(target)
        self.receipt.symlink_to(target)
        with self.assertRaisesRegex(MODULE.LocalScanLaunchError, "canonical"):
            self.prepare()


if __name__ == "__main__":
    unittest.main()
