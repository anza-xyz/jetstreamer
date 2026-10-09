import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "retire_verified_generation_cache.py"
SPEC = importlib.util.spec_from_file_location("retire_verified_generation_cache", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
import sys

sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RetireVerifiedGenerationCacheTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.private = self.root / "private"
        self.private.mkdir(mode=0o700)
        self.proc = self.root / "proc"
        self.proc.mkdir(mode=0o700)
        self.run = self.private / "qualification-208"
        self.run.mkdir(mode=0o700)
        self.cache_dir = self.run / "cache"
        self.cache_dir.mkdir(mode=0o700)
        self.cache = self.cache_dir / "rocksdb.tar.bz2"
        self.cache.write_bytes(b"verified-cache")
        self.cache.chmod(0o444)
        cache_stat = self.cache.stat()
        self.source = "gs://bucket/object#123"
        self.cache_sha = hashlib.sha256(self.cache.read_bytes()).hexdigest()
        self.cache_receipt = self.run / "cache-receipt.json"
        self.write_private_json(
            self.cache_receipt,
            {
                "schema": MODULE.CACHE_RECEIPT_SCHEMA,
                "destination": str(self.cache),
                "destination_sha256": self.cache_sha,
                "destination_identity": {
                    "device": cache_stat.st_dev,
                    "gid": cache_stat.st_gid,
                    "inode": cache_stat.st_ino,
                    "links": cache_stat.st_nlink,
                    "mode": cache_stat.st_mode & 0o777,
                    "mtime_ns": cache_stat.st_mtime_ns,
                    "size": cache_stat.st_size,
                    "uid": cache_stat.st_uid,
                },
                "source_identity": self.source,
                "source_size": cache_stat.st_size,
                "download_unit": "download.service",
                "download_invocation_id": "d" * 32,
                "download_unit_state": {
                    "LoadState": "loaded",
                    "ActiveState": "inactive",
                    "SubState": "dead",
                    "Result": "success",
                    "MainPID": "0",
                    "InvocationID": "d" * 32,
                    "NRestarts": "0",
                    "ExecMainStatus": "0",
                },
                "publication_authorized": False,
                "r2_mutations": False,
            },
        )
        self.scan_state = self.run / "scan-state.json"
        self.write_private_json(
            self.scan_state,
            {
                "schema": MODULE.SCAN_STATE_SCHEMA,
                "status": "complete",
                "source": self.source,
                "completed_unix_seconds": 1234.5,
                "matches": [],
                "retained_sst_bytes": 0,
                "archive_file": {
                    "path": str(self.cache),
                    "sha256": self.cache_sha,
                    "size": cache_stat.st_size,
                    "uid": cache_stat.st_uid,
                    "mode": cache_stat.st_mode & 0o777,
                    "device": cache_stat.st_dev,
                    "inode": cache_stat.st_ino,
                    "mtime_ns": cache_stat.st_mtime_ns,
                },
            },
        )
        self.receipts = self.run / "retirement"
        self.receipts.mkdir(mode=0o700)
        self.intent = self.receipts / "intent.json"
        self.completion = self.receipts / "completion.json"

    def tearDown(self):
        self.tempdir.cleanup()

    def write_private_json(self, path, payload):
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.chmod(0o600)

    def state(self, invocation="a" * 32):
        return MODULE.UnitState(
            unit="scan.service",
            load_state="loaded",
            active_state="inactive",
            sub_state="dead",
            result="success",
            main_pid=0,
            invocation_id=invocation,
            restarts=0,
            exec_main_status=0,
        )

    def args(self):
        return mock.Mock(
            cache_file=self.cache,
            cache_receipt=self.cache_receipt,
            scan_state=self.scan_state,
            expected_scan_state_uid=os.getuid(),
            required_unit=[("scan.service", "a" * 32)],
            private_root=self.private,
            intent_receipt=self.intent,
            completion_receipt=self.completion,
            check_only=False,
        )

    def common(self, **kwargs):
        with mock.patch.object(MODULE.os, "geteuid", return_value=0):
            return MODULE.common_evidence(
                self.args(),
                sampler=kwargs.pop("sampler", lambda _unit: self.state()),
                proc_root=kwargs.pop("proc_root", self.proc),
                expected_cache_receipt_uid=os.getuid(),
                **kwargs,
            )

    def retire(self):
        with mock.patch.object(MODULE.os, "geteuid", return_value=0):
            return MODULE.retire_cache(
                self.args(),
                sampler=lambda _unit: self.state(),
                proc_root=self.proc,
                expected_cache_receipt_uid=os.getuid(),
                expected_retirement_receipt_uid=os.getuid(),
            )

    def test_common_evidence_accepts_bound_terminal_scan(self):
        evidence, metadata = self.common()
        self.assertEqual(evidence["cache_sha256"], self.cache_sha)
        self.assertEqual(evidence["scan_matches"], 0)
        self.assertEqual(metadata.st_ino, self.cache.stat().st_ino)

    def test_rejects_running_scan_state(self):
        state = json.loads(self.scan_state.read_text())
        state["status"] = "running"
        self.write_private_json(self.scan_state, state)
        with self.assertRaisesRegex(MODULE.RetirementError, "not a complete result"):
            self.common()

    def test_rejects_changed_cache_identity(self):
        self.cache.chmod(0o644)
        with self.assertRaisesRegex(MODULE.RetirementError, "identity changed"):
            self.common()

    def test_rejects_wrong_invocation(self):
        with self.assertRaisesRegex(MODULE.RetirementError, "bound clean terminal"):
            self.common(sampler=lambda _unit: self.state("b" * 32))

    def test_accepts_receipt_bound_download_after_systemd_clears_invocation(self):
        args = self.args()
        args.required_unit = [
            ("download.service", "d" * 32),
            ("scan.service", "a" * 32),
        ]

        def sampler(unit):
            if unit == "download.service":
                return MODULE.UnitState(
                    unit=unit,
                    load_state="loaded",
                    active_state="inactive",
                    sub_state="dead",
                    result="success",
                    main_pid=0,
                    invocation_id="",
                    restarts=0,
                    exec_main_status=0,
                )
            return self.state()

        with mock.patch.object(MODULE.os, "geteuid", return_value=0):
            evidence, _ = MODULE.common_evidence(
                args,
                sampler=sampler,
                proc_root=self.proc,
                expected_cache_receipt_uid=os.getuid(),
            )
        self.assertEqual(
            evidence["required_units"][0]["invocation_evidence"],
            "sealed-cache-receipt",
        )
        self.assertEqual(
            evidence["required_units"][1]["invocation_evidence"],
            "live-systemd",
        )

    def test_empty_invocation_is_not_accepted_for_non_download_unit(self):
        empty = self.state("")
        with self.assertRaisesRegex(MODULE.RetirementError, "bound clean terminal"):
            self.common(sampler=lambda _unit: empty)

    def test_rejects_unbound_download_evidence(self):
        receipt = json.loads(self.cache_receipt.read_text())
        receipt["download_unit_state"]["InvocationID"] = "e" * 32
        self.write_private_json(self.cache_receipt, receipt)
        with self.assertRaisesRegex(MODULE.RetirementError, "download evidence"):
            self.common()

    def test_process_reference_scan_finds_fd(self):
        proc = self.proc
        process = proc / "123"
        (process / "fd").mkdir(parents=True)
        (process / "cmdline").write_bytes(b"worker\0")
        (process / "maps").write_bytes(b"")
        (process / "fd" / "7").symlink_to(self.cache)
        self.assertEqual(
            MODULE.process_references(self.cache, proc), ["pid 123 fd 7"]
        )

    def test_path_reference_does_not_match_sibling_prefix(self):
        self.assertFalse(
            MODULE.encoded_path_reference(
                os.fsencode(str(self.cache) + "-other"), os.fsencode(str(self.cache))
            )
        )

    def test_process_reference_scan_fails_closed_when_proc_is_missing(self):
        with self.assertRaisesRegex(MODULE.RetirementError, "cannot inspect process table"):
            MODULE.process_references(self.cache, self.root / "missing-proc")

    def test_process_reference_scan_fails_closed_on_permission_denied(self):
        process = self.proc / "123"
        process.mkdir()
        (process / "cmdline").write_bytes(b"worker\0")
        (process / "cmdline").chmod(0)
        with self.assertRaisesRegex(MODULE.RetirementError, "cannot inspect pid 123"):
            MODULE.process_references(self.cache, self.proc)

    def test_retirement_is_idempotent(self):
        first = self.retire()
        self.assertEqual(first["status"], "deleted")
        self.assertFalse(self.cache.exists())
        self.assertTrue(self.intent.exists())
        self.assertTrue(self.completion.exists())
        second = self.retire()
        self.assertEqual(second, first)

    def test_intent_only_interruption_resumes_after_unlink(self):
        evidence, _ = self.common()
        intent = {
            **evidence,
            "status": "deletion-intent",
            "recorded_at_utc": MODULE.utc_now(),
            "filesystem_available_bytes_before": MODULE.available_bytes(self.cache.parent),
        }
        MODULE.write_json_noclobber(
            self.intent,
            intent,
            expected_parent_uid=os.getuid(),
        )
        self.cache.unlink()
        result = self.retire()
        self.assertEqual(result["status"], "deleted")
        self.assertTrue(self.completion.exists())

    def test_completion_without_intent_is_rejected(self):
        self.write_private_json(self.completion, {"status": "deleted"})
        with self.assertRaisesRegex(MODULE.RetirementError, "without its deletion intent"):
            self.retire()


if __name__ == "__main__":
    unittest.main()
