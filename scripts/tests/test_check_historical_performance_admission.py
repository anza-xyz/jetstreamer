from dataclasses import replace
import base64
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import check_historical_performance_admission as admission


def unit(**overrides: object) -> admission.UnitState:
    base = admission.UnitState(
        "producer.service", "loaded", "inactive", "dead", "success", 0, "id", 0, 0
    )
    return replace(base, **overrides)


class HistoricalPerformanceAdmissionTest(unittest.TestCase):
    def test_terminal_and_absent_unit_gates_are_strict(self) -> None:
        admission.check_terminal_unit(unit())
        with self.assertRaises(admission.AdmissionError):
            admission.check_terminal_unit(unit(active_state="active", main_pid=12))
        admission.check_absent_unit(
            unit(load_state="not-found", invocation_id="", result="success")
        )
        with self.assertRaises(admission.AdmissionError):
            admission.check_absent_unit(unit())

    @patch("scripts.check_historical_performance_admission.subprocess.run")
    def test_samples_unit_identity(self, run: object) -> None:
        run.return_value = SimpleNamespace(
            returncode=0,
            stderr="",
            stdout=(
                "LoadState=loaded\nActiveState=inactive\nSubState=dead\nResult=success\n"
                "MainPID=0\nInvocationID=id\nNRestarts=0\nExecMainStatus=0\n"
            ),
        )
        self.assertEqual(admission.sample_unit("producer.service"), unit())

    def test_json_subset_is_recursive_but_lists_are_exact(self) -> None:
        actual = {"a": {"b": 1, "extra": True}, "list": [1, 2], "extra": "ok"}
        self.assertTrue(admission.json_contains(actual, {"a": {"b": 1}}))
        self.assertTrue(admission.json_contains(actual, {"list": [1, 2]}))
        self.assertFalse(admission.json_contains(actual, {"list": [1]}))

    def test_required_file_checks_digest_identity_and_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "artifact"
            path.write_bytes(b"artifact")
            os.chmod(path, 0o400)
            digest = admission.sha256_file(path)
            result = admission.check_file(
                {
                    "path": str(path),
                    "uid": path.stat().st_uid,
                    "gid": path.stat().st_gid,
                    "mode": "0400",
                    "size": 8,
                    "sha256": digest,
                }
            )
            self.assertEqual(result["sha256"], digest)
            with self.assertRaisesRegex(admission.AdmissionError, "SHA-256 mismatch"):
                admission.check_file(
                    {
                        "path": str(path),
                        "uid": path.stat().st_uid,
                        "gid": path.stat().st_gid,
                        "mode": "0400",
                        "sha256": "0" * 64,
                    }
                )

    def test_directory_allowed_entries_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve()
            (path / "genesis.tar.bz2").write_bytes(b"genesis")
            mode = f"{stat.S_IMODE(path.stat().st_mode):04o}"
            result = admission.check_directory(
                {
                    "path": str(path),
                    "uid": path.stat().st_uid,
                    "gid": path.stat().st_gid,
                    "mode": mode,
                    "allowed_entries": ["genesis.tar.bz2"],
                }
            )
            self.assertEqual(result["entries"], ["genesis.tar.bz2"])
            (path / "stale.jet").write_bytes(b"stale")
            with self.assertRaisesRegex(admission.AdmissionError, "entries mismatch"):
                admission.check_directory(
                    {
                        "path": str(path),
                        "uid": path.stat().st_uid,
                        "gid": path.stat().st_gid,
                        "mode": mode,
                        "allowed_entries": ["genesis.tar.bz2"],
                    }
                )

    @patch("scripts.check_historical_performance_admission.subprocess.run")
    def test_snapshot_is_rehashed_and_identity_bound(self, run: object) -> None:
        crc = base64.b64encode(b"1234").decode()
        md5 = base64.b64encode(b"1234567890123456").decode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "snapshot.tar.zst"
            path.write_bytes(b"snapshot")
            os.chmod(path, 0o400)
            run.return_value = SimpleNamespace(
                returncode=0,
                stderr="",
                stdout=json.dumps(
                    [
                        {
                            "url": str(path),
                            "digest_format": "base64",
                            "crc32c_hash": crc,
                            "md5_hash": md5,
                        }
                    ]
                ),
            )
            result = admission.check_snapshot(
                {
                    "path": str(path),
                    "gcloud_bin": "/usr/bin/gcloud",
                    "size": len(b"snapshot"),
                    "uid": path.stat().st_uid,
                    "gid": path.stat().st_gid,
                    "mode": "0400",
                    "crc32c_base64": crc,
                    "md5_base64": md5,
                }
            )
            self.assertEqual(result["md5_base64"], md5)
            with self.assertRaisesRegex(admission.AdmissionError, "do not match"):
                admission.check_snapshot(
                    {
                        "path": str(path),
                        "gcloud_bin": "/usr/bin/gcloud",
                        "size": len(b"snapshot"),
                        "uid": path.stat().st_uid,
                        "gid": path.stat().st_gid,
                        "mode": "0400",
                        "crc32c_base64": base64.b64encode(b"5678").decode(),
                        "md5_base64": md5,
                    }
                )

    def test_process_reference_scan_includes_file_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory).resolve()
            root = proc / "candidate"
            root.mkdir()
            target = root / "file"
            target.write_bytes(b"data")
            process = proc / "123"
            process.mkdir()
            (process / "cmdline").write_bytes(b"unrelated")
            (process / "maps").write_bytes(b"")
            (process / "fd").mkdir()
            (process / "fd" / "7").symlink_to(target)
            for name in ("cwd", "root", "exe"):
                (process / name).symlink_to(proc / "elsewhere")
            self.assertEqual(
                admission.process_references([root], proc),
                [f"pid 123 fd 7 references {root}"],
            )

    def test_manifest_must_be_owner_only_and_schema_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "manifest.json"
            path.write_text(json.dumps({"schema": admission.MANIFEST_SCHEMA}))
            os.chmod(path, 0o600)
            payload, digest = admission.load_manifest(path, required_uid=path.stat().st_uid)
            self.assertEqual(payload["schema"], admission.MANIFEST_SCHEMA)
            self.assertEqual(digest, admission.sha256_file(path))
            os.chmod(path, 0o644)
            with self.assertRaisesRegex(admission.AdmissionError, "unsafe identity"):
                admission.load_manifest(path, required_uid=path.stat().st_uid)

    def test_receipt_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            receipt = root / "receipt.json"
            admission.write_json_noclobber(
                receipt, {"ok": True}, required_uid=root.stat().st_uid
            )
            self.assertEqual(json.loads(receipt.read_text()), {"ok": True})
            self.assertEqual(stat.S_IMODE(receipt.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                admission.write_json_noclobber(
                    receipt, {}, required_uid=root.stat().st_uid
                )


if __name__ == "__main__":
    unittest.main()
