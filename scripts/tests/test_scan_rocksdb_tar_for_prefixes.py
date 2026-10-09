#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "scan_rocksdb_tar_for_prefixes.py"
SOURCE = "gs://example-bucket/epoch/rocksdb.tar.bz2#12345"


class BoundLocalRocksdbScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.archive = self.root / "rocksdb.tar.bz2"
        with tarfile.open(self.archive, "w:bz2") as output:
            data = b"not an SST"
            member = tarfile.TarInfo("rocksdb/README")
            member.size = len(data)
            output.addfile(member, io.BytesIO(data))
        self.archive.chmod(0o400)
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.work = self.root / "work"
        self.results = self.root / "results"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def command(self, **changes: object) -> list[str]:
        values = {
            "archive": self.archive,
            "sha256": self.digest,
            "size": self.archive.stat().st_size,
            "uid": os.getuid(),
            "source": SOURCE,
        }
        values.update(changes)
        return [
            sys.executable,
            str(SCRIPT),
            "--archive-file",
            str(values["archive"]),
            "--archive-file-sha256",
            str(values["sha256"]),
            "--archive-file-size",
            str(values["size"]),
            "--expected-archive-uid",
            str(values["uid"]),
            "--source-identity",
            str(values["source"]),
            "--target-prefix",
            "0000000000000001",
            "--work-directory",
            str(self.work),
            "--result-directory",
            str(self.results),
        ]

    def test_bound_local_archive_completes_with_generation_identity(self) -> None:
        completed = subprocess.run(self.command(), capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        state = json.loads((self.results / "scan-state.json").read_text())
        self.assertEqual(state["status"], "complete")
        self.assertEqual(state["source"], SOURCE)
        self.assertEqual(state["archive_file"]["sha256"], self.digest)
        self.assertEqual(state["archive_file"]["size"], self.archive.stat().st_size)

    def test_rejects_wrong_digest_before_creating_result_state(self) -> None:
        completed = subprocess.run(
            self.command(sha256="0" * 64), capture_output=True, text=True
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("SHA-256 mismatch", completed.stderr)
        self.assertFalse((self.results / "scan-state.json").exists())

    def test_rejects_symlinked_archive(self) -> None:
        linked = self.root / "linked.tar.bz2"
        linked.symlink_to(self.archive)
        completed = subprocess.run(
            self.command(archive=linked), capture_output=True, text=True
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("canonical non-symlink", completed.stderr)

    def test_rejects_incomplete_local_binding(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--archive-file",
                str(self.archive),
                "--target-prefix",
                "0000000000000001",
                "--work-directory",
                str(self.work),
                "--result-directory",
                str(self.results),
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("requires --archive-file-sha256", completed.stderr)


if __name__ == "__main__":
    unittest.main()
