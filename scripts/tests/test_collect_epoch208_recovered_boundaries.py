import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "collect_epoch208_recovered_boundaries.py"
SOURCE = "gs://bucket/rocksdb.tar.bz2#123"
FINAL = "FQjwC1FQU3ZRZLbMpywW7FWLyPP9v1XqyBRhk2FgwomW"


class CollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.scan = self.root / "scan.json"
        self.decoder = self.root / "decoder"
        self.output = self.root / "boundaries.json"
        self.receipt = self.root / "receipt.json"
        report = [
            {
                "slot": 89856117,
                "data_shreds": 3,
                "first_shred_index": 0,
                "last_shred_index": 2,
                "entries": 130,
                "tick_entries": 128,
                "transaction_entries": 2,
                "transactions": 2,
                "boundaries": [
                    {"tick_ordinal": 64, "entry_ordinal": 64, "hash": "Intermediate111"},
                    {"tick_ordinal": 128, "entry_ordinal": 129, "hash": FINAL},
                ],
                "final_entry_hash": FINAL,
            }
        ]
        self.decoder.write_text(
            "#!/bin/sh\nexec printf '%s\\n' " + repr(json.dumps(report)) + "\n",
            encoding="utf-8",
        )
        self.decoder.chmod(0o755)
        self.decoder_sha256 = hashlib.sha256(self.decoder.read_bytes()).hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def command(self) -> list[str]:
        return [
            str(SCRIPT),
            "--scan-state",
            str(self.scan),
            "--decoder",
            str(self.decoder),
            "--decoder-sha256",
            self.decoder_sha256,
            "--expected-source",
            SOURCE,
            "--expected-slot",
            "89856117",
            "--ticks-per-slot",
            "64",
            "--expected-boundary-count",
            "2",
            "--expected-final-blockhash",
            FINAL,
            "--output",
            str(self.output),
            "--receipt",
            str(self.receipt),
        ]

    def write_scan(self, status: str = "complete") -> None:
        self.scan.write_text(
            json.dumps(
                {
                    "schema": "jetstreamer-streaming-rocksdb-prefix-scan-v1",
                    "status": status,
                    "source": SOURCE,
                    "target_prefixes": ["00000000055B1874", "00000000055B1875"],
                    "matches": [],
                }
            ),
            encoding="utf-8",
        )

    def test_running_scan_defers_without_artifacts(self) -> None:
        self.write_scan("running")
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("deferred-running", result.stdout)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.receipt.exists())

    def test_complete_scan_collects_and_is_idempotent(self) -> None:
        self.write_scan()
        first = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertIn("collected", first.stdout)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(receipt["intermediate_boundary_hash"], "Intermediate111")
        self.assertEqual(receipt["final_boundary_hash"], FINAL)
        output_before = self.output.read_bytes()
        receipt_before = self.receipt.read_bytes()
        second = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.output.read_bytes(), output_before)
        self.assertEqual(self.receipt.read_bytes(), receipt_before)

    def test_wrong_final_hash_fails_closed(self) -> None:
        self.write_scan()
        command = self.command()
        command[command.index(FINAL)] = "WrongFinal111"
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("final boundary", result.stderr)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.receipt.exists())


if __name__ == "__main__":
    unittest.main()
