import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "collect_epoch208_gap_boundaries.py"
SOURCE = "gs://bucket/rocksdb.tar.bz2#123"
SECOND_SOURCE = "gs://bucket/other-rocksdb.tar.bz2#456"


class GapCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.scan_one = self.root / "scan-one.json"
        self.scan_two = self.root / "scan-two.json"
        self.audit = self.root / "audit.json"
        self.metadata = self.root / "metadata.json"
        self.decoder = self.root / "decoder"
        self.output = self.root / "boundaries.json"
        self.receipt = self.root / "receipt.json"
        self.gaps = [
            {
                "previous_present": 9,
                "post_gap_block": 11,
                "skipped": 1,
                "boundaries": 2,
                "key_prefix": "000000000000000B",
            },
            {
                "previous_present": 12,
                "post_gap_block": 15,
                "skipped": 2,
                "boundaries": 3,
                "key_prefix": "000000000000000F",
            },
        ]
        self.audit.write_text(
            json.dumps(
                {
                    "schema": "jetstreamer-private-epoch208-full-skipped-slot-audit-v1",
                    "summary": {
                        "gap_runs": 2,
                        "skipped_slots": 3,
                        "post_gap_blocks_requiring_original_entries": 2,
                        "total_boundaries_in_post_gap_blocks": 5,
                        "hidden_intermediate_boundaries": 3,
                    },
                    "gaps": self.gaps,
                }
            ),
            encoding="utf-8",
        )
        self.metadata.write_text(
            json.dumps(
                [
                    {
                        "jsonrpc": "2.0",
                        "id": 11,
                        "result": {
                            "parentSlot": 9,
                            "blockhash": "Final11",
                            "previousBlockhash": "Previous9",
                        },
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": 15,
                        "result": {
                            "parentSlot": 12,
                            "blockhash": "Final15",
                            "previousBlockhash": "Previous12",
                        },
                    },
                ]
            ),
            encoding="utf-8",
        )
        reports = {
            "scan-one.json": [
                {
                    "slot": 11,
                    "boundaries": [
                        {"tick_ordinal": 2, "hash": "Hidden11"},
                        {"tick_ordinal": 4, "hash": "Final11"},
                    ],
                    "final_entry_hash": "Final11",
                }
            ],
            "scan-two.json": [
                {
                    "slot": 15,
                    "boundaries": [
                        {"tick_ordinal": 2, "hash": "Hidden15a"},
                        {"tick_ordinal": 4, "hash": "Hidden15b"},
                        {"tick_ordinal": 6, "hash": "Final15"},
                    ],
                    "final_entry_hash": "Final15",
                }
            ],
        }
        self.decoder.write_text(
            "#!/usr/bin/env python3\n"
            "import json, pathlib, sys\n"
            f"reports = {reports!r}\n"
            "print(json.dumps(reports[pathlib.Path(sys.argv[1]).name]))\n",
            encoding="utf-8",
        )
        self.decoder.chmod(0o755)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def write_scans(
        self, second_status: str = "complete", second_source: str = SOURCE
    ) -> None:
        for path, prefix, status in (
            (self.scan_one, "000000000000000B", "complete"),
            (self.scan_two, "000000000000000F", second_status),
        ):
            path.write_text(
                json.dumps(
                    {
                        "schema": "jetstreamer-streaming-rocksdb-prefix-scan-v1",
                        "status": status,
                        "source": SOURCE if path == self.scan_one else second_source,
                        "target_prefixes": [prefix],
                        "matches": [],
                    }
                ),
                encoding="utf-8",
            )

    def command(self) -> list[str]:
        return [
            str(SCRIPT),
            "--scan-state",
            str(self.scan_one),
            "--scan-state",
            str(self.scan_two),
            "--gap-audit",
            str(self.audit),
            "--gap-audit-sha256",
            self.digest(self.audit),
            "--block-metadata",
            str(self.metadata),
            "--block-metadata-sha256",
            self.digest(self.metadata),
            "--decoder",
            str(self.decoder),
            "--decoder-sha256",
            self.digest(self.decoder),
            "--expected-source",
            SOURCE,
            "--ticks-per-slot",
            "2",
            "--output",
            str(self.output),
            "--receipt",
            str(self.receipt),
        ]

    def multi_source_command(self) -> list[str]:
        command = self.command()
        source_index = command.index("--expected-source")
        command[source_index : source_index + 2] = [
            "--expected-scan-source",
            str(self.scan_one),
            SOURCE,
            "--expected-scan-source",
            str(self.scan_two),
            SECOND_SOURCE,
        ]
        return command

    def test_running_scan_defers_without_artifacts(self) -> None:
        self.write_scans(second_status="running")
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("deferred-running", result.stdout)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.receipt.exists())

    def test_complete_scans_collect_all_gaps_and_are_idempotent(self) -> None:
        self.write_scans()
        first = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertIn("collected", first.stdout)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(receipt["gap_runs"], 2)
        self.assertEqual(receipt["hidden_intermediate_boundaries"], 3)
        self.assertEqual(receipt["slots"][1]["hidden_boundary_hashes"], ["Hidden15a", "Hidden15b"])
        before = (self.output.read_bytes(), self.receipt.read_bytes())
        second = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("already-collected", second.stdout)
        self.assertEqual(before, (self.output.read_bytes(), self.receipt.read_bytes()))

    def test_wrong_canonical_final_hash_fails_closed(self) -> None:
        self.write_scans()
        metadata = json.loads(self.metadata.read_text(encoding="utf-8"))
        metadata[1]["result"]["blockhash"] = "Wrong15"
        self.metadata.write_text(json.dumps(metadata), encoding="utf-8")
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("canonical blockhash", result.stderr)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.receipt.exists())

    def test_duplicate_scan_coverage_fails_closed(self) -> None:
        self.write_scans()
        scan = json.loads(self.scan_two.read_text(encoding="utf-8"))
        scan["target_prefixes"].append("000000000000000B")
        self.scan_two.write_text(json.dumps(scan), encoding="utf-8")
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("coverage 2", result.stderr)

    def test_distinct_generation_bound_scan_sources_are_collected(self) -> None:
        self.write_scans(second_source=SECOND_SOURCE)
        result = subprocess.run(self.multi_source_command(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertNotIn("scan_source", receipt)
        self.assertEqual(
            receipt["scan_sources"],
            [
                {"path": str(self.scan_one), "source": SOURCE},
                {"path": str(self.scan_two), "source": SECOND_SOURCE},
            ],
        )
        self.assertEqual(receipt["scan_states"][1]["source"], SECOND_SOURCE)

    def test_per_state_source_mismatch_fails_closed(self) -> None:
        self.write_scans(second_source=SOURCE)
        result = subprocess.run(self.multi_source_command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scan source does not match", result.stderr)

    def test_per_state_bindings_must_exactly_cover_scan_paths(self) -> None:
        self.write_scans(second_source=SECOND_SOURCE)
        command = self.multi_source_command()
        index = command.index("--expected-scan-source", command.index("--expected-scan-source") + 1)
        del command[index : index + 3]
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("do not exactly match", result.stderr)


if __name__ == "__main__":
    unittest.main()
