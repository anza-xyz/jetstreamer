import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "deploy_epoch208_recovered_poh.py"


class RecoveredPohDeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.destination_parent = self.root / "deployments"
        self.source.mkdir(mode=0o700)
        self.destination_parent.mkdir(mode=0o700)
        self.boundaries = self.source / "boundaries.json"
        self.receipt = self.source / "receipt.json"
        slots = []
        receipt_slots = []
        for index in range(25):
            slot = 89_856_117 + index * 3
            hidden = f"Hidden{index}"
            final = f"Final{index}"
            slots.append(
                {
                    "slot": slot,
                    "boundaries": [
                        {"tick_ordinal": 64, "hash": hidden},
                        {"tick_ordinal": 128, "hash": final},
                    ],
                    "final_entry_hash": final,
                }
            )
            receipt_slots.append(
                {
                    "previous_present": slot - 2,
                    "post_gap_block": slot,
                    "skipped": 1,
                    "boundaries": 2,
                    "key_prefix": f"{slot:016X}",
                    "boundary_tick_ordinals": [64, 128],
                    "hidden_boundary_hashes": [hidden],
                    "canonical_final_blockhash": final,
                    "canonical_final_blockhash_match": True,
                }
            )
        boundary_bytes = (
            json.dumps(
                {
                    "schema": "jetstreamer-epoch208-decoded-gap-boundaries-v1",
                    "slots": slots,
                },
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        self.boundaries.write_bytes(boundary_bytes)
        self.receipt.write_text(
            json.dumps(
                {
                    "schema": "jetstreamer-epoch208-recovered-gap-boundaries-v1",
                    "output": str(self.boundaries),
                    "output_sha256": hashlib.sha256(boundary_bytes).hexdigest(),
                    "gap_runs": 25,
                    "skipped_slots": 25,
                    "total_boundaries": 50,
                    "hidden_intermediate_boundaries": 25,
                    "ticks_per_slot": 64,
                    "canonical_final_blockhash_matches": True,
                    "hidden_intermediate_boundaries_distinct": True,
                    "publication_authorized": False,
                    "runtime_route_promotion_authorized": False,
                    "remote_mutations": False,
                    "r2_mutations": False,
                    "slots": receipt_slots,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self.boundaries.chmod(0o600)
        self.receipt.chmod(0o600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def command(self, destination: Path | None = None) -> list[str]:
        return [
            str(SCRIPT),
            "--boundaries",
            str(self.boundaries),
            "--receipt",
            str(self.receipt),
            "--destination",
            str(destination or self.destination_parent / "bundle"),
            "--expected-source-uid",
            str(os.getuid()),
            "--expected-destination-parent-uid",
            str(os.getuid()),
            "--expected-gap-runs",
            "25",
            "--expected-skipped-slots",
            "25",
            "--expected-total-boundaries",
            "50",
        ]

    def test_deploys_readable_immutable_bundle(self) -> None:
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        destination = self.destination_parent / "bundle"
        self.assertEqual(destination.stat().st_mode & 0o777, 0o555)
        for name in (
            "all-gap-boundaries.json",
            "collector-receipt.json",
            "deployment-manifest.json",
            "SHA256SUMS",
        ):
            self.assertEqual((destination / name).stat().st_mode & 0o777, 0o444)
        manifest = json.loads((destination / "deployment-manifest.json").read_text())
        self.assertEqual(
            manifest["runtime_environment"][
                "JETSTREAMER_HISTORICAL_RECOVERED_POH_BOUNDARIES"
            ],
            str(destination / "all-gap-boundaries.json"),
        )
        self.assertFalse(manifest["publication_authorized"])

    def test_rejects_changed_boundary_bytes(self) -> None:
        self.boundaries.write_bytes(self.boundaries.read_bytes() + b" ")
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not bind", result.stderr)
        self.assertFalse((self.destination_parent / "bundle").exists())

    def test_rejects_group_writable_source(self) -> None:
        self.boundaries.chmod(0o620)
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("group/other writable", result.stderr)

    def test_rejects_symlinked_source(self) -> None:
        alias = self.source / "boundaries-alias.json"
        alias.symlink_to(self.boundaries)
        command = self.command()
        command[command.index(str(self.boundaries))] = str(alias)
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not canonical", result.stderr)

    def test_refuses_existing_destination(self) -> None:
        destination = self.destination_parent / "bundle"
        destination.mkdir()
        result = subprocess.run(self.command(destination), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)

    def test_rejects_writable_destination_parent(self) -> None:
        self.destination_parent.chmod(0o770)
        result = subprocess.run(self.command(), text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("trusted non-writable directory", result.stderr)
        self.assertFalse((self.destination_parent / "bundle").exists())


if __name__ == "__main__":
    unittest.main()
