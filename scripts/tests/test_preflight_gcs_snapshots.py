from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import preflight_gcs_snapshots as preflight  # noqa: E402


ZERO_HASH = "1" * 32
ONE_HASH = "1" * 31 + "2"
CRC32C = "AAAAAA=="


def inventory_record(
    slot: int,
    *,
    source: str = "root",
    extension: str = ".tar.bz2",
    anchor: int | None = None,
    identity: str = ZERO_HASH,
    size: int = 100,
    generation: int | None = None,
) -> dict:
    anchor = slot if anchor is None else anchor
    generation = slot + 1 if generation is None else generation
    middle = "hourly/" if source == "hourly" else ""
    name = f"{anchor}/{middle}snapshot-{slot}-{identity}{extension}"
    return {
        "metadata": {
            "bucket": preflight.BUCKET_NAME,
            "crc32c": CRC32C,
            "generation": str(generation),
            "id": f"{preflight.BUCKET_NAME}/{name}/{generation}",
            "kind": "storage#object",
            "metageneration": "1",
            "name": name,
            "size": str(size),
        },
        "type": "cloud_object",
        "url": f"{preflight.BUCKET_URI}/{name}#{generation}",
    }


def parsed(record: dict, source: str = "root") -> preflight.SnapshotObject:
    return preflight.parse_inventory_json(json.dumps([record]), source)[0]


def replace_anchor(record: dict, anchor: str | int) -> dict:
    record = copy.deepcopy(record)
    suffix = record["metadata"]["name"].split("/", 1)[1]
    name = f"{anchor}/{suffix}"
    generation = record["metadata"]["generation"]
    record["metadata"]["name"] = name
    record["metadata"]["id"] = f"{preflight.BUCKET_NAME}/{name}/{generation}"
    record["url"] = f"{preflight.BUCKET_URI}/{name}#{generation}"
    return record


def complete_inventory() -> tuple[list[dict], list[dict]]:
    root: list[dict] = []
    hourly: list[dict] = []
    for epoch in range(preflight.FIRST_EPOCH, preflight.LAST_EPOCH + 1):
        extension = ".tar.bz2" if epoch <= 60 else ".tar.zst"
        boundary_slot = epoch * preflight.EPOCH_SLOTS - 1
        boundary = inventory_record(boundary_slot, extension=extension, size=epoch * 10)
        if epoch == preflight.FIRST_EPOCH:
            boundary = inventory_record(
                boundary_slot,
                source="hourly",
                anchor=boundary_slot - 1_000,
                extension=extension,
                size=epoch * 10,
            )
            hourly.append(boundary)
        else:
            root.append(boundary)
        root.append(
            inventory_record(
                epoch * preflight.EPOCH_SLOTS + 100,
                extension=extension,
                identity=ONE_HASH,
                size=epoch * 10 + 1,
            )
        )
    return root, hourly


class InventoryParsingTests(unittest.TestCase):
    def test_parses_real_gcloud_wrapper_and_hourly_filename_slot(self) -> None:
        slot = preflight.FIRST_EPOCH * preflight.EPOCH_SLOTS - 1
        item = parsed(
            inventory_record(slot, source="hourly", anchor=slot - 50, size=1234),
            "hourly",
        )

        self.assertEqual(item.slot, slot)
        self.assertEqual(item.anchor_slot, slot - 50)
        self.assertEqual(item.size, 1234)
        self.assertTrue(item.versioned_uri.endswith(f"#{slot + 1}"))

    def test_rejects_invalid_metadata_and_paths(self) -> None:
        slot = preflight.FIRST_EPOCH * preflight.EPOCH_SLOTS - 1
        mutations = []

        wrong_url = inventory_record(slot)
        wrong_url["url"] = wrong_url["url"].replace("#", "#9", 1)
        mutations.append(wrong_url)

        numeric_size = inventory_record(slot)
        numeric_size["metadata"]["size"] = 100
        mutations.append(numeric_size)

        missing_id = inventory_record(slot)
        del missing_id["metadata"]["id"]
        mutations.append(missing_id)

        mismatched_root = inventory_record(slot)
        old_name = mismatched_root["metadata"]["name"]
        new_name = "1/" + old_name.split("/", 1)[1]
        mismatched_root["metadata"]["name"] = new_name
        mismatched_root["metadata"]["id"] = (
            f"{preflight.BUCKET_NAME}/{new_name}/{mismatched_root['metadata']['generation']}"
        )
        mismatched_root["url"] = (
            f"{preflight.BUCKET_URI}/{new_name}#{mismatched_root['metadata']['generation']}"
        )
        mutations.append(mismatched_root)

        invalid_hash = inventory_record(slot, identity="0" * 32)
        mutations.append(invalid_hash)

        for record in mutations:
            with self.subTest(record=record):
                with self.assertRaises(preflight.PreflightError):
                    parsed(record)

        hourly = inventory_record(slot, source="hourly", anchor=slot + 1)
        with self.assertRaises(preflight.PreflightError):
            parsed(hourly, "hourly")

    def test_strict_json_rejects_duplicate_keys_but_coalesces_duplicate_rows(self) -> None:
        with self.assertRaises(preflight.PreflightError):
            preflight.parse_inventory_json('[{"type":"cloud_object","type":"prefix"}]', "root")

        record = inventory_record(preflight.FIRST_EPOCH * preflight.EPOCH_SLOTS - 1)
        items = preflight.parse_inventory_json(json.dumps([record, record]), "root")
        self.assertEqual(len(items), 1)

    def test_ignores_misplaced_object_outside_requested_history(self) -> None:
        _, relevant_end = preflight.requested_slot_range()
        slot = relevant_end + 1
        records = [
            inventory_record(slot, anchor=slot + 10),
            inventory_record(slot + 1, extension=".tar.zst.1"),
        ]

        items = preflight.parse_inventory_json(json.dumps(records), "root")

        self.assertEqual(items, ())

    def test_rejects_misplaced_object_crossing_requested_history(self) -> None:
        relevant_start, relevant_end = preflight.requested_slot_range()
        slot = relevant_end + 1
        record = inventory_record(slot, anchor=relevant_start)

        with self.assertRaises(preflight.PreflightError):
            preflight.parse_inventory_json(json.dumps([record]), "root")

    def test_requested_subrange_sets_inventory_trust_boundary(self) -> None:
        relevant_slots = preflight.requested_slot_range(12, 16)
        outside_slot = 17 * preflight.EPOCH_SLOTS
        outside = inventory_record(outside_slot, anchor=outside_slot + 1)

        self.assertEqual(
            preflight.parse_inventory_json(
                json.dumps([outside]), "root", relevant_slots
            ),
            (),
        )

    def test_unsupported_suffix_is_ignored_inside_requested_range(self) -> None:
        relevant_slots = preflight.requested_slot_range(12, 16)
        slot = 12 * preflight.EPOCH_SLOTS
        unsupported = inventory_record(slot, extension=".tar.zst.1")

        self.assertEqual(
            preflight.parse_inventory_json(
                json.dumps([unsupported]), "root", relevant_slots
            ),
            (),
        )

    def test_malformed_supported_basename_remains_fatal_outside_range(self) -> None:
        _, relevant_end = preflight.requested_slot_range(12, 16)
        malformed = inventory_record(relevant_end + 1)
        malformed_name = malformed["metadata"]["name"].replace(
            f"snapshot-{relevant_end + 1}-", "snapshot-00-"
        )
        malformed["metadata"]["name"] = malformed_name
        malformed["metadata"]["id"] = (
            f"{preflight.BUCKET_NAME}/{malformed_name}/"
            f"{malformed['metadata']['generation']}"
        )
        malformed["url"] = (
            f"{preflight.BUCKET_URI}/{malformed_name}#"
            f"{malformed['metadata']['generation']}"
        )

        with self.assertRaises(preflight.PreflightError):
            preflight.parse_inventory_json(
                json.dumps([malformed]), "root", preflight.requested_slot_range(12, 16)
            )

    def test_only_same_side_out_of_range_placements_are_ignored(self) -> None:
        relevant_start, relevant_end = preflight.requested_slot_range(12, 16)
        harmless = [
            inventory_record(relevant_start - 2, anchor=relevant_start - 1),
            replace_anchor(
                inventory_record(relevant_end + 2), "ledger-05-01-22"
            ),
            replace_anchor(
                inventory_record(relevant_end + 3), str(preflight.UINT64_MAX + 1)
            ),
        ]
        self.assertEqual(
            preflight.parse_inventory_json(
                json.dumps(harmless), "root", (relevant_start, relevant_end)
            ),
            (),
        )

        unsafe = [
            inventory_record(relevant_end + 1, anchor=relevant_start),
            inventory_record(relevant_start, anchor=relevant_end + 1),
            inventory_record(relevant_end + 1, anchor=relevant_start - 1),
            inventory_record(relevant_start, anchor=relevant_start - 1),
            replace_anchor(
                inventory_record(relevant_start), str(preflight.UINT64_MAX + 1)
            ),
        ]
        for record in unsafe:
            with self.subTest(name=record["metadata"]["name"]):
                with self.assertRaises(preflight.PreflightError):
                    preflight.parse_inventory_json(
                        json.dumps([record]),
                        "root",
                        (relevant_start, relevant_end),
                    )


class SelectionTests(unittest.TestCase):
    def test_cli_accepts_a_supported_subrange(self) -> None:
        arguments = preflight.build_argument_parser().parse_args(
            ["--first-epoch", "12", "--last-epoch", "16"]
        )

        self.assertEqual((arguments.first_epoch, arguments.last_epoch), (12, 16))

    def test_builds_all_epoch_plans_and_keeps_hourly_bootstrap_only(self) -> None:
        root_raw, hourly_raw = complete_inventory()
        root = preflight.parse_inventory_json(json.dumps(list(reversed(root_raw))), "root")
        hourly = preflight.parse_inventory_json(json.dumps(hourly_raw), "hourly")

        plans = preflight.build_epoch_plans(root, hourly)

        self.assertEqual(len(plans), 89)
        self.assertEqual(plans[0].epoch, 12)
        self.assertEqual(plans[-1].epoch, 100)
        self.assertEqual(plans[0].runtime, "solana-v1.0.23")
        self.assertEqual(plans[-1].runtime, "solana-v1.3.19")
        self.assertEqual(plans[0].bootstrap.source, "hourly")
        self.assertTrue(
            all(
                checkpoint.source == "root"
                for plan in plans
                for checkpoint in plan.checkpoints
            )
        )
        self.assertTrue(
            all(
                checkpoint.slot > plan.bootstrap.slot
                for plan in plans
                for checkpoint in plan.checkpoints
            )
        )

    def test_runtime_filtering_uses_target_epoch_before_uniqueness(self) -> None:
        length = preflight.EPOCH_SLOTS
        root_records = [
            inventory_record(60 * length - 2, extension=".tar.bz2"),
            inventory_record(60 * length - 1, extension=".tar.zst"),
            inventory_record(60 * length + 100, extension=".tar.bz2", identity=ONE_HASH),
            inventory_record(61 * length - 2, extension=".tar.bz2"),
            inventory_record(61 * length - 1, extension=".tar.zst"),
            inventory_record(61 * length + 100, extension=".tar.zst", identity=ONE_HASH),
        ]
        root = preflight.parse_inventory_json(json.dumps(root_records), "root")

        plans = preflight.build_epoch_plans(root, (), 60, 61)

        self.assertEqual(plans[0].bootstrap.slot, 60 * length - 2)
        self.assertEqual(plans[0].bootstrap.extension, ".tar.bz2")
        self.assertEqual(plans[0].runtime, "solana-v1.1.23")
        self.assertEqual(plans[1].bootstrap.slot, 61 * length - 1)
        self.assertEqual(plans[1].bootstrap.extension, ".tar.zst")
        self.assertEqual(plans[1].runtime, "solana-v1.2.32")

    def test_structurally_valid_unrelated_archive_extension_is_ignored(self) -> None:
        epoch = 12
        boundary_end = epoch * preflight.EPOCH_SLOTS - 1
        root = preflight.parse_inventory_json(
            json.dumps(
                [
                    inventory_record(boundary_end - 1, extension=".tar.bz2"),
                    inventory_record(boundary_end, extension=".tar.gz"),
                    inventory_record(epoch * preflight.EPOCH_SLOTS + 1),
                ]
            ),
            "root",
        )

        plan = preflight.build_epoch_plans(root, (), epoch, epoch)[0]

        self.assertEqual(plan.bootstrap.slot, boundary_end - 1)
        self.assertEqual(plan.bootstrap.extension, ".tar.bz2")

    def test_ambiguous_newest_bootstrap_fails_without_fallback(self) -> None:
        epoch = 61
        end = epoch * preflight.EPOCH_SLOTS - 1
        records = [
            inventory_record(end - 1, extension=".tar.bz2"),
            inventory_record(end, extension=".tar.bz2"),
            inventory_record(end, extension=".tar.zst", identity=ONE_HASH),
            inventory_record(epoch * preflight.EPOCH_SLOTS + 1, extension=".tar.bz2"),
        ]
        root = preflight.parse_inventory_json(json.dumps(records), "root")

        with self.assertRaisesRegex(preflight.PreflightError, "newest.*ambiguous"):
            preflight.build_epoch_plans(root, (), epoch, epoch)

    def test_hourly_object_does_not_satisfy_checkpoint_requirement(self) -> None:
        epoch = 12
        boundary_slot = epoch * preflight.EPOCH_SLOTS - 1
        hourly = preflight.parse_inventory_json(
            json.dumps(
                [
                    inventory_record(boundary_slot, source="hourly", anchor=boundary_slot - 10),
                    inventory_record(
                        epoch * preflight.EPOCH_SLOTS + 100,
                        source="hourly",
                        anchor=epoch * preflight.EPOCH_SLOTS,
                        identity=ONE_HASH,
                    ),
                ]
            ),
            "hourly",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "root checkpoint"):
            preflight.build_epoch_plans((), hourly, epoch, epoch)


class ReportingAndAcquisitionTests(unittest.TestCase):
    def test_manifest_fingerprint_is_order_independent_and_binds_metadata(self) -> None:
        root_raw, hourly_raw = complete_inventory()
        plans_a = preflight.build_epoch_plans(
            preflight.parse_inventory_json(json.dumps(root_raw), "root"),
            preflight.parse_inventory_json(json.dumps(hourly_raw), "hourly"),
        )
        plans_b = preflight.build_epoch_plans(
            preflight.parse_inventory_json(json.dumps(list(reversed(root_raw))), "root"),
            preflight.parse_inventory_json(json.dumps(list(reversed(hourly_raw))), "hourly"),
        )
        fingerprint_a = preflight.manifest_fingerprint(preflight.build_manifest(plans_a))
        fingerprint_b = preflight.manifest_fingerprint(preflight.build_manifest(plans_b))
        self.assertEqual(fingerprint_a, fingerprint_b)

        metadata_only = copy.deepcopy(root_raw)
        metadata_only[0]["metadata"]["metageneration"] = "2"
        plans_metadata_only = preflight.build_epoch_plans(
            preflight.parse_inventory_json(json.dumps(metadata_only), "root"),
            preflight.parse_inventory_json(json.dumps(hourly_raw), "hourly"),
        )
        self.assertEqual(
            fingerprint_a,
            preflight.manifest_fingerprint(preflight.build_manifest(plans_metadata_only)),
        )

        changed = copy.deepcopy(root_raw)
        changed[-1]["metadata"]["size"] = str(int(changed[-1]["metadata"]["size"]) + 1)
        plans_changed = preflight.build_epoch_plans(
            preflight.parse_inventory_json(json.dumps(changed), "root"),
            preflight.parse_inventory_json(json.dumps(hourly_raw), "hourly"),
        )
        self.assertNotEqual(
            fingerprint_a,
            preflight.manifest_fingerprint(preflight.build_manifest(plans_changed)),
        )

    def test_storage_report_counts_only_exact_regular_local_bootstraps(self) -> None:
        root_raw, hourly_raw = complete_inventory()
        plans = preflight.build_epoch_plans(
            preflight.parse_inventory_json(json.dumps(root_raw), "root"),
            preflight.parse_inventory_json(json.dumps(hourly_raw), "hourly"),
        )
        selected = preflight.selected_bootstraps(plans)
        with tempfile.TemporaryDirectory() as directory:
            local_root = Path(directory)
            (local_root / selected[0].filename).write_bytes(b"x" * selected[0].size)
            (local_root / selected[1].filename).write_bytes(b"wrong")

            report = preflight.build_storage_report(plans, local_root, free_bytes=10**12)

        self.assertEqual(report["selected_bootstrap_objects"], 89)
        self.assertEqual(report["present_bootstrap_bytes"], selected[0].size)
        self.assertEqual(
            report["missing_local_bootstrap_bytes"],
            report["selected_bootstrap_bytes"] - selected[0].size,
        )
        self.assertEqual(report["conflicting_local_paths"], 1)
        self.assertFalse(report["preflight_ok"])

    @mock.patch.object(preflight.subprocess, "run")
    def test_live_mode_uses_two_separate_bounded_gcloud_commands(self, run: mock.Mock) -> None:
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="[]\n", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="[]\n", stderr=""),
        ]

        root, hourly = preflight.load_inventory_texts(None, None)

        self.assertEqual((root, hourly), ("[]\n", "[]\n"))
        self.assertEqual(run.call_count, 2)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0], preflight.gcloud_inventory_command(preflight.ROOT_PATTERN))
        self.assertEqual(commands[1], preflight.gcloud_inventory_command(preflight.HOURLY_PATTERN))
        for command in commands:
            self.assertIn("--json", command)
            self.assertIn("--quiet", command)
            self.assertIn(f"--account={preflight.GCLOUD_ACCOUNT}", command)
            self.assertIn(f"--billing-project={preflight.BILLING_PROJECT}", command)

    @mock.patch.object(preflight, "run_gcloud_inventory")
    def test_fixture_mode_never_invokes_gcloud(self, run_gcloud: mock.Mock) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root_path = Path(directory) / "root.json"
            hourly_path = Path(directory) / "hourly.json"
            root_path.write_text("[]", encoding="utf-8")
            hourly_path.write_text("[]", encoding="utf-8")

            self.assertEqual(
                preflight.load_inventory_texts(root_path, hourly_path),
                ("[]", "[]"),
            )
        run_gcloud.assert_not_called()

        with self.assertRaises(preflight.PreflightError):
            preflight.load_inventory_texts(root_path, None)


if __name__ == "__main__":
    unittest.main()
