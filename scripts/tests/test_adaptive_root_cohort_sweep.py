from __future__ import annotations

import base64
import contextlib
import copy
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import shlex
import stat
import sys
import tempfile
import time
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import adaptive_root_cohort_sweep as sweep  # noqa: E402


def manifest_report(cohorts: list[tuple[int, int, str]]) -> tuple[dict, str]:
    body = {
        "schema": sweep.MANIFEST_SCHEMA,
        "bucket": "mainnet-beta-ledger-us-ny5",
        "epoch_slots": 432_000,
        "first_epoch": cohorts[0][0],
        "last_epoch": cohorts[-1][1],
        "verification_cohorts": [
            {
                "first_epoch": first,
                "last_epoch": last,
                "runtime": runtime,
                "publication_gate": sweep.PUBLICATION_GATE,
                "root_checkpoints": [{"slot": last * 432_000}],
            }
            for first, last, runtime in cohorts
        ],
    }
    fingerprint = "sha256:" + hashlib.sha256(sweep.canonical_json(body)).hexdigest()
    return {"manifest": body, "manifest_fingerprint": fingerprint}, fingerprint


def write_manifest(directory: Path, report: dict) -> Path:
    path = directory / "manifest.json"
    path.write_text(json.dumps(report))
    path.chmod(0o600)
    return path


def encoded_path(path: Path) -> str:
    return base64.b64encode(os.fsencode(path)).decode("ascii")


def command_properties(command: list[str]) -> dict[str, str]:
    return {
        item.removeprefix("--property=").split("=", 1)[0]: item.removeprefix(
            "--property="
        ).split("=", 1)[1]
        for item in command
        if item.startswith("--property=")
    }


def make_committed_receipt(
    destination: Path,
    receipt_root: Path,
    epochs: list[int],
    fingerprint: str,
) -> tuple[Path, dict]:
    def checkpoint_summary(slot: int) -> dict[str, object]:
        return {
            "slot": slot,
            "bank_hash": [1] * 32,
            "accounts_hash": [2] * 32,
            "last_blockhash": [3] * 32,
            "capitalization": 1,
            "transaction_count": 1,
            "tick_height": 1,
            "slot_complete": True,
            "write_count": 1,
            "next_write_version": 1,
        }

    members = []
    for epoch in epochs:
        archive = destination / f"epoch-{epoch}.jet"
        checksum = destination / f"epoch-{epoch}.jet.sha256"
        archive.write_bytes(f"archive-{epoch}".encode("ascii"))
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        checksum.write_text(f"{digest}  epoch-{epoch}.jet\n")
        archive.chmod(0o440)
        checksum.chmod(0o440)
        members.append(
            {
                "epoch": epoch,
                "archive_path_base64": encoded_path(archive),
                "checksum_path_base64": encoded_path(checksum),
                "manifest_path_base64": None,
                "initial_archive": None,
                "initial_manifest": None,
                "initial_checksum": None,
                "committed_archive": sweep.file_identity(archive),
                "committed_manifest": None,
                "committed_checksum": sweep.file_identity(checksum),
                "archive_sha256": digest,
            }
        )

    transaction_id = "b" * 64
    gate = {
        "kind": "sealed-root-checkpoint-cohort",
        "version": 1,
        "members": [
            {
                "epoch": epoch,
                "bootstrap": checkpoint_summary(epoch * 432_000),
                "terminal": checkpoint_summary(epoch * 432_000 + 1),
            }
            for epoch in epochs
        ],
        "verified_manifest_checkpoints": [
            {"slot": epochs[-1] * 432_000, "accounts_hash": [4] * 32}
        ],
    }
    context = {
        "schema": sweep.ROOT_CHECKPOINT_CONTEXT_SCHEMA,
        "manifest_fingerprint": list(bytes.fromhex(fingerprint.removeprefix("sha256:"))),
        "epochs": epochs,
        "gate": gate,
    }
    identity = sweep.directory_identity(destination)
    receipt = {
        "schema": sweep.RECEIPT_SCHEMA,
        "manifest_fingerprint": fingerprint,
        "transaction_id": transaction_id,
        "epochs": epochs,
        "destination": {
            "path_base64": encoded_path(destination),
            "device": identity.device,
            "inode": identity.inode,
        },
        "publication_context_base64": base64.b64encode(
            json.dumps(context, separators=(",", ":")).encode("ascii")
        ).decode("ascii"),
        "outcome": {"kind": "committed", "members": members},
        "root_checkpoint_gate": gate,
    }
    directory = (
        receipt_root
        / "destination-source"
        / "evidence"
        / "archive-batches"
        / "destination-target"
    )
    directory.mkdir(parents=True)
    path = directory / f"batch-{transaction_id}.json"
    path.write_text(json.dumps(receipt))
    path.chmod(0o600)
    return path, receipt


def fake_receipt_evidence(
    cohort: sweep.Cohort,
    receipt: Path = Path("/private/receipt.json"),
    *,
    semantic_digest: str = "a" * 64,
) -> dict[str, object]:
    identity = {
        "device": 1,
        "inode": 2,
        "mode": 0o100600,
        "uid": 1000,
        "gid": 1000,
        "link_count": 1,
        "length": 128,
        "modified_seconds": 1,
        "modified_nanoseconds": 2,
        "changed_seconds": 3,
        "changed_nanoseconds": 4,
    }
    return {
        "receipt": str(receipt),
        "receipt_identity": identity,
        "receipt_sha256": "b" * 64,
        "destination_identity": {"device": 3, "inode": 4},
        "epochs": list(cohort.epochs),
        "publication_context_sha256": semantic_digest,
        "root_checkpoint_gate_sha256": "c" * 64,
        "members": [
            {"epoch": epoch, "archive_sha256": "d" * 64}
            for epoch in cohort.epochs
        ],
    }


class ManifestTests(unittest.TestCase):
    def test_selected_range_accepts_an_unselected_transition_runtime(self) -> None:
        raw = [(1, 1, "solana-v1.0.7-to-v1.0.8")]
        raw.extend((epoch, epoch, "solana-v1.0.23") for epoch in range(2, 24))
        report, fingerprint = manifest_report(raw)
        with tempfile.TemporaryDirectory() as temporary:
            path = write_manifest(Path(temporary), report)
            cohorts = sweep.load_cohorts(path, fingerprint, 22, 23)

        self.assertEqual([item.label for item in cohorts], ["22", "23"])

    def test_range_must_not_split_a_manifest_cohort(self) -> None:
        report, fingerprint = manifest_report(
            [(31, 32, "solana-v1.1.23"), (33, 34, "solana-v1.1.23")]
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = write_manifest(Path(temporary), report)
            with self.assertRaisesRegex(sweep.SweepError, "split"):
                sweep.load_cohorts(path, fingerprint, 32, 34)

    def test_server_manifest_plans_exactly_62_units_for_22_through_100(self) -> None:
        path = Path(
            "/home/sol/.jetstreamer-private/"
            "deploy-epochs7-100-20260911-v7/preflight-1-100.json"
        )
        if not path.exists():
            self.skipTest("sealed production manifest is not installed")
        cohorts = sweep.load_cohorts(
            path,
            "sha256:888df3d89187e3fb8cd307e65eab1a4770153887965f3defd74f048504fc3f1a",
            22,
            100,
        )

        expected_labels = (
            "22 23 24 25 26 27 28 29 30 31-32 33-34 35 36 37 38-39 "
            "40-41 42-43 44 45-46 47 48 49 50-51 52 53 54-55 56-57 "
            "58-59 60 61 62 63 64 65-66 67-68 69 70 71-72 73 74 75 "
            "76 77 78 79 80 81-82 83 84 85 86-87 88 89-90 91 92 "
            "93-94 95 96 97 98 99 100"
        ).split()
        expected_runtime = {
            **{epoch: "solana-v1.0.23" for epoch in range(22, 30)},
            30: "solana-v1.1.23",  # the installed v12 manifest remains immutable
            **{epoch: "solana-v1.1.23" for epoch in range(31, 61)},
            **{epoch: "solana-v1.2.32" for epoch in range(61, 92)},
            **{epoch: "solana-v1.3.19" for epoch in range(92, 101)},
        }

        self.assertEqual([item.label for item in cohorts], expected_labels)
        self.assertEqual(sum(len(item.epochs) for item in cohorts), 79)
        self.assertTrue(
            all(
                item.runtime == expected_runtime[item.first_epoch]
                and all(expected_runtime[epoch] == item.runtime for epoch in item.epochs)
                for item in cohorts
            )
        )


class ReceiptTests(unittest.TestCase):
    def test_canonical_epoch_bounds_reject_huge_or_wrapping_values(self) -> None:
        self.assertEqual(
            sweep.canonical_receipt_epochs([sweep.MAX_U64 - 1, sweep.MAX_U64]),
            (sweep.MAX_U64 - 1, sweep.MAX_U64),
        )
        rejected = (
            [sweep.MAX_U64, sweep.MAX_U64 + 1],
            [1 << 4096],
            [-1],
            [False],
            [7, 9],
            list(range(sweep.MAX_RECEIPT_EPOCHS + 1)),
        )
        for epochs in rejected:
            with self.subTest(first=epochs[0], count=len(epochs)):
                self.assertIsNone(sweep.canonical_receipt_epochs(epochs))

    def test_receipt_rejects_a_trailing_non_object_member(self) -> None:
        fingerprint = "sha256:" + "c" * 64
        for member_set in ("outcome", "root_checkpoint_gate"):
            with self.subTest(member_set=member_set), tempfile.TemporaryDirectory() as raw:
                root = Path(raw).resolve()
                destination = root / "destination"
                receipts = root / "receipts"
                destination.mkdir()
                receipts.mkdir()
                path, receipt = make_committed_receipt(
                    destination, receipts, [7, 8], fingerprint
                )
                self.assertEqual(
                    list(
                        sweep.discover_committed_receipts(
                            destination, receipts, fingerprint, os.getuid()
                        )
                    ),
                    [sweep.Cohort(7, 8, "")],
                )
                receipt[member_set]["members"][-1] = "not-an-object"
                path.write_text(json.dumps(receipt))
                path.chmod(0o600)

                self.assertEqual(
                    sweep.discover_committed_receipts(
                        destination, receipts, fingerprint, os.getuid()
                    ),
                    {},
                )

    def test_receipt_rejects_destination_device_or_inode_mismatch(self) -> None:
        fingerprint = "sha256:" + "e" * 64
        for field in ("device", "inode"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as raw:
                root = Path(raw).resolve()
                destination = root / "destination"
                receipts = root / "receipts"
                destination.mkdir()
                receipts.mkdir()
                path, receipt = make_committed_receipt(
                    destination, receipts, [22], fingerprint
                )
                receipt["destination"][field] += 1
                path.write_text(json.dumps(receipt))
                path.chmod(0o600)

                self.assertEqual(
                    sweep.discover_committed_receipts(
                        destination,
                        receipts,
                        fingerprint,
                        os.getuid(),
                        expected_destination_identity=sweep.directory_identity(
                            destination
                        ),
                    ),
                    {},
                )

    def test_bound_source_evidence_hashes_archive_and_receipt_bytes(self) -> None:
        fingerprint = "sha256:" + "f" * 64
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            destination = root / "destination"
            receipts = root / "receipts"
            destination.mkdir()
            receipts.mkdir()
            receipt, _data = make_committed_receipt(
                destination, receipts, list(cohort.epochs), fingerprint
            )
            identity = sweep.directory_identity(destination)
            evidence = sweep.capture_committed_receipt_evidence(
                destination,
                receipts,
                cohort,
                fingerprint,
                os.getuid(),
                allow_rename_ctime=True,
                expected_destination_identity=identity,
                rehash_archives=True,
            )
            self.assertTrue(sweep.receipt_evidence_is_valid(evidence, cohort))

            archive = destination / "epoch-22.jet"
            before = archive.stat()
            archive.chmod(0o600)
            archive.write_bytes(b"tampered22")
            os.utime(archive, ns=(before.st_atime_ns, before.st_mtime_ns))
            archive.chmod(0o440)
            with self.assertRaisesRegex(sweep.SweepError, "archive hash changed"):
                sweep.capture_committed_receipt_evidence(
                    destination,
                    receipts,
                    cohort,
                    fingerprint,
                    os.getuid(),
                    allow_rename_ctime=True,
                    expected_destination_identity=identity,
                    rehash_archives=True,
                )

            receipt.write_bytes(receipt.read_bytes() + b" ")
            rebound = copy.deepcopy(evidence)
            rebound["receipt_identity"] = sweep.file_identity(receipt)
            with self.assertRaisesRegex(sweep.SweepError, "bound source receipt changed"):
                sweep.verify_bound_receipt_file(rebound)

    def test_live_receipt_tolerates_only_rename_induced_ctime_drift(self) -> None:
        fingerprint = "sha256:" + "d" * 64
        cohort = sweep.Cohort(22, 22, "")
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            lane_root = root / "lane"
            output = lane_root / "output"
            private = lane_root / "private"
            holding = lane_root / "holding"
            output.mkdir(parents=True)
            private.mkdir()
            holding.mkdir()
            receipt_path, receipt = make_committed_receipt(
                output, private, [22], fingerprint
            )
            member = receipt["outcome"]["members"][0]
            archive = output / "epoch-22.jet"
            checksum = output / "epoch-22.jet.sha256"
            original = {archive: member["committed_archive"]}

            time.sleep(0.002)
            archive.rename(holding / archive.name)
            (holding / archive.name).rename(archive)

            for path, identity in original.items():
                observed = sweep.file_identity(path)
                self.assertEqual(observed["device"], identity["device"])
                self.assertEqual(observed["inode"], identity["inode"])
                self.assertNotEqual(
                    (observed["changed_seconds"], observed["changed_nanoseconds"]),
                    (identity["changed_seconds"], identity["changed_nanoseconds"]),
                )

            self.assertEqual(
                sweep.discover_committed_receipts(
                    output, private, fingerprint, os.getuid()
                ),
                {},
            )
            self.assertEqual(
                sweep.discover_live_receipts(
                    sweep.Lane("lane", lane_root), fingerprint, os.getuid()
                ),
                {cohort: receipt_path},
            )
            checksum.rename(holding / checksum.name)
            (holding / checksum.name).rename(checksum)
            self.assertEqual(
                sweep.discover_live_receipts(
                    sweep.Lane("lane", lane_root), fingerprint, os.getuid()
                ),
                {},
            )
            archive.chmod(0o600)
            self.assertEqual(
                sweep.discover_live_receipts(
                    sweep.Lane("lane", lane_root), fingerprint, os.getuid()
                ),
                {},
            )

    def test_r2_receipts_can_replace_only_an_exact_prior_attestation(self) -> None:
        cohort = sweep.Cohort(7, 8, "solana-v1.0.23")
        digests = {7: "7" * 64, 8: "8" * 64}
        completion = {
            "members": [
                {"epoch": epoch, "archive_sha256": digests[epoch]}
                for epoch in cohort.epochs
            ],
            "recorded_unix": 100,
        }
        with tempfile.TemporaryDirectory() as raw:
            receipts = Path(raw).resolve()
            receipts.chmod(0o700)
            for epoch in cohort.epochs:
                path = receipts / f"epoch-{epoch}.r2.json"
                path.write_text(
                    json.dumps(
                        {
                            "schema": sweep.R2_RECEIPT_SCHEMA,
                            "bucket": "test-bucket",
                            "epoch": epoch,
                            "archive_key": f"epoch-{epoch}.jet",
                            "checksum_key": f"epoch-{epoch}.jet.sha256",
                            "archive_length": 1234 + epoch,
                            "archive_sha256": digests[epoch],
                            "archive_etag": f"etag-{epoch}",
                            "multipart_part_size": 5 * 1024 * 1024,
                            "r2_composite_sha256": None,
                            "remote_sha256_readback": True,
                            "verified_unix_seconds": 101,
                        }
                    )
                )
                path.chmod(0o600)

            self.assertTrue(
                sweep.r2_receipts_prove_completion(
                    receipts, "test-bucket", cohort, completion, os.getuid()
                )
            )

            bad = receipts / "epoch-8.r2.json"
            receipt = json.loads(bad.read_text())
            receipt["archive_sha256"] = "f" * 64
            bad.write_text(json.dumps(receipt))
            bad.chmod(0o600)
            with self.assertRaisesRegex(
                sweep.SweepError, "does not prove attested epoch 8"
            ):
                sweep.r2_receipts_prove_completion(
                    receipts, "test-bucket", cohort, completion, os.getuid()
                )

    def test_r2_completion_requires_every_receipt_and_a_remote_hash_proof(self) -> None:
        cohort = sweep.Cohort(9, 9, "solana-v1.0.23")
        completion = {
            "members": [{"epoch": 9, "archive_sha256": "9" * 64}],
            "recorded_unix": 200,
        }
        with tempfile.TemporaryDirectory() as raw:
            receipts = Path(raw).resolve()
            receipts.chmod(0o700)
            self.assertFalse(
                sweep.r2_receipts_prove_completion(
                    receipts, "test-bucket", cohort, completion, os.getuid()
                )
            )
            path = receipts / "epoch-9.r2.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": sweep.R2_RECEIPT_SCHEMA,
                        "bucket": "test-bucket",
                        "epoch": 9,
                        "archive_key": "epoch-9.jet",
                        "checksum_key": "epoch-9.jet.sha256",
                        "archive_length": 999,
                        "archive_sha256": "9" * 64,
                        "archive_etag": "etag-9",
                        "multipart_part_size": 5 * 1024 * 1024,
                        "r2_composite_sha256": None,
                        "remote_sha256_readback": False,
                        "verified_unix_seconds": 201,
                    }
                )
            )
            path.chmod(0o600)
            with self.assertRaisesRegex(
                sweep.SweepError, "does not prove attested epoch 9"
            ):
                sweep.r2_receipts_prove_completion(
                    receipts, "test-bucket", cohort, completion, os.getuid()
                )

            receipt = json.loads(path.read_text())
            receipt["r2_composite_sha256"] = "composite-proof"
            path.write_text(json.dumps(receipt))
            path.chmod(0o600)
            self.assertTrue(
                sweep.r2_receipts_prove_completion(
                    receipts, "test-bucket", cohort, completion, os.getuid()
                )
            )


class DirectoryBindingTests(unittest.TestCase):
    def test_bound_directory_rejects_path_inode_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            path = root / "bound"
            displaced = root / "displaced"
            path.mkdir()
            binding = sweep.BoundDirectory(path)
            try:
                path.rename(displaced)
                path.mkdir()
                with self.assertRaisesRegex(
                    sweep.SweepError, "bound directory identity changed"
                ):
                    binding.revalidate()
            finally:
                binding.close()


class EvidenceFlowTests(unittest.TestCase):
    def test_public_and_bound_source_semantics_must_match(self) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            public_dir=Path("/public"),
            public_private_root=Path("/public-private"),
            manifest_fingerprint="sha256:" + "a" * 64,
        )
        controller.sol_uid = os.getuid()
        controller.public_destination_identity = sweep.DirectoryIdentity(1, 2)
        controller.public_complete = mock.Mock(return_value=True)
        source = fake_receipt_evidence(
            cohort,
            Path("/lane/private/source.json"),
            semantic_digest="a" * 64,
        )
        public = fake_receipt_evidence(
            cohort,
            Path("/public-private/public.json"),
            semantic_digest="e" * 64,
        )

        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(
                sweep, "capture_committed_receipt_evidence", return_value=public
            ),
            mock.patch.object(sweep, "verify_bound_receipt_file"),
            self.assertRaisesRegex(sweep.SweepError, "differs from the bound source"),
        ):
            controller.capture_completion_attestation(cohort, source)


class CommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.deploy = Path("/sealed/deploy")
        self.lane = sweep.Lane("lane-c", Path("/private/sweep/lane-c"))
        self.cohort = sweep.Cohort(31, 32, "solana-v1.1.23")
        self.manifest = self.deploy / "preflight.json"
        self.fingerprint = "sha256:" + "a" * 64

    def test_r2_completion_source_requires_an_exact_bucket_binding(self) -> None:
        parser = sweep.build_argument_parser()
        base = [
            "--deploy-dir=/deploy",
            "--manifest=/deploy/manifest.json",
            "--manifest-fingerprint=sha256:" + "a" * 64,
            "--public-private-root=/private",
            "--state-dir=/state",
            "--lane=lane-a=/lane-a",
        ]
        incomplete = parser.parse_args(
            [*base, "--r2-receipt-directory=/r2-receipts"]
        )
        with self.assertRaisesRegex(sweep.SweepError, "must be supplied together"):
            sweep.validate_options(incomplete)

        complete = parser.parse_args(
            [
                *base,
                "--r2-receipt-directory=/r2-receipts",
                "--r2-bucket=test-bucket",
            ]
        )
        sweep.validate_options(complete)

    def test_private_network_check_ignores_class_control_files(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            network_class = Path(raw)
            (network_class / "lo").mkdir()
            (network_class / "bonding_masters").write_text("")
            sweep.require_private_network_namespace(network_class)

            (network_class / "eth0").mkdir()
            with self.assertRaisesRegex(sweep.SweepError, "loopback-only"):
                sweep.require_private_network_namespace(network_class)

    def test_service_identity_defaults_and_unsafe_names(self) -> None:
        parser = sweep.build_argument_parser()
        base = [
            "--deploy-dir=/deploy",
            "--manifest=/deploy/manifest.json",
            "--manifest-fingerprint=sha256:" + "a" * 64,
            "--public-private-root=/private",
            "--state-dir=/state",
            "--lane=lane-a=/lane-a",
        ]
        defaults = parser.parse_args(base)
        self.assertEqual(defaults.producer_user, "sol")
        self.assertEqual(defaults.archive_group, "horizon")

        for option in (
            "--producer-user=ubuntu\nroot",
            "--archive-group=ubuntu/root",
        ):
            with self.subTest(option=option):
                args = parser.parse_args([*base, option])
                with self.assertRaisesRegex(sweep.SweepError, "safe service"):
                    sweep.validate_options(args)

    def test_custom_service_identity_is_applied_to_both_units(self) -> None:
        common = {
            "cohort": self.cohort,
            "deploy": self.deploy,
            "manifest": self.manifest,
            "fingerprint": self.fingerprint,
            "producer_user": "ubuntu",
            "archive_group": "ubuntu",
            "producer_home": Path("/home/ubuntu"),
        }
        producer = sweep.build_producer_command(
            lane=self.lane,
            unit="producer-test.service",
            **common,
        )
        importer = sweep.build_import_command(
            receipt=Path("/private/receipt.json"),
            public_dir=Path("/home/ubuntu/horizon"),
            public_private_root=Path("/home/ubuntu/.private-public"),
            unit="import-test.service",
            **common,
        )

        for command in (producer, importer):
            with self.subTest(command=command[1]):
                properties = command_properties(command)
                environment = {
                    item.removeprefix("--setenv=")
                    for item in command
                    if item.startswith("--setenv=")
                }
                self.assertEqual(properties["User"], "ubuntu")
                self.assertEqual(properties["Group"], "ubuntu")
                self.assertEqual(properties["NoExecPaths"], "/home/ubuntu")
                self.assertIn("HOME=/home/ubuntu", environment)
                self.assertIn("USER=ubuntu", environment)
                self.assertIn("LOGNAME=ubuntu", environment)
                self.assertIn(
                    "-/home/ubuntu/.ssh",
                    properties["InaccessiblePaths"].split(),
                )
                self.assertNotIn("/home/sol", "\0".join(command))

    def test_producer_uses_one_whole_lane_mount_and_recursive_sealed_bind(self) -> None:
        command = sweep.build_producer_command(
            cohort=self.cohort,
            lane=self.lane,
            deploy=self.deploy,
            manifest=self.manifest,
            fingerprint=self.fingerprint,
            unit="test.service",
        )
        properties = command_properties(command)

        self.assertEqual(properties["ReadWritePaths"], str(self.lane.root))
        self.assertEqual(
            properties["BindReadOnlyPaths"],
            f"{self.deploy}:{self.deploy}:rbind",
        )
        self.assertEqual(
            properties["ExecPaths"].split(),
            [
                str(self.deploy / "jetstreamer-node"),
                str(self.deploy / "jetstreamer-historical-worker-v1-1-23"),
                str(self.lane.root),
            ],
        )
        self.assertNotIn(str(self.lane.private), properties["ReadWritePaths"].split())
        self.assertNotIn("/home/sol/horizon", "\0".join(command))
        self.assertIn("--root-checkpoint-cohort", command)
        self.assertIn("--verify", command)
        self.assertEqual(
            properties["RestrictNamespaces"],
            sweep.RELOAD_STABLE_RESTRICT_NAMESPACES,
        )

    def test_resource_limits_propagate_to_producer_and_importer(self) -> None:
        common = {
            "cohort": self.cohort,
            "deploy": self.deploy,
            "manifest": self.manifest,
            "fingerprint": self.fingerprint,
            "memory_high_gib": 17,
            "memory_max_gib": 19,
            "cpu_quota_percent": 725,
        }
        producer = sweep.build_producer_command(
            lane=self.lane,
            unit="producer-test.service",
            **common,
        )
        importer = sweep.build_import_command(
            receipt=Path("/private/receipt.json"),
            public_dir=Path("/home/sol/horizon"),
            public_private_root=Path("/home/sol/.private-public"),
            unit="import-test.service",
            **common,
        )

        for command in (producer, importer):
            with self.subTest(command=command[1]):
                properties = command_properties(command)
                self.assertEqual(properties["MemoryHigh"], str(17 * sweep.GIB))
                self.assertEqual(properties["MemoryMax"], str(19 * sweep.GIB))
                self.assertEqual(properties["CPUQuota"], "725%")
                self.assertEqual(properties["MemorySwapMax"], "0")

    def test_handoff_cohort_exposes_source_and_destination_workers(self) -> None:
        cohort = sweep.Cohort(
            67,
            68,
            "solana-v1.2.32-mainnet-epoch68-transition",
        )
        command = sweep.build_producer_command(
            cohort=cohort,
            lane=self.lane,
            deploy=self.deploy,
            manifest=self.manifest,
            fingerprint=self.fingerprint,
            unit="handoff-test.service",
        )
        properties = command_properties(command)
        environments = {
            item.removeprefix("--setenv=")
            for item in command
            if item.startswith("--setenv=")
        }

        self.assertEqual(
            properties["ExecPaths"].split(),
            [
                str(self.deploy / "jetstreamer-node"),
                str(
                    self.deploy
                    / "jetstreamer-historical-worker-v1-2-32-epoch68-transition"
                ),
                str(self.deploy / "jetstreamer-historical-worker-v1-2-32"),
                str(
                    self.deploy
                    / "jetstreamer-historical-worker-v1-2-24-epoch67-pre-cpi"
                ),
                str(self.lane.root),
            ],
        )
        self.assertIn(
            "JETSTREAMER_HISTORICAL_WORKER_V1_2_32_EPOCH68_TRANSITION="
            f"{self.deploy}/jetstreamer-historical-worker-v1-2-32-epoch68-transition",
            environments,
        )
        self.assertIn(
            "JETSTREAMER_HISTORICAL_WORKER_V1_2_32="
            f"{self.deploy}/jetstreamer-historical-worker-v1-2-32",
            environments,
        )
        self.assertIn(
            "JETSTREAMER_HISTORICAL_WORKER_V1_2_24_EPOCH67_PRE_CPI="
            f"{self.deploy}/jetstreamer-historical-worker-v1-2-24-epoch67-pre-cpi",
            environments,
        )
        self.assertIn("JETSTREAMER_HISTORICAL_REAP_TIMEOUT_SECS=600", environments)

    def test_v158_exception_cohorts_expose_the_v158_worker(self) -> None:
        for epoch in (154, 157):
            cohort = sweep.Cohort(epoch, epoch, "solana-v1.5.6")
            command = sweep.build_producer_command(
                cohort=cohort,
                lane=self.lane,
                deploy=self.deploy,
                manifest=self.manifest,
                fingerprint=self.fingerprint,
                unit=f"epoch{epoch}-v158-test.service",
            )
            properties = command_properties(command)
            environments = {
                item.removeprefix("--setenv=")
                for item in command
                if item.startswith("--setenv=")
            }

            self.assertEqual(
                properties["ExecPaths"].split(),
                [
                    str(self.deploy / "jetstreamer-node"),
                    str(self.deploy / "jetstreamer-historical-worker-v1-5-6"),
                    str(self.deploy / "jetstreamer-historical-worker-v1-5-8"),
                    str(self.lane.root),
                ],
            )
            self.assertIn(
                "JETSTREAMER_HISTORICAL_WORKER_V1_5_6="
                f"{self.deploy}/jetstreamer-historical-worker-v1-5-6",
                environments,
            )
            self.assertIn(
                "JETSTREAMER_HISTORICAL_WORKER_V1_5_8="
                f"{self.deploy}/jetstreamer-historical-worker-v1-5-8",
                environments,
            )

        ordinary = sweep.build_producer_command(
            cohort=sweep.Cohort(156, 156, "solana-v1.5.6"),
            lane=self.lane,
            deploy=self.deploy,
            manifest=self.manifest,
            fingerprint=self.fingerprint,
            unit="epoch156-no-handoff-test.service",
        )
        self.assertNotIn(
            str(self.deploy / "jetstreamer-historical-worker-v1-5-8"),
            command_properties(ordinary)["ExecPaths"].split(),
        )

    def test_importer_has_no_mount_namespace_or_systemd_path_bind(self) -> None:
        command = sweep.build_import_command(
            cohort=self.cohort,
            receipt=Path("/private/receipt.json"),
            deploy=self.deploy,
            manifest=self.manifest,
            fingerprint=self.fingerprint,
            public_dir=Path("/home/sol/horizon"),
            public_private_root=Path("/home/sol/.private-public"),
            unit="import-test.service",
        )
        joined = "\0".join(command)

        self.assertEqual(command[0], "/usr/bin/systemd-run")
        self.assertNotIn("unshare", joined)
        self.assertNotIn("ReadWritePaths", joined)
        self.assertNotIn("BindReadOnlyPaths", joined)
        self.assertIn("--property=PrivateNetwork=yes", command)
        self.assertIn("--property=ProtectSystem=full", command)
        self.assertIn("--property=ProtectHome=no", command)
        self.assertIn("--property=CapabilityBoundingSet=", command)
        self.assertIn("--property=NoExecPaths=/home/sol", command)
        self.assertIn("--recover-staged-cohort-only", command)
        self.assertIn("--source-cohort-receipt=/private/receipt.json", command)


class AdoptionHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.deploy = Path("/sealed/deploy")
        self.lane = sweep.Lane("lane-c", Path("/private/sweep/lane-c"))
        self.cohort = sweep.Cohort(31, 32, "solana-v1.1.23")
        self.unit = "sealed-producer.service"
        self.process = sweep.ProducerProcess(
            pid=4242,
            start_time=9876,
            cohort=self.cohort,
            output=self.lane.output,
            manifest=self.deploy / "preflight.json",
            fingerprint="sha256:" + "a" * 64,
            unit=self.unit,
            argv=(),
        )

    def hardened_properties(self, **identity: object) -> dict[str, str]:
        command = sweep.build_producer_command(
            cohort=self.cohort,
            lane=self.lane,
            deploy=self.deploy,
            manifest=self.deploy / "preflight.json",
            fingerprint="sha256:" + "a" * 64,
            unit=self.unit,
            **identity,
        )
        properties = command_properties(command)
        properties["MainPID"] = str(self.process.pid)
        properties["ControlGroup"] = f"/system.slice/{self.unit}"
        properties["RemainAfterExit"] = "no"
        properties["ExecStart"] = (
            f"{{ path={self.deploy / 'jetstreamer-node'} ; "
            f"argv[]={self.deploy / 'jetstreamer-node'} 31-32 ... ; }}"
        )
        properties["Environment"] = " ".join(
            item.removeprefix("--setenv=")
            for item in command
            if item.startswith("--setenv=")
        )
        properties["TimeoutStartUSec"] = properties.pop("TimeoutStartSec")
        properties["TimeoutStopUSec"] = properties.pop("TimeoutStopSec")
        properties["CPUQuotaPerSecUSec"] = "10s"
        properties.pop("CPUQuota")
        properties["IOSchedulingClass"] = "2"
        return properties

    def test_custom_identity_hardened_unit_is_accepted(self) -> None:
        identity = {
            "producer_user": "ubuntu",
            "archive_group": "ubuntu",
            "producer_home": Path("/home/ubuntu"),
        }
        with mock.patch.object(
            sweep,
            "systemd_properties",
            return_value=self.hardened_properties(**identity),
        ):
            self.assertTrue(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                    **identity,
                )
            )

    def test_exact_hardened_unit_is_accepted(self) -> None:
        with mock.patch.object(
            sweep, "systemd_properties", return_value=self.hardened_properties()
        ):
            self.assertTrue(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )

    def test_exact_node_authored_resume_environment_is_accepted(self) -> None:
        properties = self.hardened_properties()
        resume = (
            f"{sweep.ROOT_COHORT_RESUME_RUN_ENV}="
            "/private/sweep/lane-c/private/destination-"
            f"{'b' * 64}/work/root-cohort-31-32/run-123-456"
        )
        properties["Environment"] += f" {resume}"
        with (
            mock.patch.object(
                sweep, "systemd_properties", return_value=properties
            ),
            mock.patch.object(
                sweep,
                "root_cohort_resume_environment_is_valid",
                return_value=True,
            ) as validate_resume,
        ):
            self.assertTrue(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )
            validate_resume.assert_called_once_with(
                resume, self.process, self.cohort, self.lane
            )

    def test_resume_environment_requires_exact_private_state(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            cohort = sweep.Cohort(32, 32, self.cohort.runtime)
            process = dataclasses.replace(self.process, cohort=cohort)
            lane_root = Path(raw).resolve() / "lane-c"
            lane = sweep.Lane("lane-c", lane_root)
            lane.private.mkdir(parents=True)
            destination = lane.private / f"destination-{'c' * 64}"
            run = destination / "work" / "root-cohort-32-32" / "run-123-456"
            run.mkdir(parents=True)
            private_directories = (
                lane_root,
                lane.private,
                destination,
                destination / "work",
                run.parent,
                run,
            )
            for path in private_directories:
                path.chmod(0o700)
            state = run / "cohort-state.json"
            state.write_text(
                json.dumps(
                    {
                        "end_epoch": cohort.last_epoch,
                        "manifest_fingerprint": process.fingerprint,
                        "schema": sweep.ROOT_COHORT_RUN_STATE_SCHEMA,
                        "start_epoch": cohort.first_epoch,
                        "status": "running-private",
                    }
                )
            )
            state.chmod(0o600)
            item = f"{sweep.ROOT_COHORT_RESUME_RUN_ENV}={run}"

            self.assertTrue(
                sweep.root_cohort_resume_environment_is_valid(
                    item, process, cohort, lane
                )
            )

            state.chmod(0o640)
            self.assertFalse(
                sweep.root_cohort_resume_environment_is_valid(
                    item, process, cohort, lane
                )
            )
            state.chmod(0o600)
            changed = json.loads(state.read_text())
            changed["manifest_fingerprint"] = "sha256:" + "d" * 64
            state.write_text(json.dumps(changed))
            state.chmod(0o600)
            self.assertFalse(
                sweep.root_cohort_resume_environment_is_valid(
                    item, process, cohort, lane
                )
            )

    def test_unrecognized_extra_environment_is_rejected(self) -> None:
        properties = self.hardened_properties()
        properties["Environment"] += " UNRECOGNIZED_CAPABILITY=1"
        with mock.patch.object(
            sweep, "systemd_properties", return_value=properties
        ):
            self.assertFalse(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )

    def test_legacy_worker_without_extended_reap_window_is_accepted(self) -> None:
        properties = self.hardened_properties()
        environment = set(shlex.split(properties["Environment"]))
        environment.remove(sweep.PRODUCTION_REAP_ENV)
        properties["Environment"] = " ".join(sorted(environment))
        with mock.patch.object(
            sweep, "systemd_properties", return_value=properties
        ):
            self.assertTrue(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )

    def test_different_reap_window_is_rejected(self) -> None:
        properties = self.hardened_properties()
        properties["Environment"] = properties["Environment"].replace(
            sweep.PRODUCTION_REAP_ENV,
            "JETSTREAMER_HISTORICAL_REAP_TIMEOUT_SECS=5",
        )
        with mock.patch.object(
            sweep, "systemd_properties", return_value=properties
        ):
            self.assertFalse(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )

    def test_reload_serialized_namespace_policy_requires_live_attestation(self) -> None:
        properties = self.hardened_properties()
        properties["RestrictNamespaces"] = "no"
        with (
            mock.patch.object(sweep, "systemd_properties", return_value=properties),
            mock.patch.object(
                sweep, "reloaded_namespace_filter_is_attested", return_value=True
            ) as attested,
        ):
            self.assertTrue(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )
            attested.assert_called_once_with(self.unit, self.process)

        with (
            mock.patch.object(sweep, "systemd_properties", return_value=properties),
            mock.patch.object(
                sweep, "reloaded_namespace_filter_is_attested", return_value=False
            ),
        ):
            self.assertFalse(
                sweep.unit_is_hardened_for_adoption(
                    self.unit,
                    self.process,
                    self.cohort,
                    self.lane,
                    self.deploy,
                )
            )

    def test_reload_serialized_namespace_attestation_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            transient = root / "transient"
            proc = root / "proc"
            process_dir = proc / str(self.process.pid)
            transient.mkdir()
            process_dir.mkdir(parents=True)
            unit_file = transient / self.unit
            unit_file.write_text(
                "# This is a transient unit file, created programmatically via the systemd API.\n"
                "[Service]\nRestrictNamespaces=\n"
            )
            unit_file.chmod(0o644)
            (process_dir / "status").write_text(
                "Name:\tnode\nNoNewPrivs:\t1\nSeccomp:\t2\nSeccomp_filters:\t30\n"
            )
            fields = ["S", "1"] + ["0"] * 18
            fields[19] = str(self.process.start_time)
            (process_dir / "stat").write_text(
                f"{self.process.pid} (node) {' '.join(fields)}\n"
            )

            self.assertTrue(
                sweep.reloaded_namespace_filter_is_attested(
                    self.unit,
                    self.process,
                    transient_root=transient,
                    proc_root=proc,
                    manager_uid=os.getuid(),
                )
            )

            (process_dir / "status").write_text(
                "Name:\tnode\nNoNewPrivs:\t0\nSeccomp:\t2\nSeccomp_filters:\t30\n"
            )
            self.assertFalse(
                sweep.reloaded_namespace_filter_is_attested(
                    self.unit,
                    self.process,
                    transient_root=transient,
                    proc_root=proc,
                    manager_uid=os.getuid(),
                )
            )

    def test_any_material_hardening_drift_is_rejected(self) -> None:
        mutations = {
            "main pid": ("MainPID", "4243"),
            "memory maximum": ("MemoryMax", str(63 * sweep.GIB)),
            "privilege escalation": ("NoNewPrivileges", "no"),
            "capability": ("CapabilityBoundingSet", "cap_net_raw"),
            "deployment bind": ("BindReadOnlyPaths", "/different:/different:rbind"),
            "network family": ("RestrictAddressFamilies", "AF_UNIX AF_INET"),
            "executable": ("ExecStart", "{ path=/tmp/node ; argv[]=/tmp/node ; }"),
            "environment": ("Environment", "HOME=/tmp"),
        }
        for label, (key, value) in mutations.items():
            with self.subTest(label=label):
                properties = self.hardened_properties()
                properties[key] = value
                with mock.patch.object(
                    sweep, "systemd_properties", return_value=properties
                ):
                    self.assertFalse(
                        sweep.unit_is_hardened_for_adoption(
                            self.unit,
                            self.process,
                            self.cohort,
                            self.lane,
                            self.deploy,
                        )
                    )

    def test_discovery_requires_the_expected_executable_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            fake_proc = root / "proc"
            process_dir = fake_proc / "4242"
            deploy = root / "deploy"
            output = root / "output"
            deploy.mkdir()
            output.mkdir()
            process_dir.mkdir(parents=True)
            node = deploy / "jetstreamer-node"
            manifest = deploy / "preflight.json"
            node.write_bytes(b"sealed-node")
            manifest.write_text("manifest")
            os.link(node, process_dir / "exe")
            argv = (
                str(node),
                "22",
                str(output),
                "--verify",
                "--root-checkpoint-cohort",
                f"--cohort-manifest={manifest}",
                f"--cohort-manifest-fingerprint=sha256:{'d' * 64}",
            )
            (process_dir / "cmdline").write_bytes(
                b"\0".join(item.encode("utf-8") for item in argv) + b"\0"
            )
            stat_tail = ["S", *("0" for _ in range(18)), "9876"]
            (process_dir / "stat").write_text(
                "4242 (jetstreamer-node) " + " ".join(stat_tail)
            )
            (process_dir / "cgroup").write_text(
                "0::/system.slice/sealed-producer.service\n"
            )

            real_path = Path

            def fake_path(value: object) -> Path:
                rendered = os.fspath(value)
                if rendered == "/proc":
                    return fake_proc
                if rendered.startswith("/proc/"):
                    return fake_proc / rendered.removeprefix("/proc/")
                return real_path(rendered)

            with mock.patch.object(sweep, "Path", side_effect=fake_path):
                found = sweep.discover_producers(
                    expected_node=node, expected_uid=os.getuid()
                )
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].argv, argv)
            self.assertEqual(found[0].start_time, 9876)
            self.assertEqual(found[0].unit, "sealed-producer.service")

            (process_dir / "exe").unlink()
            (process_dir / "exe").write_bytes(b"different-node")
            with mock.patch.object(sweep, "Path", side_effect=fake_path):
                self.assertEqual(
                    sweep.discover_producers(
                        expected_node=node, expected_uid=os.getuid()
                    ),
                    (),
                )


class SystemdStatusTests(unittest.TestCase):
    def test_query_failure_is_distinct_from_authoritative_not_found(self) -> None:
        failure = mock.Mock(
            returncode=1,
            stderr="Failed to connect to bus",
            stdout="",
        )
        with mock.patch.object(sweep.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(
                sweep.SweepError,
                "failed to query systemd unit missing.service: Failed to connect to bus",
            ):
                sweep.unit_status("missing.service")

        not_found = (
            "LoadState=not-found\n"
            "ActiveState=inactive\n"
            "SubState=dead\n"
            "Result=success\n"
            "ExecMainStatus=0\n"
            "MemoryCurrent=\n"
            "MemoryPeak=\n"
        )
        missing = sweep.parse_systemd_show(not_found)
        self.assertFalse(missing.exists)
        self.assertFalse(missing.active)
        self.assertFalse(missing.succeeded)
        self.assertEqual(missing.sub_state, "dead")

        authoritative_missing = mock.Mock(
            returncode=1,
            stderr="Unit missing.service could not be found.",
            stdout=not_found,
        )
        with mock.patch.object(
            sweep.subprocess, "run", return_value=authoritative_missing
        ):
            queried = sweep.unit_status("missing.service")
        self.assertFalse(queried.exists)
        self.assertFalse(queried.active)
        self.assertFalse(queried.succeeded)
        self.assertEqual(queried.sub_state, "dead")

    def test_retained_unit_name_is_rejected_before_launch(self) -> None:
        retained = sweep.UnitStatus(
            True, False, True, "success", 0, 0, 0, "exited"
        )
        with (
            mock.patch.object(sweep, "unit_status", return_value=retained),
            self.assertRaisesRegex(sweep.SweepError, "already exists"),
        ):
            sweep.require_unused_unit_name("retained.service")


class AdmissionTests(unittest.TestCase):
    def capacity(
        self,
        *,
        active: int = 2,
        owned_active: int | None = None,
        elapsed: int = 0,
        available_gib: int = 735,
        cpus: int = 64,
        initial: int = 2,
        target: int = 6,
        lane_count: int = 8,
        maximum: int = 8,
        current: list[int | None] | None = None,
        peak: list[int | None] | None = None,
        disk_available_gib: int = 20_000,
        disk_reserve_gib: int = 512,
        disk_budget_per_worker_gib: int = 2_048,
        memory_admission_gib: int = 64,
        cpus_per_lane: int = 8,
    ) -> int:
        current = [5 * sweep.GIB] * active if current is None else current
        peak = [6 * sweep.GIB] * active if peak is None else peak
        owned_active = active if owned_active is None else owned_active
        return sweep.admission_capacity(
            memory_total=1024 * sweep.GIB,
            memory_available=available_gib * sweep.GIB,
            logical_cpus=cpus,
            active=active,
            owned_active=owned_active,
            elapsed_seconds=elapsed,
            lane_count=lane_count,
            initial=initial,
            target=target,
            maximum=maximum,
            settle_seconds=600,
            memory_max=64 * sweep.GIB,
            memory_admission=memory_admission_gib * sweep.GIB,
            memory_reserve=64 * sweep.GIB,
            protected_memory=350 * sweep.GIB,
            cpus_per_lane=cpus_per_lane,
            memory_current=current,
            memory_peak=peak,
            memory_high=48 * sweep.GIB,
            disk_available=disk_available_gib * sweep.GIB,
            disk_reserve=disk_reserve_gib * sweep.GIB,
            disk_budget_per_worker=disk_budget_per_worker_gib * sweep.GIB,
        )

    def test_starts_at_two_then_ramps_one_per_settle_interval_to_six(self) -> None:
        self.assertEqual(self.capacity(active=0, elapsed=0), 2)
        self.assertEqual(self.capacity(elapsed=599), 2)
        self.assertEqual(self.capacity(elapsed=600), 3)
        self.assertEqual(
            self.capacity(active=5, elapsed=600, initial=5, target=6), 6
        )

    def test_cpu_and_hard_memory_reservations_cap_admission(self) -> None:
        self.assertEqual(
            self.capacity(active=3, elapsed=0, cpus=32, initial=6, target=6), 4
        )
        # Only one additional 64 GiB reservation remains above the 64 GiB reserve.
        self.assertEqual(
            self.capacity(
                active=3,
                elapsed=0,
                available_gib=128,
                initial=6,
                target=6,
            ),
            4,
        )

    def test_admission_reservation_is_distinct_from_hard_cgroup_limit(self) -> None:
        self.assertEqual(
            self.capacity(
                active=10,
                owned_active=0,
                elapsed=10_000,
                available_gib=625,
                initial=4,
                target=4,
                lane_count=4,
                maximum=4,
                cpus=64,
                cpus_per_lane=3,
                disk_available_gib=4_224,
                disk_reserve_gib=1_024,
                disk_budget_per_worker_gib=225,
                memory_admission_gib=20,
            ),
            14,
        )

    def test_unknown_or_high_cgroup_memory_freezes_ramp(self) -> None:
        self.assertEqual(
            self.capacity(active=2, elapsed=10_000, current=[None, 5 * sweep.GIB]),
            2,
        )
        self.assertEqual(
            self.capacity(active=2, elapsed=10_000, peak=[49 * sweep.GIB, 5 * sweep.GIB]),
            2,
        )

    def test_unrelated_high_memory_producer_does_not_freeze_local_queue(self) -> None:
        self.assertEqual(
            self.capacity(
                active=7,
                owned_active=0,
                initial=1,
                target=1,
                lane_count=1,
                maximum=1,
                current=[],
                peak=[],
            ),
            8,
        )

    def test_disk_budget_never_stops_live_work_but_blocks_unsafe_launches(self) -> None:
        self.assertEqual(
            self.capacity(active=2, elapsed=10_000, disk_available_gib=4_100),
            2,
        )
        self.assertEqual(
            self.capacity(active=1, elapsed=10_000, disk_available_gib=5_000),
            2,
        )
        self.assertEqual(
            self.capacity(active=3, elapsed=10_000, disk_available_gib=256),
            3,
        )
        self.assertEqual(
            self.capacity(active=3, elapsed=10_000, disk_available_gib=0),
            3,
        )

    def test_configured_concurrency_can_exceed_legacy_eight_lane_cap(self) -> None:
        parser = sweep.build_argument_parser()

        def options(maximum: int) -> argparse.Namespace:
            return parser.parse_args(
                [
                    "--deploy-dir=/deploy",
                    "--manifest=/deploy/manifest.json",
                    "--manifest-fingerprint=sha256:" + "a" * 64,
                    "--public-private-root=/private",
                    "--state-dir=/state",
                    "--lane=lane-a=/lane-a",
                    "--initial-concurrency=1",
                    f"--target-concurrency={maximum}",
                    f"--max-concurrency={maximum}",
                ]
            )

        valid = options(9)
        sweep.validate_options(valid)
        self.assertEqual(valid.memory_admission_gib, valid.memory_max_gib)
        with self.assertRaisesRegex(sweep.SweepError, "max <= 32"):
            sweep.validate_options(options(33))

        excessive_memory_admission = options(9)
        excessive_memory_admission.memory_admission_gib = (
            excessive_memory_admission.memory_max_gib + 1
        )
        with self.assertRaisesRegex(
            sweep.SweepError, "memory-admission-gib must not exceed"
        ):
            sweep.validate_options(excessive_memory_admission)

    def test_local_lane_count_does_not_become_the_global_ceiling(self) -> None:
        self.assertEqual(
            self.capacity(
                active=7,
                owned_active=0,
                initial=1,
                target=1,
                lane_count=1,
                maximum=1,
            ),
            8,
        )
        self.assertEqual(
            self.capacity(
                active=0,
                owned_active=0,
                initial=1,
                target=1,
                lane_count=1,
                maximum=1,
            ),
            1,
        )
        self.assertEqual(
            self.capacity(
                active=8,
                owned_active=1,
                initial=1,
                target=1,
                lane_count=1,
                maximum=1,
            ),
            8,
        )


class PublicStateTests(unittest.TestCase):
    def test_public_batch_marker_must_be_one_safe_real_directory(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            public = Path(raw).resolve()
            marker = public / sweep.ARCHIVE_BATCH_MARKERS[0]
            marker.mkdir(mode=0o700)
            self.assertTrue(
                sweep.public_recovery_marker_present(public, os.getuid())
            )

            second = public / sweep.ARCHIVE_BATCH_MARKERS[1]
            second.mkdir(mode=0o700)
            with self.assertRaisesRegex(sweep.SweepError, "both archive batch markers"):
                sweep.public_recovery_marker_present(public, os.getuid())

            second.rmdir()
            marker.rmdir()
            marker.symlink_to(public, target_is_directory=True)
            with self.assertRaisesRegex(sweep.SweepError, "unsafe public archive batch"):
                sweep.public_recovery_marker_present(public, os.getuid())

    def test_public_batch_marker_accepts_inherited_setgid_without_group_access(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            public = Path(raw).resolve() / "horizon"
            public.mkdir()
            public.chmod(0o3770)
            marker = public / sweep.ARCHIVE_BATCH_MARKERS[0]
            marker.mkdir(mode=0o700)
            self.assertEqual(stat.S_IMODE(marker.stat().st_mode), 0o2700)
            self.assertTrue(
                sweep.public_recovery_marker_present(public, os.getuid())
            )

            marker.chmod(0o1700)
            with self.assertRaisesRegex(sweep.SweepError, "unsafe public archive batch"):
                sweep.public_recovery_marker_present(public, os.getuid())

    def test_all_complete_includes_adopt_only_cohorts(self) -> None:
        scheduled = sweep.Cohort(22, 22, "solana-v1.0.23")
        adopt_only = sweep.Cohort(21, 21, "solana-v1.0.23")
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.sol_uid = os.getuid()
        controller.managed_cohorts = (scheduled, adopt_only)
        controller.state = {
            "assignments": {},
            "import_owner": None,
            "completed": {scheduled.label: {}, adopt_only.label: {}},
        }
        controller.trusted_public_complete = mock.Mock(
            side_effect=lambda cohort, final=False: cohort == scheduled
        )

        with mock.patch.object(
            sweep, "public_recovery_marker_present", return_value=False
        ):
            self.assertFalse(controller.all_complete())
        self.assertEqual(
            controller.trusted_public_complete.call_args_list,
            [mock.call(scheduled, final=True), mock.call(adopt_only, final=True)],
        )

    def test_all_complete_does_not_rehash_an_incomplete_range(self) -> None:
        completed = sweep.Cohort(21, 21, "solana-v1.0.23")
        pending = sweep.Cohort(22, 22, "solana-v1.0.23")
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.sol_uid = os.getuid()
        controller.managed_cohorts = (completed, pending)
        controller.state = {
            "assignments": {},
            "import_owner": None,
            "completed": {completed.label: {}},
        }
        controller.trusted_public_complete = mock.Mock(return_value=True)

        with mock.patch.object(
            sweep, "public_recovery_marker_present", return_value=False
        ):
            self.assertFalse(controller.all_complete())
        controller.trusted_public_complete.assert_not_called()

    def test_batch_marker_prevents_all_complete_without_rehashing(self) -> None:
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.sol_uid = os.getuid()
        controller.managed_cohorts = (sweep.Cohort(22, 22, "solana-v1.0.23"),)
        controller.state = {
            "assignments": {},
            "import_owner": None,
            "completed": {"22": {}},
        }
        controller.trusted_public_complete = mock.Mock(return_value=True)

        with mock.patch.object(
            sweep, "public_recovery_marker_present", return_value=True
        ):
            self.assertFalse(controller.all_complete())
        controller.trusted_public_complete.assert_not_called()

    def test_attested_cohort_can_remain_complete_after_receipt_gated_r2_retirement(
        self,
    ) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        public = Path("/public")
        private = Path("/public-private")
        r2 = Path("/r2-receipts")
        record = {
            **fake_receipt_evidence(cohort, private / "receipt.json"),
            "first_epoch": 22,
            "last_epoch": 22,
            "recorded_unix": 100.0,
        }
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            public_dir=public,
            public_private_root=private,
            r2_receipt_directory=r2,
            r2_bucket="test-bucket",
            manifest_fingerprint="sha256:" + "a" * 64,
        )
        controller.sol_uid = os.getuid()
        controller.horizon_gid = os.getgid()
        controller.public_destination_identity = sweep.DirectoryIdentity(3, 4)
        controller.state = {"completed": {cohort.label: record}}
        binding = mock.Mock()
        controller.directory_bindings = {r2: binding}
        controller.revalidate_operational_directories = mock.Mock()

        with (
            mock.patch.object(sweep, "identity_matches", return_value=True),
            mock.patch.object(
                sweep, "checksum_file", return_value=record["receipt_sha256"]
            ),
            mock.patch.object(
                sweep, "public_archive_namespace_complete", return_value=False
            ),
            mock.patch.object(
                sweep, "r2_receipts_prove_completion", return_value=True
            ) as r2_proof,
            mock.patch.object(sweep, "discover_committed_receipts") as discover,
        ):
            self.assertTrue(controller.trusted_public_complete(cohort, final=True))

        binding.revalidate.assert_called_once_with()
        r2_proof.assert_called_once_with(
            r2, "test-bucket", cohort, record, os.getuid()
        )
        discover.assert_not_called()


class StateTests(unittest.TestCase):
    def test_state_rejects_configuration_drift(self) -> None:
        fingerprint = "sha256:" + "e" * 64
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            path = root / "state.json"
            state = sweep.load_state(path, fingerprint, root, "a" * 64, 2)
            path.write_text(json.dumps(state))
            path.chmod(0o600)
            self.assertEqual(
                sweep.load_state(path, fingerprint, root, "a" * 64, 2), state
            )
            with self.assertRaisesRegex(sweep.SweepError, "sealed sweep"):
                sweep.load_state(path, fingerprint, root, "b" * 64, 2)

    def test_assignment_state_enforces_adoption_and_import_invariants(self) -> None:
        scheduled = sweep.Cohort(22, 22, "solana-v1.0.23")
        adopt_only = sweep.Cohort(21, 21, "solana-v1.0.23")
        cohorts = {
            (scheduled.first_epoch, scheduled.last_epoch): scheduled,
            (adopt_only.first_epoch, adopt_only.last_epoch): adopt_only,
        }
        lanes = {
            "lane-a": sweep.Lane("lane-a", Path("/lane-a")),
            "lane-b": sweep.Lane("lane-b", Path("/lane-b")),
        }
        state = {
            "ramp_limit": 2,
            "ramp_started_unix": 1.0,
            "last_ramp_unix": 2.0,
            "attempts": {"21": 1, "22": 1},
            "assignments": {
                "lane-a": {
                    "phase": "producer",
                    "first_epoch": 22,
                    "last_epoch": 22,
                    "unit": "producer-a.service",
                    "adopt_only": False,
                },
                "lane-b": {
                    "phase": "staged",
                    "first_epoch": 21,
                    "last_epoch": 21,
                    "receipt": "/receipts/21.json",
                    "adopt_only": True,
                },
            },
            "completed": {},
            "import_owner": "lane-b",
        }
        scheduled_bounds = {(22, 22)}
        sweep.validate_state_assignments(state, lanes, cohorts, scheduled_bounds)

        invalid = copy.deepcopy(state)
        invalid["assignments"]["lane-b"]["adopt_only"] = False
        with self.assertRaisesRegex(sweep.SweepError, "inconsistent adopt-only"):
            sweep.validate_state_assignments(
                invalid, lanes, cohorts, scheduled_bounds
            )

        invalid = copy.deepcopy(state)
        invalid["import_owner"] = "lane-a"
        with self.assertRaisesRegex(sweep.SweepError, "recoverable assignment"):
            sweep.validate_state_assignments(
                invalid, lanes, cohorts, scheduled_bounds
            )

        invalid = copy.deepcopy(state)
        invalid["last_ramp_unix"] = float("nan")
        with self.assertRaisesRegex(sweep.SweepError, "ramp metadata"):
            sweep.validate_state_assignments(
                invalid, lanes, cohorts, scheduled_bounds
            )

        invalid = copy.deepcopy(state)
        invalid["assignments"]["lane-a"].update(
            phase="importing",
            receipt="/lane-a/private/22.json",
            import_unit="import-a.service",
            source_evidence=fake_receipt_evidence(
                scheduled, Path("/lane-a/private/22.json")
            ),
        )
        invalid["assignments"]["lane-b"].update(
            phase="importing",
            receipt="/lane-b/private/21.json",
            import_unit="import-b.service",
            source_evidence=fake_receipt_evidence(
                adopt_only, Path("/lane-b/private/21.json")
            ),
        )
        invalid["import_owner"] = "lane-a"
        with (
            mock.patch.object(
                sweep,
                "directory_identity",
                return_value=sweep.DirectoryIdentity(3, 4),
            ),
            self.assertRaisesRegex(sweep.SweepError, "serialize importer ownership"),
        ):
            sweep.validate_state_assignments(invalid, lanes, cohorts, scheduled_bounds)

    def test_assignment_state_rejects_malformed_source_recovery_attempts(self) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        state = {
            "ramp_limit": 1,
            "ramp_started_unix": 1.0,
            "last_ramp_unix": 2.0,
            "attempts": {"22": 1},
            "assignments": {
                "lane-a": {
                    "phase": "producer",
                    "first_epoch": 22,
                    "last_epoch": 22,
                    "unit": "producer-a.service",
                    "adopt_only": False,
                    "source_recovery_attempts": "one",
                }
            },
            "completed": {},
            "import_owner": None,
        }
        with self.assertRaisesRegex(
            sweep.SweepError, "invalid source_recovery_attempts"
        ):
            sweep.validate_state_assignments(
                state,
                {"lane-a": sweep.Lane("lane-a", Path("/lane-a"))},
                {(22, 22): cohort},
                {(22, 22)},
            )


class SourceRecoveryTests(unittest.TestCase):
    def test_public_marker_allows_only_a_safe_retained_source_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            lane_root = root / "lane"
            (lane_root / "private").mkdir(parents=True)
            (lane_root / "output").mkdir()
            public = root / "public"
            public.mkdir()
            (public / sweep.ARCHIVE_BATCH_MARKERS[0]).mkdir(mode=0o700)
            receipt = lane_root / "private" / "receipt.json"
            receipt.write_text("{}")
            receipt.chmod(0o600)
            cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
            controller = object.__new__(sweep.Controller)
            controller.args = mock.Mock(
                public_dir=public,
                manifest_fingerprint="sha256:" + "f" * 64,
            )
            controller.lanes = {"lane": sweep.Lane("lane", lane_root)}
            controller.sol_uid = os.getuid()

            with mock.patch.object(sweep, "discover_live_receipts") as discover:
                self.assertEqual(
                    controller.require_live_source_receipt("lane", cohort, receipt),
                    receipt,
                )
            discover.assert_not_called()

            receipt.chmod(0o640)
            with self.assertRaisesRegex(sweep.SweepError, "unsafe"):
                controller.require_live_source_receipt("lane", cohort, receipt)

    def test_public_recovery_marker_requires_a_persisted_import_owner(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            public = Path(raw).resolve()
            (public / sweep.ARCHIVE_BATCH_MARKERS[0]).mkdir(mode=0o700)
            controller = object.__new__(sweep.Controller)
            controller.args = mock.Mock(public_dir=public)
            controller.sol_uid = os.getuid()
            controller.state = {"assignments": {}, "import_owner": None}

            with self.assertRaisesRegex(sweep.SweepError, "no root-state import owner"):
                controller.import_one()

    def test_lane_recovery_marker_relaunches_only_the_import_owner(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            public = root / "public"
            public.mkdir()
            lane_root = root / "lane"
            (lane_root / "output").mkdir(parents=True)
            (lane_root / "private").mkdir()
            (lane_root / "output" / sweep.ARCHIVE_BATCH_MARKERS[0]).mkdir(
                mode=0o700
            )
            cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
            raw_assignment = {
                "phase": "staged",
                "first_epoch": 22,
                "last_epoch": 22,
                "receipt": str(lane_root / "private" / "receipt.json"),
                "adopt_only": False,
            }
            controller = object.__new__(sweep.Controller)
            controller.args = mock.Mock(public_dir=public)
            controller.sol_uid = os.getuid()
            controller.lanes = {"lane": sweep.Lane("lane", lane_root)}
            controller.by_bounds = {(22, 22): cohort}
            controller.state = {
                "assignments": {"lane": raw_assignment},
                "import_owner": "lane",
            }
            controller.launch_producer = mock.Mock(return_value=True)

            self.assertTrue(controller.import_one())
            self.assertEqual(raw_assignment["phase"], "producer")
            self.assertIsNone(controller.state["import_owner"])
            controller.launch_producer.assert_called_once_with(
                "lane", raw_assignment, cohort, source_recovery=True
            )


class CrashSafetyTests(unittest.TestCase):
    def staged_import_controller(
        self,
    ) -> tuple[sweep.Controller, sweep.Cohort, dict[str, object]]:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        lane = sweep.Lane("lane-a", Path("/lanes/lane-a"))
        assignment: dict[str, object] = {
            "phase": "staged",
            "first_epoch": 22,
            "last_epoch": 22,
            "receipt": "/lanes/lane-a/private/receipt.json",
            "adopt_only": False,
        }
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            public_dir=Path("/public"),
            public_private_root=Path("/public-private"),
            deploy_dir=Path("/sealed/deploy"),
            manifest=Path("/sealed/deploy/preflight.json"),
            manifest_fingerprint="sha256:" + "f" * 64,
            controller_id="test",
            memory_high_gib=17,
            memory_max_gib=19,
            cpu_quota_percent=725,
            producer_user="sol",
            archive_group="horizon",
        )
        controller.sol_uid = os.getuid()
        controller.producer_home = Path("/home/sol")
        controller.lanes = {lane.name: lane}
        controller.by_bounds = {(22, 22): cohort}
        controller.state = {
            "assignments": {lane.name: assignment},
            "completed": {},
            "import_owner": None,
            "sequence": 0,
        }
        controller.save = mock.Mock()
        controller.require_live_source_receipt = mock.Mock(
            return_value=Path(assignment["receipt"])
        )
        controller.lane_output_identity = mock.Mock(
            return_value=sweep.DirectoryIdentity(1, 2)
        )
        controller.serialized_public_import_admission = mock.Mock(
            return_value=contextlib.nullcontext()
        )
        return controller, cohort, assignment

    def test_staged_public_cohort_is_finalized_through_bound_attestation(self) -> None:
        controller, cohort, assignment = self.staged_import_controller()
        source_evidence = fake_receipt_evidence(cohort)
        assignment["source_evidence"] = source_evidence
        controller.state["import_owner"] = "lane-a"
        controller.public_complete = mock.Mock(return_value=True)
        controller.capture_completion_attestation = mock.Mock(
            return_value={"durable": True}
        )

        controller.refresh_assignments()

        self.assertNotIn("lane-a", controller.state["assignments"])
        self.assertIsNone(controller.state["import_owner"])
        self.assertEqual(controller.state["completed"], {"22": {"durable": True}})
        controller.public_complete.assert_called_once_with(cohort, rehash=False)
        controller.capture_completion_attestation.assert_called_once_with(
            cohort, source_evidence
        )
        controller.save.assert_called_once()

    def test_import_waits_while_public_destination_is_claimed(self) -> None:
        controller, _cohort, assignment = self.staged_import_controller()
        public_claim = sweep.EpochClaim(
            pid=1234,
            start_time=5678,
            first_epoch=67,
            last_epoch=68,
            output=controller.args.public_dir,
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="direct-public-writer.service",
        )
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(
                sweep, "discover_epoch_claims", return_value=(public_claim,)
            ),
        ):
            self.assertFalse(controller.import_one())

        self.assertEqual(assignment["phase"], "staged")
        self.assertIsNone(controller.state["import_owner"])
        controller.require_live_source_receipt.assert_not_called()
        controller.save.assert_not_called()

    def test_import_rechecks_public_claim_under_serialized_admission(self) -> None:
        controller, cohort, assignment = self.staged_import_controller()
        public_claim = sweep.EpochClaim(
            pid=1234,
            start_time=5678,
            first_epoch=67,
            last_epoch=68,
            output=controller.args.public_dir,
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="other-controller-import.service",
        )
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(
                sweep, "discover_epoch_claims", side_effect=((), (public_claim,))
            ),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep,
                "capture_committed_receipt_evidence",
                return_value=fake_receipt_evidence(cohort),
            ) as capture,
            mock.patch.object(sweep.subprocess, "run") as launch,
        ):
            self.assertFalse(controller.import_one())

        capture.assert_called_once()
        controller.serialized_public_import_admission.assert_called_once()
        launch.assert_not_called()
        self.assertEqual(assignment["phase"], "staged")
        self.assertIsNone(controller.state["import_owner"])
        self.assertEqual(controller.state["sequence"], 0)
        controller.save.assert_not_called()

    def test_successful_import_is_durable_before_unit_retirement(self) -> None:
        controller, cohort, assignment = self.staged_import_controller()
        assignment["phase"] = "importing"
        assignment["import_unit"] = "import-test.service"
        assignment["source_evidence"] = fake_receipt_evidence(cohort)
        controller.state["import_owner"] = "lane-a"
        status = sweep.UnitStatus(
            True, True, False, "success", 0, 1024, 2048, "exited"
        )
        controller.public_state_after_import = mock.Mock(return_value=(True, False))
        controller.capture_completion_attestation = mock.Mock(
            return_value={"durable": True}
        )
        events: list[tuple[str, object, object, object]] = []

        def record_save() -> None:
            events.append(
                (
                    "save",
                    "lane-a" in controller.state["assignments"],
                    controller.state["import_owner"],
                    copy.deepcopy(controller.state["completed"]),
                )
            )

        def record_retirement(unit: str, retired_status: sweep.UnitStatus) -> None:
            events.append(
                (
                    "retire",
                    "lane-a" in controller.state["assignments"],
                    controller.state["import_owner"],
                    copy.deepcopy(controller.state["completed"]),
                )
            )
            self.assertEqual(unit, "import-test.service")
            self.assertEqual(retired_status, status)

        controller.save.side_effect = record_save
        controller.retire_import_unit = mock.Mock(side_effect=record_retirement)
        with mock.patch.object(sweep, "unit_status", return_value=status):
            controller.refresh_assignments()

        self.assertEqual([event[0] for event in events], ["save", "retire"])
        for _kind, assignment_present, owner, completed in events:
            self.assertFalse(assignment_present)
            self.assertIsNone(owner)
            self.assertEqual(completed, {"22": {"durable": True}})

    def test_nonzero_systemd_run_with_existing_unit_remains_owned(self) -> None:
        controller, _cohort, assignment = self.staged_import_controller()
        launch_result = mock.Mock(returncode=1, stderr="manager reply lost", stdout="")
        absent = sweep.UnitStatus(
            False, False, False, "not-found", None, None, None, "dead"
        )
        retained = sweep.UnitStatus(
            True, True, False, "success", None, 1024, 1024, "running"
        )
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep, "build_import_command", return_value=["systemd-run"]
            ),
            mock.patch.object(
                sweep,
                "capture_committed_receipt_evidence",
                return_value=fake_receipt_evidence(_cohort),
            ),
            mock.patch.object(sweep, "verify_bound_receipt_file"),
            mock.patch.object(sweep.subprocess, "run", return_value=launch_result),
            mock.patch.object(sweep, "unit_status", side_effect=(absent, retained)),
        ):
            self.assertTrue(controller.import_one())

        self.assertEqual(assignment["phase"], "importing")
        self.assertEqual(controller.state["import_owner"], "lane-a")
        self.assertEqual(assignment["import_launch_warning"], "manager reply lost")
        self.assertNotIn("failure", assignment)
        self.assertEqual(controller.save.call_count, 2)

    def test_stale_import_unit_is_rejected_before_state_claim(self) -> None:
        controller, cohort, assignment = self.staged_import_controller()
        retained = sweep.UnitStatus(
            True, False, True, "success", 0, 0, 0, "exited"
        )
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep,
                "capture_committed_receipt_evidence",
                return_value=fake_receipt_evidence(cohort),
            ),
            mock.patch.object(sweep, "unit_status", return_value=retained),
            mock.patch.object(sweep, "build_import_command") as build_command,
            self.assertRaisesRegex(sweep.SweepError, "already exists"),
        ):
            controller.import_one()

        self.assertEqual(assignment["phase"], "staged")
        self.assertIsNone(controller.state["import_owner"])
        self.assertEqual(controller.state["sequence"], 0)
        controller.save.assert_not_called()
        build_command.assert_not_called()

    def test_nonzero_systemd_run_without_unit_quarantines_import(self) -> None:
        controller, _cohort, assignment = self.staged_import_controller()
        launch_result = mock.Mock(returncode=1, stderr="unit not created", stdout="")
        absent = sweep.UnitStatus(
            False, False, False, "not-found", None, None, None, ""
        )
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep, "build_import_command", return_value=["systemd-run"]
            ),
            mock.patch.object(
                sweep,
                "capture_committed_receipt_evidence",
                return_value=fake_receipt_evidence(_cohort),
            ),
            mock.patch.object(sweep, "verify_bound_receipt_file"),
            mock.patch.object(sweep.subprocess, "run", return_value=launch_result),
            mock.patch.object(sweep, "unit_status", side_effect=(absent, absent)),
        ):
            self.assertFalse(controller.import_one())

        self.assertEqual(assignment["phase"], "import_failed")
        self.assertIsNone(controller.state["import_owner"])
        self.assertIn("unit not created", assignment["failure"])
        self.assertEqual(controller.save.call_count, 2)

    def test_schedule_never_persists_temporary_failed_assignment(self) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        lane = sweep.Lane("lane-a", Path("/lane-a"))
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            public_dir=Path("/public"),
            deploy_dir=Path("/sealed/deploy"),
            manifest=Path("/sealed/deploy/preflight.json"),
            manifest_fingerprint="sha256:" + "a" * 64,
            controller_id="test",
            gcloud_account=sweep.DEFAULT_ACCOUNT,
            gcloud_project=sweep.DEFAULT_PROJECT,
            memory_high_gib=17,
            memory_max_gib=19,
            cpu_quota_percent=725,
            producer_user="sol",
            archive_group="horizon",
        )
        controller.sol_uid = os.getuid()
        controller.producer_home = Path("/home/sol")
        controller.managed_cohorts = (cohort,)
        controller.scheduled_bounds = {(22, 22)}
        controller.lanes = {lane.name: lane}
        controller.state = {
            "assignments": {},
            "attempts": {},
            "import_owner": None,
            "last_ramp_unix": 1.0,
            "sequence": 0,
        }
        controller.desired_capacity = mock.Mock(return_value=1)
        controller.trusted_public_complete = mock.Mock(return_value=False)
        persisted_phases: list[str] = []

        def record_save() -> None:
            raw = controller.state["assignments"].get("lane-a")
            persisted_phases.append(raw["phase"] if raw is not None else "absent")

        controller.save = mock.Mock(side_effect=record_save)
        controller.receipt_location_for_cohort = mock.Mock(return_value=None)
        launched = mock.Mock(returncode=0, stderr="", stdout="")
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(sweep, "discover_epoch_claims", return_value=()),
            mock.patch.object(sweep, "public_output_files", return_value=()),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep, "build_producer_command", return_value=["systemd-run"]
            ),
            mock.patch.object(
                sweep,
                "unit_status",
                return_value=sweep.UnitStatus(
                    False, False, False, "not-found", None, None, None, "dead"
                ),
            ),
            mock.patch.object(sweep.subprocess, "run", return_value=launched),
        ):
            self.assertEqual(controller._schedule_locked(), 1)

        self.assertEqual(persisted_phases, ["launching", "producer"])
        self.assertNotIn("failed", persisted_phases)

    def test_schedule_serializes_cross_controller_admission(self) -> None:
        controller = object.__new__(sweep.Controller)
        admission = mock.Mock(return_value=contextlib.nullcontext())
        controller.serialized_public_import_admission = admission
        controller._schedule_locked = mock.Mock(return_value=1)

        self.assertEqual(controller.schedule(), 1)
        admission.assert_called_once_with()
        controller._schedule_locked.assert_called_once_with()

    def test_nonzero_producer_launch_with_existing_unit_remains_launching(self) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        lane = sweep.Lane("lane-a", Path("/lane-a"))
        assignment: dict[str, object] = {
            "phase": "failed",
            "first_epoch": 22,
            "last_epoch": 22,
            "adopt_only": False,
        }
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            deploy_dir=Path("/sealed/deploy"),
            manifest=Path("/sealed/deploy/preflight.json"),
            manifest_fingerprint="sha256:" + "a" * 64,
            controller_id="test",
            gcloud_account=sweep.DEFAULT_ACCOUNT,
            gcloud_project=sweep.DEFAULT_PROJECT,
            memory_high_gib=17,
            memory_max_gib=19,
            cpu_quota_percent=725,
            producer_user="sol",
            archive_group="horizon",
        )
        controller.sol_uid = os.getuid()
        controller.producer_home = Path("/home/sol")
        controller.lanes = {lane.name: lane}
        controller.state = {"sequence": 0, "attempts": {}}
        controller.save = mock.Mock()
        controller.receipt_location_for_cohort = mock.Mock(return_value=None)
        launch_result = mock.Mock(
            returncode=1, stderr="manager reply lost", stdout=""
        )
        absent = sweep.UnitStatus(
            False, False, False, "not-found", None, None, None, "dead"
        )
        retained = sweep.UnitStatus(
            True, True, False, "success", None, 1024, 1024, "running"
        )
        with (
            mock.patch.object(sweep, "discover_epoch_claims", return_value=()),
            mock.patch.object(sweep, "verify_deployment"),
            mock.patch.object(
                sweep, "build_producer_command", return_value=["systemd-run"]
            ),
            mock.patch.object(sweep.subprocess, "run", return_value=launch_result),
            mock.patch.object(sweep, "unit_status", side_effect=(absent, retained)),
        ):
            self.assertTrue(
                controller.launch_producer(
                    lane.name, assignment, cohort, source_recovery=False
                )
            )

        self.assertEqual(assignment["phase"], "launching")
        self.assertEqual(assignment["launch_warning"], "manager reply lost")
        self.assertNotIn("failure", assignment)
        self.assertEqual(controller.save.call_count, 2)


class SchedulingTests(unittest.TestCase):
    def test_receipt_appearing_before_launch_is_adopted_without_duplicate(self) -> None:
        fingerprint = "sha256:" + "a" * 64
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            lanes = {
                name: sweep.Lane(name, root / name) for name in ("lane-a", "lane-b")
            }
            for lane in lanes.values():
                lane.output.mkdir(parents=True)
                lane.private.mkdir()
            receipt, _data = make_committed_receipt(
                lanes["lane-b"].output,
                lanes["lane-b"].private,
                list(cohort.epochs),
                fingerprint,
            )
            assignment: dict[str, object] = {
                "phase": "failed",
                "first_epoch": cohort.first_epoch,
                "last_epoch": cohort.last_epoch,
                "adopt_only": False,
            }
            controller = object.__new__(sweep.Controller)
            controller.args = mock.Mock(manifest_fingerprint=fingerprint)
            controller.sol_uid = os.getuid()
            controller.lanes = lanes
            controller.scheduled_bounds = {(22, 22)}
            controller.state = {
                "assignments": {"lane-a": assignment},
                "attempts": {},
                "sequence": 0,
            }
            controller.save = mock.Mock()

            with (
                mock.patch.object(sweep, "discover_epoch_claims", return_value=()),
                mock.patch.object(sweep, "verify_deployment") as verify_deployment,
                mock.patch.object(sweep.subprocess, "run") as run,
            ):
                self.assertFalse(
                    controller.launch_producer(
                        "lane-a", assignment, cohort, source_recovery=False
                    )
                )

            self.assertNotIn("lane-a", controller.state["assignments"])
            self.assertIs(controller.state["assignments"]["lane-b"], assignment)
            self.assertEqual(assignment["phase"], "staged")
            self.assertEqual(assignment["receipt"], str(receipt))
            verify_deployment.assert_not_called()
            run.assert_not_called()

    def test_dead_orphan_marker_is_reported_but_live_lane_claim_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            lane = sweep.Lane("lane-a", root / "lane-a")
            lane.output.mkdir(parents=True)
            lane.private.mkdir()
            (lane.output / sweep.ARCHIVE_BATCH_MARKERS[0]).mkdir(mode=0o700)
            controller = object.__new__(sweep.Controller)
            controller.sol_uid = os.getuid()
            controller.lanes = {lane.name: lane}
            controller.state = {"assignments": {}}

            with (
                mock.patch.object(sweep, "discover_epoch_claims", return_value=()),
                self.assertRaisesRegex(sweep.SweepError, "unassigned lane"),
            ):
                controller.require_no_orphan_lane_markers()

            claim = sweep.EpochClaim(
                pid=77,
                start_time=99,
                first_epoch=22,
                last_epoch=22,
                output=lane.output,
                executable_argument=Path("/sealed/jetstreamer-node"),
                unit="live.service",
            )
            with mock.patch.object(
                sweep, "discover_epoch_claims", return_value=(claim,)
            ):
                controller.require_no_orphan_lane_markers()

    def test_adopt_only_cohort_is_fallback_work_without_duplicate_claims(self) -> None:
        adopt_only = sweep.Cohort(21, 21, "solana-v1.0.23")
        scheduled = sweep.Cohort(22, 22, "solana-v1.0.23")
        lane = sweep.Lane("lane-a", Path("/lane-a"))
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.sol_uid = os.getuid()
        controller.managed_cohorts = (adopt_only, scheduled)
        controller.scheduled_bounds = {(22, 22)}
        controller.lanes = {lane.name: lane}
        controller.state = {"assignments": {}, "import_owner": None}
        controller.desired_capacity = mock.Mock(return_value=1)
        controller.trusted_public_complete = mock.Mock(return_value=False)
        controller.launch_producer = mock.Mock(return_value=True)
        controller.save = mock.Mock()

        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(sweep, "discover_epoch_claims", return_value=()),
            mock.patch.object(sweep, "public_output_files", return_value=()),
        ):
            self.assertEqual(controller._schedule_locked(), 1)

        launched = controller.launch_producer.call_args.args
        self.assertEqual(launched[2], adopt_only)
        self.assertTrue(launched[1]["adopt_only"])

        live_claim = sweep.EpochClaim(
            pid=77,
            start_time=99,
            first_epoch=21,
            last_epoch=21,
            output=Path("/legacy-lane/output"),
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="legacy.service",
        )
        controller.state = {"assignments": {}, "import_owner": None}
        controller.desired_capacity.return_value = 2
        controller.launch_producer.reset_mock()
        with (
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(
                sweep, "discover_epoch_claims", return_value=(live_claim,)
            ),
            mock.patch.object(sweep, "public_output_files", return_value=()),
        ):
            self.assertEqual(controller._schedule_locked(), 1)

        launched = controller.launch_producer.call_args.args
        self.assertEqual(launched[2], scheduled)
        self.assertFalse(launched[1]["adopt_only"])

    def test_stale_producer_assignments_cannot_inflate_ramp_limit(self) -> None:
        first = sweep.Cohort(22, 22, "solana-v1.0.23")
        second = sweep.Cohort(23, 23, "solana-v1.0.23")
        lanes = {
            "lane-a": sweep.Lane("lane-a", Path("/lane-a")),
            "lane-b": sweep.Lane("lane-b", Path("/lane-b")),
        }
        controller = object.__new__(sweep.Controller)
        controller.args = mock.Mock(
            manifest=Path("/sealed/preflight.json"),
            manifest_fingerprint="sha256:" + "a" * 64,
            target_concurrency=6,
        )
        controller.node = Path("/sealed/jetstreamer-node")
        controller.sol_uid = os.getuid()
        controller.lanes = lanes
        controller.by_bounds = {(22, 22): first, (23, 23): second}
        controller.scheduled_bounds = {(22, 22), (23, 23)}
        controller.state = {
            "ramp_limit": 1,
            "attempts": {"22": 1, "23": 1},
            "assignments": {
                "lane-a": {
                    "phase": "producer",
                    "first_epoch": 22,
                    "last_epoch": 22,
                    "unit": "dead-a.service",
                    "adopt_only": False,
                },
                "lane-b": {
                    "phase": "producer",
                    "first_epoch": 23,
                    "last_epoch": 23,
                    "unit": "dead-b.service",
                    "adopt_only": False,
                },
            },
        }
        controller.save = mock.Mock()

        with mock.patch.object(sweep, "discover_producers", return_value=()):
            controller.adopt_live_producers()

        self.assertEqual(controller.state["ramp_limit"], 1)
        controller.save.assert_not_called()


class RecoveryTests(unittest.TestCase):
    def test_proc_stat_parser_handles_spaces_and_closing_parenthesis(self) -> None:
        tail = ["S"] + [str(index) for index in range(4, 53)]
        # In the tail, starttime field 22 contains decimal 22.
        data = "7 (worker ) with spaces) " + " ".join(tail)
        self.assertEqual(sweep.parse_proc_stat_start_time(data), 22)
        self.assertEqual(sweep.parse_proc_stat_parent_pid(data), 4)

    def test_bounded_qualification_requires_private_verified_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            output = root / "qualification"
            output.mkdir()
            args = [
                "/sealed/jetstreamer-node",
                "154",
                str(output),
                "--verify",
                "--qualification-end-slot=66958784",
                f"--horizon-output={output}/epoch-154.jet",
            ]
            self.assertTrue(sweep.is_bounded_qualification(args, output))
            for invalid in (
                [*args, "--no-verify"],
                [*args, "--root-checkpoint-cohort"],
                [*args, "--recover-staged-cohort-only"],
                [item for item in args if item != "--verify"],
                [item for item in args if not item.startswith("--horizon-output=")],
                [*args, "--qualification-end-slot=66958785"],
                [
                    item
                    if not item.startswith("--horizon-output=")
                    else f"--horizon-output={root}/shared/epoch-154.jet"
                    for item in args
                ],
            ):
                self.assertFalse(sweep.is_bounded_qualification(invalid, output))

    def test_direct_supervisor_child_is_one_epoch_claim(self) -> None:
        parent = sweep.EpochClaim(
            pid=10,
            start_time=100,
            first_epoch=1,
            last_epoch=1,
            output=Path("/private/output"),
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="epoch-1.service",
        )
        child = sweep.EpochClaim(
            pid=11,
            start_time=101,
            first_epoch=1,
            last_epoch=1,
            output=parent.output,
            executable_argument=parent.executable_argument,
            unit=parent.unit,
            parent_pid=parent.pid,
        )

        self.assertEqual(
            sweep.collapse_supervised_epoch_claims((parent, child)), (parent,)
        )

    def test_orphan_or_cross_unit_child_remains_a_conflicting_claim(self) -> None:
        parent = sweep.EpochClaim(
            pid=10,
            start_time=100,
            first_epoch=1,
            last_epoch=1,
            output=Path("/private/output"),
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="epoch-1.service",
        )
        cross_unit = dataclasses.replace(
            parent,
            pid=11,
            start_time=101,
            unit="other.service",
            parent_pid=parent.pid,
        )
        orphan = dataclasses.replace(
            parent,
            pid=12,
            start_time=102,
            parent_pid=999,
        )

        observed = sweep.collapse_supervised_epoch_claims(
            (parent, cross_unit, orphan)
        )
        self.assertEqual(observed, (parent, cross_unit, orphan))
        with self.assertRaisesRegex(sweep.SweepError, "overlapping epoch"):
            sweep.require_disjoint_epoch_claims(observed)

    def test_private_bounded_qualification_may_overlap_production(self) -> None:
        production = sweep.EpochClaim(
            pid=10,
            start_time=100,
            first_epoch=154,
            last_epoch=154,
            output=Path("/private/production"),
            executable_argument=Path("/sealed/jetstreamer-node"),
            unit="epoch-154.service",
        )
        qualification = dataclasses.replace(
            production,
            pid=11,
            start_time=101,
            output=Path("/private/qualification"),
            unit="epoch-154-qualification.service",
            bounded_qualification=True,
        )

        sweep.require_disjoint_epoch_claims((production, qualification))

        with self.assertRaisesRegex(sweep.SweepError, "overlapping epoch"):
            sweep.require_disjoint_epoch_claims(
                (production, dataclasses.replace(qualification, bounded_qualification=False))
            )
        with self.assertRaisesRegex(sweep.SweepError, "overlapping epoch or output"):
            sweep.require_disjoint_epoch_claims(
                (production, dataclasses.replace(qualification, output=production.output))
            )

    def test_importing_state_reverts_to_staged_after_controller_restart(self) -> None:
        controller = object.__new__(sweep.Controller)
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        controller.by_bounds = {(22, 22): cohort}
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.lanes = {"lane-c": sweep.Lane("lane-c", Path("/lane-c"))}
        controller.sol_uid = 1001
        controller.state = {
            "assignments": {
                "lane-c": {
                    "phase": "importing",
                    "first_epoch": 22,
                    "last_epoch": 22,
                    "receipt": "/receipt",
                }
            }
        }
        controller.save = mock.Mock()

        controller.public_complete = mock.Mock(return_value=False)
        with mock.patch.object(sweep, "unit_status"):
            controller.refresh_assignments()

        self.assertEqual(controller.state["assignments"]["lane-c"]["phase"], "staged")
        controller.save.assert_called_once()

    def test_launching_state_continues_to_track_an_active_unit(self) -> None:
        controller = object.__new__(sweep.Controller)
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        controller.by_bounds = {(22, 22): cohort}
        controller.args = mock.Mock(public_dir=Path("/public"))
        controller.lanes = {"lane-c": sweep.Lane("lane-c", Path("/lane-c"))}
        controller.sol_uid = 1001
        controller.state = {
            "assignments": {
                "lane-c": {
                    "phase": "launching",
                    "first_epoch": 22,
                    "last_epoch": 22,
                    "unit": "cohort.service",
                }
            }
        }
        controller.save = mock.Mock()
        active = sweep.UnitStatus(True, True, False, "success", None, 1, 1)

        controller.public_complete = mock.Mock(return_value=False)
        controller.reconcile_active_assignment = mock.Mock(return_value=True)
        with mock.patch.object(sweep, "unit_status", return_value=active):
            controller.refresh_assignments()

        self.assertEqual(controller.state["assignments"]["lane-c"]["phase"], "launching")
        controller.reconcile_active_assignment.assert_called_once_with(
            "lane-c", controller.state["assignments"]["lane-c"], cohort
        )
        controller.save.assert_not_called()

    def test_missing_collected_producer_with_receipt_stages_normally(self) -> None:
        cohort = sweep.Cohort(22, 22, "solana-v1.0.23")
        lane = sweep.Lane("lane-c", Path("/lane-c"))
        receipt = Path("/lane-c/private/receipt.json")
        assignment = {
            "phase": "producer",
            "first_epoch": 22,
            "last_epoch": 22,
            "unit": "collected.service",
            "source_recovery": True,
            "source_recovery_attempts": 1,
        }
        controller = object.__new__(sweep.Controller)
        controller.by_bounds = {(22, 22): cohort}
        controller.args = mock.Mock(
            public_dir=Path("/public"),
            manifest_fingerprint="sha256:" + "a" * 64,
        )
        controller.lanes = {lane.name: lane}
        controller.sol_uid = os.getuid()
        controller.state = {"assignments": {lane.name: assignment}}
        controller.save = mock.Mock()
        controller.lane_output_identity = mock.Mock(
            return_value=sweep.DirectoryIdentity(1, 2)
        )
        missing = sweep.UnitStatus(
            False, False, False, "not-found", None, None, None, "dead"
        )

        with (
            mock.patch.object(sweep, "unit_status", return_value=missing),
            mock.patch.object(
                sweep, "public_recovery_marker_present", return_value=False
            ),
            mock.patch.object(
                sweep,
                "discover_live_receipts",
                return_value={sweep.Cohort(22, 22, ""): receipt},
            ),
        ):
            controller.refresh_assignments()

        self.assertEqual(assignment["phase"], "staged")
        self.assertEqual(assignment["receipt"], str(receipt))
        self.assertNotIn("source_recovery", assignment)
        self.assertNotIn("source_recovery_attempts", assignment)
        controller.save.assert_called_once()


if __name__ == "__main__":
    unittest.main()
