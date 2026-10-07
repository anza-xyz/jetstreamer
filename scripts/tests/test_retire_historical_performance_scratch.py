import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import unittest
from unittest.mock import patch

from scripts import retire_historical_performance_scratch as retire


def successful_unit(name: str, invocation: str = "invocation") -> retire.UnitState:
    return retire.UnitState(
        unit=name,
        load_state="loaded",
        active_state="inactive",
        sub_state="dead",
        result="success",
        main_pid=0,
        invocation_id=invocation,
        restarts=0,
        exec_main_code=1,
        exec_main_status=0,
    )


def sealed_payloads(root: Path, manifest_path: Path, results_path: Path) -> tuple[dict, dict]:
    manifest_variants = []
    evidence_variants = []
    for name in retire.VARIANTS:
        lane = root / name
        unit_name = f"horizon-perf-epoch202@{name}.service"
        canary = lane / "canary-receipt.json"
        scratch = lane / "scratch"
        archive = lane / "output" / "epoch-202-through-87695515.jet"
        manifest_variants.append(
            {
                "name": name,
                "unit": unit_name,
                "canary_receipt": str(canary),
                "scratch": str(scratch),
                "archive": str(archive),
                "wave_metrics": name.startswith("waves"),
            }
        )
        evidence_variants.append(
            {
                "name": name,
                "unit": {
                    "unit": unit_name,
                    "load_state": "loaded",
                    "active_state": "inactive",
                    "sub_state": "dead",
                    "result": "success",
                    "main_pid": 0,
                    "invocation_id": "invocation",
                    "restarts": 0,
                    "exec_main_code": 1,
                    "exec_main_status": 0,
                },
                "canary_receipt": str(canary),
                "canary_receipt_sha256": "a" * 64,
                "scratch": {
                    "path": str(scratch),
                    "physical_bytes": 100,
                    "apparent_bytes": 90,
                    "regular_files": 2,
                    "directories": 3,
                },
                "accounts_state": {"path": str(scratch / "runtime" / "accounts-state")},
                "archive": {"path": str(archive), "size": 10},
            }
        )
    manifest = {
        "schema": retire.MANIFEST_SCHEMA,
        "target_slot": 100,
        "variants": manifest_variants,
        "future_receipt": str(results_path),
        "selection_authorized": False,
        "publication_authorized": False,
        "remote_mutations_authorized": False,
        "r2_mutations_authorized": False,
    }
    manifest_sha256 = hashlib.sha256(
        (json.dumps(manifest, sort_keys=True) + "\n").encode()
    ).hexdigest()
    results = {
        "schema": retire.RESULTS_SCHEMA,
        "manifest": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "evidence": {
            "target_slot": 100,
            "variants": evidence_variants,
            "selection_authorized": False,
        },
        "remote_mutations": False,
        "r2_mutations": False,
    }
    return manifest, results


class HistoricalPerformanceScratchRetirementTest(unittest.TestCase):
    def test_terminal_success_is_strict(self) -> None:
        unit = "horizon-perf-epoch202@singleton-t16.service"
        retire.require_terminal_success(successful_unit(unit), "invocation")
        for field, value in (
            ("active_state", "active"),
            ("result", "exit-code"),
            ("main_pid", 42),
            ("restarts", 1),
            ("invocation_id", "different"),
        ):
            state = successful_unit(unit)
            state = retire.UnitState(**{**state.__dict__, field: value})
            with self.subTest(field=field), self.assertRaises(retire.RetirementError):
                retire.require_terminal_success(state, "invocation")

    @patch("scripts.retire_historical_performance_scratch.os.path.ismount", return_value=False)
    def test_scratch_path_is_one_of_four_exact_real_directories(self, _ismount: object) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            scratch = root / "singleton-t16" / "scratch"
            scratch.mkdir(parents=True)
            outside = root / "outside" / "scratch"
            outside.mkdir(parents=True)
            with patch.object(retire, "PRIVATE_ROOT", root):
                self.assertEqual(retire.require_exact_scratch(scratch), scratch)
                with self.assertRaisesRegex(retire.RetirementError, "admitted set"):
                    retire.require_exact_scratch(outside)

    def test_process_reference_scan_finds_command_maps_and_fd(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = root / "scratch"
            scratch.mkdir()
            referenced = scratch / "store"
            referenced.write_bytes(b"data")
            process = root / "123"
            process.mkdir()
            (process / "cmdline").write_bytes(os.fsencode(str(scratch)))
            (process / "maps").write_bytes(os.fsencode(str(referenced)))
            (process / "fd").mkdir()
            (process / "cwd").symlink_to(root)
            (process / "root").symlink_to(root)
            (process / "exe").symlink_to(root / "binary")
            (process / "fd" / "5").symlink_to(referenced)
            references = retire.process_references([scratch], root)
            self.assertEqual(len(references), 3)

    def test_results_receipt_binds_all_exact_scratch_statistics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            manifest_path = root / "manifest.json"
            results_path = root / "results.json"
            with patch.object(retire, "PRIVATE_ROOT", root):
                manifest, results = sealed_payloads(root, manifest_path, results_path)
                manifest_digest = results["manifest_sha256"]
                bindings = retire.validate_manifest_and_results(
                    manifest_path,
                    manifest,
                    manifest_digest,
                    results_path,
                    results,
                )
                self.assertEqual([item.name for item in bindings], list(retire.VARIANTS))
                self.assertEqual(bindings[0].physical_bytes, 100)
                broken = json.loads(json.dumps(results))
                broken["evidence"]["variants"][0]["scratch"]["path"] += "-wrong"
                with self.assertRaises(retire.RetirementError):
                    retire.validate_manifest_and_results(
                        manifest_path,
                        manifest,
                        manifest_digest,
                        results_path,
                        broken,
                    )

    def test_receipt_write_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            receipt = root / "receipt.json"
            with patch.object(retire, "require_receipt_parent", return_value=receipt):
                retire.write_json_noclobber(receipt, {"ok": True})
                self.assertEqual(json.loads(receipt.read_text()), {"ok": True})
                self.assertEqual(stat.S_IMODE(receipt.stat().st_mode), 0o600)
                with self.assertRaises(FileExistsError):
                    retire.write_json_noclobber(receipt, {"ok": False})

    @patch("scripts.retire_historical_performance_scratch.os.path.ismount", return_value=False)
    @patch("scripts.retire_historical_performance_scratch.os.geteuid", return_value=0)
    def test_resume_from_intent_deletes_only_remaining_exact_trees(
        self, _geteuid: object, _ismount: object
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            receipts = root / "retirement-receipts"
            receipts.mkdir(mode=0o700)
            manifest_path = root / "manifest.json"
            results_path = root / "results.json"
            plan_path = root / "plan.json"
            intent_path = receipts / "intent.json"
            completion_path = receipts / "complete.json"
            keep = root / "keep"
            keep.write_bytes(b"evidence")
            with patch.object(retire, "PRIVATE_ROOT", root):
                manifest, results = sealed_payloads(root, manifest_path, results_path)
                manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
                results_path.write_text(json.dumps(results, sort_keys=True) + "\n")
                bindings = retire.validate_manifest_and_results(
                    manifest_path,
                    manifest,
                    results["manifest_sha256"],
                    results_path,
                    results,
                )
                units = [successful_unit(binding.unit) for binding in bindings]
                for binding in bindings:
                    binding.scratch.mkdir(parents=True)
                    (binding.scratch / "store").write_bytes(b"scratch")
                common = retire.receipt_common(
                    plan_path,
                    "b" * 64,
                    manifest_path,
                    results["manifest_sha256"],
                    results_path,
                    hashlib.sha256(results_path.read_bytes()).hexdigest(),
                    bindings,
                    units,
                )
                intent = {
                    **common,
                    "status": "deletion-intent-fsynced",
                    "observed_at_utc": "now",
                    "available_bytes_before": 1,
                }
                intent_path.write_text(json.dumps(intent, sort_keys=True) + "\n")
                os.chmod(intent_path, 0o600)
                shutil.rmtree(bindings[0].scratch)

                plan = {
                    "schema": retire.PLAN_SCHEMA,
                    "results_manifest": str(manifest_path),
                    "results_manifest_sha256": results["manifest_sha256"],
                    "results_receipt": str(results_path),
                    "intent_receipt": str(intent_path),
                    "completion_receipt": str(completion_path),
                    "retirer": {
                        "path": str(Path(retire.__file__).resolve()),
                        "sha256": retire.sha256_file(Path(retire.__file__).resolve()),
                    },
                    "variants": [
                        {"name": item.name, "scratch": str(item.scratch)}
                        for item in bindings
                    ],
                    "preserve_diagnostic_outputs": True,
                    "publication_authorized": False,
                    "remote_mutations_authorized": False,
                    "r2_mutations_authorized": False,
                }
                plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n")
                os.chmod(plan_path, 0o600)
                plan_digest = hashlib.sha256(plan_path.read_bytes()).hexdigest()
                intent["retirement_plan"] = str(plan_path)
                intent["retirement_plan_sha256"] = plan_digest
                intent_path.write_text(json.dumps(intent, sort_keys=True) + "\n")

                def unprivileged_json(path: Path, _description: str, required_uid: int = 0):
                    del required_uid
                    payload = json.loads(path.read_text())
                    return payload, hashlib.sha256(path.read_bytes()).hexdigest()

                def sampled(unit: str) -> retire.UnitState:
                    return successful_unit(unit)

                with (
                    patch.object(retire, "load_json_file", side_effect=unprivileged_json),
                    patch.object(retire, "sample_unit", side_effect=sampled),
                    patch.object(retire, "process_references", return_value=[]),
                    patch.object(
                        retire, "require_receipt_parent", side_effect=lambda path: path
                    ),
                ):
                    arguments = [
                        f"--retirement-plan={plan_path}",
                        f"--manifest={manifest_path}",
                        f"--results-receipt={results_path}",
                        f"--intent-receipt={intent_path}",
                        f"--completion-receipt={completion_path}",
                    ]
                    self.assertEqual(retire.main(arguments), 0)
                    self.assertEqual(retire.main(arguments), 0)
                self.assertTrue(completion_path.is_file())
                self.assertTrue(all(not item.scratch.exists() for item in bindings))
                self.assertEqual(keep.read_bytes(), b"evidence")


if __name__ == "__main__":
    unittest.main()
