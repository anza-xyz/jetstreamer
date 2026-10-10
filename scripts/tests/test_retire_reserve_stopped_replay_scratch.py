import argparse
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import retire_reserve_stopped_replay_scratch as retire


FINGERPRINT = "sha256:" + "a" * 64


def stopped_unit(unit: str, invocation: str = "b" * 32) -> retire.UnitSample:
    return retire.UnitSample(
        unit=unit,
        load_state="not-found",
        active_state="inactive",
        sub_state="dead",
        result="success",
        main_pid=0,
        invocation_id=invocation,
        restarts=0,
    )


class ReserveStoppedScratchRetirementTest(unittest.TestCase):
    def make_run(self, root: Path) -> tuple[argparse.Namespace, Path]:
        destination = root / ("destination-" + "c" * 64)
        run = destination / "work" / "root-cohort-186-186" / "run-123-456"
        scratch = run / "scratch"
        for path in (destination, destination / "work", run.parent, run, scratch):
            path.mkdir(exist_ok=True)
            path.chmod(0o700)
        for name in ("archives", "inputs"):
            directory = run / name
            directory.mkdir()
            directory.chmod(0o700)
        state = {
            "end_epoch": 186,
            "manifest_fingerprint": FINGERPRINT,
            "schema": retire.RUN_STATE_SCHEMA,
            "start_epoch": 186,
            "status": "running-private",
        }
        (run / "cohort-state.json").write_text(json.dumps(state))
        (run / "cohort-state.json").chmod(0o600)
        (run / "archives" / "epoch-186.jet").write_bytes(b"partial")
        (run / "inputs" / "snapshot.tar.zst").write_bytes(b"snapshot")
        args = argparse.Namespace(
            run_directory=run,
            scratch=scratch,
            start_epoch=186,
            end_epoch=186,
            manifest_fingerprint=FINGERPRINT,
        )
        return args, run

    def test_run_validation_binds_cursor_free_state_and_preserved_files(self) -> None:
        with tempfile.TemporaryDirectory(dir=".") as raw:
            args, run = self.make_run(Path(raw).resolve())
            evidence = retire.validate_run(args, allow_missing_scratch=False)
            self.assertEqual(evidence["scratch"], str(args.scratch))
            self.assertEqual(
                [Path(item["path"]).name for item in evidence["archives"]],
                ["epoch-186.jet"],
            )
            self.assertEqual(
                [Path(item["path"]).name for item in evidence["inputs"]],
                ["snapshot.tar.zst"],
            )

            changed = json.loads((run / "cohort-state.json").read_text())
            changed["resume_cursor"] = 123
            (run / "cohort-state.json").write_text(json.dumps(changed))
            with self.assertRaisesRegex(retire.RetirementError, "cursor-free"):
                retire.validate_run(args, allow_missing_scratch=False)

    def test_run_validation_rejects_unexpected_entries_and_symlink_scratch(self) -> None:
        with tempfile.TemporaryDirectory(dir=".") as raw:
            args, run = self.make_run(Path(raw).resolve())
            (run / "unexpected").write_bytes(b"no")
            with self.assertRaisesRegex(retire.RetirementError, "unexpected entries"):
                retire.validate_run(args, allow_missing_scratch=False)
            (run / "unexpected").unlink()
            args.scratch.rmdir()
            args.scratch.symlink_to(run / "inputs", target_is_directory=True)
            with self.assertRaisesRegex(retire.RetirementError, "unsafe identity"):
                retire.validate_run(args, allow_missing_scratch=False)

    def test_external_bootstrap_binds_empty_inputs_and_restart_semantics(self) -> None:
        with tempfile.TemporaryDirectory(dir=".") as raw:
            root = Path(raw).resolve()
            args, run = self.make_run(root)
            (run / "inputs" / "snapshot.tar.zst").unlink()
            with self.assertRaisesRegex(retire.RetirementError, "preserved directory"):
                retire.validate_run(args, allow_missing_scratch=False)

            package = root / "sealed-package"
            package.mkdir()
            bootstrap = package / "snapshot-123.tar.zst"
            node = package / "jetstreamer-node"
            bootstrap.write_bytes(b"sealed bootstrap")
            node.write_bytes(b"sealed node")
            bootstrap_sha256 = hashlib.sha256(bootstrap.read_bytes()).hexdigest()
            node_sha256 = hashlib.sha256(node.read_bytes()).hexdigest()
            manifest = {
                "schema": "jetstreamer-private-contiguous-deployment-v1",
                "range": [186, 186],
                "manifest_fingerprint": FINGERPRINT,
                "parent_sha256": node_sha256,
                "bootstrap": {
                    "filename": bootstrap.name,
                    "size": bootstrap.stat().st_size,
                    "path": str(bootstrap),
                    "epochs": [186, 186],
                    "sha256": bootstrap_sha256,
                },
            }
            manifest_path = package / "deployment-manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            sums = package / "SHA256SUMS"
            sums.write_text(
                f"{manifest_sha256}  deployment-manifest.json\n"
                f"{node_sha256}  jetstreamer-node\n"
                f"{bootstrap_sha256}  {bootstrap.name}\n"
            )
            for path in (bootstrap, node, manifest_path, sums):
                path.chmod(0o444)
            package.chmod(0o555)
            args.producer_unit = "producer.service"
            args.retained_external_bootstrap = bootstrap
            properties = (
                "ExecStart={ path=/sealed/node ; argv[]=/sealed/node "
                f"--snapshot-archive={bootstrap} ; }}\n"
                "Environment=JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true\n"
            )
            try:
                with mock.patch.object(
                    retire.subprocess, "check_output", return_value=properties
                ):
                    external = retire.validate_retained_external_bootstrap(
                        args, expected_uid=os.getuid()
                    )
                evidence = retire.validate_run(
                    args,
                    allow_missing_scratch=False,
                    external_bootstrap=external,
                )
                self.assertEqual(evidence["inputs"], [])
                self.assertEqual(
                    evidence["retained_external_bootstrap"]["sha256"],
                    bootstrap_sha256,
                )

                missing_clear = properties.replace(
                    "JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true",
                    "JETSTREAMER_CLEAR_ACCOUNTS_ON_START=false",
                )
                with mock.patch.object(
                    retire.subprocess, "check_output", return_value=missing_clear
                ):
                    with self.assertRaisesRegex(
                        retire.RetirementError, "restart-and-clear"
                    ):
                        retire.validate_retained_external_bootstrap(
                            args, expected_uid=os.getuid()
                        )

                manifest["bootstrap"]["size_bytes"] = (
                    bootstrap.stat().st_size + 1
                )
                manifest_path.chmod(0o600)
                manifest_path.write_text(json.dumps(manifest))
                manifest_path.chmod(0o444)
                with self.assertRaisesRegex(
                    retire.RetirementError, "conflicting size aliases"
                ):
                    retire.validate_retained_external_bootstrap(
                        args, expected_uid=os.getuid()
                    )
            finally:
                package.chmod(0o700)

    def test_exact_delete_preserves_run_evidence(self) -> None:
        with tempfile.TemporaryDirectory(dir=".") as raw:
            args, run = self.make_run(Path(raw).resolve())
            (args.scratch / "nested").mkdir()
            (args.scratch / "nested" / "store").write_bytes(b"accounts")
            before_archive = (run / "archives" / "epoch-186.jet").read_bytes()
            before_input = (run / "inputs" / "snapshot.tar.zst").read_bytes()
            retire.delete_exact_tree(args.scratch)
            self.assertFalse(args.scratch.exists())
            self.assertEqual(
                (run / "archives" / "epoch-186.jet").read_bytes(), before_archive
            )
            self.assertEqual(
                (run / "inputs" / "snapshot.tar.zst").read_bytes(), before_input
            )
            evidence = retire.validate_run(args, allow_missing_scratch=True)
            self.assertIsNone(evidence["scratch_identity"])

    def test_process_scan_covers_command_maps_links_and_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proc = Path(raw).resolve()
            scratch = proc / "scratch"
            scratch.mkdir()
            target = scratch / "store"
            target.write_bytes(b"x")
            process = proc / "123"
            process.mkdir()
            (process / "cmdline").write_bytes(os.fsencode(str(scratch)))
            (process / "maps").write_bytes(os.fsencode(str(scratch)))
            (process / "fd").mkdir()
            (process / "fd" / "7").symlink_to(target)
            (process / "cwd").symlink_to(scratch)
            (process / "root").symlink_to(proc)
            (process / "exe").symlink_to(proc / "binary")
            with mock.patch.object(retire, "ancestor_pids", return_value={os.getpid()}):
                self.assertEqual(
                    retire.process_references(scratch, proc),
                    ["pid 123 cmdline", "pid 123 maps", "pid 123 cwd", "pid 123 fd 7"],
                )

    def test_caller_command_line_is_ignored_but_real_references_are_not(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proc = Path(raw).resolve()
            scratch = proc / "scratch"
            scratch.mkdir()
            process = proc / "123"
            process.mkdir()
            (process / "cmdline").write_bytes(os.fsencode(str(scratch)))
            (process / "maps").write_bytes(b"")
            (process / "fd").mkdir()
            for name in ("cwd", "root", "exe"):
                (process / name).symlink_to(proc)
            with mock.patch.object(retire, "ancestor_pids", return_value={123}):
                self.assertEqual(retire.process_references(scratch, proc), [])
            (process / "maps").write_bytes(os.fsencode(str(scratch)))
            with mock.patch.object(retire, "ancestor_pids", return_value={123}):
                self.assertEqual(retire.process_references(scratch, proc), ["pid 123 maps"])

    def test_terminal_unit_check_rejects_live_or_replaced_units(self) -> None:
        expected = "b" * 32
        retire.require_current_stopped(stopped_unit("producer.service"), expected, "producer")
        with self.assertRaisesRegex(retire.RetirementError, "still live"):
            retire.require_current_stopped(
                replace(
                    stopped_unit("producer.service"),
                    active_state="active",
                    main_pid=9,
                ),
                expected,
                "producer",
            )
        with self.assertRaisesRegex(retire.RetirementError, "invocation changed"):
            retire.require_current_stopped(
                replace(stopped_unit("producer.service"), invocation_id="d" * 32),
                expected,
                "producer",
            )

    def test_guard_receipts_bind_live_trip_and_terminal_stop(self) -> None:
        controller = "jetstreamer-epochs182-200-controller.service"
        producer = "jetstreamer-root-sweep-epoch186.service"
        controller_id = "a" * 32
        producer_id = "b" * 32
        args = argparse.Namespace(
            guard_intent=Path("/guard/intent.json"),
            guard_completion=Path("/guard/completion.json"),
            controller_unit=controller,
            controller_invocation_id=controller_id,
            producer_unit=producer,
            producer_invocation_id=producer_id,
        )
        intent = {
            "schema": retire.GUARD_INTENT_SCHEMA,
            "controller_unit": controller,
            "expected_controller_invocation_id": controller_id,
            "producer_unit": producer,
            "expected_producer_invocation_id": producer_id,
            "filesystem": "/home/sol",
            "minimum_free_bytes": 200,
            "available_bytes": 199,
            "reason": "actual available bytes fell below the sealed reserve stop floor",
            "stop_order": ["controller", "producer"],
            "remote_mutations": False,
            "r2_mutations": False,
            "controller_sample": {
                "unit": controller,
                "active_state": "active",
                "main_pid": 10,
                "restarts": 0,
                "invocation_id": controller_id,
            },
            "producer_sample": {
                "unit": producer,
                "active_state": "active",
                "main_pid": 11,
                "restarts": 0,
                "invocation_id": producer_id,
            },
        }
        completion = {
            "schema": retire.GUARD_COMPLETION_SCHEMA,
            "controller_unit": controller,
            "expected_controller_invocation_id": controller_id,
            "producer_unit": producer,
            "expected_producer_invocation_id": producer_id,
            "filesystem": "/home/sol",
            "minimum_free_bytes": 200,
            "intent_receipt": str(args.guard_intent),
            "scratch_deleted": False,
            "remote_mutations": False,
            "r2_mutations": False,
            "controller_final": {
                "unit": controller,
                "active_state": "inactive",
                "main_pid": 0,
                "restarts": 0,
                "result": "success",
                "invocation_id": "",
            },
            "producer_final": {
                "unit": producer,
                "active_state": "inactive",
                "main_pid": 0,
                "restarts": 0,
                "result": "success",
                "invocation_id": producer_id,
            },
        }
        with mock.patch.object(
            retire,
            "require_root_private_file",
            side_effect=[(intent, "1" * 64), (completion, "2" * 64)],
        ):
            evidence = retire.validate_guard_receipts(args)
        self.assertEqual(evidence["trip_available_bytes"], 199)

        changed = json.loads(json.dumps(completion))
        changed["producer_final"]["active_state"] = "active"
        with mock.patch.object(
            retire,
            "require_root_private_file",
            side_effect=[(intent, "1" * 64), (changed, "2" * 64)],
        ):
            with self.assertRaisesRegex(retire.RetirementError, "terminal producer"):
                retire.validate_guard_receipts(args)

        current_intent = json.loads(json.dumps(intent))
        current_intent["reason"] = (
            "one or more sealed adaptive replay guard conditions tripped"
        )
        current_intent["trip_reasons"] = [
            "available bytes 199 below floor 200"
        ]
        with mock.patch.object(
            retire,
            "require_root_private_file",
            side_effect=[(current_intent, "1" * 64), (completion, "2" * 64)],
        ):
            evidence = retire.validate_guard_receipts(args)
        self.assertEqual(evidence["trip_available_bytes"], 199)

        current_intent["trip_reasons"].append(
            "producer worker VMAs 300 above ceiling 200"
        )
        with mock.patch.object(
            retire,
            "require_root_private_file",
            side_effect=[(current_intent, "1" * 64), (completion, "2" * 64)],
        ):
            with self.assertRaisesRegex(retire.RetirementError, "expected trip"):
                retire.validate_guard_receipts(args)


if __name__ == "__main__":
    unittest.main()
