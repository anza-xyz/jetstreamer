import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import retire_historical_replay_scratch as retire


def successful_unit(name: str = "producer.service") -> retire.UnitState:
    return retire.UnitState(name, "loaded", "inactive", "dead", "success", 0, "abc", 0, 1, 0)


class HistoricalReplayScratchRetirementTest(unittest.TestCase):
    @patch("scripts.retire_historical_replay_scratch.os.path.ismount", return_value=False)
    @patch("scripts.retire_historical_replay_scratch.os.geteuid", return_value=0)
    def test_scratch_must_be_exact_and_directly_beneath_qualification_root(
        self, _geteuid: object, _ismount: object
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            qualification = root / "qualification-204"
            qualification.mkdir()
            scratch = qualification / "replay-scratch-v15-retry3"
            scratch.mkdir()
            self.assertEqual(retire.require_scratch(scratch, root), scratch)
            nested = qualification / "nested" / "replay-scratch-bad"
            nested.mkdir(parents=True)
            with self.assertRaisesRegex(retire.RetirementError, "directly beneath"):
                retire.require_scratch(nested, root)

    def test_terminal_success_is_strict(self) -> None:
        retire.require_terminal_success(successful_unit())
        for field, value in (("active_state", "active"), ("result", "exit-code"), ("restarts", 1)):
            state = successful_unit()
            state = retire.UnitState(**{**state.__dict__, field: value})
            with self.subTest(field=field), self.assertRaises(retire.RetirementError):
                retire.require_terminal_success(state)

    @patch("scripts.retire_historical_replay_scratch.subprocess.run")
    def test_samples_complete_systemd_identity(self, run: object) -> None:
        run.return_value = SimpleNamespace(
            returncode=0,
            stderr="",
            stdout=(
                "LoadState=loaded\nActiveState=inactive\nSubState=dead\nResult=success\n"
                "MainPID=0\nInvocationID=abc\nNRestarts=0\nExecMainCode=1\nExecMainStatus=0\n"
            ),
        )
        self.assertEqual(retire.sample_unit("producer.service"), successful_unit("producer.service"))

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
            references = retire.process_references(scratch, root)
            self.assertEqual(len(references), 3)

    def test_delete_exact_tree_removes_only_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = root / "replay-scratch-test"
            scratch.mkdir()
            (scratch / "nested").mkdir()
            (scratch / "nested" / "store").write_bytes(b"data")
            keep = root / "keep"
            keep.write_bytes(b"evidence")
            retire.delete_exact_tree(scratch)
            self.assertFalse(scratch.exists())
            self.assertEqual(keep.read_bytes(), b"evidence")

    def test_validation_receipt_binds_private_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "epoch-204.jet"
            manifest = root / "epoch-204.jet.segment.json"
            receipt = root / "receipt.json"
            archive.write_bytes(b"archive")
            manifest.write_bytes(b"manifest")
            payload = {
                "schema": retire.VALIDATION_SCHEMA,
                "validation": "pass",
                "epoch": 204,
                "archive": str(archive),
                "archive_sha256": "a" * 64,
                "archive_bytes": archive.stat().st_size,
                "archive_uid": archive.stat().st_uid,
                "archive_gid": archive.stat().st_gid,
                "archive_mode": f"{stat.S_IMODE(archive.stat().st_mode):04o}",
                "manifest": str(manifest),
                "manifest_bytes": manifest.stat().st_size,
                "manifest_uid": manifest.stat().st_uid,
                "manifest_gid": manifest.stat().st_gid,
                "manifest_mode": f"{stat.S_IMODE(manifest.stat().st_mode):04o}",
                "canonical_checksum_sidecar": str(archive) + ".sha256",
                "canonical_checksum_sidecar_absent": True,
            }
            receipt.write_text(json.dumps(payload))
            os.chmod(receipt, 0o600)
            self.assertEqual(
                retire.read_validation_receipt(
                    receipt, 204, archive, required_uid=receipt.stat().st_uid
                ),
                payload,
            )

    def test_receipt_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            receipt = root / "receipt.json"
            retire.write_json_noclobber(receipt, {"ok": True})
            self.assertEqual(json.loads(receipt.read_text()), {"ok": True})
            self.assertEqual(stat.S_IMODE(receipt.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                retire.write_json_noclobber(receipt, {})

    @patch("scripts.retire_historical_replay_scratch.os.path.ismount", return_value=False)
    @patch("scripts.retire_historical_replay_scratch.os.geteuid", return_value=0)
    def test_intent_only_interruption_resumes_and_completion_is_idempotent(
        self, _geteuid: object, _ismount: object
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            qualification = root / "qualification-204"
            scratch = qualification / "replay-scratch-v15-retry3"
            output = qualification / "diagnostic-output"
            receipts = root / "scratch-retirement-receipts"
            scratch.mkdir(parents=True)
            output.mkdir()
            receipts.mkdir(mode=0o700)
            (scratch / "store").write_bytes(b"scratch")
            archive = output / "epoch-204.jet"
            archive.write_bytes(b"diagnostic archive")
            validation_receipt = root / "validation.json"
            validation_receipt.write_text("{}")
            intent = receipts / "intent.json"
            completion = receipts / "complete.json"
            producer = "producer.service"
            validator = "validator.service"
            arguments = [
                f"--scratch={scratch}",
                f"--confirm-delete-exact={scratch}",
                f"--producer-unit={producer}",
                f"--validator-unit={validator}",
                f"--validation-receipt={validation_receipt}",
                "--expected-epoch=204",
                f"--expected-archive={archive}",
                f"--intent-receipt={intent}",
                f"--completion-receipt={completion}",
            ]

            def sample(unit: str) -> retire.UnitState:
                return successful_unit(unit)

            with (
                patch.object(retire, "PRIVATE_ROOT", root),
                patch.object(retire, "ROOT_UID", os.getuid()),
                patch.object(retire, "sample_unit", side_effect=sample),
                patch.object(
                    retire,
                    "read_validation_receipt",
                    return_value={"archive_sha256": "a" * 64},
                ),
                patch.object(retire, "process_references", return_value=[]),
            ):
                with patch.object(
                    retire,
                    "delete_exact_tree",
                    side_effect=retire.RetirementError("simulated interruption"),
                ):
                    with self.assertRaisesRegex(retire.RetirementError, "interruption"):
                        retire.main(arguments)
                self.assertTrue(intent.is_file())
                self.assertFalse(completion.exists())
                self.assertTrue(scratch.is_dir())
                with patch("builtins.print"):
                    self.assertEqual(retire.main(arguments), 0)
                    self.assertEqual(retire.main(arguments), 0)
            self.assertFalse(scratch.exists())
            self.assertTrue(completion.is_file())
            completion_payload = json.loads(completion.read_text())
            self.assertEqual(completion_payload["status"], "deleted")
            self.assertEqual(completion_payload["intent_receipt"], str(intent))


if __name__ == "__main__":
    unittest.main()
