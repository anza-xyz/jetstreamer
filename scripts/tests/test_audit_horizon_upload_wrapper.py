import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "audit_horizon_upload_wrapper.py"
SPEC = importlib.util.spec_from_file_location("audit_horizon_upload_wrapper", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AuditHorizonUploadWrapperTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.pipeline = self.root / "pipeline"
        self.pipeline.write_bytes(b"pipeline-v1")
        self.pipeline.chmod(0o555)
        self.verifier = self.root / "verifier"
        self.verifier.write_bytes(b"verifier-v1")
        self.verifier.chmod(0o555)
        self.pipeline_sha = hashlib.sha256(self.pipeline.read_bytes()).hexdigest()
        self.verifier_sha = hashlib.sha256(self.verifier.read_bytes()).hexdigest()

    def tearDown(self):
        self.tempdir.cleanup()

    def write_wrapper(self, body):
        wrapper = self.root / "upload.zsh"
        wrapper.write_text("#!/usr/bin/zsh\n" + body, encoding="utf-8")
        wrapper.chmod(0o555)
        return wrapper

    def run_audit(self, wrapper, *bindings, receipt_states=()):
        command = [sys.executable, str(SCRIPT), str(wrapper)]
        for binding in bindings:
            command.extend(["--binding", binding])
        for receipt_state in receipt_states:
            command.extend(["--receipt-state-directory", receipt_state])
        return subprocess.run(command, text=True, capture_output=True, check=False)

    def test_accepts_exact_deployed_hashes(self):
        wrapper = self.write_wrapper(
            f"  --plugin-pipeline-sha256={self.pipeline_sha} \\\n"
            f"  --full-verifier-sha256={self.verifier_sha}\n"
        )
        result = self.run_audit(
            wrapper,
            f"plugin-pipeline-sha256={self.pipeline}",
            f"full-verifier-sha256={self.verifier}",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt["status"], "pass")
        self.assertEqual(len(receipt["bindings"]), 2)

    def test_accepts_actual_hash_in_transition_allowlist(self):
        old_sha = "0" * 64
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={old_sha},{self.pipeline_sha}\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={self.pipeline}"
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_mistyped_configured_hash(self):
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha[:-1]}0\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={self.pipeline}"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not admit", result.stderr)

    def test_rejects_unaudited_wrapper_hash_option(self):
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha}\n"
            f"--full-verifier-sha256={self.verifier_sha}\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={self.pipeline}"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("binding set does not match", result.stderr)

    def test_rejects_duplicate_wrapper_option(self):
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha}\n"
            f"--plugin-pipeline-sha256={self.pipeline_sha}\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={self.pipeline}"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("wrapper repeats", result.stderr)

    def test_rejects_symlink_target(self):
        target = self.root / "pipeline-link"
        target.symlink_to(self.pipeline)
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha}\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={target}"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("not a symlink", result.stderr)

    def test_accepts_exact_verifier_receipt_children(self):
        full_state = self.root / "full-audit"
        plugin_state = self.root / "plugin-audit"
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha} \\\n"
            f"--full-receipt-directory={full_state / 'receipts'} \\\n"
            f"--plugin-receipt-directory={plugin_state / 'receipts'}\n"
        )
        result = self.run_audit(
            wrapper,
            f"plugin-pipeline-sha256={self.pipeline}",
            receipt_states=(
                f"full-receipt-directory={full_state}",
                f"plugin-receipt-directory={plugin_state}",
            ),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual(len(receipt["receipt_directories"]), 2)

    def test_rejects_receipt_parent_instead_of_receipts_child(self):
        full_state = self.root / "full-audit"
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha} \\\n"
            f"--full-receipt-directory={full_state}\n"
        )
        result = self.run_audit(
            wrapper,
            f"plugin-pipeline-sha256={self.pipeline}",
            receipt_states=(f"full-receipt-directory={full_state}",),
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("verifier receipt child", result.stderr)

    def test_rejects_unbound_receipt_directory(self):
        full_state = self.root / "full-audit"
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha} \\\n"
            f"--full-receipt-directory={full_state / 'receipts'}\n"
        )
        result = self.run_audit(
            wrapper, f"plugin-pipeline-sha256={self.pipeline}"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("receipt state binding set does not match", result.stderr)

    def test_output_is_durable_json_and_no_clobber(self):
        wrapper = self.write_wrapper(
            f"--plugin-pipeline-sha256={self.pipeline_sha}\n"
        )
        output = self.root / "audit.json"
        command = [
            sys.executable,
            str(SCRIPT),
            str(wrapper),
            "--binding",
            f"plugin-pipeline-sha256={self.pipeline}",
            "--output",
            str(output),
        ]
        first = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(json.loads(output.read_text())["status"], "pass")
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)

        second = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(second.returncode, 1)
        self.assertIn("refusing to overwrite", second.stderr)


if __name__ == "__main__":
    unittest.main()
