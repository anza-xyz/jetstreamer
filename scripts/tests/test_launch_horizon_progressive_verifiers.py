import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "launch_horizon_progressive_verifiers.py"


class LaunchHorizonProgressiveVerifiersTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.archive = self.root / "epoch-214.jet"
        self.archive.write_bytes(b"archive-bytes")
        self.sidecar = self.root / "epoch-214.jet.sha256"
        self.sidecar.write_text(f"{'1' * 64}  epoch-214.jet\n", encoding="ascii")
        self.state = self.root / "systemctl-state.json"
        self.state.write_text(
            json.dumps(
                {
                    "producer.service": self.sample(
                        active="active", sub="running", invocation="a" * 32
                    ),
                    "watch.path": self.sample(
                        active="active", sub="running", invocation="b" * 32
                    ),
                    "full.service": self.sample(),
                    "plugin.service": self.sample(),
                    "upload.service": self.sample(),
                }
            ),
            encoding="utf-8",
        )
        self.systemctl = self.root / "systemctl"
        self.systemctl.write_text(
            textwrap.dedent(
                f"""\
                #!{sys.executable}
                import json
                import pathlib
                import sys

                state_path = pathlib.Path({str(self.state)!r})
                state = json.loads(state_path.read_text())
                if sys.argv[1] == "show":
                    for key, value in state[sys.argv[2]].items():
                        print(f"{{key}}={{value}}")
                    raise SystemExit(0)
                if sys.argv[1:3] == ["start", "--no-block"]:
                    for index, unit in enumerate(sys.argv[3:]):
                        state[unit]["ActiveState"] = "activating"
                        state[unit]["SubState"] = "start"
                        state[unit]["InvocationID"] = str(index + 1) * 32
                    state_path.write_text(json.dumps(state))
                    raise SystemExit(0)
                if sys.argv[1] == "stop":
                    state[sys.argv[2]]["LoadState"] = "not-found"
                    state[sys.argv[2]]["ActiveState"] = "inactive"
                    state[sys.argv[2]]["SubState"] = "dead"
                    state[sys.argv[2]]["InvocationID"] = ""
                    state_path.write_text(json.dumps(state))
                    raise SystemExit(0)
                raise SystemExit(2)
                """
            ),
            encoding="utf-8",
        )
        self.systemctl.chmod(0o755)
        self.intent = self.root / "intent.json"
        self.completion = self.root / "completion.json"

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def sample(active="inactive", sub="dead", invocation=""):
        return {
            "LoadState": "loaded",
            "ActiveState": active,
            "SubState": sub,
            "InvocationID": invocation,
            "NRestarts": "0",
            "Result": "success",
            "ExecMainStatus": "0",
        }

    def command(self):
        return [
            sys.executable,
            str(SCRIPT),
            f"--archive={self.archive}",
            f"--sidecar={self.sidecar}",
            "--producer-unit=producer.service",
            f"--expected-producer-invocation-id={'a' * 32}",
            "--full-unit=full.service",
            "--plugin-unit=plugin.service",
            "--upload-unit=upload.service",
            "--path-unit=watch.path",
            f"--intent-receipt={self.intent}",
            f"--completion-receipt={self.completion}",
            f"--systemctl={self.systemctl}",
            "--start-timeout-seconds=2",
        ]

    def run_launcher(self):
        environment = os.environ.copy()
        environment["INVOCATION_ID"] = "c" * 32
        return subprocess.run(
            self.command(),
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_launches_verifiers_and_uploader_and_writes_durable_receipts(self):
        result = self.run_launcher()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(self.intent.read_text())["producer"]["NRestarts"], 0)
        completion = json.loads(self.completion.read_text())
        self.assertEqual(completion["full_after"]["ActiveState"], "activating")
        self.assertEqual(completion["plugin_after"]["ActiveState"], "activating")
        self.assertEqual(completion["upload_after"]["ActiveState"], "activating")
        state = json.loads(self.state.read_text())
        self.assertEqual(state["watch.path"]["ActiveState"], "inactive")
        self.assertEqual(state["watch.path"]["SubState"], "dead")
        self.assertEqual(stat.S_IMODE(self.intent.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.completion.stat().st_mode), 0o600)

    def test_matching_completion_is_idempotent(self):
        first = self.run_launcher()
        self.assertEqual(first.returncode, 0, first.stderr)
        before = self.completion.read_bytes()
        state = json.loads(self.state.read_text())
        state["producer.service"]["ActiveState"] = "inactive"
        state["producer.service"]["SubState"] = "dead"
        state["producer.service"]["InvocationID"] = "d" * 32
        state["watch.path"]["ActiveState"] = "inactive"
        state["watch.path"]["SubState"] = "dead"
        self.state.write_text(json.dumps(state))
        second = self.run_launcher()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.completion.read_bytes(), before)

    def test_stale_successful_verifier_invocations_are_restarted(self):
        state = json.loads(self.state.read_text())
        state["full.service"]["InvocationID"] = "d" * 32
        state["plugin.service"]["InvocationID"] = "e" * 32
        self.state.write_text(json.dumps(state))
        result = self.run_launcher()
        self.assertEqual(result.returncode, 0, result.stderr)
        completion = json.loads(self.completion.read_text())
        self.assertEqual(completion["full_after"]["InvocationID"], "1" * 32)
        self.assertEqual(completion["plugin_after"]["InvocationID"], "2" * 32)
        self.assertEqual(completion["upload_after"]["InvocationID"], "3" * 32)

    def test_remains_compatible_when_no_uploader_is_requested(self):
        command = [item for item in self.command() if not item.startswith("--upload-unit=")]
        environment = os.environ.copy()
        environment["INVOCATION_ID"] = "c" * 32
        result = subprocess.run(
            command, env=environment, text=True, capture_output=True, check=False
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        completion = json.loads(self.completion.read_text())
        self.assertIsNone(completion["upload_after"])
        state = json.loads(self.state.read_text())
        self.assertEqual(state["upload.service"]["InvocationID"], "")

    def test_rejects_wrong_producer_invocation(self):
        command = self.command()
        command[command.index(f"--expected-producer-invocation-id={'a' * 32}")] = (
            f"--expected-producer-invocation-id={'d' * 32}"
        )
        environment = os.environ.copy()
        environment["INVOCATION_ID"] = "c" * 32
        result = subprocess.run(
            command, env=environment, text=True, capture_output=True, check=False
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("producer identity/state mismatch", result.stderr)
        self.assertFalse(self.intent.exists())

    def test_rejects_noncanonical_sidecar(self):
        self.sidecar.write_text(f"{'1' * 64} epoch-214.jet\n", encoding="ascii")
        result = self.run_launcher()
        self.assertEqual(result.returncode, 1)
        self.assertIn("not canonical", result.stderr)
        self.assertFalse(self.intent.exists())

    def test_rejects_symlink_archive(self):
        real = self.root / "real.jet"
        self.archive.rename(real)
        self.archive.symlink_to(real)
        result = self.run_launcher()
        self.assertEqual(result.returncode, 1)
        self.assertIn("not a symlink", result.stderr)


if __name__ == "__main__":
    unittest.main()
