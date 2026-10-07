import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import call, patch

from scripts import launch_historical_performance_canaries as launch


class HistoricalPerformanceLaunchTest(unittest.TestCase):
    @patch("scripts.launch_historical_performance_canaries.sample_unit")
    @patch("scripts.launch_historical_performance_canaries.run_systemctl")
    @patch("scripts.launch_historical_performance_canaries.fsync_directory")
    @patch("scripts.launch_historical_performance_canaries.install_prepared_unit_noclobber")
    @patch("scripts.launch_historical_performance_canaries.prepare_unit_installation")
    @patch("scripts.launch_historical_performance_canaries.require_unit_inactive")
    @patch("scripts.launch_historical_performance_canaries.require_unit_absent")
    @patch("scripts.launch_historical_performance_canaries.validate_admission")
    def test_launch_installs_reloads_and_starts_one_transaction(
        self,
        validate_admission: object,
        require_absent: object,
        require_inactive: object,
        prepare: object,
        install: object,
        _fsync: object,
        systemctl: object,
        sample: object,
    ) -> None:
        admission = {"manifest_sha256": "a" * 64}
        validate_admission.return_value = (admission, "b" * 64)
        monitor = launch.UnitState(
            "old.timer", "loaded", "inactive", "dead", "success", 0, "old", 0
        )
        require_inactive.return_value = monitor
        prepared = launch.PreparedInstallation(
            Path("/drafts/cohort.service"),
            Path("/etc/systemd/system/cohort.service"),
            "c" * 64,
            b"unit",
        )
        prepare.return_value = prepared
        install.return_value = {"destination": "/etc/systemd/system/cohort.service"}
        service = launch.UnitState(
            "cohort.service", "loaded", "active", "running", "success", 123, "new", 0
        )
        timer = launch.UnitState(
            "guard.timer", "loaded", "active", "waiting", "success", 0, "timer", 0
        )
        sample.side_effect = [service, timer]
        manifest = {
            "admission_receipt": "/admission.json",
            "start_units": ["cohort.service", "guard.timer"],
            "service_units": ["cohort.service"],
            "timer_units": ["guard.timer"],
            "inactive_monitor_unit": "old.timer",
            "installations": [
                {
                    "source": "/drafts/cohort.service",
                    "destination": "/etc/systemd/system/cohort.service",
                    "sha256": "c" * 64,
                }
            ],
        }
        evidence = launch.launch(manifest)
        self.assertEqual(evidence["admission_receipt_sha256"], "b" * 64)
        self.assertEqual(
            systemctl.call_args_list,
            [call(["daemon-reload"]), call(["start", "cohort.service", "guard.timer"])],
        )
        self.assertEqual(require_absent.call_count, 2)
        self.assertEqual(require_inactive.call_count, 2)

    @patch("scripts.launch_historical_performance_canaries.install_prepared_unit_noclobber")
    @patch("scripts.launch_historical_performance_canaries.prepare_unit_installation")
    @patch("scripts.launch_historical_performance_canaries.require_unit_inactive")
    @patch("scripts.launch_historical_performance_canaries.require_unit_absent")
    @patch("scripts.launch_historical_performance_canaries.validate_admission")
    def test_all_unit_drafts_are_prevalidated_before_first_installation(
        self,
        validate_admission: object,
        _require_absent: object,
        require_inactive: object,
        prepare: object,
        install: object,
    ) -> None:
        validate_admission.return_value = ({"manifest_sha256": "a" * 64}, "b" * 64)
        require_inactive.return_value = launch.UnitState(
            "old.timer", "loaded", "inactive", "dead", "success", 0, "old", 0
        )
        first = launch.PreparedInstallation(
            Path("/drafts/one.service"),
            Path("/etc/systemd/system/one.service"),
            "c" * 64,
            b"one",
        )
        prepare.side_effect = [first, launch.LaunchError("invalid second draft")]
        manifest = {
            "admission_receipt": "/admission.json",
            "start_units": ["cohort.service", "guard.timer"],
            "service_units": ["cohort.service"],
            "timer_units": ["guard.timer"],
            "inactive_monitor_unit": "old.timer",
            "installations": [
                {
                    "source": "/drafts/one.service",
                    "destination": "/etc/systemd/system/one.service",
                    "sha256": "c" * 64,
                },
                {
                    "source": "/drafts/two.service",
                    "destination": "/etc/systemd/system/two.service",
                    "sha256": "d" * 64,
                },
            ],
        }
        with self.assertRaisesRegex(launch.LaunchError, "invalid second"):
            launch.launch(manifest)
        install.assert_not_called()

    def test_install_unit_is_noclobber_and_digest_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            drafts = root / "drafts"
            systemd = root / "systemd"
            drafts.mkdir()
            systemd.mkdir()
            source = drafts / "cohort.service"
            source.write_bytes(b"[Service]\nType=oneshot\n")
            os.chmod(source, 0o444)
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            destination = systemd / source.name
            result = launch.install_unit_noclobber(
                source,
                destination,
                digest,
                systemd,
                required_uid=source.stat().st_uid,
            )
            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(result["sha256"], digest)
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o644)
            with self.assertRaises(FileExistsError):
                launch.install_unit_noclobber(
                    source,
                    destination,
                    digest,
                    systemd,
                    required_uid=source.stat().st_uid,
                )

    def test_admission_receipt_must_bind_manifest_and_mutation_state(self) -> None:
        valid = {
            "schema": launch.ADMISSION_SCHEMA,
            "admitted": True,
            "manifest_sha256": "a" * 64,
            "services_installed": False,
            "services_started": False,
            "r2_mutations": False,
        }
        with patch.object(launch, "require_root_file", return_value=(valid, "b" * 64)):
            receipt, digest = launch.validate_admission(
                {
                    "admission_receipt": "/receipt.json",
                    "admission_manifest_sha256": "a" * 64,
                }
            )
            self.assertEqual(receipt, valid)
            self.assertEqual(digest, "b" * 64)
            invalid = {**valid, "services_started": True}
            with patch.object(launch, "require_root_file", return_value=(invalid, "c" * 64)):
                with self.assertRaises(launch.LaunchError):
                    launch.validate_admission(
                        {
                            "admission_receipt": "/receipt.json",
                            "admission_manifest_sha256": "a" * 64,
                        }
                    )

    @patch("scripts.launch_historical_performance_canaries.subprocess.run")
    def test_stop_cohort_is_one_systemd_transaction(self, run: object) -> None:
        run.return_value = SimpleNamespace(returncode=0, stderr="")
        result = launch.stop_cohort(["a.service", "guard.timer"])
        run.assert_called_once_with(
            ["systemctl", "stop", "a.service", "guard.timer"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result["return_code"], 0)

    def test_string_lists_must_be_nonempty_and_unique(self) -> None:
        self.assertEqual(launch.require_string_list({"x": ["a", "b"]}, "x"), ["a", "b"])
        for value in ([], ["a", "a"], [1]):
            with self.subTest(value=value), self.assertRaises(launch.LaunchError):
                launch.require_string_list({"x": value}, "x")

    def test_launch_receipt_is_owner_only_and_noclobber(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            os.chmod(root, 0o700)
            receipt = root / "receipt.json"
            launch.write_json_noclobber(
                receipt, {"ok": True}, required_uid=root.stat().st_uid
            )
            self.assertEqual(json.loads(receipt.read_text()), {"ok": True})
            self.assertEqual(stat.S_IMODE(receipt.stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
