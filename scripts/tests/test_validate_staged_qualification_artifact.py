#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "validate_staged_qualification_artifact.py"
SPEC = importlib.util.spec_from_file_location("staging_validator", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class StagedQualificationArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source.jet"
        self.staged = self.root / "staged.jet"
        self.source_manifest = self.root / "source.jet.segment.json"
        self.staged_manifest = self.root / "staged.jet.segment.json"
        self.authorization = self.root / "authorization.json"
        self.receipt = self.root / "receipt.json"
        self.source.write_bytes(b"archive")
        self.staged.write_bytes(b"archive")
        self.archive_sha256 = hashlib.sha256(b"archive").hexdigest()
        manifest = {
            "epoch": 154,
            "output_slot_start": 100,
            "output_slot_count": 3,
            "terminal": {"slot": 102},
            "archive_sha256": self.archive_sha256,
        }
        manifest_bytes = (json.dumps(manifest, sort_keys=True) + "\n").encode()
        self.source_manifest.write_bytes(manifest_bytes)
        self.staged_manifest.write_bytes(manifest_bytes)
        self.manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        for path in (
            self.source,
            self.staged,
            self.source_manifest,
            self.staged_manifest,
        ):
            path.chmod(0o600)
        source_metadata = self.source.stat()
        self.authorization_payload = {
            "schema": "jetstreamer-qualification-staging-authorization-v1",
            "epoch": 154,
            "source": {
                "path": str(self.source),
                "sha256_pre_staging": self.archive_sha256,
                "identity": MODULE.identity(source_metadata),
            },
            "destination": {
                "archive": str(self.staged),
                "manifest": str(self.staged_manifest),
            },
            "authorization": {
                "stage_diagnostic_artifact": True,
                "launch_diagnostic_validation_after_staging_receipt": True,
                "launch_production_replay": False,
                "publish_diagnostic_artifact": False,
                "publish_production_archive": False,
                "canonical_sidecar": False,
                "plugin": False,
                "r2": False,
                "delete_qualification_scratch": False,
            },
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def args(self, **changes: object) -> argparse.Namespace:
        values = {
            "authorization": self.authorization,
            "source": self.source,
            "source_manifest": self.source_manifest,
            "staged": self.staged,
            "staged_manifest": self.staged_manifest,
            "receipt": self.receipt,
            "archive_sha256": self.archive_sha256,
            "manifest_sha256": self.manifest_sha256,
            "epoch": 154,
            "output_start": 100,
            "output_count": 3,
            "terminal": 102,
        }
        values.update(changes)
        return argparse.Namespace(**values)

    def validate(self, **changes: object) -> dict[str, object]:
        captured: dict[str, object] = {}

        def capture(_path: Path, payload: dict[str, object]) -> None:
            captured.update(payload)

        with (
            patch.object(
                MODULE,
                "load_authorization",
                return_value=(self.authorization_payload, "a" * 64),
            ),
            patch.object(MODULE, "publish_json", side_effect=capture),
        ):
            result = MODULE.validate(self.args(**changes))
        self.assertEqual(result, captured)
        return result

    def test_accepts_independent_matching_stage(self) -> None:
        result = self.validate()
        self.assertEqual(result["epoch"], 154)
        self.assertTrue(result["authorization_effect"]["launch_diagnostic_validation"])
        self.assertFalse(result["authorization_effect"]["publish"])

    def test_rejects_staged_archive_digest_mismatch(self) -> None:
        self.staged.write_bytes(b"different")
        self.staged.chmod(0o600)
        with self.assertRaisesRegex(MODULE.StagingValidationError, "archive SHA-256"):
            self.validate()

    def test_rejects_hardlinked_stage(self) -> None:
        self.staged.unlink()
        self.staged.hardlink_to(self.source)
        with self.assertRaisesRegex(MODULE.StagingValidationError, "unsafe identity"):
            self.validate()

    def test_rejects_publication_grant(self) -> None:
        self.authorization_payload["authorization"]["r2"] = True
        with self.assertRaisesRegex(MODULE.StagingValidationError, "does not deny r2"):
            self.validate()

    def test_rejects_manifest_range_mismatch(self) -> None:
        with self.assertRaisesRegex(MODULE.StagingValidationError, "manifest range"):
            self.validate(terminal=103)


if __name__ == "__main__":
    unittest.main()
