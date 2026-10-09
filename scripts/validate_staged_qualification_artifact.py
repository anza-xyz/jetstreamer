#!/usr/bin/env python3
"""Verify a staged qualification archive and durably authorize validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time
from typing import Sequence


SHA256 = re.compile(r"^[0-9a-f]{64}$")


class StagingValidationError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def identity(metadata: os.stat_result) -> dict[str, int | str]:
    return {
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "nlink": metadata.st_nlink,
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
        "length": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
    }


def require_regular(path: Path, label: str) -> os.stat_result:
    if not path.is_absolute():
        raise StagingValidationError(f"{label} path must be absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise StagingValidationError(f"cannot inspect {label} {path}: {error}") from error
    if resolved != path or not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise StagingValidationError(f"{label} has an unsafe identity: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise StagingValidationError(f"{label} is not owner-only: {path}")
    return metadata


def sha256_stable(path: Path, label: str) -> tuple[str, os.stat_result]:
    before = require_regular(path, label)
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    after = require_regular(path, label)
    if identity(before) != identity(after):
        raise StagingValidationError(f"{label} changed while hashing: {path}")
    return digest.hexdigest(), after


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_json(path: Path, payload: dict[str, object]) -> None:
    if path.exists() or path.is_symlink():
        raise StagingValidationError(f"receipt already exists: {path}")
    parent = path.parent.resolve(strict=True)
    parent_metadata = parent.stat()
    if (
        parent != path.parent
        or not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != 0
        or stat.S_IMODE(parent_metadata.st_mode) & 0o077
    ):
        raise StagingValidationError("receipt directory must be root-owned and owner-only")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        os.fchmod(descriptor, 0o600)
        encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
        offset = 0
        while offset < len(encoded):
            offset += os.write(descriptor, encoded[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise StagingValidationError(f"receipt appeared during validation: {path}") from error
        fsync_directory(parent)
        published = True
        temporary.unlink()
        fsync_directory(parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not published and temporary.exists():
            temporary.unlink()
            fsync_directory(parent)


def load_authorization(path: Path) -> tuple[dict[str, object], str]:
    authorization_sha256, metadata = sha256_stable(path, "authorization")
    if metadata.st_uid != 0:
        raise StagingValidationError("authorization must be root-owned")
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise StagingValidationError(f"cannot parse authorization: {error}") from error
    if not isinstance(payload, dict):
        raise StagingValidationError("authorization must be a JSON object")
    return payload, authorization_sha256


def validate(args: argparse.Namespace) -> dict[str, object]:
    for value in (args.archive_sha256, args.manifest_sha256):
        if not SHA256.fullmatch(value):
            raise StagingValidationError("expected SHA-256 is not canonical lowercase hex")

    authorization, authorization_sha256 = load_authorization(args.authorization)
    if authorization.get("schema") != "jetstreamer-qualification-staging-authorization-v1":
        raise StagingValidationError("unexpected authorization schema")
    if authorization.get("epoch") != args.epoch:
        raise StagingValidationError("authorization epoch mismatch")
    grants = authorization.get("authorization")
    if not isinstance(grants, dict):
        raise StagingValidationError("authorization grants are absent")
    if grants.get("stage_diagnostic_artifact") is not True or grants.get(
        "launch_diagnostic_validation_after_staging_receipt"
    ) is not True:
        raise StagingValidationError("diagnostic staging or validation is not authorized")
    for forbidden in (
        "launch_production_replay",
        "publish_diagnostic_artifact",
        "publish_production_archive",
        "canonical_sidecar",
        "plugin",
        "r2",
        "delete_qualification_scratch",
    ):
        if grants.get(forbidden) is not False:
            raise StagingValidationError(f"authorization does not deny {forbidden}")

    authorized_source = authorization.get("source")
    authorized_destination = authorization.get("destination")
    if not isinstance(authorized_source, dict) or not isinstance(authorized_destination, dict):
        raise StagingValidationError("authorization paths are absent")
    if authorized_source.get("path") != str(args.source):
        raise StagingValidationError("source path is not authorized")
    if authorized_source.get("sha256_pre_staging") != args.archive_sha256:
        raise StagingValidationError("pre-staging source digest mismatch")
    if authorized_destination.get("archive") != str(args.staged):
        raise StagingValidationError("staged archive path is not authorized")
    if authorized_destination.get("manifest") != str(args.staged_manifest):
        raise StagingValidationError("staged manifest path is not authorized")

    source_sha256, source_metadata = sha256_stable(args.source, "source archive")
    staged_sha256, staged_metadata = sha256_stable(args.staged, "staged archive")
    source_manifest_sha256, source_manifest_metadata = sha256_stable(
        args.source_manifest, "source manifest"
    )
    staged_manifest_sha256, staged_manifest_metadata = sha256_stable(
        args.staged_manifest, "staged manifest"
    )
    if source_sha256 != args.archive_sha256 or staged_sha256 != args.archive_sha256:
        raise StagingValidationError("archive SHA-256 mismatch")
    if (
        source_manifest_sha256 != args.manifest_sha256
        or staged_manifest_sha256 != args.manifest_sha256
    ):
        raise StagingValidationError("manifest SHA-256 mismatch")
    if (source_metadata.st_dev, source_metadata.st_ino) == (
        staged_metadata.st_dev,
        staged_metadata.st_ino,
    ):
        raise StagingValidationError("staged archive is not an independent inode")
    if identity(source_metadata) != authorized_source.get("identity"):
        raise StagingValidationError("source identity changed since authorization")

    try:
        manifest = json.loads(args.staged_manifest.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise StagingValidationError(f"cannot parse staged manifest: {error}") from error
    if not isinstance(manifest, dict):
        raise StagingValidationError("staged manifest must be a JSON object")
    output_start = manifest.get("output_slot_start")
    output_count = manifest.get("output_slot_count")
    terminal = manifest.get("terminal")
    if not (
        manifest.get("epoch") == args.epoch
        and output_start == args.output_start
        and output_count == args.output_count
        and isinstance(terminal, dict)
        and terminal.get("slot") == args.terminal
        and output_start + output_count - 1 == args.terminal
        and manifest.get("archive_sha256") == args.archive_sha256
    ):
        raise StagingValidationError("staged manifest range or archive binding mismatch")

    payload: dict[str, object] = {
        "schema": "jetstreamer-staged-qualification-validation-v1",
        "created_at_unix": int(time.time()),
        "epoch": args.epoch,
        "range": {
            "output_start": args.output_start,
            "output_count": args.output_count,
            "terminal": args.terminal,
        },
        "authorization": {
            "path": str(args.authorization),
            "sha256": authorization_sha256,
        },
        "source": {
            "path": str(args.source),
            "sha256_post_staging": source_sha256,
            "identity": identity(source_metadata),
        },
        "source_manifest": {
            "path": str(args.source_manifest),
            "sha256": source_manifest_sha256,
            "identity": identity(source_manifest_metadata),
        },
        "staged": {
            "path": str(args.staged),
            "sha256": staged_sha256,
            "identity": identity(staged_metadata),
        },
        "staged_manifest": {
            "path": str(args.staged_manifest),
            "sha256": staged_manifest_sha256,
            "identity": identity(staged_manifest_metadata),
        },
        "authorization_effect": {
            "launch_diagnostic_validation": True,
            "launch_production_replay": False,
            "publish": False,
            "canonical_sidecar": False,
            "plugin": False,
            "r2": False,
            "delete_source": False,
        },
    }
    publish_json(args.receipt, payload)
    return payload


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorization", type=absolute_path, required=True)
    parser.add_argument("--source", type=absolute_path, required=True)
    parser.add_argument("--source-manifest", type=absolute_path, required=True)
    parser.add_argument("--staged", type=absolute_path, required=True)
    parser.add_argument("--staged-manifest", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--output-start", type=int, required=True)
    parser.add_argument("--output-count", type=int, required=True)
    parser.add_argument("--terminal", type=int, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise StagingValidationError("staging validator must run as root")
    payload = validate(parse_args(sys.argv[1:] if argv is None else argv))
    print(json.dumps(payload, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except StagingValidationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
