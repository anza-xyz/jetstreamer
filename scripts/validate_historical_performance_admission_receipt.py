#!/usr/bin/env python3
"""Validate that one sealed admission receipt authorizes an exact manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Sequence


ADMISSION_MANIFEST_SCHEMA = "jetstreamer-historical-performance-admission-v1"
ADMISSION_RECEIPT_SCHEMA = "jetstreamer-historical-performance-admission-receipt-v1"
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ValidationError(RuntimeError):
    """The admission receipt does not authorize the exact sealed manifest."""


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_uid,
        stat.S_IMODE(metadata.st_mode),
    )


def read_safe_json(
    path: Path,
    description: str,
    *,
    required_uid: int = 0,
) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise ValidationError(f"{description} path must be absolute")
    descriptor = -1
    try:
        parent = path.parent.resolve(strict=True)
        parent_metadata = parent.lstat()
        path_metadata = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        payload = json.loads(b"".join(chunks))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read {description} {path}: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        parent != path.parent
        or not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or parent_metadata.st_uid != required_uid
        or stat.S_IMODE(parent_metadata.st_mode) & 0o022
        or not stat.S_ISREG(path_metadata.st_mode)
        or stat.S_ISLNK(path_metadata.st_mode)
        or path_metadata.st_uid != required_uid
        or path_metadata.st_nlink != 1
        or stat.S_IMODE(path_metadata.st_mode) & 0o077
        or not (
            file_identity(path_metadata)
            == file_identity(before)
            == file_identity(after)
            == file_identity(current)
        )
        or not isinstance(payload, dict)
    ):
        raise ValidationError(f"{description} has an unsafe identity: {path}")
    return payload, digest.hexdigest()


def validate(
    receipt_path: Path,
    manifest_path: Path,
    expected_manifest_sha256: str,
    *,
    required_uid: int = 0,
) -> dict[str, object]:
    if SHA256.fullmatch(expected_manifest_sha256) is None:
        raise ValidationError("expected manifest SHA-256 must be lowercase hexadecimal")
    manifest, manifest_sha256 = read_safe_json(
        manifest_path,
        "admission manifest",
        required_uid=required_uid,
    )
    if manifest.get("schema") != ADMISSION_MANIFEST_SCHEMA:
        raise ValidationError("admission manifest has an unsupported schema")
    if manifest_sha256 != expected_manifest_sha256:
        raise ValidationError(
            "admission manifest SHA-256 does not match the sealed controller binding"
        )
    receipt, receipt_sha256 = read_safe_json(
        receipt_path,
        "admission receipt",
        required_uid=required_uid,
    )
    if (
        receipt.get("schema") != ADMISSION_RECEIPT_SCHEMA
        or receipt.get("admitted") is not True
        or receipt.get("manifest") != str(manifest_path)
        or receipt.get("manifest_sha256") != expected_manifest_sha256
        or receipt.get("services_installed") is not False
        or receipt.get("services_started") is not False
        or receipt.get("remote_mutations") is not False
        or receipt.get("r2_mutations") is not False
        or not isinstance(receipt.get("evidence"), dict)
    ):
        raise ValidationError("admission receipt does not authorize the sealed manifest")
    return {
        "schema": "jetstreamer-historical-performance-admission-validation-v1",
        "status": "pass",
        "receipt": str(receipt_path),
        "receipt_sha256": receipt_sha256,
        "manifest": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "remote_mutations": False,
        "r2_mutations": False,
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--expected-manifest-sha256", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise ValidationError("admission validation must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    result = validate(
        args.receipt,
        args.manifest,
        args.expected_manifest_sha256,
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
