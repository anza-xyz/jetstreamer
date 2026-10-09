#!/usr/bin/env python3
"""Verify and immutably publish a generation-bound local object cache."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from datetime import datetime, timezone
from typing import Callable, Sequence


GENERATION = re.compile(r"^gs://.+#[0-9]+$")
INVOCATION = re.compile(r"^[0-9a-f]{32}$")
UNIT = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")


class CacheVerificationError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sample_unit(unit: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "-p",
            "LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts,ExecMainStatus,ExecStart",
            "--no-pager",
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise CacheVerificationError(
            f"cannot inspect download unit {unit}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    return dict(line.split("=", 1) for line in completed.stdout.splitlines() if "=" in line)


def require_directory(path: Path, uid: int, label: str) -> None:
    try:
        resolved = path.resolve(strict=True)
        metadata = path.stat()
    except OSError as error:
        raise CacheVerificationError(f"cannot inspect {label} {path}: {error}") from error
    if (
        resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise CacheVerificationError(f"{label} must be canonical, owner-only, and uid {uid}")


def write_receipt(
    path: Path,
    payload: dict[str, object],
    owner_uid: int = 0,
    owner_gid: int = 0,
) -> str:
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, owner_uid, owner_gid)
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)
    return hashlib.sha256(data).hexdigest()


def verify_and_publish(
    args: argparse.Namespace,
    *,
    unit_sampler: Callable[[str], dict[str, str]] = sample_unit,
    final_uid: int = 0,
    final_gid: int = 0,
    receipt_uid: int = 0,
    receipt_gid: int = 0,
) -> dict[str, object]:
    if os.geteuid() != final_uid and final_uid == 0:
        raise CacheVerificationError("cache verifier must run as root")
    if not UNIT.fullmatch(args.download_unit):
        raise CacheVerificationError("invalid download unit name")
    if not INVOCATION.fullmatch(args.expected_download_invocation):
        raise CacheVerificationError("invalid expected download invocation ID")
    if not GENERATION.fullmatch(args.source_identity):
        raise CacheVerificationError("source identity must include an immutable generation")
    if args.expected_size <= 0 or args.expected_stage_uid < 0:
        raise CacheVerificationError("expected size and uid are invalid")
    try:
        decoded_md5 = base64.b64decode(args.expected_md5_base64, validate=True)
        base64.b64decode(args.expected_crc32c_base64, validate=True)
    except ValueError as error:
        raise CacheVerificationError("expected hashes must be canonical base64") from error
    if len(decoded_md5) != 16:
        raise CacheVerificationError("expected MD5 must decode to 16 bytes")
    if args.stage.parent != args.destination.parent:
        raise CacheVerificationError("stage and destination must share one directory")
    require_directory(args.stage.parent, args.expected_stage_uid, "cache directory")
    require_directory(
        args.receipt.parent, args.expected_receipt_parent_uid, "receipt directory"
    )
    if args.destination.exists() or args.destination.is_symlink():
        raise CacheVerificationError(f"destination already exists: {args.destination}")
    if args.receipt.exists() or args.receipt.is_symlink():
        raise CacheVerificationError(f"receipt already exists: {args.receipt}")

    unit = unit_sampler(args.download_unit)
    expected_unit = {
        "LoadState": "loaded",
        "ActiveState": "inactive",
        "SubState": "dead",
        "Result": "success",
        "MainPID": "0",
        "InvocationID": args.expected_download_invocation,
        "NRestarts": "0",
        "ExecMainStatus": "0",
    }
    for key, expected in expected_unit.items():
        if unit.get(key) != expected:
            raise CacheVerificationError(
                f"download unit is not a clean terminal success: {key}={unit.get(key)!r}"
            )

    try:
        if args.stage.resolve(strict=True) != args.stage:
            raise CacheVerificationError("stage path is not canonical")
        stage_path_metadata = args.stage.lstat()
    except OSError as error:
        raise CacheVerificationError(f"cannot inspect stage file: {error}") from error
    if (
        not stat.S_ISREG(stage_path_metadata.st_mode)
        or stage_path_metadata.st_uid != args.expected_stage_uid
        or stage_path_metadata.st_nlink != 1
        or stage_path_metadata.st_mode & 0o022
        or stage_path_metadata.st_size != args.expected_size
    ):
        raise CacheVerificationError("stage file has an unsafe or unexpected identity")

    descriptor = os.open(args.stage, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(descriptor)
        sha256 = hashlib.sha256()
        md5 = hashlib.md5(usedforsecurity=False)
        observed_size = 0
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            observed_size += len(chunk)
            sha256.update(chunk)
            md5.update(chunk)
        after = os.fstat(descriptor)
        current = args.stage.stat(follow_symlinks=False)
        if not (
            stat.S_ISREG(current.st_mode)
            and file_identity(before) == file_identity(after) == file_identity(current)
            and observed_size == args.expected_size
        ):
            raise CacheVerificationError("stage file changed while hashing")
        observed_md5 = base64.b64encode(md5.digest()).decode()
        if observed_md5 != args.expected_md5_base64:
            raise CacheVerificationError(
                f"stage MD5 mismatch: expected {args.expected_md5_base64}, got {observed_md5}"
            )
        observed_sha256 = sha256.hexdigest()
        os.fchown(descriptor, final_uid, final_gid)
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    if args.destination.exists() or args.destination.is_symlink():
        raise CacheVerificationError(f"destination appeared during verification: {args.destination}")
    try:
        os.link(args.stage, args.destination, follow_symlinks=False)
    except FileExistsError as error:
        raise CacheVerificationError(
            f"destination appeared during verification: {args.destination}"
        ) from error
    fsync_directory(args.destination.parent)
    args.stage.unlink()
    fsync_directory(args.destination.parent)
    final = args.destination.stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(final.st_mode)
        or final.st_uid != final_uid
        or final.st_gid != final_gid
        or final.st_nlink != 1
        or stat.S_IMODE(final.st_mode) != 0o444
        or final.st_size != args.expected_size
    ):
        raise CacheVerificationError("published cache has an unsafe identity")

    payload: dict[str, object] = {
        "schema": "jetstreamer-generation-bound-local-cache-v1",
        "recorded_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_identity": args.source_identity,
        "source_size": args.expected_size,
        "source_md5_base64": args.expected_md5_base64,
        "source_crc32c_base64": args.expected_crc32c_base64,
        "download_unit": args.download_unit,
        "download_unit_state": unit,
        "download_invocation_id": args.expected_download_invocation,
        "stage": str(args.stage),
        "destination": str(args.destination),
        "destination_sha256": observed_sha256,
        "destination_md5_base64": observed_md5,
        "destination_identity": {
            "device": final.st_dev,
            "inode": final.st_ino,
            "size": final.st_size,
            "mtime_ns": final.st_mtime_ns,
            "uid": final.st_uid,
            "gid": final.st_gid,
            "mode": stat.S_IMODE(final.st_mode),
            "links": final.st_nlink,
        },
        "gcloud_download_checksum_validation_required": True,
        "publication_authorized": False,
        "runtime_route_promotion_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    receipt_sha256 = write_receipt(
        args.receipt, payload, owner_uid=receipt_uid, owner_gid=receipt_gid
    )
    return {
        "destination": str(args.destination),
        "destination_sha256": observed_sha256,
        "receipt": str(args.receipt),
        "receipt_sha256": receipt_sha256,
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=absolute_path, required=True)
    parser.add_argument("--destination", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--expected-size", type=int, required=True)
    parser.add_argument("--expected-md5-base64", required=True)
    parser.add_argument("--expected-crc32c-base64", required=True)
    parser.add_argument("--download-unit", required=True)
    parser.add_argument("--expected-download-invocation", required=True)
    parser.add_argument("--expected-stage-uid", type=int, default=0)
    parser.add_argument("--expected-receipt-parent-uid", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    result = verify_and_publish(args)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CacheVerificationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
