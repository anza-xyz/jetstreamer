#!/usr/bin/env python3
"""Launch a local RocksDB scan from an exact sealed cache receipt."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Sequence


GENERATION = re.compile(r"^gs://.+#[0-9]+$")
INVOCATION = re.compile(r"^[0-9a-f]{32}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
UNIT = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")


class LocalScanLaunchError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def hash_regular_file(
    path: Path,
    *,
    expected_uid: int,
    expected_mode: int | None = None,
    max_bytes: int | None = None,
) -> tuple[str, os.stat_result, bytes]:
    try:
        if path.resolve(strict=True) != path:
            raise LocalScanLaunchError(f"path is not canonical: {path}")
        path_metadata = path.lstat()
    except OSError as error:
        raise LocalScanLaunchError(f"cannot inspect {path}: {error}") from error
    if (
        not stat.S_ISREG(path_metadata.st_mode)
        or path_metadata.st_uid != expected_uid
        or path_metadata.st_nlink != 1
        or path_metadata.st_mode & 0o022
        or (expected_mode is not None and stat.S_IMODE(path_metadata.st_mode) != expected_mode)
        or (max_bytes is not None and path_metadata.st_size > max_bytes)
    ):
        raise LocalScanLaunchError(f"unsafe file identity: {path}")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            observed += len(chunk)
            if max_bytes is not None and observed > max_bytes:
                raise LocalScanLaunchError(f"file exceeds size bound: {path}")
            digest.update(chunk)
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        if not (
            stat.S_ISREG(current.st_mode)
            and file_identity(before) == file_identity(after) == file_identity(current)
        ):
            raise LocalScanLaunchError(f"file changed while reading: {path}")
        return digest.hexdigest(), before, b"".join(chunks)
    finally:
        os.close(descriptor)


def require_empty_directory(path: Path, *, uid: int, label: str) -> None:
    try:
        if path.resolve(strict=True) != path:
            raise LocalScanLaunchError(f"{label} is not canonical: {path}")
        metadata = path.stat()
        entries = list(path.iterdir())
    except OSError as error:
        raise LocalScanLaunchError(f"cannot inspect {label} {path}: {error}") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise LocalScanLaunchError(f"{label} must be owner-only and uid {uid}: {path}")
    if entries:
        raise LocalScanLaunchError(f"{label} must be empty: {path}")


def parse_receipt(
    args: argparse.Namespace,
    *,
    trusted_uid: int = 0,
) -> tuple[dict[str, object], str]:
    receipt_sha256, _metadata, raw = hash_regular_file(
        args.cache_receipt,
        expected_uid=trusted_uid,
        expected_mode=0o600,
        max_bytes=64 * 1024,
    )
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LocalScanLaunchError(f"invalid cache receipt JSON: {error}") from error
    if not isinstance(receipt, dict):
        raise LocalScanLaunchError("cache receipt must contain one JSON object")

    expected = {
        "schema": "jetstreamer-generation-bound-local-cache-v1",
        "source_identity": args.source_identity,
        "source_size": args.expected_size,
        "source_md5_base64": args.expected_md5_base64,
        "source_crc32c_base64": args.expected_crc32c_base64,
        "download_unit": args.download_unit,
        "download_invocation_id": args.expected_download_invocation,
        "destination": str(args.archive_file),
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise LocalScanLaunchError(
                f"cache receipt field {key!r} mismatch: expected {value!r}, "
                f"got {receipt.get(key)!r}"
            )
    archive_sha256 = receipt.get("destination_sha256")
    if not isinstance(archive_sha256, str) or not SHA256.fullmatch(archive_sha256):
        raise LocalScanLaunchError("cache receipt has an invalid destination SHA-256")
    if receipt.get("destination_md5_base64") != args.expected_md5_base64:
        raise LocalScanLaunchError("cache receipt destination MD5 mismatch")
    unit_state = receipt.get("download_unit_state")
    expected_unit_state = {
        "LoadState": "loaded",
        "ActiveState": "inactive",
        "SubState": "dead",
        "Result": "success",
        "MainPID": "0",
        "InvocationID": args.expected_download_invocation,
        "NRestarts": "0",
        "ExecMainStatus": "0",
    }
    if not isinstance(unit_state, dict):
        raise LocalScanLaunchError("cache receipt download unit state is missing")
    for key, value in expected_unit_state.items():
        if unit_state.get(key) != value:
            raise LocalScanLaunchError(
                f"cache receipt download unit state {key!r} mismatch"
            )
    for key in (
        "gcloud_download_checksum_validation_required",
        "publication_authorized",
        "runtime_route_promotion_authorized",
        "remote_mutations",
        "r2_mutations",
    ):
        expected_value = key == "gcloud_download_checksum_validation_required"
        if receipt.get(key) is not expected_value:
            raise LocalScanLaunchError(f"cache receipt policy field {key!r} mismatch")
    return receipt, receipt_sha256


def validate_archive(
    args: argparse.Namespace,
    receipt: dict[str, object],
    *,
    trusted_uid: int = 0,
) -> str:
    try:
        if args.archive_file.resolve(strict=True) != args.archive_file:
            raise LocalScanLaunchError("archive path is not canonical")
        current = args.archive_file.lstat()
    except OSError as error:
        raise LocalScanLaunchError(f"cannot inspect archive: {error}") from error
    identity = receipt.get("destination_identity")
    if not isinstance(identity, dict):
        raise LocalScanLaunchError("cache receipt destination identity is missing")
    expected_identity = {
        "device": current.st_dev,
        "inode": current.st_ino,
        "size": current.st_size,
        "mtime_ns": current.st_mtime_ns,
        "uid": current.st_uid,
        "gid": current.st_gid,
        "mode": stat.S_IMODE(current.st_mode),
        "links": current.st_nlink,
    }
    if identity != expected_identity:
        raise LocalScanLaunchError("archive identity does not match the cache receipt")
    if (
        not stat.S_ISREG(current.st_mode)
        or current.st_uid != trusted_uid
        or current.st_gid != trusted_uid
        or current.st_nlink != 1
        or stat.S_IMODE(current.st_mode) != 0o444
        or current.st_size != args.expected_size
    ):
        raise LocalScanLaunchError("sealed archive has an unsafe identity")
    return str(receipt["destination_sha256"])


def validate_configuration(args: argparse.Namespace) -> tuple[list[str], dict[str, str]]:
    if os.geteuid() != 0:
        raise LocalScanLaunchError("local scan launcher must run as root")
    if not GENERATION.fullmatch(args.source_identity):
        raise LocalScanLaunchError("source identity must contain an immutable generation")
    if not UNIT.fullmatch(args.download_unit):
        raise LocalScanLaunchError("invalid download unit")
    if not INVOCATION.fullmatch(args.expected_download_invocation):
        raise LocalScanLaunchError("invalid download invocation ID")
    if not SHA256.fullmatch(args.scanner_sha256) or not SHA256.fullmatch(args.sst_dump_sha256):
        raise LocalScanLaunchError("tool SHA-256 values must be canonical lowercase hex")
    if args.expected_size <= 0 or args.run_uid <= 0 or args.run_gid <= 0:
        raise LocalScanLaunchError("size, run uid, and run gid must be positive")
    try:
        if len(base64.b64decode(args.expected_md5_base64, validate=True)) != 16:
            raise ValueError("wrong MD5 length")
        base64.b64decode(args.expected_crc32c_base64, validate=True)
    except ValueError as error:
        raise LocalScanLaunchError("expected checksums must be canonical base64") from error
    if args.work_directory == args.result_directory:
        raise LocalScanLaunchError("work and result directories must be distinct")

    scanner_sha256, scanner_metadata, _ = hash_regular_file(
        args.scanner, expected_uid=0, expected_mode=0o555
    )
    if scanner_sha256 != args.scanner_sha256 or not scanner_metadata.st_mode & stat.S_IXUSR:
        raise LocalScanLaunchError("scanner executable does not match its pinned digest")
    sst_dump_sha256, sst_dump_metadata, _ = hash_regular_file(
        args.sst_dump, expected_uid=0
    )
    if sst_dump_sha256 != args.sst_dump_sha256 or not sst_dump_metadata.st_mode & stat.S_IXUSR:
        raise LocalScanLaunchError("sst_dump executable does not match its pinned digest")
    receipt, receipt_sha256 = parse_receipt(args)
    archive_sha256 = validate_archive(args, receipt)
    require_empty_directory(args.work_directory, uid=args.run_uid, label="work directory")
    require_empty_directory(args.result_directory, uid=args.run_uid, label="result directory")

    command = [
        str(args.scanner),
        "--archive-file",
        str(args.archive_file),
        "--archive-file-sha256",
        archive_sha256,
        "--archive-file-size",
        str(args.expected_size),
        "--expected-archive-uid",
        "0",
        "--source-identity",
        args.source_identity,
    ]
    for prefix in args.target_prefix:
        command.extend(("--target-prefix", prefix))
    command.extend(("--work-directory", str(args.work_directory)))
    command.extend(("--result-directory", str(args.result_directory)))
    command.extend(("--max-sst-bytes", str(args.max_sst_bytes)))
    command.extend(("--max-retained-bytes", str(args.max_retained_bytes)))
    command.extend(("--max-matches", str(args.max_matches)))
    command.extend(("--progress-every-ssts", str(args.progress_every_ssts)))
    command.extend(("--sst-dump", str(args.sst_dump)))
    for family in args.candidate_column_family:
        command.extend(("--candidate-column-family", family))
    evidence = {
        "cache_receipt": str(args.cache_receipt),
        "cache_receipt_sha256": receipt_sha256,
        "archive_file": str(args.archive_file),
        "archive_sha256": archive_sha256,
        "scanner_sha256": scanner_sha256,
        "sst_dump_sha256": sst_dump_sha256,
    }
    return command, evidence


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-receipt", type=absolute_path, required=True)
    parser.add_argument("--archive-file", type=absolute_path, required=True)
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--expected-size", type=int, required=True)
    parser.add_argument("--expected-md5-base64", required=True)
    parser.add_argument("--expected-crc32c-base64", required=True)
    parser.add_argument("--download-unit", required=True)
    parser.add_argument("--expected-download-invocation", required=True)
    parser.add_argument("--scanner", type=absolute_path, required=True)
    parser.add_argument("--scanner-sha256", required=True)
    parser.add_argument("--sst-dump", type=absolute_path, required=True)
    parser.add_argument("--sst-dump-sha256", required=True)
    parser.add_argument("--run-uid", type=int, required=True)
    parser.add_argument("--run-gid", type=int, required=True)
    parser.add_argument("--target-prefix", action="append", required=True)
    parser.add_argument("--candidate-column-family", action="append", default=[])
    parser.add_argument("--work-directory", type=absolute_path, required=True)
    parser.add_argument("--result-directory", type=absolute_path, required=True)
    parser.add_argument("--max-sst-bytes", type=int, default=2 * 1024**3)
    parser.add_argument("--max-retained-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--max-matches", type=int, default=100000)
    parser.add_argument("--progress-every-ssts", type=int, default=100)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    command, evidence = validate_configuration(args)
    print(json.dumps(evidence, sort_keys=True), flush=True)
    os.umask(0o077)
    os.setgroups([])
    os.setgid(args.run_gid)
    os.setuid(args.run_uid)
    environment = {"LANG": "C.UTF-8", "PATH": "/usr/bin:/bin"}
    os.execve(command[0], command, environment)
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LocalScanLaunchError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
