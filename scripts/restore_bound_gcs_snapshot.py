#!/usr/bin/env python3
"""Restore one generation-pinned GCS snapshot with durable local evidence."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Sequence


RECEIPT_SCHEMA = "jetstreamer-bound-gcs-snapshot-restore-v1"
ALLOWED_BUCKET = "mainnet-beta-ledger-us-ny5"
VERSIONED_URI = re.compile(
    rf"^gs://{ALLOWED_BUCKET}/(?P<object>[^#]+)#(?P<generation>[1-9][0-9]*)$"
)
SNAPSHOT_NAME = re.compile(
    r"^snapshot-(?P<slot>0|[1-9][0-9]*)-[1-9A-HJ-NP-Za-km-z]+\.tar\.(?:zst|lz4|bz2)$"
)
CLOUD_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@:-]*$")


class RestoreError(RuntimeError):
    """A fail-closed snapshot restore error."""


def canonical_base64(value: str, decoded_size: int, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise RestoreError(f"{name} must be a nonempty base64 string")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise RestoreError(f"{name} is not canonical base64") from error
    if len(decoded) != decoded_size or base64.b64encode(decoded).decode() != value:
        raise RestoreError(f"{name} must be canonical base64 for {decoded_size} bytes")
    return value


def parse_versioned_uri(value: str) -> tuple[str, int, int]:
    match = VERSIONED_URI.fullmatch(value)
    if match is None:
        raise RestoreError(
            f"versioned URI must identify one immutable object generation in gs://{ALLOWED_BUCKET}"
        )
    object_name = match.group("object")
    if "//" in object_name or any(part in ("", ".", "..") for part in object_name.split("/")):
        raise RestoreError("versioned URI has an unsafe object path")
    filename = object_name.rsplit("/", 1)[-1]
    name_match = SNAPSHOT_NAME.fullmatch(filename)
    if name_match is None:
        raise RestoreError("versioned URI does not name a supported snapshot archive")
    anchor = object_name.split("/", 1)[0]
    if not anchor.isdigit():
        raise RestoreError("versioned URI does not have a numeric snapshot anchor")
    slot = int(name_match.group("slot"))
    pieces = object_name.split("/")
    if pieces == [str(slot), filename]:
        pass
    elif len(pieces) == 3 and pieces[1] == "hourly" and int(anchor) <= slot:
        pass
    else:
        raise RestoreError("snapshot object location is not bound to its slot")
    return filename, int(match.group("generation")), slot


def require_executable(path: Path) -> Path:
    if not path.is_absolute():
        raise RestoreError("gcloud executable must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as error:
        raise RestoreError(f"cannot resolve gcloud executable {path}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode) or not os.access(resolved, os.X_OK):
        raise RestoreError(f"gcloud executable is not an executable regular file: {resolved}")
    if resolved.name != "gcloud":
        raise RestoreError(f"gcloud executable must resolve to a file named gcloud: {resolved}")
    return resolved


def require_cloud_identity(value: str, description: str) -> str:
    if CLOUD_IDENTITY.fullmatch(value) is None:
        raise RestoreError(
            f"{description} must be a nonempty single token containing only cloud identity characters"
        )
    return value


def require_private_directory(path: Path, description: str) -> Path:
    if not path.is_absolute():
        raise RestoreError(f"{description} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as error:
        raise RestoreError(f"cannot resolve {description} {path}: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise RestoreError(f"{description} is not a directory: {resolved}")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RestoreError(f"{description} must be owner-only and owned by the effective user: {resolved}")
    return resolved


def require_filesystem(path: Path) -> Path:
    if not path.is_absolute():
        raise RestoreError("filesystem must be an absolute directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise RestoreError(f"cannot resolve filesystem {path}: {error}") from error
    if not resolved.is_dir():
        raise RestoreError(f"filesystem is not a directory: {resolved}")
    return resolved


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def require_free_space(path: Path, minimum_free_bytes: int) -> int:
    free_bytes = available_bytes(path)
    if free_bytes < minimum_free_bytes:
        raise RestoreError(
            f"available bytes {free_bytes} below restore floor {minimum_free_bytes} on {path}"
        )
    return free_bytes


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run_checked(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RestoreError(f"command failed with status {completed.returncode}: {detail}")
    return completed


def local_hashes(gcloud: Path, path: Path) -> tuple[str, str]:
    completed = run_checked(
        [str(gcloud), "storage", "hash", str(path), "--format=json", "--quiet"]
    )
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RestoreError("gcloud storage hash returned invalid JSON") from error
    if not isinstance(report, list) or len(report) != 1 or not isinstance(report[0], dict):
        raise RestoreError("gcloud storage hash did not return exactly one result")
    item = report[0]
    crc32c = item.get("crc32c_hash")
    md5 = item.get("md5_hash")
    if item.get("url") != str(path):
        raise RestoreError("gcloud storage hash reported an unexpected path")
    if (
        item.get("digest_format") != "base64"
        or not isinstance(crc32c, str)
        or not isinstance(md5, str)
    ):
        raise RestoreError("gcloud storage hash omitted required base64 hashes")
    return canonical_base64(crc32c, 4, "actual CRC32C"), canonical_base64(
        md5, 16, "actual MD5"
    )


def download_command(
    gcloud: Path,
    versioned_uri: str,
    destination: Path,
    account: str,
    billing_project: str,
) -> list[str]:
    return [
        str(gcloud),
        "storage",
        "cp",
        f"--account={account}",
        f"--billing-project={billing_project}",
        versioned_uri,
        str(destination),
        "--quiet",
    ]


def describe_command(
    gcloud: Path,
    versioned_uri: str,
    account: str,
    billing_project: str,
) -> list[str]:
    return [
        str(gcloud),
        "storage",
        "objects",
        "describe",
        f"--account={account}",
        f"--billing-project={billing_project}",
        versioned_uri,
        "--format=json",
        "--quiet",
    ]


def describe_remote_snapshot(
    gcloud: Path,
    versioned_uri: str,
    account: str,
    billing_project: str,
    expected_size: int,
    expected_crc32c: str,
    expected_md5: str,
) -> dict[str, Any]:
    match = VERSIONED_URI.fullmatch(versioned_uri)
    if match is None:
        raise RestoreError("cannot describe an invalid versioned URI")
    completed = run_checked(
        describe_command(gcloud, versioned_uri, account, billing_project)
    )
    try:
        metadata = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RestoreError("gcloud object describe returned invalid JSON") from error
    if not isinstance(metadata, dict):
        raise RestoreError("gcloud object describe did not return one metadata object")
    object_name = match.group("object")
    generation_text = match.group("generation")
    if metadata.get("bucket") != ALLOWED_BUCKET or metadata.get("name") != object_name:
        raise RestoreError("remote snapshot metadata identifies an unexpected object")
    if metadata.get("generation") != generation_text:
        raise RestoreError("remote snapshot metadata generation mismatch")
    if metadata.get("size") != str(expected_size):
        raise RestoreError("remote snapshot metadata size mismatch")
    actual_crc32c = canonical_base64(metadata.get("crc32c"), 4, "remote CRC32C")
    actual_md5 = canonical_base64(metadata.get("md5Hash"), 16, "remote MD5")
    if actual_crc32c != expected_crc32c or actual_md5 != expected_md5:
        raise RestoreError("remote snapshot metadata hashes do not match sealed expectations")
    expected_id = f"{ALLOWED_BUCKET}/{object_name}/{generation_text}"
    if metadata.get("id") != expected_id:
        raise RestoreError("remote snapshot metadata id mismatch")
    return {
        "bucket": ALLOWED_BUCKET,
        "name": object_name,
        "generation": int(generation_text),
        "size": expected_size,
        "crc32c_base64": actual_crc32c,
        "md5_base64": actual_md5,
        "id": expected_id,
    }


def validate_file(
    gcloud: Path,
    path: Path,
    expected_size: int,
    expected_crc32c: str,
    expected_md5: str,
) -> dict[str, Any]:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise RestoreError(f"cannot inspect restored snapshot {path}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise RestoreError(f"restored snapshot must be a singly linked regular file: {path}")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RestoreError(f"restored snapshot has unsafe ownership or mode: {path}")
    if metadata.st_size != expected_size:
        raise RestoreError(
            f"restored snapshot size mismatch: expected {expected_size}, got {metadata.st_size}"
        )
    before = (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)
    actual_crc32c, actual_md5 = local_hashes(gcloud, path)
    after_metadata = path.lstat()
    after = (
        after_metadata.st_dev,
        after_metadata.st_ino,
        after_metadata.st_size,
        after_metadata.st_mtime_ns,
    )
    if before != after:
        raise RestoreError("restored snapshot changed while it was hashed")
    if actual_crc32c != expected_crc32c:
        raise RestoreError(
            f"restored snapshot CRC32C mismatch: expected {expected_crc32c}, got {actual_crc32c}"
        )
    if actual_md5 != expected_md5:
        raise RestoreError(
            f"restored snapshot MD5 mismatch: expected {expected_md5}, got {actual_md5}"
        )
    return {
        "device": after_metadata.st_dev,
        "inode": after_metadata.st_ino,
        "size": after_metadata.st_size,
        "mtime_ns": after_metadata.st_mtime_ns,
        "mode": stat.S_IMODE(after_metadata.st_mode),
        "uid": after_metadata.st_uid,
        "crc32c_base64": actual_crc32c,
        "md5_base64": actual_md5,
    }


def fsync_file_and_directory(path: Path) -> None:
    with path.open("rb") as restored:
        os.fsync(restored.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_noclobber(temporary: Path, destination: Path) -> bool:
    try:
        os.link(temporary, destination, follow_symlinks=False)
    except FileExistsError:
        return False
    fsync_file_and_directory(destination)
    return True


def write_receipt_noclobber(path: Path, payload: dict[str, Any]) -> None:
    parent = require_private_directory(path.parent, "receipt directory")
    destination = parent / path.name
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
    finally:
        os.close(descriptor)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gcloud-bin", type=Path, required=True)
    parser.add_argument("--gcloud-account", required=True)
    parser.add_argument("--billing-project", required=True)
    parser.add_argument("--versioned-uri", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--expected-size", type=int, required=True)
    parser.add_argument("--expected-crc32c", required=True)
    parser.add_argument("--expected-md5", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--filesystem", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    return parser


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        filename, generation, slot = parse_versioned_uri(args.versioned_uri)
        args.gcloud_bin = require_executable(args.gcloud_bin)
        args.gcloud_account = require_cloud_identity(
            args.gcloud_account, "gcloud account"
        )
        args.billing_project = require_cloud_identity(
            args.billing_project, "billing project"
        )
        if args.expected_size < 1:
            raise RestoreError("expected size must be positive")
        if args.minimum_free_bytes < 1:
            raise RestoreError("minimum free bytes must be positive")
        args.expected_crc32c = canonical_base64(args.expected_crc32c, 4, "expected CRC32C")
        args.expected_md5 = canonical_base64(args.expected_md5, 16, "expected MD5")
        if not args.destination.is_absolute() or args.destination.name != filename:
            raise RestoreError("destination must be absolute and preserve the bound snapshot filename")
        args.destination_parent = require_private_directory(
            args.destination.parent, "snapshot destination directory"
        )
        args.destination = args.destination_parent / filename
        if not args.receipt.is_absolute() or args.receipt.name in ("", ".", ".."):
            raise RestoreError("receipt must be an absolute file path")
        args.filesystem = require_filesystem(args.filesystem)
        args.generation = generation
        args.snapshot_slot = slot
    except RestoreError as error:
        parser.error(str(error))
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    destination: Path = args.destination
    free_bytes_before = require_free_space(args.filesystem, args.minimum_free_bytes)
    remote_object = describe_remote_snapshot(
        args.gcloud_bin,
        args.versioned_uri,
        args.gcloud_account,
        args.billing_project,
        args.expected_size,
        args.expected_crc32c,
        args.expected_md5,
    )
    if destination.exists() or destination.is_symlink():
        details = validate_file(
            args.gcloud_bin,
            destination,
            args.expected_size,
            args.expected_crc32c,
            args.expected_md5,
        )
        source = "preexisting-verified"
    else:
        with tempfile.TemporaryDirectory(
            prefix=f".{destination.name}.", suffix=".download", dir=args.destination_parent
        ) as temporary_directory:
            temporary = Path(temporary_directory) / "payload"
            run_checked(
                download_command(
                    args.gcloud_bin,
                    args.versioned_uri,
                    temporary,
                    args.gcloud_account,
                    args.billing_project,
                )
            )
            os.chmod(temporary, 0o600, follow_symlinks=False)
            details = validate_file(
                args.gcloud_bin,
                temporary,
                args.expected_size,
                args.expected_crc32c,
                args.expected_md5,
            )
            fsync_file_and_directory(temporary)
            require_free_space(args.filesystem, args.minimum_free_bytes)
            if publish_noclobber(temporary, destination):
                source = "generation-pinned-download"
            else:
                details = validate_file(
                    args.gcloud_bin,
                    destination,
                    args.expected_size,
                    args.expected_crc32c,
                    args.expected_md5,
                )
                source = "concurrent-verified-publication"

    free_bytes_after = available_bytes(args.filesystem)

    payload = {
        "schema": RECEIPT_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "versioned_uri": args.versioned_uri,
        "generation": args.generation,
        "snapshot_slot": args.snapshot_slot,
        "destination": str(destination),
        "file": details,
        "gcloud_bin": str(args.gcloud_bin),
        "gcloud_bin_sha256": sha256_file(args.gcloud_bin),
        "gcloud_account": args.gcloud_account,
        "billing_project": args.billing_project,
        "remote_object": remote_object,
        "script_sha256": sha256_file(Path(__file__).resolve(strict=True)),
        "filesystem": str(args.filesystem),
        "minimum_free_bytes": args.minimum_free_bytes,
        "available_bytes_before": free_bytes_before,
        "available_bytes_after": free_bytes_after,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_receipt_noclobber(args.receipt, payload)
    print(json.dumps(payload, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RestoreError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
