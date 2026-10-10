#!/usr/bin/env python3
"""Retire one verified snapshot cache after an immutable package copy is sealed.

The target paths live in a root-owned plan file so they do not appear in the
scanner's parent command line.  The cache is deleted only after the restore
receipt, immutable deployment member, package SHA256SUMS, full hashes, and the
live-process reference scan all agree.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any


PLAN_SCHEMA = "jetstreamer-snapshot-cache-retirement-plan-v1"
RESTORE_SCHEMA = "jetstreamer-bound-gcs-snapshot-restore-v1"
RETIREMENT_SCHEMA = "jetstreamer-verified-snapshot-cache-retirement-v1"
CAPABILITY_SCHEMA = "jetstreamer-private-node-manifest-capability-v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class RetirementError(RuntimeError):
    """A fail-closed snapshot-cache retirement error."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def read_safe_json(path: Path, *, expected_uid: int) -> tuple[dict[str, Any], str]:
    descriptor = -1
    try:
        metadata = path.lstat()
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
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RetirementError(f"cannot read JSON evidence {path}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or not (
            file_identity(metadata)
            == file_identity(before)
            == file_identity(after)
            == file_identity(current)
        )
        or not isinstance(payload, dict)
    ):
        raise RetirementError(f"JSON evidence has an unsafe identity: {path}")
    return payload, digest.hexdigest()


def exact_regular_file(
    path: Path,
    *,
    expected_uid: int,
    owner_only: bool,
    immutable: bool,
) -> os.stat_result:
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as exc:
        raise RetirementError(f"cannot inspect file {path}: {exc}") from exc
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not path.is_absolute()
        or resolved != path
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_nlink != 1
        or (owner_only and mode & 0o077)
        or (immutable and mode & 0o222)
    ):
        raise RetirementError(f"file has an unsafe identity: {path}")
    return metadata


def exact_private_root(path: Path, *, expected_uid: int) -> Path:
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as exc:
        raise RetirementError(f"cannot inspect private root {path}: {exc}") from exc
    if (
        not path.is_absolute()
        or resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError(f"private root has an unsafe identity: {path}")
    return resolved


def hash_stable(path: Path, expected: os.stat_result) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.stat(follow_symlinks=False)
    identity = file_identity(expected)
    if identity != file_identity(before) or identity != file_identity(after) or identity != file_identity(current):
        raise RetirementError(f"file changed while hashing: {path}")
    return digest.hexdigest()


def encoded_path_reference(content: bytes, needle: bytes) -> bool:
    offset = 0
    boundaries = b"/\0\n\r\t '\";|&()<>[]{}"
    while True:
        index = content.find(needle, offset)
        if index < 0:
            return False
        end = index + len(needle)
        if end == len(content) or content[end] in boundaries:
            return True
        offset = index + 1


def process_references(cache_path: Path, proc_root: Path = Path("/proc")) -> list[str]:
    needle = os.fsencode(str(cache_path))
    references: list[str] = []
    try:
        processes = list(proc_root.iterdir())
    except OSError as exc:
        raise RetirementError(f"cannot inspect process table {proc_root}: {exc}") from exc
    for process in processes:
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        pid = process.name
        try:
            command = (process / "cmdline").read_bytes()
            maps = (process / "maps").read_bytes()
            if encoded_path_reference(command, needle):
                references.append(f"pid {pid} command line")
            if encoded_path_reference(maps, needle):
                references.append(f"pid {pid} memory maps")
        except (FileNotFoundError, ProcessLookupError):
            pass
        except PermissionError as exc:
            raise RetirementError(f"cannot inspect pid {pid} command/maps: {exc}") from exc
        for name in ("cwd", "root", "exe"):
            try:
                target = Path(os.readlink(process / name).removesuffix(" (deleted)"))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as exc:
                raise RetirementError(f"cannot inspect pid {pid} {name}: {exc}") from exc
            except OSError as exc:
                raise RetirementError(f"cannot inspect pid {pid} {name}: {exc}") from exc
            if target == cache_path:
                references.append(f"pid {pid} {name}")
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            descriptors = []
        except PermissionError as exc:
            raise RetirementError(f"cannot inspect pid {pid} descriptors: {exc}") from exc
        for descriptor in descriptors:
            try:
                target = Path(os.readlink(descriptor).removesuffix(" (deleted)"))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as exc:
                raise RetirementError(f"cannot inspect pid {pid} fd {descriptor.name}: {exc}") from exc
            except OSError as exc:
                raise RetirementError(f"cannot inspect pid {pid} fd {descriptor.name}: {exc}") from exc
            if target == cache_path:
                references.append(f"pid {pid} fd {descriptor.name}")
    return references


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def require_receipt_path(path: Path, *, expected_parent_uid: int = 0) -> Path:
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.lstat()
    except OSError as exc:
        raise RetirementError(f"cannot inspect receipt parent: {exc}") from exc
    if (
        not path.is_absolute()
        or parent != path.parent
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_parent_uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError("receipt parent must be exact root-owned owner-only directory")
    return path


def write_json_noclobber(
    path: Path, payload: dict[str, Any], *, expected_parent_uid: int = 0
) -> str:
    destination = require_receipt_path(
        path, expected_parent_uid=expected_parent_uid
    )
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
    fsync_directory(destination.parent)
    return hashlib.sha256(data).hexdigest()


def plan_path(payload: dict[str, Any], field: str) -> Path:
    value = payload.get(field)
    if not isinstance(value, str):
        raise RetirementError(f"plan field {field} must be a path string")
    path = Path(value)
    if not path.is_absolute():
        raise RetirementError(f"plan field {field} must be absolute")
    return path


def parse_sha256sums(path: Path, filename: str) -> str:
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise RetirementError(f"cannot read package SHA256SUMS: {exc}") from exc
    matches = []
    for line in lines:
        digest, separator, name = line.partition("  ")
        if separator and name == filename and SHA256_RE.fullmatch(digest):
            matches.append(digest)
    if len(matches) != 1:
        raise RetirementError("package SHA256SUMS must bind the deployment member exactly once")
    return matches[0]


def validate_evidence(
    plan_file: Path,
    *,
    proc_root: Path = Path("/proc"),
    expected_plan_uid: int = 0,
    expected_deployment_uid: int = 0,
    expected_receipt_uid: int = 0,
    expected_capability_uid: int = 0,
) -> tuple[dict[str, Any], os.stat_result]:
    if os.geteuid() != 0:
        raise RetirementError("snapshot cache retirement must run as root")
    plan, plan_sha = read_safe_json(plan_file, expected_uid=expected_plan_uid)
    if plan.get("schema") != PLAN_SCHEMA:
        raise RetirementError("retirement plan schema mismatch")
    expected_cache_uid = plan.get("expected_cache_uid")
    if (
        isinstance(expected_cache_uid, bool)
        or not isinstance(expected_cache_uid, int)
        or expected_cache_uid < 0
    ):
        raise RetirementError("plan expected_cache_uid is invalid")
    private_root = exact_private_root(
        plan_path(plan, "private_root"), expected_uid=expected_cache_uid
    )
    cache = plan_path(plan, "cache_file")
    try:
        cache.parent.resolve(strict=True).relative_to(private_root)
    except (OSError, ValueError) as exc:
        raise RetirementError("cache is outside the bound private root") from exc
    cache_metadata = exact_regular_file(
        cache, expected_uid=expected_cache_uid, owner_only=True, immutable=False
    )

    restore_receipt = plan_path(plan, "restore_receipt")
    restore, restore_sha = read_safe_json(
        restore_receipt, expected_uid=expected_cache_uid
    )
    identity = restore.get("file")
    expected_identity = {
        "device": cache_metadata.st_dev,
        "inode": cache_metadata.st_ino,
        "size": cache_metadata.st_size,
        "mtime_ns": cache_metadata.st_mtime_ns,
        "mode": stat.S_IMODE(cache_metadata.st_mode),
        "uid": cache_metadata.st_uid,
    }
    if (
        restore.get("schema") != RESTORE_SCHEMA
        or restore.get("destination") != str(cache)
        or not isinstance(identity, dict)
        or any(identity.get(key) != value for key, value in expected_identity.items())
        or restore.get("remote_mutations") is not False
        or restore.get("r2_mutations") is not False
    ):
        raise RetirementError("restore receipt does not bind the exact cache identity")

    deployment = plan_path(plan, "deployment_file")
    deployment_metadata = exact_regular_file(
        deployment,
        expected_uid=expected_deployment_uid,
        owner_only=False,
        immutable=True,
    )
    package_metadata = deployment.parent.lstat()
    if (
        not stat.S_ISDIR(package_metadata.st_mode)
        or stat.S_ISLNK(package_metadata.st_mode)
        or package_metadata.st_uid != expected_deployment_uid
        or stat.S_IMODE(package_metadata.st_mode) & 0o222
        or deployment.name != cache.name
        or deployment_metadata.st_size != cache_metadata.st_size
        or (deployment_metadata.st_dev, deployment_metadata.st_ino)
        == (cache_metadata.st_dev, cache_metadata.st_ino)
    ):
        raise RetirementError("deployment member is not an independent immutable package copy")
    sums = plan_path(plan, "sha256sums_file")
    if sums.parent != deployment.parent:
        raise RetirementError("SHA256SUMS must be in the deployment directory")
    sums_metadata = exact_regular_file(
        sums,
        expected_uid=expected_deployment_uid,
        owner_only=False,
        immutable=True,
    )
    expected_sha = parse_sha256sums(sums, deployment.name)
    cache_sha = hash_stable(cache, cache_metadata)
    deployment_sha = hash_stable(deployment, deployment_metadata)
    if cache_sha != expected_sha or deployment_sha != expected_sha:
        raise RetirementError("cache and immutable deployment hashes do not match SHA256SUMS")
    sums_sha = hash_stable(sums, sums_metadata)
    capability_receipt = plan_path(plan, "capability_receipt")
    capability, capability_sha = read_safe_json(
        capability_receipt, expected_uid=expected_capability_uid
    )
    capability_deployment = capability.get("deployment")
    cleanup = capability.get("probe_cleanup")
    if (
        capability.get("schema") != CAPABILITY_SCHEMA
        or not isinstance(capability_deployment, dict)
        or capability_deployment.get("path") != str(deployment.parent)
        or capability_deployment.get("sha256sums_sha256") != sums_sha
        or capability_deployment.get("bootstrap_is_direct_singly_linked_member")
        is not True
        or not isinstance(cleanup, dict)
        or cleanup.get("trees_deleted") is not True
        or cleanup.get("parent_directory_fsynced") is not True
        or not str(cleanup.get("process_reference_scan", "")).startswith(
            "zero references"
        )
        or capability.get("publication_authorized") is not False
        or capability.get("remote_mutations") is not False
        or capability.get("r2_mutations") is not False
    ):
        raise RetirementError(
            "capability receipt does not bind the immutable deployment and clean probes"
        )
    references = process_references(cache, proc_root)
    if references:
        raise RetirementError("cache still has live process references: " + ", ".join(references))
    intent = require_receipt_path(
        plan_path(plan, "intent_receipt"),
        expected_parent_uid=expected_receipt_uid,
    )
    completion = require_receipt_path(
        plan_path(plan, "completion_receipt"),
        expected_parent_uid=expected_receipt_uid,
    )
    if intent == completion:
        raise RetirementError("intent and completion receipts must differ")
    return (
        {
            "schema": RETIREMENT_SCHEMA,
            "script_sha256": hashlib.sha256(
                Path(__file__).resolve(strict=True).read_bytes()
            ).hexdigest(),
            "plan": str(plan_file),
            "plan_sha256": plan_sha,
            "cache_file": str(cache),
            "cache_size_bytes": cache_metadata.st_size,
            "cache_allocated_bytes": cache_metadata.st_blocks * 512,
            "cache_device": cache_metadata.st_dev,
            "cache_inode": cache_metadata.st_ino,
            "cache_mtime_ns": cache_metadata.st_mtime_ns,
            "cache_sha256": cache_sha,
            "restore_receipt": str(restore_receipt),
            "restore_receipt_sha256": restore_sha,
            "source_identity": restore.get("versioned_uri"),
            "capability_receipt": str(capability_receipt),
            "capability_receipt_sha256": capability_sha,
            "deployment_file": str(deployment),
            "deployment_sha256": deployment_sha,
            "deployment_device": deployment_metadata.st_dev,
            "deployment_inode": deployment_metadata.st_ino,
            "sha256sums_file": str(sums),
            "sha256sums_sha256": sums_sha,
            "process_references": [],
            "remote_mutations": False,
            "r2_mutations": False,
        },
        cache_metadata,
    )


def retire_cache(
    plan_file: Path,
    *,
    proc_root: Path = Path("/proc"),
    expected_plan_uid: int = 0,
    expected_deployment_uid: int = 0,
    expected_receipt_uid: int = 0,
    expected_capability_uid: int = 0,
    check_only: bool = False,
) -> dict[str, Any]:
    evidence, cache_metadata = validate_evidence(
        plan_file,
        proc_root=proc_root,
        expected_plan_uid=expected_plan_uid,
        expected_deployment_uid=expected_deployment_uid,
        expected_receipt_uid=expected_receipt_uid,
        expected_capability_uid=expected_capability_uid,
    )
    plan, _ = read_safe_json(plan_file, expected_uid=expected_plan_uid)
    intent = plan_path(plan, "intent_receipt")
    completion = plan_path(plan, "completion_receipt")
    if intent.exists() or intent.is_symlink() or completion.exists() or completion.is_symlink():
        raise RetirementError("retirement receipt already exists; manual recovery is required")
    if check_only:
        return {**evidence, "status": "verified-not-deleted"}
    free_before = available_bytes(Path(evidence["cache_file"]).parent)
    intent_payload = {
        **evidence,
        "status": "prepared",
        "recorded_at_utc": utc_now(),
        "filesystem_available_bytes_before": free_before,
    }
    intent_sha = write_json_noclobber(
        intent, intent_payload, expected_parent_uid=expected_receipt_uid
    )
    cache = Path(evidence["cache_file"])
    references = process_references(cache, proc_root)
    current = cache.stat(follow_symlinks=False)
    if references or file_identity(current) != file_identity(cache_metadata):
        raise RetirementError("cache changed or gained a live reference after durable intent")
    cache.unlink()
    fsync_directory(cache.parent)
    free_after = available_bytes(cache.parent)
    completion_payload = {
        **evidence,
        "status": "deleted",
        "recorded_at_utc": utc_now(),
        "intent_receipt": str(intent),
        "intent_receipt_sha256": intent_sha,
        "filesystem_available_bytes_before": free_before,
        "filesystem_available_bytes_after": free_after,
    }
    write_json_noclobber(
        completion,
        completion_payload,
        expected_parent_uid=expected_receipt_uid,
    )
    return completion_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--check-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = retire_cache(args.plan, check_only=args.check_only)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RetirementError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
