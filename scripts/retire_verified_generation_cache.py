#!/usr/bin/env python3
"""Retire one verified generation-bound cache after its bound consumer succeeds."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any, Callable


CACHE_RECEIPT_SCHEMA = "jetstreamer-generation-bound-local-cache-v1"
SCAN_STATE_SCHEMA = "jetstreamer-streaming-rocksdb-prefix-scan-v1"
RETIREMENT_SCHEMA = "jetstreamer-verified-generation-cache-retirement-v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UNIT_NAME_RE = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")


class RetirementError(RuntimeError):
    """A fail-closed cache-retirement error."""


@dataclass(frozen=True)
class UnitState:
    unit: str
    load_state: str
    active_state: str
    sub_state: str
    result: str
    main_pid: int
    invocation_id: str
    restarts: int
    exec_main_status: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            fields[name] = value
    return fields


def sample_unit(unit: str) -> UnitState:
    if UNIT_NAME_RE.fullmatch(unit) is None:
        raise RetirementError(f"invalid systemd service name: {unit}")
    result = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts,ExecMainStatus",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RetirementError(
            f"cannot inspect {unit}: {result.stderr.strip() or result.stdout.strip()}"
        )
    fields = parse_systemctl_show(result.stdout)
    return UnitState(
        unit=unit,
        load_state=fields.get("LoadState", "unknown"),
        active_state=fields.get("ActiveState", "unknown"),
        sub_state=fields.get("SubState", "unknown"),
        result=fields.get("Result", "unknown"),
        main_pid=int(fields.get("MainPID", "0") or 0),
        invocation_id=fields.get("InvocationID", ""),
        restarts=int(fields.get("NRestarts", "0") or 0),
        exec_main_status=int(fields.get("ExecMainStatus", "0") or 0),
    )


def parse_unit_binding(value: str) -> tuple[str, str]:
    unit, separator, invocation_id = value.partition("=")
    if (
        not separator
        or UNIT_NAME_RE.fullmatch(unit) is None
        or re.fullmatch(r"[0-9a-f]{32}", invocation_id) is None
    ):
        raise argparse.ArgumentTypeError(
            "unit binding must be NAME.service=32-lowercase-hex-invocation-id"
        )
    return unit, invocation_id


def require_terminal_success(state: UnitState, expected_invocation: str) -> None:
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.sub_state != "dead"
        or state.result != "success"
        or state.main_pid != 0
        or state.invocation_id != expected_invocation
        or state.restarts != 0
        or state.exec_main_status != 0
    ):
        raise RetirementError(
            "unit has not reached the bound clean terminal success: " f"{state}"
        )


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def read_safe_json(
    path: Path,
    *,
    expected_uid: int,
) -> tuple[dict[str, Any], os.stat_result, str]:
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
    return payload, metadata, digest.hexdigest()


def require_cache_file(path: Path, private_root: Path) -> tuple[Path, os.stat_result]:
    if os.geteuid() != 0:
        raise RetirementError("cache retirement must run as root")
    if not path.is_absolute() or not private_root.is_absolute():
        raise RetirementError("cache and private-root paths must be absolute")
    try:
        resolved_root = private_root.resolve(strict=True)
        resolved_parent = path.parent.resolve(strict=True)
        resolved_parent.relative_to(resolved_root)
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except (OSError, ValueError) as exc:
        raise RetirementError(f"unsafe or unavailable cache path {path}: {exc}") from exc
    if resolved != path or not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RetirementError(f"cache path must be an exact regular file: {path}")
    if metadata.st_nlink != 1 or metadata.st_mode & 0o022:
        raise RetirementError(f"cache file has an unsafe mutable identity: {path}")
    return resolved, metadata


def validate_cache_receipt(
    receipt_path: Path,
    cache_path: Path,
    cache_metadata: os.stat_result,
    *,
    expected_uid: int = 0,
) -> tuple[dict[str, Any], str]:
    receipt, receipt_sha = validate_cache_receipt_without_file(
        receipt_path,
        cache_path,
        expected_uid=expected_uid,
    )
    identity = receipt["destination_identity"]
    actual_identity = {
        "device": cache_metadata.st_dev,
        "gid": cache_metadata.st_gid,
        "inode": cache_metadata.st_ino,
        "links": cache_metadata.st_nlink,
        "mode": stat.S_IMODE(cache_metadata.st_mode),
        "mtime_ns": cache_metadata.st_mtime_ns,
        "size": cache_metadata.st_size,
        "uid": cache_metadata.st_uid,
    }
    if identity != actual_identity or receipt.get("source_size") != cache_metadata.st_size:
        raise RetirementError("cache identity changed after verification")
    return receipt, receipt_sha


def validate_cache_receipt_without_file(
    receipt_path: Path,
    cache_path: Path,
    *,
    expected_uid: int = 0,
) -> tuple[dict[str, Any], str]:
    receipt, _, receipt_sha = read_safe_json(receipt_path, expected_uid=expected_uid)
    if (
        receipt.get("schema") != CACHE_RECEIPT_SCHEMA
        or receipt.get("destination") != str(cache_path)
        or receipt.get("publication_authorized") is not False
        or receipt.get("r2_mutations") is not False
    ):
        raise RetirementError("cache receipt does not bind the expected private cache")
    digest = receipt.get("destination_sha256")
    identity = receipt.get("destination_identity")
    if SHA256_RE.fullmatch(digest or "") is None or not isinstance(identity, dict):
        raise RetirementError("cache receipt has invalid digest or identity evidence")
    expected_identity_fields = {
        "device",
        "gid",
        "inode",
        "links",
        "mode",
        "mtime_ns",
        "size",
        "uid",
    }
    if (
        set(identity) != expected_identity_fields
        or any(
            not isinstance(identity.get(field), int) or isinstance(identity.get(field), bool)
            for field in expected_identity_fields
        )
        or identity["links"] != 1
        or identity["mode"] & 0o022
        or receipt.get("source_size") != identity["size"]
    ):
        raise RetirementError("cache receipt has invalid destination identity evidence")
    source = receipt.get("source_identity")
    if not isinstance(source, str) or re.fullmatch(r"gs://.+#[0-9]+", source) is None:
        raise RetirementError("cache receipt lacks a generation-bound source identity")
    download_unit = receipt.get("download_unit")
    download_invocation = receipt.get("download_invocation_id")
    download_state = receipt.get("download_unit_state")
    expected_download_state = {
        "LoadState": "loaded",
        "ActiveState": "inactive",
        "SubState": "dead",
        "Result": "success",
        "MainPID": "0",
        "InvocationID": download_invocation,
        "NRestarts": "0",
        "ExecMainStatus": "0",
    }
    if (
        not isinstance(download_unit, str)
        or UNIT_NAME_RE.fullmatch(download_unit) is None
        or not isinstance(download_invocation, str)
        or re.fullmatch(r"[0-9a-f]{32}", download_invocation) is None
        or not isinstance(download_state, dict)
        or any(download_state.get(key) != value for key, value in expected_download_state.items())
    ):
        raise RetirementError("cache receipt lacks bound successful download evidence")
    return receipt, receipt_sha


def validate_scan_state(
    state_path: Path,
    expected_uid: int,
    cache_path: Path,
    cache_receipt: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    state, _, state_sha = read_safe_json(state_path, expected_uid=expected_uid)
    archive = state.get("archive_file")
    identity = cache_receipt["destination_identity"]
    expected_archive = {
        "path": str(cache_path),
        "sha256": cache_receipt["destination_sha256"],
        "size": identity["size"],
        "uid": identity["uid"],
        "mode": identity["mode"],
        "device": identity["device"],
        "inode": identity["inode"],
        "mtime_ns": identity["mtime_ns"],
    }
    if (
        state.get("schema") != SCAN_STATE_SCHEMA
        or state.get("status") != "complete"
        or state.get("source") != cache_receipt.get("source_identity")
        or archive != expected_archive
        or not isinstance(state.get("completed_unix_seconds"), (int, float))
        or isinstance(state.get("completed_unix_seconds"), bool)
        or not isinstance(state.get("matches"), list)
        or not isinstance(state.get("retained_sst_bytes"), int)
        or isinstance(state.get("retained_sst_bytes"), bool)
        or state.get("retained_sst_bytes", -1) < 0
    ):
        raise RetirementError("scan state is not a complete result for the exact cache")
    return state, state_sha


def encoded_path_reference(content: bytes, needle: bytes) -> bool:
    offset = 0
    path_boundaries = b"/\0\n\r\t '\";|&()<>[]{}"
    while True:
        index = content.find(needle, offset)
        if index < 0:
            return False
        end = index + len(needle)
        if end == len(content) or content[end] in path_boundaries:
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
            if encoded_path_reference(command, needle):
                references.append(f"pid {pid} command line")
            maps = (process / "maps").read_bytes()
            if encoded_path_reference(maps, needle):
                references.append(f"pid {pid} memory maps")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
        for name in ("cwd", "root", "exe"):
            try:
                target = Path(os.readlink(process / name).removesuffix(" (deleted)"))
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            if target == cache_path:
                references.append(f"pid {pid} {name}")
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            descriptors = []
        for descriptor in descriptors:
            try:
                target = Path(os.readlink(descriptor).removesuffix(" (deleted)"))
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            if target == cache_path:
                references.append(f"pid {pid} fd {descriptor.name}")
    return references


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def require_receipt_path(path: Path, *, expected_parent_uid: int = 0) -> Path:
    if not path.is_absolute():
        raise RetirementError("retirement receipt paths must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.lstat()
    except OSError as exc:
        raise RetirementError(f"cannot inspect retirement receipt parent: {exc}") from exc
    if (
        parent != path.parent
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_parent_uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError("retirement receipt parent must be exact root-owned owner-only directory")
    return path


def write_json_noclobber(
    path: Path,
    payload: dict[str, Any],
    *,
    expected_parent_uid: int = 0,
) -> None:
    destination = require_receipt_path(path, expected_parent_uid=expected_parent_uid)
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
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
    directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def common_evidence(
    args: argparse.Namespace,
    sampler: Callable[[str], UnitState] = sample_unit,
    proc_root: Path = Path("/proc"),
    *,
    expected_cache_receipt_uid: int = 0,
    allow_missing_cache: bool = False,
) -> tuple[dict[str, Any], os.stat_result | None]:
    if os.geteuid() != 0:
        raise RetirementError("cache retirement must run as root")
    cache_metadata: os.stat_result | None = None
    if args.cache_file.exists() or args.cache_file.is_symlink():
        cache_path, cache_metadata = require_cache_file(args.cache_file, args.private_root)
    else:
        if not allow_missing_cache:
            raise RetirementError(f"cache file is unavailable: {args.cache_file}")
        if not args.cache_file.is_absolute() or not args.private_root.is_absolute():
            raise RetirementError("cache and private-root paths must be absolute")
        try:
            resolved_root = args.private_root.resolve(strict=True)
            resolved_parent = args.cache_file.parent.resolve(strict=True)
            resolved_parent.relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise RetirementError(
                f"unsafe or unavailable missing cache path {args.cache_file}: {exc}"
            ) from exc
        if resolved_parent != args.cache_file.parent:
            raise RetirementError(f"cache parent changed identity: {args.cache_file.parent}")
        cache_path = args.cache_file

    if cache_metadata is None:
        cache_receipt, cache_receipt_sha = validate_cache_receipt_without_file(
            args.cache_receipt,
            cache_path,
            expected_uid=expected_cache_receipt_uid,
        )
    else:
        cache_receipt, cache_receipt_sha = validate_cache_receipt(
            args.cache_receipt,
            cache_path,
            cache_metadata,
            expected_uid=expected_cache_receipt_uid,
        )
    scan_state, scan_state_sha = validate_scan_state(
        args.scan_state,
        args.expected_scan_state_uid,
        cache_path,
        cache_receipt,
    )
    states = []
    for unit, expected_invocation in args.required_unit:
        state = sampler(unit)
        require_terminal_success(state, expected_invocation)
        states.append(asdict(state))
    references = process_references(cache_path, proc_root)
    if references:
        raise RetirementError("cache still has live process references: " + ", ".join(references))
    return (
        {
            "schema": RETIREMENT_SCHEMA,
            "cache_file": str(cache_path),
            "cache_size_bytes": cache_receipt["destination_identity"]["size"],
            "cache_device": cache_receipt["destination_identity"]["device"],
            "cache_inode": cache_receipt["destination_identity"]["inode"],
            "cache_mtime_ns": cache_receipt["destination_identity"]["mtime_ns"],
            "cache_sha256": cache_receipt["destination_sha256"],
            "source_identity": cache_receipt["source_identity"],
            "cache_download_unit": cache_receipt["download_unit"],
            "cache_download_invocation_id": cache_receipt["download_invocation_id"],
            "cache_download_unit_state": cache_receipt["download_unit_state"],
            "cache_receipt": str(args.cache_receipt),
            "cache_receipt_sha256": cache_receipt_sha,
            "scan_state": str(args.scan_state),
            "scan_state_sha256": scan_state_sha,
            "scan_completed_unix_seconds": scan_state["completed_unix_seconds"],
            "scan_matches": len(scan_state.get("matches", [])),
            "scan_retained_sst_bytes": scan_state.get("retained_sst_bytes"),
            "required_units": states,
            "process_references": [],
            "r2_mutations": False,
        },
        cache_metadata,
    )


def validate_retirement_receipt(
    path: Path,
    expected_status: str,
    evidence: dict[str, Any],
    *,
    expected_uid: int = 0,
) -> tuple[dict[str, Any], str]:
    payload, _, receipt_sha = read_safe_json(path, expected_uid=expected_uid)
    if payload.get("status") != expected_status or any(
        payload.get(key) != value for key, value in evidence.items()
    ):
        raise RetirementError(f"retirement receipt does not bind this exact cleanup: {path}")
    recorded = payload.get("recorded_at_utc")
    free_before = payload.get("filesystem_available_bytes_before")
    if not isinstance(recorded, str) or not isinstance(free_before, int) or isinstance(
        free_before, bool
    ):
        raise RetirementError(f"retirement receipt has invalid durable evidence: {path}")
    return payload, receipt_sha


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-file", required=True, type=Path)
    parser.add_argument("--cache-receipt", required=True, type=Path)
    parser.add_argument("--scan-state", required=True, type=Path)
    parser.add_argument("--expected-scan-state-uid", type=int, required=True)
    parser.add_argument(
        "--required-unit",
        action="append",
        type=parse_unit_binding,
        required=True,
        metavar="NAME.service=INVOCATION_ID",
    )
    parser.add_argument("--private-root", required=True, type=Path)
    parser.add_argument("--intent-receipt", required=True, type=Path)
    parser.add_argument("--completion-receipt", required=True, type=Path)
    parser.add_argument("--check-only", action="store_true")
    return parser.parse_args()


def retire_cache(
    args: argparse.Namespace,
    sampler: Callable[[str], UnitState] = sample_unit,
    proc_root: Path = Path("/proc"),
    *,
    expected_cache_receipt_uid: int = 0,
    expected_retirement_receipt_uid: int = 0,
) -> dict[str, Any]:
    if args.expected_scan_state_uid < 0:
        raise RetirementError("expected scan-state uid must not be negative")
    if args.intent_receipt == args.completion_receipt:
        raise RetirementError("intent and completion receipts must be distinct")
    require_receipt_path(
        args.intent_receipt,
        expected_parent_uid=expected_retirement_receipt_uid,
    )
    require_receipt_path(
        args.completion_receipt,
        expected_parent_uid=expected_retirement_receipt_uid,
    )
    intent_exists = args.intent_receipt.exists() or args.intent_receipt.is_symlink()
    completion_exists = (
        args.completion_receipt.exists() or args.completion_receipt.is_symlink()
    )
    if completion_exists and not intent_exists:
        raise RetirementError("completion receipt exists without its deletion intent")

    if args.check_only:
        evidence, _ = common_evidence(
            args,
            sampler,
            proc_root,
            expected_cache_receipt_uid=expected_cache_receipt_uid,
        )
        return {**evidence, "status": "eligible"}

    evidence, first_identity = common_evidence(
        args,
        sampler,
        proc_root,
        expected_cache_receipt_uid=expected_cache_receipt_uid,
        allow_missing_cache=intent_exists,
    )
    if intent_exists:
        intent, intent_sha = validate_retirement_receipt(
            args.intent_receipt,
            "deletion-intent",
            evidence,
            expected_uid=expected_retirement_receipt_uid,
        )
    else:
        if first_identity is None:
            raise RetirementError("cache disappeared before deletion intent")
        intent = {
            **evidence,
            "status": "deletion-intent",
            "recorded_at_utc": utc_now(),
            "filesystem_available_bytes_before": available_bytes(args.cache_file.parent),
        }
        write_json_noclobber(
            args.intent_receipt,
            intent,
            expected_parent_uid=expected_retirement_receipt_uid,
        )
        intent, intent_sha = validate_retirement_receipt(
            args.intent_receipt,
            "deletion-intent",
            evidence,
            expected_uid=expected_retirement_receipt_uid,
        )

    if completion_exists:
        completion, _ = validate_retirement_receipt(
            args.completion_receipt,
            "deleted",
            evidence,
            expected_uid=expected_retirement_receipt_uid,
        )
        if (
            completion.get("intent_receipt") != str(args.intent_receipt)
            or completion.get("intent_receipt_sha256") != intent_sha
            or not isinstance(completion.get("filesystem_available_bytes_after"), int)
            or isinstance(completion.get("filesystem_available_bytes_after"), bool)
        ):
            raise RetirementError("completion receipt does not bind the deletion intent")
        if args.cache_file.exists() or args.cache_file.is_symlink():
            raise RetirementError("completion receipt exists while cache remains")
        return completion

    if args.cache_file.exists() or args.cache_file.is_symlink():
        rechecked, second_identity = common_evidence(
            args,
            sampler,
            proc_root,
            expected_cache_receipt_uid=expected_cache_receipt_uid,
        )
        if first_identity is None:
            # A prior intent cannot authorize a replacement cache at the same path.
            raise RetirementError("cache appeared after an interrupted deletion")
        if evidence != rechecked or second_identity is None or (
            first_identity.st_dev,
            first_identity.st_ino,
            first_identity.st_size,
            first_identity.st_mtime_ns,
        ) != (
            second_identity.st_dev,
            second_identity.st_ino,
            second_identity.st_size,
            second_identity.st_mtime_ns,
        ):
            raise RetirementError("cache evidence changed after deletion intent")
        os.unlink(args.cache_file)
        directory = os.open(
            args.cache_file.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    if args.cache_file.exists() or args.cache_file.is_symlink():
        raise RetirementError("cache file remains after unlink")

    preserved, preserved_identity = common_evidence(
        args,
        sampler,
        proc_root,
        expected_cache_receipt_uid=expected_cache_receipt_uid,
        allow_missing_cache=True,
    )
    if preserved != evidence or preserved_identity is not None:
        raise RetirementError("preserved evidence changed during cache deletion")

    completion = {
        **evidence,
        "status": "deleted",
        "recorded_at_utc": utc_now(),
        "intent_receipt": str(args.intent_receipt),
        "intent_receipt_sha256": intent_sha,
        "filesystem_available_bytes_before": intent["filesystem_available_bytes_before"],
        "filesystem_available_bytes_after": available_bytes(args.cache_file.parent),
    }
    write_json_noclobber(
        args.completion_receipt,
        completion,
        expected_parent_uid=expected_retirement_receipt_uid,
    )
    return completion


def main() -> int:
    args = parse_args()
    try:
        result = retire_cache(args)
        print(json.dumps(result, sort_keys=True))
        return 0
    except RetirementError as exc:
        print(f"cache retirement failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
