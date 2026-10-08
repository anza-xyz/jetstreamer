#!/usr/bin/env python3
"""Fail-closed admission check for a sealed historical performance cohort."""

from __future__ import annotations

import argparse
import base64
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
from typing import Any, Sequence


MANIFEST_SCHEMA = "jetstreamer-historical-performance-admission-v1"
RECEIPT_SCHEMA = "jetstreamer-historical-performance-admission-receipt-v1"
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@-]+\.(?:service|timer)$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AdmissionError(RuntimeError):
    """One or more admission requirements are not satisfied."""


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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            fields[name] = value
    return fields


def sample_unit(unit: str) -> UnitState:
    if UNIT_NAME.fullmatch(unit) is None:
        raise AdmissionError(f"invalid systemd unit name: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts,ExecMainStatus",
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AdmissionError(
            f"cannot inspect {unit}: {completed.stderr.strip() or completed.stdout.strip()}"
        )
    fields = parse_systemctl_show(completed.stdout)
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


def check_terminal_unit(state: UnitState) -> None:
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.result != "success"
        or state.main_pid != 0
        or state.restarts != 0
        or state.exec_main_status != 0
    ):
        raise AdmissionError(f"required unit has not reached clean terminal success: {state}")


def check_absent_unit(state: UnitState) -> None:
    if state.load_state != "not-found" or state.active_state != "inactive":
        raise AdmissionError(f"unit must remain absent before admission: {state}")


def parse_mode(value: object, context: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"0[0-7]{3}", value) is None:
        raise AdmissionError(f"{context} mode must be four octal digits")
    return int(value, 8)


def require_absolute_path(value: object, context: str) -> Path:
    if not isinstance(value, str):
        raise AdmissionError(f"{context} path must be a string")
    path = Path(value)
    if not path.is_absolute():
        raise AdmissionError(f"{context} path must be absolute: {path}")
    return path


def check_file(
    spec: dict[str, Any], *, allow_unbound_sha256: bool = False
) -> dict[str, Any]:
    path = require_absolute_path(spec.get("path"), "required file")
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise AdmissionError(f"required file unavailable {path}: {error}") from error
    if resolved != path or not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise AdmissionError(f"required file has unsafe identity: {path}")
    expected_mode = parse_mode(spec.get("mode"), str(path))
    identity = (
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )
    expected_identity = (spec.get("uid"), spec.get("gid"), expected_mode)
    if identity != expected_identity:
        raise AdmissionError(
            f"required file identity mismatch for {path}: expected {expected_identity}, got {identity}"
        )
    expected_size = spec.get("size")
    if expected_size is not None and metadata.st_size != expected_size:
        raise AdmissionError(
            f"required file size mismatch for {path}: expected {expected_size}, got {metadata.st_size}"
        )
    expected_sha256 = spec.get("sha256")
    actual_sha256 = sha256_file(path)
    if expected_sha256 is None and allow_unbound_sha256:
        pass
    elif not isinstance(expected_sha256, str) or SHA256.fullmatch(expected_sha256) is None:
        raise AdmissionError(f"required file has invalid expected SHA-256: {path}")
    elif actual_sha256 != expected_sha256:
        raise AdmissionError(
            f"required file SHA-256 mismatch for {path}: expected {expected_sha256}, got {actual_sha256}"
        )
    return {
        "path": str(path),
        "size": metadata.st_size,
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
        "sha256": actual_sha256,
    }


def check_directory(spec: dict[str, Any]) -> dict[str, Any]:
    path = require_absolute_path(spec.get("path"), "required directory")
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise AdmissionError(f"required directory unavailable {path}: {error}") from error
    if resolved != path or not stat.S_ISDIR(metadata.st_mode):
        raise AdmissionError(f"required directory has unsafe identity: {path}")
    expected = (spec.get("uid"), spec.get("gid"), parse_mode(spec.get("mode"), str(path)))
    actual = (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode))
    if actual != expected:
        raise AdmissionError(
            f"required directory identity mismatch for {path}: expected {expected}, got {actual}"
        )
    allowed = spec.get("allowed_entries")
    entries = sorted(item.name for item in path.iterdir())
    if allowed is not None:
        if not isinstance(allowed, list) or not all(isinstance(item, str) for item in allowed):
            raise AdmissionError(f"allowed_entries must be a string list for {path}")
        if entries != sorted(allowed):
            raise AdmissionError(
                f"directory entries mismatch for {path}: expected {sorted(allowed)}, got {entries}"
            )
    return {
        "path": str(path),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
        "entries": entries,
    }


def json_contains(actual: object, expected: object) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and json_contains(actual[key], value)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return actual == expected
    return actual == expected


def check_json_receipt(spec: dict[str, Any]) -> dict[str, Any]:
    file_result = check_file(spec, allow_unbound_sha256=True)
    path = Path(file_result["path"])
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise AdmissionError(f"invalid JSON receipt {path}: {error}") from error
    expected = spec.get("contains")
    if not isinstance(expected, dict) or not json_contains(payload, expected):
        raise AdmissionError(f"JSON receipt does not contain required bindings: {path}")
    return {**file_result, "contains": expected}


def canonical_base64(value: object, size: int, context: str) -> str:
    if not isinstance(value, str):
        raise AdmissionError(f"{context} must be base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise AdmissionError(f"{context} is not canonical base64") from error
    if len(decoded) != size or base64.b64encode(decoded).decode() != value:
        raise AdmissionError(f"{context} must encode exactly {size} bytes")
    return value


def check_snapshot(spec: dict[str, Any]) -> dict[str, Any]:
    path = require_absolute_path(spec.get("path"), "snapshot")
    gcloud = require_absolute_path(spec.get("gcloud_bin"), "gcloud")
    expected_size = spec.get("size")
    if not isinstance(expected_size, int) or expected_size < 1:
        raise AdmissionError("snapshot size must be positive")
    expected_crc = canonical_base64(spec.get("crc32c_base64"), 4, "snapshot CRC32C")
    expected_md5 = canonical_base64(spec.get("md5_base64"), 16, "snapshot MD5")
    try:
        before = path.lstat()
    except OSError as error:
        raise AdmissionError(f"snapshot unavailable {path}: {error}") from error
    expected_mode = parse_mode(spec.get("mode"), str(path))
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_size != expected_size
        or (before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode))
        != (spec.get("uid"), spec.get("gid"), expected_mode)
    ):
        raise AdmissionError(f"snapshot has unsafe identity or unexpected size: {path}")
    completed = subprocess.run(
        [str(gcloud), "storage", "hash", str(path), "--format=json", "--quiet"],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AdmissionError(
            f"snapshot hashing failed: {completed.stderr.strip() or completed.stdout.strip()}"
        )
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise AdmissionError("gcloud storage hash returned invalid JSON") from error
    if not isinstance(report, list) or len(report) != 1 or not isinstance(report[0], dict):
        raise AdmissionError("gcloud storage hash did not return exactly one result")
    item = report[0]
    actual_crc = canonical_base64(item.get("crc32c_hash"), 4, "actual snapshot CRC32C")
    actual_md5 = canonical_base64(item.get("md5_hash"), 16, "actual snapshot MD5")
    after = path.lstat()
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise AdmissionError("snapshot changed while it was hashed")
    if item.get("url") != str(path) or item.get("digest_format") != "base64":
        raise AdmissionError("gcloud storage hash reported an unexpected path or format")
    if actual_crc != expected_crc or actual_md5 != expected_md5:
        raise AdmissionError("snapshot content hashes do not match the sealed manifest")
    return {
        "path": str(path),
        "size": before.st_size,
        "crc32c_base64": actual_crc,
        "md5_base64": actual_md5,
    }


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def memory_available_bytes(path: Path = Path("/proc/meminfo")) -> int:
    try:
        lines = path.read_text().splitlines()
    except OSError as error:
        raise AdmissionError(f"cannot read memory availability from {path}: {error}") from error
    values: dict[str, int] = {}
    for line in lines:
        fields = line.split()
        if len(fields) == 3 and fields[0].endswith(":") and fields[2] == "kB":
            try:
                values[fields[0][:-1]] = int(fields[1]) * 1024
            except ValueError:
                continue
    available = values.get("MemAvailable")
    if available is None or available < 1:
        raise AdmissionError(f"{path} lacks a positive MemAvailable value")
    return available


def process_references(roots: Sequence[Path], proc_root: Path = Path("/proc")) -> list[str]:
    needles = tuple(os.fsencode(str(root)) for root in roots)
    references: list[str] = []
    for process in proc_root.iterdir():
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        try:
            command = (process / "cmdline").read_bytes()
            maps = (process / "maps").read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            command = b""
            maps = b""
        for root, needle in zip(roots, needles, strict=True):
            if needle in command or needle in maps:
                references.append(f"pid {process.name} references {root}")
        for directory_name in ("cwd", "root", "exe"):
            try:
                target = Path(os.readlink(process / directory_name).removesuffix(" (deleted)"))
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            for root in roots:
                try:
                    target.relative_to(root)
                except ValueError:
                    continue
                references.append(f"pid {process.name} {directory_name} references {root}")
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            descriptors = []
        for descriptor in descriptors:
            try:
                target = Path(os.readlink(descriptor).removesuffix(" (deleted)"))
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            for root in roots:
                try:
                    target.relative_to(root)
                except ValueError:
                    continue
                references.append(
                    f"pid {process.name} fd {descriptor.name} references {root}"
                )
    return sorted(set(references))


def write_json_noclobber(
    path: Path, payload: dict[str, Any], required_uid: int = 0
) -> None:
    if not path.is_absolute():
        raise AdmissionError("receipt path must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.stat()
    except OSError as error:
        raise AdmissionError(f"cannot resolve receipt directory: {error}") from error
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise AdmissionError(f"receipt directory must be root-owned and owner-only: {parent}")
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


def load_manifest(path: Path, required_uid: int = 0) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise AdmissionError("manifest path must be absolute")
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise AdmissionError(f"cannot read admission manifest {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != required_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise AdmissionError("admission manifest has unsafe identity")
    if not isinstance(payload, dict) or payload.get("schema") != MANIFEST_SCHEMA:
        raise AdmissionError("admission manifest has an unsupported schema")
    return payload, sha256_file(path)


def require_list(manifest: dict[str, Any], name: str) -> list[Any]:
    value = manifest.get(name)
    if not isinstance(value, list):
        raise AdmissionError(f"manifest {name} must be a list")
    return value


def run_admission(manifest: dict[str, Any]) -> dict[str, Any]:
    terminal_units = [sample_unit(item) for item in require_list(manifest, "terminal_units")]
    for state in terminal_units:
        check_terminal_unit(state)
    absent_units = [sample_unit(item) for item in require_list(manifest, "absent_units")]
    for state in absent_units:
        check_absent_unit(state)

    absent_paths: list[str] = []
    for value in require_list(manifest, "absent_paths"):
        path = require_absolute_path(value, "absent")
        if path.exists() or path.is_symlink():
            raise AdmissionError(f"path must be absent before admission: {path}")
        absent_paths.append(str(path))

    files = [check_file(item) for item in require_list(manifest, "files")]
    receipts = [
        check_json_receipt(item) for item in require_list(manifest, "json_receipts")
    ]
    directories = [
        check_directory(item) for item in require_list(manifest, "directories")
    ]
    snapshot_spec = manifest.get("snapshot")
    if not isinstance(snapshot_spec, dict):
        raise AdmissionError("manifest snapshot must be an object")
    snapshot = check_snapshot(snapshot_spec)

    filesystem = require_absolute_path(manifest.get("filesystem"), "filesystem")
    try:
        filesystem = filesystem.resolve(strict=True)
    except OSError as error:
        raise AdmissionError(f"cannot resolve admission filesystem: {error}") from error
    if not filesystem.is_dir():
        raise AdmissionError(f"admission filesystem is not a directory: {filesystem}")
    minimum_free_bytes = manifest.get("minimum_free_bytes")
    if not isinstance(minimum_free_bytes, int) or minimum_free_bytes < 1:
        raise AdmissionError("minimum_free_bytes must be positive")
    free_bytes = available_bytes(filesystem)
    if free_bytes < minimum_free_bytes:
        raise AdmissionError(
            f"available bytes {free_bytes} below admission floor {minimum_free_bytes}"
        )

    minimum_memory_available_bytes = manifest.get("minimum_memory_available_bytes")
    meminfo_path = require_absolute_path(
        manifest.get("meminfo_path", "/proc/meminfo"), "memory information"
    )
    available_memory = memory_available_bytes(meminfo_path)
    if (
        minimum_memory_available_bytes is not None
        and (
            not isinstance(minimum_memory_available_bytes, int)
            or isinstance(minimum_memory_available_bytes, bool)
            or minimum_memory_available_bytes < 1
        )
    ):
        raise AdmissionError("minimum_memory_available_bytes must be positive")
    if (
        minimum_memory_available_bytes is not None
        and available_memory < minimum_memory_available_bytes
    ):
        raise AdmissionError(
            f"available memory {available_memory} below admission floor "
            f"{minimum_memory_available_bytes}"
        )

    vm_path = require_absolute_path(manifest.get("vm_max_map_count_path"), "VMA ceiling")
    minimum_vmas = manifest.get("minimum_vm_max_map_count")
    try:
        actual_vmas = int(vm_path.read_text().strip())
    except (OSError, ValueError) as error:
        raise AdmissionError(f"cannot read VMA ceiling: {error}") from error
    if not isinstance(minimum_vmas, int) or minimum_vmas < 1 or actual_vmas < minimum_vmas:
        raise AdmissionError(
            f"vm.max_map_count {actual_vmas} below admission floor {minimum_vmas}"
        )

    roots = [
        require_absolute_path(value, "process reference root")
        for value in require_list(manifest, "no_process_reference_roots")
    ]
    references = process_references(roots)
    if references:
        raise AdmissionError("live process references candidate roots: " + "; ".join(references))

    return {
        "terminal_units": [asdict(item) for item in terminal_units],
        "absent_units": [asdict(item) for item in absent_units],
        "absent_paths": absent_paths,
        "files": files,
        "json_receipts": receipts,
        "directories": directories,
        "snapshot": snapshot,
        "filesystem": str(filesystem),
        "available_bytes": free_bytes,
        "minimum_free_bytes": minimum_free_bytes,
        "meminfo_path": str(meminfo_path),
        "memory_available_bytes": available_memory,
        "minimum_memory_available_bytes": minimum_memory_available_bytes,
        "vm_max_map_count": actual_vmas,
        "minimum_vm_max_map_count": minimum_vmas,
        "no_process_reference_roots": [str(item) for item in roots],
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise AdmissionError("performance admission must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.receipt.exists() or args.receipt.is_symlink():
        raise AdmissionError(f"receipt path already exists: {args.receipt}")
    manifest, manifest_sha256 = load_manifest(args.manifest)
    evidence = run_admission(manifest)
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest": str(args.manifest),
        "manifest_sha256": manifest_sha256,
        "admitted": True,
        "evidence": evidence,
        "services_installed": False,
        "services_started": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_json_noclobber(args.receipt, receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AdmissionError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
