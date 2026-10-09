#!/usr/bin/env python3
"""Delete one bound replay scratch tree after terminal independent validation."""

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
from typing import Any, Sequence


RECEIPT_SCHEMA = "jetstreamer-historical-scratch-retirement-v1"
VALIDATION_SCHEMA = "jetstreamer-focused-qualification-artifact-validation-v1"
PRIVATE_ROOT = Path("/home/ubuntu/.jetstreamer-private")
ROOT_UID = 0
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")


class RetirementError(RuntimeError):
    """A fail-closed scratch-retirement error."""


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
    exec_main_code: int
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
        raise RetirementError(f"invalid systemd service name: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts,ExecMainCode,ExecMainStatus",
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RetirementError(
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
        exec_main_code=int(fields.get("ExecMainCode", "0") or 0),
        exec_main_status=int(fields.get("ExecMainStatus", "0") or 0),
    )


def require_terminal_success(state: UnitState) -> None:
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.result != "success"
        or state.main_pid != 0
        or state.restarts != 0
        or state.exec_main_status != 0
    ):
        raise RetirementError(f"unit has not reached clean terminal success: {state}")


def require_scratch(
    path: Path, private_root: Path | None = None, *, allow_missing: bool = False
) -> Path:
    if os.geteuid() != 0:
        raise RetirementError("scratch retirement must run as root")
    if not path.is_absolute():
        raise RetirementError("scratch path must be absolute")
    if private_root is None:
        private_root = PRIVATE_ROOT
    try:
        resolved_root = private_root.resolve(strict=True)
        resolved_parent = path.parent.resolve(strict=True)
        resolved_parent.relative_to(resolved_root)
    except (OSError, ValueError) as error:
        raise RetirementError(f"unsafe or unavailable scratch path {path}: {error}") from error
    if (
        resolved_parent != path.parent
        or not path.name.startswith("replay-scratch-")
        or resolved_parent.parent != resolved_root
    ):
        raise RetirementError(
            f"scratch path must be directly beneath one private qualification root: {path}"
        )
    if not path.exists() and not path.is_symlink():
        if allow_missing:
            return path
        raise RetirementError(f"scratch path is unavailable: {path}")
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise RetirementError(f"unsafe or unavailable scratch path {path}: {error}") from error
    if resolved != path or not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RetirementError(f"scratch path is not an exact real directory: {path}")
    if os.path.ismount(resolved):
        raise RetirementError(f"scratch path is not an admitted replay scratch tree: {resolved}")
    return resolved


def require_private_parent(path: Path, description: str) -> Path:
    if not path.is_absolute():
        raise RetirementError(f"{description} must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.stat()
    except OSError as error:
        raise RetirementError(f"cannot resolve {description} parent: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RetirementError(f"{description} parent must be owner-only: {parent}")
    return parent / path.name


def read_validation_receipt(
    path: Path,
    expected_epoch: int,
    expected_archive: Path,
    required_uid: int | None = None,
) -> dict[str, Any]:
    if required_uid is None:
        required_uid = ROOT_UID
    try:
        metadata = path.lstat()
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RetirementError(f"cannot read validation receipt {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != required_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError(f"validation receipt has unsafe identity: {path}")
    expected = str(expected_archive)
    if (
        not isinstance(raw, dict)
        or raw.get("schema") != VALIDATION_SCHEMA
        or raw.get("validation") != "pass"
        or raw.get("epoch") != expected_epoch
        or raw.get("archive") != expected
        or raw.get("canonical_checksum_sidecar_absent") is not True
    ):
        raise RetirementError("validation receipt does not bind the expected private artifact")
    archive_digest = raw.get("archive_sha256")
    if not isinstance(archive_digest, str) or re.fullmatch(r"[0-9a-f]{64}", archive_digest) is None:
        raise RetirementError("validation receipt has an invalid archive digest")
    manifest = raw.get("manifest")
    if not isinstance(manifest, str):
        raise RetirementError("validation receipt does not bind a segment manifest")
    expected_identities = (
        (
            expected_archive,
            raw.get("archive_bytes"),
            raw.get("archive_uid"),
            raw.get("archive_gid"),
            raw.get("archive_mode"),
        ),
        (
            Path(manifest),
            raw.get("manifest_bytes"),
            raw.get("manifest_uid"),
            raw.get("manifest_gid"),
            raw.get("manifest_mode"),
        ),
    )
    for artifact, expected_size, expected_uid, expected_gid, expected_mode in expected_identities:
        try:
            artifact_metadata = artifact.lstat()
        except OSError as error:
            raise RetirementError(f"validated artifact is unavailable: {artifact}: {error}") from error
        if not stat.S_ISREG(artifact_metadata.st_mode):
            raise RetirementError(f"validated artifact is not a regular file: {artifact}")
        identity = (
            artifact_metadata.st_size,
            artifact_metadata.st_uid,
            artifact_metadata.st_gid,
            f"{stat.S_IMODE(artifact_metadata.st_mode):04o}",
        )
        if identity != (expected_size, expected_uid, expected_gid, expected_mode):
            raise RetirementError(f"validated artifact identity changed: {artifact}")
    sidecar = raw.get("canonical_checksum_sidecar")
    if not isinstance(sidecar, str) or Path(sidecar).exists() or Path(sidecar).is_symlink():
        raise RetirementError("private validation artifact has an unexpected canonical sidecar")
    return raw


def _read_proc_file(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as error:
        raise RetirementError(f"cannot inspect process evidence {path}: {error}") from error


def _read_proc_link(path: Path) -> str | None:
    try:
        return os.readlink(path).removesuffix(" (deleted)")
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as error:
        raise RetirementError(f"cannot inspect process link {path}: {error}") from error


def process_references(scratch: Path, proc_root: Path = Path("/proc")) -> list[str]:
    needle = os.fsencode(str(scratch))
    references: list[str] = []
    try:
        processes = list(proc_root.iterdir())
    except OSError as error:
        raise RetirementError(f"cannot enumerate processes: {error}") from error
    for process in processes:
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        pid = process.name
        command = _read_proc_file(process / "cmdline")
        if command is not None and encoded_path_reference(command, needle):
            references.append(f"pid {pid} command line")
        maps = _read_proc_file(process / "maps")
        if maps is not None and encoded_path_reference(maps, needle):
            references.append(f"pid {pid} memory maps")
        for name in ("cwd", "root", "exe"):
            target = _read_proc_link(process / name)
            if target is None:
                continue
            try:
                Path(target).relative_to(scratch)
            except ValueError:
                continue
            references.append(f"pid {pid} {name}")
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            descriptors = []
        except OSError as error:
            raise RetirementError(f"cannot inspect pid {pid} descriptors: {error}") from error
        for descriptor in descriptors:
            target = _read_proc_link(descriptor)
            if target is None:
                continue
            try:
                Path(target).relative_to(scratch)
            except ValueError:
                continue
            references.append(f"pid {pid} fd {descriptor.name}")
    return references


def encoded_path_reference(content: bytes, needle: bytes) -> bool:
    """Match an exact path or descendant, but not a sibling sharing its prefix."""
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


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    destination = require_private_parent(path, "retirement receipt")
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
    directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_retirement_receipt(
    path: Path,
    expected_status: str,
    common: dict[str, Any],
    required_uid: int | None = None,
) -> tuple[dict[str, Any], str]:
    if required_uid is None:
        required_uid = ROOT_UID
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RetirementError(f"cannot read retirement receipt {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != required_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or not isinstance(payload, dict)
    ):
        raise RetirementError(f"retirement receipt has unsafe identity: {path}")
    expected = {
        "schema": RECEIPT_SCHEMA,
        "status": expected_status,
        "scratch": common["scratch"],
        "validation_receipt": common["validation_receipt"],
        "validation_receipt_sha256": common["validation_receipt_sha256"],
        "epoch": common["epoch"],
        "archive": common["archive"],
        "archive_sha256": common["archive_sha256"],
        "r2_mutations": False,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise RetirementError(f"retirement receipt does not bind this exact cleanup: {path}")
    for key in ("producer", "validator"):
        recorded = payload.get(key)
        current = common[key]
        if not isinstance(recorded, dict) or recorded.get("unit") != current.get("unit"):
            raise RetirementError(f"retirement receipt has an invalid {key} binding")
    return payload, sha256_file(path)


def delete_exact_tree(path: Path) -> None:
    completed = subprocess.run(
        ["/usr/bin/find", str(path), "-xdev", "-depth", "-delete"],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or path.exists() or path.is_symlink():
        raise RetirementError(
            f"scratch deletion did not complete: {completed.stderr.strip() or completed.stdout.strip()}"
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--confirm-delete-exact", type=Path, required=True)
    parser.add_argument("--producer-unit", required=True)
    parser.add_argument("--validator-unit", required=True)
    parser.add_argument("--validation-receipt", type=Path, required=True)
    parser.add_argument("--expected-epoch", type=int, required=True)
    parser.add_argument("--expected-archive", type=Path, required=True)
    parser.add_argument("--intent-receipt", type=Path, required=True)
    parser.add_argument("--completion-receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.expected_epoch < 0 or not args.expected_archive.is_absolute():
        parser.error("expected epoch and archive are invalid")
    if args.scratch != args.confirm_delete_exact:
        parser.error("--confirm-delete-exact must exactly repeat --scratch")
    if args.intent_receipt == args.completion_receipt:
        parser.error("intent and completion receipts must differ")
    for unit in (args.producer_unit, args.validator_unit):
        if UNIT_NAME.fullmatch(unit) is None:
            parser.error(f"invalid systemd service name: {unit}")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    intent_exists = args.intent_receipt.exists() or args.intent_receipt.is_symlink()
    completion_exists = (
        args.completion_receipt.exists() or args.completion_receipt.is_symlink()
    )
    if completion_exists and not intent_exists:
        raise RetirementError("completion receipt exists without its deletion intent")
    scratch = require_scratch(args.scratch, allow_missing=intent_exists)
    producer = sample_unit(args.producer_unit)
    validator = sample_unit(args.validator_unit)
    require_terminal_success(producer)
    require_terminal_success(validator)
    validation = read_validation_receipt(
        args.validation_receipt, args.expected_epoch, args.expected_archive
    )
    common = {
        "schema": RECEIPT_SCHEMA,
        "scratch": str(scratch),
        "producer": asdict(producer),
        "validator": asdict(validator),
        "validation_receipt": str(args.validation_receipt),
        "validation_receipt_sha256": sha256_file(args.validation_receipt),
        "epoch": args.expected_epoch,
        "archive": str(args.expected_archive),
        "archive_sha256": validation["archive_sha256"],
        "r2_mutations": False,
    }
    references = process_references(scratch)
    if references:
        raise RetirementError("live process still references scratch: " + "; ".join(references))
    if intent_exists:
        intent, intent_sha256 = read_retirement_receipt(
            args.intent_receipt, "deletion-intent-fsynced", common
        )
        free_before = intent.get("available_bytes_before")
        if not isinstance(free_before, int) or isinstance(free_before, bool):
            raise RetirementError("deletion intent has invalid pre-cleanup free bytes")
    else:
        free_before = available_bytes(scratch.parent)
        write_json_noclobber(
            args.intent_receipt,
            {
                **common,
                "status": "deletion-intent-fsynced",
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "available_bytes_before": free_before,
            },
        )
        intent_sha256 = sha256_file(args.intent_receipt)
    if completion_exists:
        completion, _ = read_retirement_receipt(
            args.completion_receipt, "deleted", common
        )
        if completion.get("intent_receipt") != str(args.intent_receipt) or completion.get(
            "intent_receipt_sha256"
        ) != intent_sha256:
            raise RetirementError("completion receipt does not bind the deletion intent")
        if scratch.exists() or scratch.is_symlink():
            raise RetirementError("completion receipt exists while scratch remains")
        print(json.dumps(completion, sort_keys=True), flush=True)
        return 0
    if scratch.exists() or scratch.is_symlink():
        require_scratch(scratch)
        delete_exact_tree(scratch)
    completion = {
        **common,
        "status": "deleted",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "intent_receipt": str(args.intent_receipt),
        "intent_receipt_sha256": intent_sha256,
        "available_bytes_before": free_before,
        "available_bytes_after": available_bytes(args.expected_archive.parent),
    }
    write_json_noclobber(args.completion_receipt, completion)
    print(json.dumps(completion, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RetirementError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
