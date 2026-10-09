#!/usr/bin/env python3
"""Retire only the scratch of a reserve-stopped bootstrap-replay cohort.

This is deliberately narrower than failed-run cleanup.  It preserves the
node-authored run directory, retained bootstrap, partial archives, and cohort
state so a later JETSTREAMER_ROOT_COHORT_RESUME_RUN launch can re-admit that
evidence.  The operator must separately establish that the selected node
restarts the incomplete member from its sealed bootstrap and does not carry a
replay cursor in scratch.
"""

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


GUARD_INTENT_SCHEMA = "jetstreamer-adaptive-replay-reserve-stop-intent-v1"
GUARD_COMPLETION_SCHEMA = "jetstreamer-adaptive-replay-reserve-stop-completion-v1"
RUN_STATE_SCHEMA = "jetstreamer-root-cohort-run-v1"
INTENT_SCHEMA = "jetstreamer-reserve-stopped-scratch-retirement-intent-v1"
COMPLETION_SCHEMA = "jetstreamer-reserve-stopped-scratch-retirement-completion-v1"
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")
INVOCATION_ID = re.compile(r"^[0-9a-f]{32}$")
FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
RUN_NAME = re.compile(r"^run-[0-9]+-[0-9]+$")


class RetirementError(RuntimeError):
    """A reserve-stopped scratch retirement invariant was not satisfied."""


@dataclass(frozen=True)
class UnitSample:
    unit: str
    load_state: str
    active_state: str
    sub_state: str
    result: str
    main_pid: int
    invocation_id: str
    restarts: int


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


def sample_unit(unit: str) -> UnitSample:
    completed = subprocess.run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    fields = parse_systemctl_show(completed.stdout)
    return UnitSample(
        unit=unit,
        load_state=fields.get("LoadState", "unknown"),
        active_state=fields.get("ActiveState", "unknown"),
        sub_state=fields.get("SubState", "unknown"),
        result=fields.get("Result", "unknown"),
        main_pid=int(fields.get("MainPID", "0") or 0),
        invocation_id=fields.get("InvocationID", ""),
        restarts=int(fields.get("NRestarts", "0") or 0),
    )


def require_root_private_file(path: Path, description: str) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise RetirementError(f"{description} path must be absolute")
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RetirementError(f"cannot read {description} {path}: {error}") from error
    if (
        resolved != path
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or not isinstance(payload, dict)
    ):
        raise RetirementError(f"{description} has unsafe identity: {path}")
    return payload, sha256_file(path)


def validate_guard_receipts(args: argparse.Namespace) -> dict[str, Any]:
    intent, intent_sha256 = require_root_private_file(
        args.guard_intent, "reserve-stop intent"
    )
    completion, completion_sha256 = require_root_private_file(
        args.guard_completion, "reserve-stop completion"
    )
    common = {
        "controller_unit": args.controller_unit,
        "expected_controller_invocation_id": args.controller_invocation_id,
        "producer_unit": args.producer_unit,
        "expected_producer_invocation_id": args.producer_invocation_id,
    }
    if (
        intent.get("schema") != GUARD_INTENT_SCHEMA
        or any(intent.get(key) != value for key, value in common.items())
        or intent.get("reason")
        != "actual available bytes fell below the sealed reserve stop floor"
        or intent.get("stop_order") != ["controller", "producer"]
        or intent.get("remote_mutations") is not False
        or intent.get("r2_mutations") is not False
        or not isinstance(intent.get("available_bytes"), int)
        or not isinstance(intent.get("minimum_free_bytes"), int)
        or intent["available_bytes"] >= intent["minimum_free_bytes"]
    ):
        raise RetirementError("reserve-stop intent does not bind the expected trip")
    for label, expected_invocation in (
        ("controller", args.controller_invocation_id),
        ("producer", args.producer_invocation_id),
    ):
        sample = intent.get(f"{label}_sample")
        if (
            not isinstance(sample, dict)
            or sample.get("unit") != common[f"{label}_unit"]
            or sample.get("active_state") != "active"
            or sample.get("main_pid", 0) <= 0
            or sample.get("restarts") != 0
            or sample.get("invocation_id") != expected_invocation
        ):
            raise RetirementError(f"reserve-stop intent lacks live {label} evidence")
    if (
        completion.get("schema") != GUARD_COMPLETION_SCHEMA
        or any(completion.get(key) != value for key, value in common.items())
        or completion.get("intent_receipt") != str(args.guard_intent)
        or completion.get("filesystem") != intent.get("filesystem")
        or completion.get("minimum_free_bytes") != intent.get("minimum_free_bytes")
        or completion.get("scratch_deleted") is not False
        or completion.get("remote_mutations") is not False
        or completion.get("r2_mutations") is not False
    ):
        raise RetirementError("reserve-stop completion does not bind the expected stop")
    for label, expected_invocation in (
        ("controller", args.controller_invocation_id),
        ("producer", args.producer_invocation_id),
    ):
        final = completion.get(f"{label}_final")
        if (
            not isinstance(final, dict)
            or final.get("unit") != common[f"{label}_unit"]
            or final.get("active_state") != "inactive"
            or final.get("main_pid") != 0
            or final.get("restarts") != 0
            or final.get("result") != "success"
            or final.get("invocation_id") not in ("", expected_invocation)
        ):
            raise RetirementError(f"reserve-stop completion lacks terminal {label} evidence")
    return {
        "guard_intent": str(args.guard_intent),
        "guard_intent_sha256": intent_sha256,
        "guard_completion": str(args.guard_completion),
        "guard_completion_sha256": completion_sha256,
        "minimum_free_bytes": intent["minimum_free_bytes"],
        "trip_available_bytes": intent["available_bytes"],
    }


def require_current_stopped(
    sample: UnitSample, expected_invocation: str, label: str
) -> None:
    if sample.active_state in {"active", "activating", "reloading", "deactivating"}:
        raise RetirementError(f"{label} is still live: {sample}")
    if sample.main_pid != 0 or sample.restarts != 0:
        raise RetirementError(f"{label} has an unsafe terminal state: {sample}")
    if sample.invocation_id not in ("", expected_invocation):
        raise RetirementError(f"{label} invocation changed after the reserve stop")


def require_exact_directory(path: Path, description: str, uid: int | None = None) -> os.stat_result:
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise RetirementError(f"cannot inspect {description} {path}: {error}") from error
    if (
        resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or os.path.ismount(path)
        or (uid is not None and metadata.st_uid != uid)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError(f"{description} has unsafe identity: {path}")
    return metadata


def directory_files(path: Path, uid: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    entries = sorted(path.iterdir(), key=lambda item: item.name)
    if not entries or len(entries) > 16:
        raise RetirementError(f"preserved directory must contain 1..16 files: {path}")
    for entry in entries:
        metadata = entry.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != uid
            or metadata.st_nlink != 1
            or entry.resolve(strict=True) != entry
        ):
            raise RetirementError(f"preserved artifact has unsafe identity: {entry}")
        result.append(
            {
                "path": str(entry),
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "size": metadata.st_size,
                "mtime_ns": metadata.st_mtime_ns,
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
                "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
            }
        )
    return result


def validate_run(args: argparse.Namespace, *, allow_missing_scratch: bool) -> dict[str, Any]:
    run = args.run_directory
    if (
        not run.is_absolute()
        or RUN_NAME.fullmatch(run.name) is None
        or run.parent.name != f"root-cohort-{args.start_epoch}-{args.end_epoch}"
        or run.parent.parent.name != "work"
        or args.scratch != run / "scratch"
    ):
        raise RetirementError("run and scratch paths do not name the exact sealed cohort layout")
    run_metadata = require_exact_directory(run, "cohort run")
    uid = run_metadata.st_uid
    state_path = run / "cohort-state.json"
    try:
        state_metadata = state_path.lstat()
        state = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RetirementError(f"cannot read cohort run state: {error}") from error
    expected_state = {
        "end_epoch": args.end_epoch,
        "manifest_fingerprint": args.manifest_fingerprint,
        "schema": RUN_STATE_SCHEMA,
        "start_epoch": args.start_epoch,
        "status": "running-private",
    }
    if (
        state != expected_state
        or not stat.S_ISREG(state_metadata.st_mode)
        or state_metadata.st_uid != uid
        or state_metadata.st_nlink != 1
        or stat.S_IMODE(state_metadata.st_mode) != 0o600
    ):
        raise RetirementError("cohort state is not the exact cursor-free running-private schema")
    archives = run / "archives"
    inputs = run / "inputs"
    archive_metadata = require_exact_directory(archives, "retained archives", uid)
    input_metadata = require_exact_directory(inputs, "retained inputs", uid)
    allowed = {"archives", "cohort-state.json", "inputs", "scratch"}
    actual = {entry.name for entry in run.iterdir()}
    if actual - allowed or not {"archives", "cohort-state.json", "inputs"} <= actual:
        raise RetirementError(f"cohort run has unexpected entries: {sorted(actual)}")
    scratch_identity = None
    if args.scratch.exists() or args.scratch.is_symlink():
        scratch_metadata = require_exact_directory(args.scratch, "replay scratch", uid)
        if scratch_metadata.st_dev != run_metadata.st_dev:
            raise RetirementError("replay scratch is on a different filesystem")
        scratch_identity = {
            "device": scratch_metadata.st_dev,
            "inode": scratch_metadata.st_ino,
            "uid": scratch_metadata.st_uid,
            "gid": scratch_metadata.st_gid,
            "mode": f"{stat.S_IMODE(scratch_metadata.st_mode):04o}",
        }
    elif not allow_missing_scratch:
        raise RetirementError(f"replay scratch is missing: {args.scratch}")
    return {
        "run_directory": str(run),
        "run_device": run_metadata.st_dev,
        "run_inode": run_metadata.st_ino,
        "run_uid": uid,
        "run_gid": run_metadata.st_gid,
        "run_mode": f"{stat.S_IMODE(run_metadata.st_mode):04o}",
        "cohort_state": str(state_path),
        "cohort_state_sha256": sha256_file(state_path),
        "scratch": str(args.scratch),
        "scratch_identity": scratch_identity,
        "archives_directory": str(archives),
        "archives_device": archive_metadata.st_dev,
        "archives_inode": archive_metadata.st_ino,
        "archives": directory_files(archives, uid),
        "inputs_directory": str(inputs),
        "inputs_device": input_metadata.st_dev,
        "inputs_inode": input_metadata.st_ino,
        "inputs": directory_files(inputs, uid),
    }


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


def ancestor_pids(proc_root: Path = Path("/proc")) -> set[int]:
    ancestors: set[int] = set()
    current = os.getpid()
    while current > 1 and current not in ancestors:
        ancestors.add(current)
        try:
            suffix = (proc_root / str(current) / "stat").read_text().rsplit(")", 1)[1]
            current = int(suffix.split()[1])
        except (FileNotFoundError, ProcessLookupError, OSError, ValueError, IndexError):
            break
    ancestors.add(current)
    return ancestors


def process_references(scratch: Path, proc_root: Path = Path("/proc")) -> list[str]:
    needle = os.fsencode(str(scratch))
    references: list[str] = []
    ancestors = ancestor_pids(proc_root)
    try:
        processes = list(proc_root.iterdir())
    except OSError as error:
        raise RetirementError(f"cannot enumerate processes: {error}") from error
    for process in processes:
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        pid = process.name
        for label in ("cmdline", "maps"):
            content = _read_proc_file(process / label)
            # An exec wrapper's command line necessarily names --scratch.  It
            # is not a live filesystem reference, but maps/fds/cwd from that
            # same ancestor remain fully checked below.
            if (
                content is not None
                and needle in content
                and not (label == "cmdline" and int(pid) in ancestors)
            ):
                references.append(f"pid {pid} {label}")
        for label in ("cwd", "root", "exe"):
            target = _read_proc_link(process / label)
            if target is None:
                continue
            try:
                Path(target).relative_to(scratch)
            except ValueError:
                continue
            references.append(f"pid {pid} {label}")
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


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def require_receipt_parent(path: Path) -> Path:
    if not path.is_absolute() or path.name in ("", ".", ".."):
        raise RetirementError("retirement receipt must be an absolute file path")
    parent = path.parent.resolve(strict=True)
    metadata = parent.stat()
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RetirementError("retirement receipt directory must be root-owned and owner-only")
    return parent / path.name


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    destination = require_receipt_parent(path)
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


def stable_common(args: argparse.Namespace, guard: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    tool = Path(__file__).resolve(strict=True)
    return {
        "controller_unit": args.controller_unit,
        "controller_invocation_id": args.controller_invocation_id,
        "producer_unit": args.producer_unit,
        "producer_invocation_id": args.producer_invocation_id,
        "start_epoch": args.start_epoch,
        "end_epoch": args.end_epoch,
        "manifest_fingerprint": args.manifest_fingerprint,
        "replay_resume_semantics": "restart-from-sealed-bootstrap-without-scratch-cursor",
        "retirement_tool": {
            "path": str(tool),
            "sha256": sha256_file(tool),
        },
        "guard": guard,
        "run": run,
        "remote_mutations": False,
        "r2_mutations": False,
    }


def read_bound_retirement_receipt(
    path: Path, schema: str, common: dict[str, Any]
) -> dict[str, Any]:
    payload, _ = require_root_private_file(path, "scratch-retirement receipt")
    expected = {"schema": schema, **common}
    if any(payload.get(key) != value for key, value in expected.items()):
        raise RetirementError(f"scratch-retirement receipt binding changed: {path}")
    return payload


def delete_exact_tree(path: Path) -> None:
    completed = subprocess.run(
        ["/usr/bin/find", str(path), "-xdev", "-depth", "-delete"],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or path.exists() or path.is_symlink():
        raise RetirementError(
            "scratch deletion did not complete: "
            + (completed.stderr.strip() or completed.stdout.strip())
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-intent", type=Path, required=True)
    parser.add_argument("--guard-completion", type=Path, required=True)
    parser.add_argument("--controller-unit", required=True)
    parser.add_argument("--controller-invocation-id", required=True)
    parser.add_argument("--producer-unit", required=True)
    parser.add_argument("--producer-invocation-id", required=True)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--confirm-delete-exact", type=Path, required=True)
    parser.add_argument("--start-epoch", type=int, required=True)
    parser.add_argument("--end-epoch", type=int, required=True)
    parser.add_argument("--manifest-fingerprint", required=True)
    parser.add_argument(
        "--confirm-restart-from-sealed-bootstrap-without-scratch-cursor",
        action="store_true",
    )
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--intent-receipt", type=Path, required=True)
    parser.add_argument("--completion-receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        parser.error("reserve-stopped scratch retirement must run as root")
    if args.scratch != args.confirm_delete_exact:
        parser.error("--confirm-delete-exact must exactly repeat --scratch")
    if not args.confirm_restart_from_sealed_bootstrap_without_scratch_cursor:
        parser.error("the exact bootstrap-restart semantic confirmation is required")
    if args.start_epoch < 0 or args.end_epoch < args.start_epoch:
        parser.error("epoch range is invalid")
    if FINGERPRINT.fullmatch(args.manifest_fingerprint) is None:
        parser.error("manifest fingerprint must be sha256:<64 lowercase hex>")
    for value, label in (
        (args.controller_unit, "controller unit"),
        (args.producer_unit, "producer unit"),
    ):
        if UNIT_NAME.fullmatch(value) is None:
            parser.error(f"invalid {label}")
    for value, label in (
        (args.controller_invocation_id, "controller invocation"),
        (args.producer_invocation_id, "producer invocation"),
    ):
        if INVOCATION_ID.fullmatch(value) is None:
            parser.error(f"invalid {label}")
    if args.intent_receipt == args.completion_receipt:
        parser.error("intent and completion receipts must differ")
    try:
        require_receipt_parent(args.intent_receipt)
        require_receipt_parent(args.completion_receipt)
    except RetirementError as error:
        parser.error(str(error))
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    intent_exists = args.intent_receipt.exists() or args.intent_receipt.is_symlink()
    completion_exists = args.completion_receipt.exists() or args.completion_receipt.is_symlink()
    if completion_exists and not intent_exists:
        raise RetirementError("completion receipt exists without deletion intent")

    guard = validate_guard_receipts(args)
    controller = sample_unit(args.controller_unit)
    producer = sample_unit(args.producer_unit)
    require_current_stopped(controller, args.controller_invocation_id, "controller")
    require_current_stopped(producer, args.producer_invocation_id, "producer")
    run = validate_run(args, allow_missing_scratch=intent_exists)

    if args.check_only:
        references = process_references(args.scratch) if args.scratch.exists() else []
        if references:
            raise RetirementError(
                "live process still references replay scratch: " + "; ".join(references)
            )
        print(
            json.dumps(
                {
                    "check_only": True,
                    "controller": asdict(controller),
                    "producer": asdict(producer),
                    "guard": guard,
                    "run": run,
                    "retirement_tool": stable_common(args, guard, run)[
                        "retirement_tool"
                    ],
                    "scratch_delete_authorized": False,
                    "remote_mutations": False,
                    "r2_mutations": False,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0

    if intent_exists:
        prior_payload, _ = require_root_private_file(
            args.intent_receipt, "scratch-retirement intent"
        )
        prior_run = prior_payload.get("run")
        if (
            not isinstance(prior_run, dict)
            or not isinstance(prior_run.get("scratch_identity"), dict)
        ):
            raise RetirementError("scratch-retirement intent lacks original scratch identity")
        if (
            run["scratch_identity"] is not None
            and run["scratch_identity"] != prior_run["scratch_identity"]
        ):
            raise RetirementError("replay scratch identity changed after deletion intent")
        current_bound = {**run, "scratch_identity": prior_run["scratch_identity"]}
        if current_bound != prior_run:
            raise RetirementError("preserved run evidence changed after deletion intent")
        common = stable_common(args, guard, prior_run)
        prior = read_bound_retirement_receipt(
            args.intent_receipt, INTENT_SCHEMA, common
        )
        free_before = prior.get("available_bytes_before")
        if not isinstance(free_before, int):
            raise RetirementError("scratch-retirement intent has invalid free-space evidence")
    else:
        common = stable_common(args, guard, run)
        free_before = available_bytes(args.run_directory)
        write_json_noclobber(
            args.intent_receipt,
            {
                "schema": INTENT_SCHEMA,
                **common,
                "status": "deletion-intent-fsynced",
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "available_bytes_before": free_before,
            },
        )

    if completion_exists:
        completion = read_bound_retirement_receipt(
            args.completion_receipt, COMPLETION_SCHEMA, common
        )
        if args.scratch.exists() or args.scratch.is_symlink():
            raise RetirementError("completion receipt exists while scratch remains")
        print(json.dumps(completion, sort_keys=True), flush=True)
        return 0

    references = process_references(args.scratch) if args.scratch.exists() else []
    if references:
        raise RetirementError("live process still references replay scratch: " + "; ".join(references))
    require_current_stopped(
        sample_unit(args.controller_unit), args.controller_invocation_id, "controller"
    )
    require_current_stopped(
        sample_unit(args.producer_unit), args.producer_invocation_id, "producer"
    )
    if args.scratch.exists() or args.scratch.is_symlink():
        delete_exact_tree(args.scratch)
    preserved = validate_run(args, allow_missing_scratch=True)
    expected_preserved = {**common["run"], "scratch_identity": None}
    if preserved != expected_preserved:
        raise RetirementError("preserved run evidence changed during scratch deletion")
    free_after = available_bytes(args.run_directory)
    completion = {
        "schema": COMPLETION_SCHEMA,
        **common,
        "status": "deleted",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "intent_receipt": str(args.intent_receipt),
        "intent_receipt_sha256": sha256_file(args.intent_receipt),
        "available_bytes_before": free_before,
        "available_bytes_after": free_after,
        "available_bytes_recovered": max(0, free_after - free_before),
        "scratch_deleted": True,
        "run_directory_preserved": True,
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
