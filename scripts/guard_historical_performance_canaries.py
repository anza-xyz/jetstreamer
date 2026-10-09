#!/usr/bin/env python3
"""Guard a private historical performance cohort with fail-closed tripwires."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence


RECEIPT_SCHEMA = "jetstreamer-historical-performance-guard-trip-v1"
TARGET_STOP_INTENT_SCHEMA = "jetstreamer-historical-performance-target-stop-v1"
MAX_TARGET_STOP_INTENT_BYTES = 16 * 1024
UNIT_PREFIXES = ("horizon-perf-epoch", "horizon-qualify-")
TIMER_UNIT = re.compile(
    r"^(?:horizon-perf-epoch[0-9]+-guard(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?"
    r"|horizon-monitor-recurring-[A-Za-z0-9@_.-]+)\.timer$"
)


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
    control_group: str
    worker_count: int
    worker_pid: int | None
    worker_vmas: int | None


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    return fields


def read_proc_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None


def is_historical_worker_command(command: bytes) -> bool:
    executable = command.split(b"\0", 1)[0]
    basename = executable.rsplit(b"/", 1)[-1]
    return executable.endswith(b"/bound-worker/historical-worker") or basename.startswith(
        b"jetstreamer-historical-worker-v"
    )


def worker_vmas(
    control_group: str,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    proc_root: Path = Path("/proc"),
) -> tuple[int, int | None, int | None]:
    if not control_group.startswith("/") or ".." in Path(control_group).parts:
        raise ValueError(f"invalid control group: {control_group}")
    group = cgroup_root / control_group.lstrip("/")
    if not group.is_dir():
        return 0, None, None
    candidates: set[int] = set()
    for process_file in group.rglob("cgroup.procs"):
        try:
            candidates.update(int(value) for value in process_file.read_text().split())
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
    workers: list[tuple[int, int | None]] = []
    for pid in sorted(candidates):
        command = read_proc_bytes(proc_root / str(pid) / "cmdline")
        if not command or not is_historical_worker_command(command):
            continue
        maps = read_proc_bytes(proc_root / str(pid) / "maps")
        workers.append((pid, maps.count(b"\n") if maps is not None else None))
    if len(workers) != 1:
        return len(workers), None, None
    pid, vmas = workers[0]
    return 1, pid, vmas


def sample_unit(unit: str) -> UnitSample:
    if not unit.startswith(UNIT_PREFIXES) or not unit.endswith(".service"):
        raise ValueError(f"invalid canary unit: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,"
            "NRestarts,ControlGroup",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    fields = parse_systemctl_show(completed.stdout)
    control_group = fields.get("ControlGroup", "")
    worker_count, worker_pid, vmas = (
        worker_vmas(control_group) if control_group else (0, None, None)
    )
    return UnitSample(
        unit=unit,
        load_state=fields.get("LoadState", "unknown"),
        active_state=fields.get("ActiveState", "unknown"),
        sub_state=fields.get("SubState", "unknown"),
        result=fields.get("Result", "unknown"),
        main_pid=int(fields.get("MainPID", "0") or 0),
        invocation_id=fields.get("InvocationID", ""),
        restarts=int(fields.get("NRestarts", "0") or 0),
        control_group=control_group,
        worker_count=worker_count,
        worker_pid=worker_pid,
        worker_vmas=vmas,
    )


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def trip_reasons(
    free_bytes: int,
    minimum_free_bytes: int,
    maximum_worker_vmas: int,
    samples: Sequence[UnitSample],
    expected_invocations: Mapping[str, str] | None = None,
    workerless_drain_units: set[str] | None = None,
) -> list[str]:
    reasons: list[str] = []
    expected_invocations = expected_invocations or {}
    workerless_drain_units = workerless_drain_units or set()
    if free_bytes < minimum_free_bytes:
        reasons.append(
            f"available bytes {free_bytes} below floor {minimum_free_bytes}"
        )
    for sample in samples:
        if sample.load_state != "loaded":
            reasons.append(f"{sample.unit} is not loaded: {sample.load_state}")
        if sample.active_state == "failed" or sample.result not in ("success", ""):
            reasons.append(
                f"{sample.unit} terminal failure: state={sample.active_state} "
                f"result={sample.result}"
            )
        if sample.restarts:
            reasons.append(f"{sample.unit} unexpectedly restarted {sample.restarts} time(s)")
        if sample.active_state in ("active", "activating"):
            expected_invocation = expected_invocations.get(sample.unit)
            if expected_invocation and sample.invocation_id != expected_invocation:
                reasons.append(
                    f"{sample.unit} invocation {sample.invocation_id or '<empty>'} "
                    f"does not match {expected_invocation}"
                )
            workerless_drain = (
                sample.unit in workerless_drain_units and sample.worker_count == 0
            )
            if not workerless_drain and (
                sample.worker_count != 1 or sample.worker_vmas is None
            ):
                reasons.append(
                    f"{sample.unit} has {sample.worker_count} identifiable historical workers"
                )
        if sample.worker_vmas is not None and sample.worker_vmas > maximum_worker_vmas:
            reasons.append(
                f"{sample.unit} worker VMAs {sample.worker_vmas} exceed {maximum_worker_vmas}"
            )
    return reasons


def load_target_stop_intent(
    path: Path,
    *,
    required_uid: int,
    expected_invocation: str,
    expected_target_slot: int,
    observed_unix_seconds: float,
    maximum_age_seconds: int,
) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate target-stop intent key {key!r}")
            result[key] = value
        return result

    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != required_uid
            or metadata.st_mode & 0o077
            or metadata.st_size > MAX_TARGET_STOP_INTENT_BYTES
        ):
            raise ValueError(f"unsafe target-stop intent identity: {path}")
        data = os.read(descriptor, metadata.st_size + 1)
        if len(data) != metadata.st_size:
            raise ValueError(f"target-stop intent changed while read: {path}")
        after = os.fstat(descriptor)
        if (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_mode,
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_nlink,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ValueError(f"target-stop intent changed while read: {path}")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(data, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"invalid target-stop intent JSON: {path}") from error
    expected_keys = {
        "schema",
        "systemd_invocation_id",
        "target_slot",
        "observed_stop_slot",
        "child_pid",
        "controlled_stop_signal",
        "scratch_snapshot_captured",
        "external_signal",
        "written_unix_seconds",
    }
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise ValueError(f"target-stop intent has unexpected fields: {path}")
    written = payload.get("written_unix_seconds")
    observed_stop_slot = payload.get("observed_stop_slot")
    child_pid = payload.get("child_pid")
    if (
        payload.get("schema") != TARGET_STOP_INTENT_SCHEMA
        or payload.get("systemd_invocation_id") != expected_invocation
        or payload.get("target_slot") != expected_target_slot
        or not isinstance(observed_stop_slot, int)
        or isinstance(observed_stop_slot, bool)
        or observed_stop_slot < expected_target_slot
        or not isinstance(child_pid, int)
        or isinstance(child_pid, bool)
        or child_pid <= 0
        or payload.get("controlled_stop_signal") != signal.SIGINT
        or payload.get("scratch_snapshot_captured") is not True
        or payload.get("external_signal") is not None
        or not isinstance(written, (int, float))
        or isinstance(written, bool)
        or written > observed_unix_seconds + 5
        or observed_unix_seconds - written > maximum_age_seconds
    ):
        raise ValueError(f"target-stop intent does not prove bounded drainage: {path}")
    return payload


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise ValueError("receipt path must be absolute")
    parent = path.parent.resolve(strict=True)
    if parent.stat().st_mode & 0o077:
        raise ValueError(f"receipt directory is not owner-only: {parent}")
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


def stop_active_units(samples: Sequence[UnitSample]) -> list[dict[str, Any]]:
    units = [
        sample.unit
        for sample in samples
        if sample.active_state in ("active", "activating")
    ]
    if not units:
        return []
    completed = subprocess.run(
        ["systemctl", "stop", *units],
        capture_output=True,
        text=True,
    )
    return [
        {
            "units": units,
            "return_code": completed.returncode,
            "stderr": completed.stderr.strip(),
        }
    ]


def cohort_running(samples: Sequence[UnitSample]) -> bool:
    return any(
        sample.active_state in ("active", "activating") for sample in samples
    )


def stop_timer(unit: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["systemctl", "stop", unit],
        capture_output=True,
        text=True,
    )
    return {
        "unit": unit,
        "return_code": completed.returncode,
        "stderr": completed.stderr.strip(),
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filesystem", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    parser.add_argument("--maximum-worker-vmas", type=int, required=True)
    parser.add_argument("--receipt-directory", type=Path, required=True)
    parser.add_argument("--timer-unit", required=True)
    parser.add_argument(
        "--expected-invocation",
        action="append",
        default=[],
        metavar="UNIT=INVOCATION_ID",
        help="bind every guarded unit to its exact 32-hex systemd invocation ID",
    )
    parser.add_argument(
        "--target-stop-intent",
        action="append",
        default=[],
        metavar="UNIT=PATH",
        help=(
            "bind each unit to the runner's durable post-target/pre-SIGINT "
            "handoff receipt"
        ),
    )
    parser.add_argument("--target-slot", type=int)
    parser.add_argument("--target-stop-intent-uid", type=int)
    parser.add_argument("--maximum-target-drain-seconds", type=int, default=600)
    parser.add_argument("units", nargs="+")
    args = parser.parse_args(argv)
    if not args.filesystem.is_absolute() or not args.filesystem.is_dir():
        parser.error("--filesystem must be an existing absolute directory")
    if (
        args.minimum_free_bytes < 1
        or args.maximum_worker_vmas < 1
        or args.maximum_target_drain_seconds < 1
    ):
        parser.error("tripwire values must be positive")
    if not args.receipt_directory.is_absolute() or not args.receipt_directory.is_dir():
        parser.error("--receipt-directory must be an existing absolute directory")
    if TIMER_UNIT.fullmatch(args.timer_unit) is None:
        parser.error("--timer-unit must be a dedicated Horizon performance guard timer")
    for unit in args.units:
        if not unit.startswith(UNIT_PREFIXES) or not unit.endswith(".service"):
            parser.error(f"invalid canary unit: {unit}")
    expected_invocations: dict[str, str] = {}
    for binding in args.expected_invocation:
        unit, separator, invocation_id = binding.partition("=")
        if (
            not separator
            or unit not in args.units
            or re.fullmatch(r"[0-9a-f]{32}", invocation_id) is None
            or unit in expected_invocations
        ):
            parser.error(
                "--expected-invocation must uniquely bind a guarded unit to a "
                "32-hex invocation ID"
            )
        expected_invocations[unit] = invocation_id
    if expected_invocations and set(expected_invocations) != set(args.units):
        parser.error("--expected-invocation must bind every guarded unit")
    target_stop_intents: dict[str, Path] = {}
    for binding in args.target_stop_intent:
        unit, separator, raw_path = binding.partition("=")
        path = Path(raw_path)
        if (
            not separator
            or unit not in args.units
            or not path.is_absolute()
            or unit in target_stop_intents
        ):
            parser.error(
                "--target-stop-intent must uniquely bind a guarded unit to an "
                "absolute path"
            )
        target_stop_intents[unit] = path
    if target_stop_intents:
        if set(target_stop_intents) != set(args.units):
            parser.error("--target-stop-intent must bind every guarded unit")
        if not expected_invocations:
            parser.error("target-stop intents require exact invocation bindings")
        if (
            args.target_slot is None
            or args.target_slot < 1
            or args.target_stop_intent_uid is None
            or args.target_stop_intent_uid < 0
        ):
            parser.error(
                "target-stop intents require a positive target/drain timeout and "
                "a non-negative owner UID"
            )
        if len(set(target_stop_intents.values())) != len(target_stop_intents):
            parser.error("target-stop intent paths must be distinct")
    elif args.target_slot is not None or args.target_stop_intent_uid is not None:
        parser.error("target-stop intent options require --target-stop-intent")
    args.expected_invocations = expected_invocations
    args.target_stop_intents = target_stop_intents
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    observed_at = time.time()
    free_bytes = available_bytes(args.filesystem)
    samples = [sample_unit(unit) for unit in args.units]
    workerless_drain_units: set[str] = set()
    intent_evidence: dict[str, dict[str, Any]] = {}
    intent_errors: list[str] = []
    for sample in samples:
        path = args.target_stop_intents.get(sample.unit)
        if path is None:
            continue
        if sample.active_state not in ("active", "activating"):
            intent_evidence[sample.unit] = {
                "path": str(path),
                "status": "unit-not-active",
            }
            continue
        try:
            intent = load_target_stop_intent(
                path,
                required_uid=args.target_stop_intent_uid,
                expected_invocation=args.expected_invocations[sample.unit],
                expected_target_slot=args.target_slot,
                observed_unix_seconds=observed_at,
                maximum_age_seconds=args.maximum_target_drain_seconds,
            )
        except FileNotFoundError:
            intent_evidence[sample.unit] = {
                "path": str(path),
                "status": "absent-before-target",
            }
            if sample.worker_count == 0:
                intent_errors.append(
                    f"{sample.unit} has no target-stop intent while workerless"
                )
        except (OSError, ValueError) as error:
            intent_evidence[sample.unit] = {
                "path": str(path),
                "status": "invalid",
                "error": str(error),
            }
            intent_errors.append(f"{sample.unit} target-stop intent is invalid")
        else:
            intent_evidence[sample.unit] = {
                "path": str(path),
                "status": "valid-post-target-drain",
                "written_unix_seconds": intent["written_unix_seconds"],
                "observed_stop_slot": intent["observed_stop_slot"],
            }
            if sample.worker_count == 0:
                workerless_drain_units.add(sample.unit)
    reasons = trip_reasons(
        free_bytes,
        args.minimum_free_bytes,
        args.maximum_worker_vmas,
        samples,
        args.expected_invocations,
        workerless_drain_units,
    )
    reasons.extend(intent_errors)
    report: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "observed_unix_seconds": observed_at,
        "filesystem": str(args.filesystem),
        "available_bytes": free_bytes,
        "minimum_free_bytes": args.minimum_free_bytes,
        "maximum_worker_vmas": args.maximum_worker_vmas,
        "expected_invocations": args.expected_invocations,
        "timer_unit": args.timer_unit,
        "samples": [asdict(sample) for sample in samples],
        "target_stop_intents": intent_evidence,
        "trip_reasons": reasons,
    }
    print(json.dumps(report, sort_keys=True), flush=True)
    if not reasons:
        if not cohort_running(samples):
            print(
                json.dumps(
                    {"cohort_finished": True, "timer_stop": stop_timer(args.timer_unit)},
                    sort_keys=True,
                ),
                flush=True,
            )
        return 0
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(observed_at))
    write_json_noclobber(args.receipt_directory / f"guard-trip-{stamp}.json", report)
    stop_outcomes = stop_active_units(samples)
    print(
        json.dumps(
            {
                "stop_outcomes": stop_outcomes,
                "timer_stop": stop_timer(args.timer_unit),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
