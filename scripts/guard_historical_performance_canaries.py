#!/usr/bin/env python3
"""Guard a private historical performance cohort with fail-closed tripwires."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Sequence


RECEIPT_SCHEMA = "jetstreamer-historical-performance-guard-trip-v1"
UNIT_PREFIX = "horizon-perf-epoch"
TIMER_UNIT = "horizon-perf-epoch202-guard.timer"


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


def worker_vmas(control_group: str, cgroup_root: Path = Path("/sys/fs/cgroup")) -> tuple[int | None, int | None]:
    if not control_group.startswith("/") or ".." in Path(control_group).parts:
        raise ValueError(f"invalid control group: {control_group}")
    group = cgroup_root / control_group.lstrip("/")
    if not group.is_dir():
        return None, None
    candidates: set[int] = set()
    for process_file in group.rglob("cgroup.procs"):
        try:
            candidates.update(int(value) for value in process_file.read_text().split())
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
    for pid in sorted(candidates):
        command = read_proc_bytes(Path("/proc") / str(pid) / "cmdline")
        if not command or b"jetstreamer-historical-worker-v1-6-16" not in command:
            continue
        maps = read_proc_bytes(Path("/proc") / str(pid) / "maps")
        if maps is not None:
            return pid, maps.count(b"\n")
    return None, None


def sample_unit(unit: str) -> UnitSample:
    if not unit.startswith(UNIT_PREFIX) or not unit.endswith(".service"):
        raise ValueError(f"invalid canary unit: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts,ControlGroup",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    fields = parse_systemctl_show(completed.stdout)
    control_group = fields.get("ControlGroup", "")
    worker_pid, vmas = worker_vmas(control_group) if control_group else (None, None)
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
) -> list[str]:
    reasons: list[str] = []
    if free_bytes < minimum_free_bytes:
        reasons.append(
            f"available bytes {free_bytes} below floor {minimum_free_bytes}"
        )
    for sample in samples:
        if sample.load_state != "loaded":
            reasons.append(f"{sample.unit} is not loaded: {sample.load_state}")
        if sample.active_state == "failed" or sample.result not in ("success", ""):
            reasons.append(
                f"{sample.unit} terminal failure: state={sample.active_state} result={sample.result}"
            )
        if sample.restarts:
            reasons.append(f"{sample.unit} unexpectedly restarted {sample.restarts} time(s)")
        if sample.worker_vmas is not None and sample.worker_vmas > maximum_worker_vmas:
            reasons.append(
                f"{sample.unit} worker VMAs {sample.worker_vmas} exceed {maximum_worker_vmas}"
            )
    return reasons


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


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filesystem", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    parser.add_argument("--maximum-worker-vmas", type=int, required=True)
    parser.add_argument("--receipt-directory", type=Path, required=True)
    parser.add_argument("--timer-unit", required=True, choices=[TIMER_UNIT])
    parser.add_argument("units", nargs="+")
    args = parser.parse_args(argv)
    if not args.filesystem.is_absolute() or not args.filesystem.is_dir():
        parser.error("--filesystem must be an existing absolute directory")
    if args.minimum_free_bytes < 1 or args.maximum_worker_vmas < 1:
        parser.error("tripwire values must be positive")
    if not args.receipt_directory.is_absolute() or not args.receipt_directory.is_dir():
        parser.error("--receipt-directory must be an existing absolute directory")
    for unit in args.units:
        if not unit.startswith(UNIT_PREFIX) or not unit.endswith(".service"):
            parser.error(f"invalid canary unit: {unit}")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    observed_at = time.time()
    free_bytes = available_bytes(args.filesystem)
    samples = [sample_unit(unit) for unit in args.units]
    reasons = trip_reasons(
        free_bytes,
        args.minimum_free_bytes,
        args.maximum_worker_vmas,
        samples,
    )
    report: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "observed_unix_seconds": observed_at,
        "filesystem": str(args.filesystem),
        "available_bytes": free_bytes,
        "minimum_free_bytes": args.minimum_free_bytes,
        "maximum_worker_vmas": args.maximum_worker_vmas,
        "timer_unit": args.timer_unit,
        "samples": [asdict(sample) for sample in samples],
        "trip_reasons": reasons,
    }
    print(json.dumps(report, sort_keys=True), flush=True)
    if not reasons:
        return 0
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(observed_at))
    write_json_noclobber(args.receipt_directory / f"guard-trip-{stamp}.json", report)
    stop_outcomes = stop_active_units(samples)
    timer_stop = subprocess.run(
        ["systemctl", "stop", args.timer_unit],
        capture_output=True,
        text=True,
    )
    print(
        json.dumps(
            {
                "stop_outcomes": stop_outcomes,
                "timer_stop": {
                    "unit": args.timer_unit,
                    "return_code": timer_stop.returncode,
                    "stderr": timer_stop.stderr.strip(),
                },
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
