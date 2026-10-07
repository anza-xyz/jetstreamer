#!/usr/bin/env python3
"""Run a private historical replay canary until an observed target slot."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import resource
import signal
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence


PROGRESS = re.compile(
    r"\bprogress slot (?P<slot>[0-9]+)/[0-9]+.*?"
    r"\btxs=(?P<transactions>[0-9]+)\s+accounts=(?P<account_updates>[0-9]+)"
)
PROGRESS_SLOT = re.compile(r"\bprogress slot ([0-9]+)/")
RECEIPT_SCHEMA = "jetstreamer-historical-performance-canary-v2"


def parse_progress(line: str) -> dict[str, int] | None:
    match = PROGRESS.search(line)
    if not match:
        return None
    return {name: int(value) for name, value in match.groupdict().items()}


def parse_progress_slot(line: str) -> int | None:
    match = PROGRESS_SLOT.search(line)
    return int(match.group(1)) if match else None


def read_integer(path: Path) -> int | None:
    try:
        value = path.read_text().strip()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    if not value or value == "max":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def read_counter_file(path: Path) -> dict[str, int]:
    counters: dict[str, int] = {}
    try:
        lines = path.read_text().splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return counters
    for line in lines:
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            counters[fields[0]] = int(fields[1])
        except ValueError:
            continue
    return counters


def read_pressure_file(path: Path) -> dict[str, int]:
    counters: dict[str, int] = {}
    try:
        lines = path.read_text().splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return counters
    for line in lines:
        fields = line.split()
        if not fields:
            continue
        for field in fields[1:]:
            if not field.startswith("total="):
                continue
            try:
                counters[f"{fields[0]}_total_usec"] = int(field.split("=", 1)[1])
            except ValueError:
                pass
    return counters


def read_io_stat(path: Path) -> dict[str, int]:
    totals: dict[str, int] = {}
    try:
        lines = path.read_text().splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return totals
    for line in lines:
        fields = line.split()
        for field in fields[1:]:
            if "=" not in field:
                continue
            name, raw_value = field.split("=", 1)
            try:
                totals[name] = totals.get(name, 0) + int(raw_value)
            except ValueError:
                continue
    return totals


def current_cgroup_path(
    membership: Path = Path("/proc/self/cgroup"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Path | None:
    try:
        lines = membership.read_text().splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    for line in lines:
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        hierarchy, controllers, relative = fields
        if hierarchy != "0" or controllers:
            continue
        root = cgroup_root.resolve()
        candidate = (root / relative.lstrip("/")).resolve()
        if candidate == root or root in candidate.parents:
            return candidate
    return None


def cgroup_snapshot(cgroup: Path | None) -> dict[str, Any]:
    if cgroup is None:
        return {"available": False}
    gauges = {
        name: read_integer(cgroup / name)
        for name in ("memory.current", "memory.peak", "pids.current", "pids.peak")
    }
    counters: dict[str, int] = {}
    for prefix, filename in (
        ("cpu", "cpu.stat"),
        ("memory_stat", "memory.stat"),
        ("memory_events", "memory.events"),
    ):
        counters.update(
            {
                f"{prefix}.{key}": value
                for key, value in read_counter_file(cgroup / filename).items()
            }
        )
    counters.update(
        {
            f"memory_pressure.{key}": value
            for key, value in read_pressure_file(cgroup / "memory.pressure").items()
        }
    )
    counters.update(
        {
            f"io.{key}": value
            for key, value in read_io_stat(cgroup / "io.stat").items()
        }
    )
    return {
        "available": True,
        "path": str(cgroup),
        "gauges": gauges,
        "counters": counters,
    }


def counter_deltas(
    before: Mapping[str, int], after: Mapping[str, int]
) -> dict[str, int]:
    return {
        key: after[key] - before[key]
        for key in sorted(before.keys() & after.keys())
        if after[key] >= before[key]
    }


def available_bytes(path: Path = Path("/")) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def usage_fields(usage: resource.struct_rusage) -> dict[str, int | float]:
    return {
        "user_cpu_seconds": usage.ru_utime,
        "system_cpu_seconds": usage.ru_stime,
        "maximum_resident_set_kib": usage.ru_maxrss,
        "minor_faults": usage.ru_minflt,
        "major_faults": usage.ru_majflt,
        "input_operations": usage.ru_inblock,
        "output_operations": usage.ru_oublock,
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
    }


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise ValueError("receipt path must be absolute")
    parent = path.parent.resolve(strict=True)
    if parent.stat().st_mode & 0o077:
        raise ValueError(f"receipt directory is not owner-only: {parent}")
    canonical = parent / path.name
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(canonical, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
    finally:
        os.close(descriptor)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def normalized_exit_code(return_code: int) -> int:
    if return_code >= 0:
        return return_code
    return min(255, 128 + abs(return_code))


def run_canary(target_slot: int, receipt: Path, command: Sequence[str]) -> int:
    if target_slot < 1:
        raise ValueError("target slot must be positive")
    if not command:
        raise ValueError("missing canary command")
    executable = Path(command[0])
    if not executable.is_absolute() or not executable.is_file():
        raise ValueError("canary executable must be an absolute regular file")
    if not os.access(executable, os.X_OK):
        raise ValueError("canary executable is not executable")
    if receipt.exists():
        raise FileExistsError(f"refusing to overwrite receipt: {receipt}")

    cgroup = current_cgroup_path()
    cgroup_before = cgroup_snapshot(cgroup)
    child_usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    filesystem_available_before = available_bytes()
    started_unix = time.time()
    started_monotonic = time.monotonic()
    process = subprocess.Popen(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    external_signal: int | None = None
    target_reached = False
    observed_stop_slot: int | None = None
    first_progress: dict[str, int | float] | None = None
    final_progress: dict[str, int | float] | None = None

    def forward_signal(signum: int, _frame: object) -> None:
        nonlocal external_signal
        external_signal = signum
        if process.poll() is None:
            process.send_signal(signum)

    previous_handlers = {
        signum: signal.signal(signum, forward_signal)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        assert process.stdout is not None
        with process.stdout:
            for line in process.stdout:
                print(line, end="", flush=True)
                progress = parse_progress(line)
                slot = parse_progress_slot(line)
                if progress is not None:
                    sampled = {**progress, "observed_unix_seconds": time.time()}
                    if first_progress is None:
                        first_progress = sampled
                    final_progress = sampled
                if slot is not None and (
                    observed_stop_slot is None or slot > observed_stop_slot
                ):
                    observed_stop_slot = slot
                if not target_reached and slot is not None and slot >= target_slot:
                    target_reached = True
                    process.send_signal(signal.SIGINT)
        return_code = process.wait()
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    completed_unix = time.time()
    elapsed_seconds = time.monotonic() - started_monotonic
    cgroup_after = cgroup_snapshot(cgroup)
    child_usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    before_counters = cgroup_before.get("counters", {})
    after_counters = cgroup_after.get("counters", {})
    cgroup_delta = counter_deltas(before_counters, after_counters)
    child_usage_before_fields = usage_fields(child_usage_before)
    child_usage_after_fields = usage_fields(child_usage_after)
    child_usage_delta = {
        key: after - child_usage_before_fields[key]
        for key, after in child_usage_after_fields.items()
        if isinstance(after, (int, float))
        and after >= child_usage_before_fields[key]
    }
    progress_rates: dict[str, float | int] | None = None
    if first_progress is not None and final_progress is not None:
        progress_elapsed = (
            final_progress["observed_unix_seconds"]
            - first_progress["observed_unix_seconds"]
        )
        if progress_elapsed > 0:
            progress_rates = {
                "elapsed_seconds": progress_elapsed,
                "slots_per_second": (
                    final_progress["slot"] - first_progress["slot"]
                )
                / progress_elapsed,
                "transactions_per_second": (
                    final_progress["transactions"] - first_progress["transactions"]
                )
                / progress_elapsed,
                "account_updates_per_second": (
                    final_progress["account_updates"]
                    - first_progress["account_updates"]
                )
                / progress_elapsed,
            }
    cpu_usage_usec = cgroup_delta.get("cpu.usage_usec")
    payload = {
        "schema": RECEIPT_SCHEMA,
        "target_slot": target_slot,
        "target_reached": target_reached,
        "observed_stop_slot": observed_stop_slot,
        "observed_overshoot_slots": (
            max(0, observed_stop_slot - target_slot)
            if observed_stop_slot is not None
            else None
        ),
        "started_unix_seconds": started_unix,
        "completed_unix_seconds": completed_unix,
        "elapsed_seconds": elapsed_seconds,
        "child_pid": process.pid,
        "child_return_code": return_code,
        "external_signal": external_signal,
        "command": list(command),
        "first_progress": first_progress,
        "final_progress": final_progress,
        "progress_rates": progress_rates,
        "cgroup_before": cgroup_before,
        "cgroup_after": cgroup_after,
        "cgroup_counter_deltas": cgroup_delta,
        "effective_cpu_cores": (
            cpu_usage_usec / 1_000_000 / elapsed_seconds
            if cpu_usage_usec is not None and elapsed_seconds > 0
            else None
        ),
        "child_resource_usage_delta": child_usage_delta,
        "filesystem_available_bytes_before": filesystem_available_before,
        "filesystem_available_bytes_after": available_bytes(),
    }
    write_json_noclobber(receipt, payload)
    return normalized_exit_code(return_code)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-slot", type=int, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("missing command after --")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        return run_canary(args.target_slot, args.receipt, args.command)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
