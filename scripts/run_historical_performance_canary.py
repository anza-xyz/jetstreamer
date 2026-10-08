#!/usr/bin/env python3
"""Run a private historical replay canary until an observed target slot."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import resource
import signal
import stat
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
APPENDVEC_FILE = re.compile(r"^(?P<slot>[0-9]+)\.(?P<store_id>[0-9]+)$")


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


def replay_scratch_from_command(command: Sequence[str]) -> Path | None:
    values = [
        item.split("=", 1)[1]
        for item in command
        if item.startswith("--replay-scratch=")
    ]
    if not values:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError(
            "canary command must contain at most one nonempty --replay-scratch"
        )
    scratch = Path(values[0])
    if not scratch.is_absolute():
        raise ValueError("--replay-scratch must be absolute")
    return scratch


def tree_statistics(
    root: Path, collect_appendvec_slots: bool = False
) -> dict[str, object]:
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"scratch measurement root is not a real directory: {root}")
    device = metadata.st_dev
    physical_bytes = metadata.st_blocks * 512
    apparent_bytes = metadata.st_size
    files = 0
    directories = 1
    symlinks = 0
    file_size_histogram: Counter[str] = Counter()
    appendvec_stores_per_slot: Counter[int] = Counter()
    unrecognized_regular_files = 0
    for directory, names, filenames in os.walk(root, topdown=True, followlinks=False):
        directory_path = Path(directory)
        retained_names: list[str] = []
        for name in names:
            entry = directory_path / name
            entry_metadata = entry.lstat()
            if stat.S_ISLNK(entry_metadata.st_mode):
                symlinks += 1
                continue
            if not stat.S_ISDIR(entry_metadata.st_mode) or entry_metadata.st_dev != device:
                continue
            directories += 1
            physical_bytes += entry_metadata.st_blocks * 512
            apparent_bytes += entry_metadata.st_size
            retained_names.append(name)
        names[:] = retained_names
        for name in filenames:
            entry = directory_path / name
            entry_metadata = entry.lstat()
            if stat.S_ISLNK(entry_metadata.st_mode):
                symlinks += 1
                continue
            if not stat.S_ISREG(entry_metadata.st_mode) or entry_metadata.st_dev != device:
                continue
            files += 1
            physical_bytes += entry_metadata.st_blocks * 512
            apparent_bytes += entry_metadata.st_size
            size = entry_metadata.st_size
            if size <= 4 * 1024 * 1024:
                bucket = "le_4_mib"
            elif size <= 8 * 1024 * 1024:
                bucket = "gt_4_le_8_mib"
            elif size <= 16 * 1024 * 1024:
                bucket = "gt_8_le_16_mib"
            elif size <= 64 * 1024 * 1024:
                bucket = "gt_16_le_64_mib"
            elif size <= 256 * 1024 * 1024:
                bucket = "gt_64_le_256_mib"
            elif size <= 1024 * 1024 * 1024:
                bucket = "gt_256_mib_le_1_gib"
            else:
                bucket = "gt_1_gib"
            file_size_histogram[bucket] += 1
            if collect_appendvec_slots:
                match = APPENDVEC_FILE.fullmatch(name)
                if match is None:
                    unrecognized_regular_files += 1
                else:
                    appendvec_stores_per_slot[int(match.group("slot"))] += 1
    result: dict[str, object] = {
        "path": str(root),
        "device": device,
        "physical_bytes": physical_bytes,
        "apparent_bytes": apparent_bytes,
        "regular_files": files,
        "directories": directories,
        "symlinks_not_followed": symlinks,
        "file_size_histogram": dict(sorted(file_size_histogram.items())),
    }
    if collect_appendvec_slots:
        store_count_to_slot_count = Counter(appendvec_stores_per_slot.values())
        slot_count = len(appendvec_stores_per_slot)
        appendvec_files = sum(appendvec_stores_per_slot.values())
        result["appendvec_store_fanout"] = {
            "recognized_appendvec_files": appendvec_files,
            "unrecognized_regular_files": unrecognized_regular_files,
            "slots_with_stores": slot_count,
            "minimum_stores_per_slot": min(
                appendvec_stores_per_slot.values(), default=None
            ),
            "maximum_stores_per_slot": max(
                appendvec_stores_per_slot.values(), default=None
            ),
            "mean_stores_per_slot": (
                appendvec_files / slot_count if slot_count else None
            ),
            "store_count_to_slot_count": {
                str(count): store_count_to_slot_count[count]
                for count in sorted(store_count_to_slot_count)
            },
        }
    return result


def snapshot_replay_scratch(scratch: Path) -> dict[str, object]:
    matches = [
        item
        for item in scratch.glob(
            ".historical-runtime/*/jetstreamer-historical-*/accounts-state"
        )
        if item.is_dir() and not item.is_symlink()
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one live accounts-state beneath {scratch}, found {len(matches)}"
        )
    return {
        "captured_unix_seconds": time.time(),
        "scratch": tree_statistics(scratch),
        "accounts_state": tree_statistics(matches[0], collect_appendvec_slots=True),
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


def runner_exit_code(target_reached: bool, child_return_code: int) -> int:
    """Normalize the legacy worker's controlled post-target shutdown.

    The v1.6 historical worker can acknowledge SIGINT, close its ready-entry
    channel, and exit 1 even though the bounded target was already observed.
    Keep the actual child code in the receipt, but let systemd regard that
    narrowly defined post-target outcome as a completed diagnostic canary.
    """
    if target_reached and child_return_code in (0, 1):
        return 0
    return normalized_exit_code(child_return_code)


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
    systemd_invocation_id = os.environ.get("INVOCATION_ID")
    started_unix = time.time()
    started_monotonic = time.monotonic()
    process = subprocess.Popen(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    external_signal: int | None = None
    target_reached = False
    observed_stop_slot: int | None = None
    first_progress: dict[str, int | float] | None = None
    final_progress: dict[str, int | float] | None = None
    replay_scratch = replay_scratch_from_command(command)
    scratch_snapshot: dict[str, object] | None = None
    scratch_snapshot_error: str | None = None

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
                    if replay_scratch is not None:
                        os.killpg(process.pid, signal.SIGSTOP)
                        try:
                            scratch_snapshot = snapshot_replay_scratch(replay_scratch)
                        except (OSError, ValueError) as error:
                            scratch_snapshot_error = str(error)
                        finally:
                            os.killpg(process.pid, signal.SIGCONT)
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
    normalized_runner_exit_code = runner_exit_code(target_reached, return_code)
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
        "controlled_stop_signal": signal.SIGINT if target_reached else None,
        "runner_return_code": normalized_runner_exit_code,
        "scratch_snapshot": scratch_snapshot,
        "scratch_snapshot_error": scratch_snapshot_error,
        "external_signal": external_signal,
        "command": list(command),
        "systemd_invocation_id": systemd_invocation_id,
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
    if target_reached and replay_scratch is not None and scratch_snapshot is None:
        return 2
    return normalized_runner_exit_code


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
