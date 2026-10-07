#!/usr/bin/env python3
"""Run a private historical replay canary until an observed target slot."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Any, Sequence


PROGRESS_SLOT = re.compile(r"\bprogress slot ([0-9]+)/")
RECEIPT_SCHEMA = "jetstreamer-historical-performance-canary-v1"


def parse_progress_slot(line: str) -> int | None:
    match = PROGRESS_SLOT.search(line)
    return int(match.group(1)) if match else None


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
                slot = parse_progress_slot(line)
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
        "elapsed_seconds": time.monotonic() - started_monotonic,
        "child_pid": process.pid,
        "child_return_code": return_code,
        "external_signal": external_signal,
        "command": list(command),
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
