#!/usr/bin/env python3
"""Stop one exact historical replay before it crosses a filesystem reserve."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any, Sequence


INTENT_SCHEMA = "jetstreamer-historical-replay-reserve-stop-intent-v1"
COMPLETION_SCHEMA = "jetstreamer-historical-replay-reserve-stop-completion-v1"
UNIT_NAME = re.compile(r"^horizon-qualify-[A-Za-z0-9@_.-]+\.service$")
TIMER_NAME = re.compile(r"^horizon-monitor-recurring-[A-Za-z0-9@_.-]+\.timer$")
INVOCATION_ID = re.compile(r"^[0-9a-f]{32}$")


class GuardError(RuntimeError):
    """A reserve guard invariant was not satisfied."""


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


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    return fields


def sample_unit(unit: str) -> UnitSample:
    completed = subprocess.run(
        [
            "systemctl",
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


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def require_private_parent(path: Path) -> Path:
    if not path.is_absolute() or path.name in ("", ".", ".."):
        raise GuardError("receipt path must be an absolute file path")
    parent = path.parent.resolve(strict=True)
    metadata = parent.stat()
    if metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
        raise GuardError(f"receipt directory must be owner-only: {parent}")
    return parent


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    parent = require_private_parent(path)
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
    fsync_directory(parent)


def read_bound_intent(
    path: Path,
    unit: str,
    invocation_id: str,
    filesystem: Path,
    minimum_free_bytes: int,
) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        raw = path.read_text()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise GuardError(f"cannot read reserve-stop intent {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o077
    ):
        raise GuardError(f"reserve-stop intent has unsafe identity: {path}")
    expected = {
        "schema": INTENT_SCHEMA,
        "unit": unit,
        "expected_invocation_id": invocation_id,
        "filesystem": str(filesystem),
        "minimum_free_bytes": minimum_free_bytes,
    }
    if not isinstance(payload, dict) or any(payload.get(key) != value for key, value in expected.items()):
        raise GuardError("reserve-stop intent does not match the sealed guard")
    return payload


def read_bound_completion(
    path: Path,
    intent_path: Path,
    unit: str,
    invocation_id: str,
    filesystem: Path,
    minimum_free_bytes: int,
) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        raw = path.read_text()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise GuardError(f"cannot read reserve-stop completion {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o077
    ):
        raise GuardError(f"reserve-stop completion has unsafe identity: {path}")
    expected = {
        "schema": COMPLETION_SCHEMA,
        "intent_receipt": str(intent_path),
        "unit": unit,
        "expected_invocation_id": invocation_id,
        "filesystem": str(filesystem),
        "minimum_free_bytes": minimum_free_bytes,
        "scratch_deleted": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    if not isinstance(payload, dict) or any(payload.get(key) != value for key, value in expected.items()):
        raise GuardError("reserve-stop completion does not match the sealed guard")
    return payload


def run_systemctl(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", *arguments],
        capture_output=True,
        text=True,
    )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--expected-invocation-id", required=True)
    parser.add_argument("--filesystem", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    parser.add_argument("--monitor-timer", required=True)
    parser.add_argument("--intent-receipt", type=Path, required=True)
    parser.add_argument("--completion-receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    if UNIT_NAME.fullmatch(args.unit) is None:
        parser.error("--unit must be one historical qualification service")
    if INVOCATION_ID.fullmatch(args.expected_invocation_id) is None:
        parser.error("--expected-invocation-id must be 32 lowercase hexadecimal characters")
    if TIMER_NAME.fullmatch(args.monitor_timer) is None:
        parser.error("--monitor-timer must be one recurring Horizon monitor timer")
    if not args.filesystem.is_absolute() or not args.filesystem.is_dir():
        parser.error("--filesystem must be an existing absolute directory")
    if args.minimum_free_bytes < 1:
        parser.error("--minimum-free-bytes must be positive")
    try:
        require_private_parent(args.intent_receipt)
        require_private_parent(args.completion_receipt)
    except GuardError as error:
        parser.error(str(error))
    if args.intent_receipt == args.completion_receipt:
        parser.error("intent and completion receipts must be distinct")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.completion_receipt.exists():
        read_bound_completion(
            args.completion_receipt,
            args.intent_receipt,
            args.unit,
            args.expected_invocation_id,
            args.filesystem,
            args.minimum_free_bytes,
        )
        timer_result = run_systemctl(["stop", args.monitor_timer])
        if timer_result.returncode != 0:
            raise GuardError(f"failed to stop monitor timer: {timer_result.stderr.strip()}")
        return 0

    free_bytes = available_bytes(args.filesystem)
    sample = sample_unit(args.unit)
    if sample.load_state != "loaded":
        raise GuardError(f"guarded unit is not loaded: {sample.load_state}")
    if sample.invocation_id != args.expected_invocation_id:
        raise GuardError("guarded unit invocation changed; refusing to stop an unbound process")
    if sample.restarts != 0:
        raise GuardError(f"guarded unit unexpectedly restarted {sample.restarts} time(s)")

    intent_exists = args.intent_receipt.exists()
    if not intent_exists and sample.active_state not in ("active", "activating"):
        print(
            json.dumps(
                {
                    "available_bytes": free_bytes,
                    "minimum_free_bytes": args.minimum_free_bytes,
                    "sample": asdict(sample),
                    "trip": False,
                    "unit_running": False,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    if not intent_exists and free_bytes >= args.minimum_free_bytes:
        print(
            json.dumps(
                {
                    "available_bytes": free_bytes,
                    "minimum_free_bytes": args.minimum_free_bytes,
                    "sample": asdict(sample),
                    "trip": False,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0

    if intent_exists:
        intent = read_bound_intent(
            args.intent_receipt,
            args.unit,
            args.expected_invocation_id,
            args.filesystem,
            args.minimum_free_bytes,
        )
    else:
        intent = {
            "schema": INTENT_SCHEMA,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "unit": args.unit,
            "expected_invocation_id": args.expected_invocation_id,
            "filesystem": str(args.filesystem),
            "available_bytes": free_bytes,
            "minimum_free_bytes": args.minimum_free_bytes,
            "sample": asdict(sample),
            "reason": "actual available bytes fell below the sealed reserve stop floor",
            "remote_mutations": False,
            "r2_mutations": False,
        }
        write_json_noclobber(args.intent_receipt, intent)

    stop_result: dict[str, Any] | None = None
    if sample.active_state in ("active", "activating"):
        completed = run_systemctl(["stop", args.unit])
        stop_result = {
            "return_code": completed.returncode,
            "stderr": completed.stderr.strip(),
        }
        if completed.returncode != 0:
            raise GuardError(f"failed to stop guarded unit: {completed.stderr.strip()}")

    final_sample = sample_unit(args.unit)
    if final_sample.active_state in ("active", "activating") or final_sample.main_pid != 0:
        raise GuardError("guarded unit remained active after reserve stop")
    completion = {
        "schema": COMPLETION_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "intent_receipt": str(args.intent_receipt),
        "unit": args.unit,
        "expected_invocation_id": args.expected_invocation_id,
        "filesystem": str(args.filesystem),
        "available_bytes_after_stop": available_bytes(args.filesystem),
        "minimum_free_bytes": args.minimum_free_bytes,
        "final_sample": asdict(final_sample),
        "stop_result": stop_result,
        "scratch_deleted": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_json_noclobber(args.completion_receipt, completion)
    timer_result = run_systemctl(["stop", args.monitor_timer])
    print(
        json.dumps(
            {
                "completion": completion,
                "intent": intent,
                "monitor_timer_stop_return_code": timer_result.returncode,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GuardError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
