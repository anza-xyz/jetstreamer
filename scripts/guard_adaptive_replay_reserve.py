#!/usr/bin/env python3
"""Stop one adaptive replay and its controller at a filesystem reserve."""

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


INTENT_SCHEMA = "jetstreamer-adaptive-replay-reserve-stop-intent-v1"
COMPLETION_SCHEMA = "jetstreamer-adaptive-replay-reserve-stop-completion-v1"
CONTROLLER_UNIT = re.compile(
    r"^jetstreamer-(?:epoch|epochs)[A-Za-z0-9@_.-]*controller[A-Za-z0-9@_.-]*\.service$"
)
PRODUCER_UNIT = re.compile(r"^jetstreamer-root-sweep-[A-Za-z0-9@_.-]+\.service$")
TIMER_UNIT = re.compile(r"^jetstreamer-adaptive-reserve-[A-Za-z0-9@_.-]+\.timer$")
INVOCATION_ID = re.compile(r"^[0-9a-f]{32}$")


class GuardError(RuntimeError):
    """An adaptive reserve-guard invariant was not satisfied."""


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
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    descriptor = os.open(
        parent / path.name,
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


def receipt_binding(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "controller_unit": args.controller_unit,
        "expected_controller_invocation_id": args.expected_controller_invocation_id,
        "producer_unit": args.producer_unit,
        "expected_producer_invocation_id": args.expected_producer_invocation_id,
        "filesystem": str(args.filesystem),
        "minimum_free_bytes": args.minimum_free_bytes,
    }


def read_bound_receipt(
    path: Path,
    schema: str,
    args: argparse.Namespace,
    *,
    intent_path: Path | None = None,
) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise GuardError(f"cannot read reserve-stop receipt {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o077
    ):
        raise GuardError(f"reserve-stop receipt has unsafe identity: {path}")
    expected: dict[str, Any] = {"schema": schema, **receipt_binding(args)}
    if intent_path is not None:
        expected.update(
            {
                "intent_receipt": str(intent_path),
                "scratch_deleted": False,
                "remote_mutations": False,
                "r2_mutations": False,
            }
        )
    if not isinstance(payload, dict) or any(
        payload.get(key) != value for key, value in expected.items()
    ):
        raise GuardError("reserve-stop receipt does not match the sealed guard")
    return payload


def run_systemctl(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/usr/bin/systemctl", *arguments],
        capture_output=True,
        text=True,
    )


def require_root() -> None:
    if os.geteuid() != 0:
        raise GuardError("adaptive reserve guard must run as root")


def running(sample: UnitSample) -> bool:
    return sample.active_state in {"active", "activating"}


def require_original_running(sample: UnitSample, invocation_id: str, label: str) -> None:
    if sample.load_state != "loaded":
        raise GuardError(f"{label} unit is not loaded: {sample.load_state}")
    if sample.invocation_id != invocation_id:
        raise GuardError(f"{label} invocation changed; refusing to stop an unbound process")
    if sample.restarts != 0:
        raise GuardError(f"{label} unexpectedly restarted {sample.restarts} time(s)")
    if not running(sample) or sample.main_pid <= 0:
        raise GuardError(f"{label} is not running")


def require_bound_or_stopped(sample: UnitSample, invocation_id: str, label: str) -> None:
    if running(sample):
        require_original_running(sample, invocation_id, label)
        return
    if sample.main_pid != 0:
        raise GuardError(f"{label} is inactive but still has main pid {sample.main_pid}")
    # A collected transient producer is reported as not-found with an empty
    # invocation ID. A persistent controller normally retains its last ID.
    if sample.invocation_id not in {"", invocation_id}:
        raise GuardError(f"{label} was replaced after the reserve-stop intent")


def stop_bound_unit(
    sample: UnitSample,
    invocation_id: str,
    label: str,
) -> dict[str, Any] | None:
    require_bound_or_stopped(sample, invocation_id, label)
    if not running(sample):
        return None
    completed = run_systemctl(["stop", sample.unit])
    result = {
        "return_code": completed.returncode,
        "stderr": completed.stderr.strip(),
    }
    if completed.returncode != 0:
        raise GuardError(f"failed to stop {label}: {completed.stderr.strip()}")
    return result


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--controller-unit", required=True)
    parser.add_argument("--expected-controller-invocation-id", required=True)
    parser.add_argument("--producer-unit", required=True)
    parser.add_argument("--expected-producer-invocation-id", required=True)
    parser.add_argument("--filesystem", type=Path, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, required=True)
    parser.add_argument("--guard-timer", required=True)
    parser.add_argument("--intent-receipt", type=Path, required=True)
    parser.add_argument("--completion-receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        require_root()
    except GuardError as error:
        parser.error(str(error))
    if CONTROLLER_UNIT.fullmatch(args.controller_unit) is None:
        parser.error("--controller-unit must name one adaptive controller service")
    if PRODUCER_UNIT.fullmatch(args.producer_unit) is None:
        parser.error("--producer-unit must name one adaptive root-sweep service")
    if TIMER_UNIT.fullmatch(args.guard_timer) is None:
        parser.error("--guard-timer must name one adaptive reserve timer")
    for value, option in (
        (args.expected_controller_invocation_id, "--expected-controller-invocation-id"),
        (args.expected_producer_invocation_id, "--expected-producer-invocation-id"),
    ):
        if INVOCATION_ID.fullmatch(value) is None:
            parser.error(f"{option} must be 32 lowercase hexadecimal characters")
    try:
        filesystem = args.filesystem.resolve(strict=True)
    except OSError as error:
        parser.error(f"--filesystem is unavailable: {error}")
    if filesystem != args.filesystem or not filesystem.is_dir():
        parser.error("--filesystem must be an exact existing absolute directory")
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
        read_bound_receipt(
            args.completion_receipt,
            COMPLETION_SCHEMA,
            args,
            intent_path=args.intent_receipt,
        )
        timer_result = run_systemctl(["stop", args.guard_timer])
        if timer_result.returncode != 0:
            raise GuardError(f"failed to stop guard timer: {timer_result.stderr.strip()}")
        return 0

    free_bytes = available_bytes(args.filesystem)
    intent_exists = args.intent_receipt.exists()
    controller = sample_unit(args.controller_unit)
    producer = sample_unit(args.producer_unit)

    if not intent_exists:
        if not running(controller) or not running(producer):
            print(
                json.dumps(
                    {
                        "available_bytes": free_bytes,
                        "minimum_free_bytes": args.minimum_free_bytes,
                        "controller": asdict(controller),
                        "producer": asdict(producer),
                        "trip": False,
                        "armed": False,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            return 0
        require_original_running(
            controller, args.expected_controller_invocation_id, "controller"
        )
        require_original_running(
            producer, args.expected_producer_invocation_id, "producer"
        )
        if free_bytes >= args.minimum_free_bytes:
            print(
                json.dumps(
                    {
                        "available_bytes": free_bytes,
                        "minimum_free_bytes": args.minimum_free_bytes,
                        "controller": asdict(controller),
                        "producer": asdict(producer),
                        "trip": False,
                        "armed": True,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            return 0
        intent = {
            "schema": INTENT_SCHEMA,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            **receipt_binding(args),
            "available_bytes": free_bytes,
            "controller_sample": asdict(controller),
            "producer_sample": asdict(producer),
            "reason": "actual available bytes fell below the sealed reserve stop floor",
            "stop_order": ["controller", "producer"],
            "remote_mutations": False,
            "r2_mutations": False,
        }
        write_json_noclobber(args.intent_receipt, intent)
    else:
        intent = read_bound_receipt(args.intent_receipt, INTENT_SCHEMA, args)

    controller_stop = stop_bound_unit(
        controller, args.expected_controller_invocation_id, "controller"
    )
    controller_final = sample_unit(args.controller_unit)
    require_bound_or_stopped(
        controller_final, args.expected_controller_invocation_id, "controller"
    )
    if running(controller_final):
        raise GuardError("controller remained active after reserve stop")

    # Resample only after the controller is confirmed stopped. This prevents
    # a controller race from replacing work while the selected producer is
    # being stopped.
    producer_current = sample_unit(args.producer_unit)
    producer_stop = stop_bound_unit(
        producer_current, args.expected_producer_invocation_id, "producer"
    )
    producer_final = sample_unit(args.producer_unit)
    require_bound_or_stopped(
        producer_final, args.expected_producer_invocation_id, "producer"
    )
    if running(producer_final):
        raise GuardError("producer remained active after reserve stop")

    completion = {
        "schema": COMPLETION_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "intent_receipt": str(args.intent_receipt),
        **receipt_binding(args),
        "available_bytes_after_stop": available_bytes(args.filesystem),
        "controller_final": asdict(controller_final),
        "producer_final": asdict(producer_final),
        "controller_stop": controller_stop,
        "producer_stop": producer_stop,
        "scratch_deleted": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_json_noclobber(args.completion_receipt, completion)
    timer_result = run_systemctl(["stop", args.guard_timer])
    print(
        json.dumps(
            {
                "completion": completion,
                "intent": intent,
                "guard_timer_stop_return_code": timer_result.returncode,
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
