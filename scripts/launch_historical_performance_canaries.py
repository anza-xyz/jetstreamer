#!/usr/bin/env python3
"""Install and start a sealed historical performance cohort after admission."""

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


MANIFEST_SCHEMA = "jetstreamer-historical-performance-launch-v1"
ADMISSION_SCHEMA = "jetstreamer-historical-performance-admission-receipt-v1"
RECEIPT_SCHEMA = "jetstreamer-historical-performance-launch-receipt-v1"
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@-]+\.(?:service|timer)$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SYSTEMD_DIRECTORY = Path("/etc/systemd/system")


class LaunchError(RuntimeError):
    """A fail-closed cohort-launch error."""


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


@dataclass(frozen=True)
class PreparedInstallation:
    source: Path
    destination: Path
    sha256: str
    data: bytes


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def require_root_file(path: Path, description: str) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise LaunchError(f"{description} path must be absolute")
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LaunchError(f"cannot read {description} {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise LaunchError(f"{description} has unsafe identity: {path}")
    if not isinstance(payload, dict):
        raise LaunchError(f"{description} must be a JSON object")
    return payload, sha256_file(path)


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            fields[name] = value
    return fields


def sample_unit(unit: str) -> UnitState:
    if UNIT_NAME.fullmatch(unit) is None:
        raise LaunchError(f"invalid systemd unit name: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,NRestarts",
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LaunchError(
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
    )


def run_systemctl(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["systemctl", *arguments], capture_output=True, text=True
    )
    if completed.returncode != 0:
        raise LaunchError(
            f"systemctl {' '.join(arguments)} failed: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    return completed


def require_unit_absent(unit: str) -> None:
    state = sample_unit(unit)
    if state.load_state != "not-found" or state.active_state != "inactive":
        raise LaunchError(f"unit must be absent before installation: {state}")


def require_unit_inactive(unit: str) -> UnitState:
    state = sample_unit(unit)
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.result != "success"
        or state.main_pid != 0
    ):
        raise LaunchError(f"unit must be inactive before daemon-reload: {state}")
    return state


def prepare_unit_installation(
    source: Path,
    destination: Path,
    expected_sha256: str,
    systemd_directory: Path = SYSTEMD_DIRECTORY,
    required_uid: int = 0,
) -> PreparedInstallation:
    if (
        not source.is_absolute()
        or not destination.is_absolute()
        or destination.parent != systemd_directory
        or UNIT_NAME.fullmatch(destination.name) is None
        or SHA256.fullmatch(expected_sha256) is None
    ):
        raise LaunchError("invalid sealed unit installation binding")
    try:
        source_metadata = source.lstat()
        source_resolved = source.resolve(strict=True)
    except OSError as error:
        raise LaunchError(f"cannot inspect unit draft {source}: {error}") from error
    if (
        source_resolved != source
        or not stat.S_ISREG(source_metadata.st_mode)
        or source_metadata.st_uid != required_uid
        or source_metadata.st_nlink != 1
        or stat.S_IMODE(source_metadata.st_mode) != 0o444
    ):
        raise LaunchError(f"unit draft has unsafe identity: {source}")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"systemd unit destination already exists: {destination}")
    data = source.read_bytes()
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise LaunchError(
            f"unit draft SHA-256 mismatch for {source}: expected {expected_sha256}, got {actual_sha256}"
        )
    return PreparedInstallation(source, destination, actual_sha256, data)


def install_prepared_unit_noclobber(
    prepared: PreparedInstallation,
) -> dict[str, Any]:
    descriptor = os.open(
        prepared.destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o644,
    )
    try:
        os.fchmod(descriptor, 0o644)
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(prepared.data)
            output.flush()
            os.fsync(output.fileno())
    finally:
        os.close(descriptor)
    return {
        "source": str(prepared.source),
        "destination": str(prepared.destination),
        "sha256": prepared.sha256,
        "bytes": len(prepared.data),
    }


def install_unit_noclobber(
    source: Path,
    destination: Path,
    expected_sha256: str,
    systemd_directory: Path = SYSTEMD_DIRECTORY,
    required_uid: int = 0,
) -> dict[str, Any]:
    prepared = prepare_unit_installation(
        source,
        destination,
        expected_sha256,
        systemd_directory,
        required_uid,
    )
    return install_prepared_unit_noclobber(prepared)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stop_cohort(units: Sequence[str]) -> dict[str, Any]:
    completed = subprocess.run(
        ["systemctl", "stop", *units], capture_output=True, text=True
    )
    return {
        "units": list(units),
        "return_code": completed.returncode,
        "stderr": completed.stderr.strip(),
    }


def write_json_noclobber(
    path: Path, payload: dict[str, Any], required_uid: int = 0
) -> None:
    if not path.is_absolute():
        raise LaunchError("launch receipt path must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.stat()
    except OSError as error:
        raise LaunchError(f"cannot resolve launch receipt directory: {error}") from error
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise LaunchError(f"launch receipt directory must be root-owned and owner-only: {parent}")
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


def load_launch_manifest(path: Path) -> tuple[dict[str, Any], str]:
    payload, digest = require_root_file(path, "launch manifest")
    if payload.get("schema") != MANIFEST_SCHEMA:
        raise LaunchError("launch manifest has an unsupported schema")
    return payload, digest


def require_string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise LaunchError(f"launch manifest {key} must be a nonempty string list")
    if len(value) != len(set(value)):
        raise LaunchError(f"launch manifest {key} contains duplicates")
    return value


def validate_admission(manifest: dict[str, Any]) -> tuple[dict[str, Any], str]:
    path_value = manifest.get("admission_receipt")
    expected_manifest_sha = manifest.get("admission_manifest_sha256")
    if not isinstance(path_value, str) or not isinstance(expected_manifest_sha, str):
        raise LaunchError("launch manifest has invalid admission binding")
    receipt, digest = require_root_file(Path(path_value), "admission receipt")
    if (
        receipt.get("schema") != ADMISSION_SCHEMA
        or receipt.get("admitted") is not True
        or receipt.get("manifest_sha256") != expected_manifest_sha
        or receipt.get("services_installed") is not False
        or receipt.get("services_started") is not False
        or receipt.get("r2_mutations") is not False
    ):
        raise LaunchError("admission receipt does not authorize the sealed launch")
    return receipt, digest


def launch(manifest: dict[str, Any]) -> dict[str, Any]:
    admission, admission_sha256 = validate_admission(manifest)
    start_units = require_string_list(manifest, "start_units")
    service_units = require_string_list(manifest, "service_units")
    timer_units = require_string_list(manifest, "timer_units")
    if sorted(start_units) != sorted(service_units + timer_units):
        raise LaunchError("start_units must exactly equal service_units plus timer_units")
    for unit in start_units:
        if UNIT_NAME.fullmatch(unit) is None:
            raise LaunchError(f"invalid launch unit: {unit}")
        require_unit_absent(unit)

    monitor_unit = manifest.get("inactive_monitor_unit")
    if not isinstance(monitor_unit, str):
        raise LaunchError("inactive_monitor_unit must be a systemd unit")
    monitor_before = require_unit_inactive(monitor_unit)

    raw_installations = manifest.get("installations")
    if not isinstance(raw_installations, list) or not raw_installations:
        raise LaunchError("launch manifest installations must be a nonempty list")
    prepared_installations: list[PreparedInstallation] = []
    destinations: set[Path] = set()
    for raw in raw_installations:
        if not isinstance(raw, dict):
            raise LaunchError("launch installation must be an object")
        source = Path(str(raw.get("source", "")))
        destination = Path(str(raw.get("destination", "")))
        digest = raw.get("sha256")
        if destination in destinations or not isinstance(digest, str):
            raise LaunchError("duplicate or invalid launch installation")
        destinations.add(destination)
        prepared_installations.append(
            prepare_unit_installation(source, destination, digest)
        )
    installed = [
        install_prepared_unit_noclobber(item) for item in prepared_installations
    ]
    fsync_directory(SYSTEMD_DIRECTORY)
    run_systemctl(["daemon-reload"])
    monitor_after = require_unit_inactive(monitor_unit)

    started_at = datetime.now(timezone.utc).isoformat()
    try:
        run_systemctl(["start", *start_units])
        states = [sample_unit(unit) for unit in start_units]
        for state in states:
            if state.unit in service_units:
                if state.active_state != "active" or state.sub_state != "running" or state.main_pid < 1:
                    raise LaunchError(f"canary service did not become active: {state}")
            elif state.unit in timer_units:
                if state.active_state != "active" or state.sub_state not in ("waiting", "running"):
                    raise LaunchError(f"guard timer did not become active: {state}")
    except LaunchError:
        stop_cohort(start_units)
        raise

    return {
        "admission_receipt": manifest["admission_receipt"],
        "admission_receipt_sha256": admission_sha256,
        "admission_manifest_sha256": admission["manifest_sha256"],
        "installed": installed,
        "monitor_before": asdict(monitor_before),
        "monitor_after": asdict(monitor_after),
        "started_at_utc": started_at,
        "units": [asdict(item) for item in states],
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise LaunchError("performance launch must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.receipt.exists() or args.receipt.is_symlink():
        raise LaunchError(f"launch receipt path already exists: {args.receipt}")
    manifest, manifest_sha256 = load_launch_manifest(args.manifest)
    evidence = launch(manifest)
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest": str(args.manifest),
        "manifest_sha256": manifest_sha256,
        "launched": True,
        "evidence": evidence,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_json_noclobber(args.receipt, receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LaunchError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
