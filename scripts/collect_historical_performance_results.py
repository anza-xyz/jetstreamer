#!/usr/bin/env python3
"""Collect a sealed, read-only result receipt for a historical replay A/B cohort."""

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
from typing import Any, Iterable, Sequence


MANIFEST_SCHEMA = "jetstreamer-historical-performance-results-v1"
LAUNCH_SCHEMA = "jetstreamer-historical-performance-launch-receipt-v1"
CANARY_SCHEMA = "jetstreamer-historical-performance-canary-v2"
GUARD_SCHEMA = "jetstreamer-historical-performance-guard-trip-v1"
RECEIPT_SCHEMA = "jetstreamer-historical-performance-results-receipt-v1"
PRIVATE_ROOT = Path("/home/ubuntu/.jetstreamer-private/performance-ab-202")
VARIANT_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
UNIT_NAMESPACE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
UNIT_NAME = re.compile(
    r"^horizon-perf-epoch[0-9]+(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?@[a-z0-9-]+\.service$"
)
GUARD_UNIT = re.compile(
    r"^horizon-perf-epoch[0-9]+-guard(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?\.service$"
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
APPENDVEC_FILE = re.compile(r"^(?P<slot>[0-9]+)\.(?P<store_id>[0-9]+)$")
WAVE_METRICS = re.compile(
    r"historical execution wave metrics: reason=(?P<reason>\S+) "
    r"transactions=(?P<transactions>[0-9]+) waves=(?P<waves>[0-9]+) "
    r"singleton_waves=(?P<singleton_waves>[0-9]+) "
    r"maximum_wave_size=(?P<maximum_wave_size>[0-9]+) "
    r"size1=(?P<size1>[0-9]+) size2=(?P<size2>[0-9]+) "
    r"size3=(?P<size3>[0-9]+) size4=(?P<size4>[0-9]+) "
    r"size5=(?P<size5>[0-9]+) size6_7=(?P<size6_7>[0-9]+) "
    r"size8plus=(?P<size8plus>[0-9]+)"
)


class CollectionError(RuntimeError):
    """A fail-closed performance-result collection error."""


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
    cpu_usage_nsec: int | None
    memory_peak_bytes: int | None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_collector_binding(
    manifest: dict[str, Any], executable: Path | None = None
) -> dict[str, str]:
    binding = manifest.get("collector")
    if not isinstance(binding, dict):
        raise CollectionError("results manifest lacks collector binding")
    raw_path = binding.get("path")
    expected_sha256 = binding.get("sha256")
    if (
        not isinstance(raw_path, str)
        or not Path(raw_path).is_absolute()
        or not isinstance(expected_sha256, str)
        or SHA256.fullmatch(expected_sha256) is None
    ):
        raise CollectionError("results manifest has invalid collector binding")
    declared = Path(raw_path)
    running = Path(__file__) if executable is None else executable
    try:
        metadata = declared.lstat()
        resolved_declared = declared.resolve(strict=True)
        resolved_running = running.resolve(strict=True)
    except OSError as error:
        raise CollectionError(f"cannot resolve collector binding: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or resolved_declared != resolved_running
    ):
        raise CollectionError("collector binding has unsafe or mismatched identity")
    actual_sha256 = sha256_file(resolved_declared)
    if actual_sha256 != expected_sha256:
        raise CollectionError("collector executable digest does not match manifest")
    return {"path": str(resolved_declared), "sha256": actual_sha256}


def load_json_file(
    path: Path,
    description: str,
    *,
    required_uid: int | None = None,
    owner_only: bool = True,
) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise CollectionError(f"{description} path must be absolute")
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise CollectionError(f"cannot read {description} {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (required_uid is not None and metadata.st_uid != required_uid)
        or (owner_only and stat.S_IMODE(metadata.st_mode) & 0o077)
    ):
        raise CollectionError(f"{description} has unsafe identity: {path}")
    if not isinstance(payload, dict):
        raise CollectionError(f"{description} must be a JSON object")
    return payload, sha256_file(path)


def parse_systemctl_show(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            fields[name] = value
    return fields


def optional_integer(value: str | None) -> int | None:
    if value in (None, "", "[not set]", "infinity"):
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed < 2**63 - 1 else None


def sample_unit(unit: str) -> UnitState:
    if UNIT_NAME.fullmatch(unit) is None:
        raise CollectionError(f"invalid canary unit: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,"
            "NRestarts,ExecMainCode,ExecMainStatus,CPUUsageNSec,MemoryPeak",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise CollectionError(
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
        cpu_usage_nsec=optional_integer(fields.get("CPUUsageNSec")),
        memory_peak_bytes=optional_integer(fields.get("MemoryPeak")),
    )


def require_terminal_success(state: UnitState, expected_invocation: str) -> None:
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.result != "success"
        or state.main_pid != 0
        or state.restarts != 0
        or state.exec_main_code not in (0, 1)
        or state.exec_main_status != 0
        or state.invocation_id not in ("", expected_invocation)
    ):
        raise CollectionError(f"canary is not exact terminal success: {state}")


def require_under(path: Path, parent: Path, description: str) -> Path:
    try:
        resolved_parent = parent.resolve(strict=True)
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise CollectionError(f"cannot resolve {description}: {error}") from error
    if resolved_parent not in resolved.parents:
        raise CollectionError(f"{description} escapes sealed root: {resolved}")
    return resolved


def size_bucket(size: int) -> str:
    mib = 1024 * 1024
    thresholds = (
        (4 * mib, "le_4_mib"),
        (8 * mib, "gt_4_le_8_mib"),
        (16 * mib, "gt_8_le_16_mib"),
        (64 * mib, "gt_16_le_64_mib"),
        (256 * mib, "gt_64_le_256_mib"),
        (1024 * mib, "gt_256_mib_le_1_gib"),
    )
    for threshold, name in thresholds:
        if size <= threshold:
            return name
    return "gt_1_gib"


def tree_statistics(
    path: Path, *, collect_appendvec_slots: bool = False
) -> dict[str, Any]:
    root = path.resolve(strict=True)
    root_metadata = root.lstat()
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise CollectionError(f"statistics root is not a directory: {root}")
    device = root_metadata.st_dev
    files = 0
    directories = 1
    symlinks = 0
    physical_bytes = root_metadata.st_blocks * 512
    apparent_bytes = root_metadata.st_size
    histogram: dict[str, int] = {}
    appendvec_stores_per_slot: dict[int, int] = {}
    appendvec_files = 0
    unrecognized_regular_files = 0
    for directory, names, filenames in os.walk(root, topdown=True, followlinks=False):
        directory_path = Path(directory)
        retained_names: list[str] = []
        for name in names:
            entry = directory_path / name
            metadata = entry.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                symlinks += 1
                continue
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_dev != device:
                continue
            directories += 1
            physical_bytes += metadata.st_blocks * 512
            apparent_bytes += metadata.st_size
            retained_names.append(name)
        names[:] = retained_names
        for name in filenames:
            entry = directory_path / name
            metadata = entry.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                symlinks += 1
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_dev != device:
                continue
            files += 1
            physical_bytes += metadata.st_blocks * 512
            apparent_bytes += metadata.st_size
            bucket = size_bucket(metadata.st_size)
            histogram[bucket] = histogram.get(bucket, 0) + 1
            if collect_appendvec_slots:
                match = APPENDVEC_FILE.fullmatch(name)
                if match is None:
                    unrecognized_regular_files += 1
                else:
                    slot = int(match.group("slot"))
                    appendvec_files += 1
                    appendvec_stores_per_slot[slot] = (
                        appendvec_stores_per_slot.get(slot, 0) + 1
                    )
    result = {
        "path": str(root),
        "device": device,
        "physical_bytes": physical_bytes,
        "apparent_bytes": apparent_bytes,
        "regular_files": files,
        "directories": directories,
        "symlinks_not_followed": symlinks,
        "file_size_histogram": dict(sorted(histogram.items())),
    }
    if collect_appendvec_slots:
        store_count_histogram: dict[int, int] = {}
        for count in appendvec_stores_per_slot.values():
            store_count_histogram[count] = store_count_histogram.get(count, 0) + 1
        slots = len(appendvec_stores_per_slot)
        result["appendvec_store_fanout"] = {
            "recognized_appendvec_files": appendvec_files,
            "unrecognized_regular_files": unrecognized_regular_files,
            "slots_with_stores": slots,
            "minimum_stores_per_slot": (
                min(appendvec_stores_per_slot.values()) if slots else None
            ),
            "maximum_stores_per_slot": (
                max(appendvec_stores_per_slot.values()) if slots else None
            ),
            "mean_stores_per_slot": (appendvec_files / slots if slots else None),
            "store_count_to_slot_count": {
                str(count): store_count_histogram[count]
                for count in sorted(store_count_histogram)
            },
        }
    return result


def find_accounts_state(scratch: Path) -> Path:
    matches = [
        item
        for item in scratch.glob(
            ".historical-runtime/*/jetstreamer-historical-*/accounts-state"
        )
        if item.is_dir() and not item.is_symlink()
    ]
    if len(matches) != 1:
        raise CollectionError(
            f"expected exactly one accounts-state beneath {scratch}, found {len(matches)}"
        )
    return matches[0]


def journal_lines(arguments: Sequence[str]) -> list[str]:
    completed = subprocess.run(
        ["journalctl", *arguments, "--no-pager", "-o", "cat"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise CollectionError(
            f"journalctl failed: {completed.stderr.strip() or completed.stdout.strip()}"
        )
    return completed.stdout.splitlines()


def parse_wave_metrics(lines: Iterable[str]) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for line in lines:
        match = WAVE_METRICS.search(line)
        if not match:
            continue
        latest = {
            name: (value if name == "reason" else int(value))
            for name, value in match.groupdict().items()
        }
    if latest is not None and latest["waves"]:
        latest["singleton_wave_fraction"] = (
            latest["singleton_waves"] / latest["waves"]
        )
        latest["average_transactions_per_wave"] = (
            latest["transactions"] / latest["waves"]
        )
    return latest


def parse_guard_samples(
    lines: Iterable[str], units: set[str]
) -> dict[str, dict[str, Any]]:
    results = {
        unit: {"samples": 0, "maximum_worker_vmas": None} for unit in sorted(units)
    }
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("schema") != GUARD_SCHEMA:
            continue
        for sample in payload.get("samples", []):
            if not isinstance(sample, dict) or sample.get("unit") not in units:
                continue
            result = results[sample["unit"]]
            result["samples"] += 1
            value = sample.get("worker_vmas")
            if isinstance(value, int):
                current = result["maximum_worker_vmas"]
                result["maximum_worker_vmas"] = value if current is None else max(current, value)
    return results


def validate_canary_receipt(
    payload: dict[str, Any], expected_target: int, expected_invocation: str
) -> dict[str, Any]:
    final = payload.get("final_progress")
    first = payload.get("first_progress")
    rates = payload.get("progress_rates")
    if (
        payload.get("schema") != CANARY_SCHEMA
        or payload.get("systemd_invocation_id") != expected_invocation
        or payload.get("target_slot") != expected_target
        or payload.get("target_reached") is not True
        or payload.get("child_return_code") != 0
        or payload.get("external_signal") is not None
        or not isinstance(first, dict)
        or not isinstance(final, dict)
        or not isinstance(rates, dict)
        or not isinstance(final.get("slot"), int)
        or not isinstance(first.get("transactions"), int)
        or not isinstance(final.get("transactions"), int)
        or not isinstance(first.get("account_updates"), int)
        or not isinstance(final.get("account_updates"), int)
        or final["slot"] < expected_target
    ):
        raise CollectionError("canary receipt does not prove a successful bounded run")
    transaction_delta = final.get("transactions", 0) - first.get("transactions", 0)
    update_delta = final.get("account_updates", 0) - first.get("account_updates", 0)
    if transaction_delta <= 0 or update_delta <= 0:
        raise CollectionError("canary progress counters did not advance")
    return {
        "first_progress": first,
        "final_progress": final,
        "progress_rates": rates,
        "transaction_delta": transaction_delta,
        "account_update_delta": update_delta,
        "elapsed_seconds": payload.get("elapsed_seconds"),
        "effective_cpu_cores": payload.get("effective_cpu_cores"),
        "cgroup_counter_deltas": payload.get("cgroup_counter_deltas"),
        "cgroup_after": payload.get("cgroup_after"),
        "child_resource_usage_delta": payload.get("child_resource_usage_delta"),
        "filesystem_available_bytes_before": payload.get(
            "filesystem_available_bytes_before"
        ),
        "filesystem_available_bytes_after": payload.get(
            "filesystem_available_bytes_after"
        ),
        "observed_overshoot_slots": payload.get("observed_overshoot_slots"),
    }


def validate_unit_namespace(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or UNIT_NAMESPACE.fullmatch(value) is None:
        raise CollectionError("invalid unit namespace")
    return value


def expected_unit(name: str, unit_namespace: str | None) -> str:
    namespace = f"-{unit_namespace}" if unit_namespace is not None else ""
    return f"horizon-perf-epoch202{namespace}@{name}.service"


def validate_variant_paths(
    raw: dict[str, Any], unit_namespace: str | None = None
) -> dict[str, Any]:
    name = raw.get("name")
    if not isinstance(name, str) or VARIANT_NAME.fullmatch(name) is None:
        raise CollectionError("invalid variant name")
    root = PRIVATE_ROOT / name
    expected = {
        "unit": expected_unit(name, unit_namespace),
        "canary_receipt": root / "canary-receipt.json",
        "scratch": root / "scratch",
        "archive": root / "output" / "epoch-202-through-87695515.jet",
    }
    for key, expected_value in expected.items():
        actual = raw.get(key)
        if str(expected_value) != actual:
            raise CollectionError(f"variant {name} has invalid {key}: {actual}")
    if not isinstance(raw.get("wave_metrics"), bool):
        raise CollectionError(f"variant {name} has invalid wave_metrics flag")
    return {**raw, "root": root}


def validate_variant_set(
    variants_raw: object, unit_namespace: str | None = None
) -> list[dict[str, Any]]:
    if not isinstance(variants_raw, list):
        raise CollectionError("results manifest variants must be a list")
    variants = [
        validate_variant_paths(item, unit_namespace)
        for item in variants_raw
        if isinstance(item, dict)
    ]
    if (
        len(variants) != len(variants_raw)
        or not 2 <= len(variants) <= 16
        or len({item["name"] for item in variants}) != len(variants)
        or len({item["unit"] for item in variants}) != len(variants)
    ):
        raise CollectionError(
            "results manifest must bind between two and sixteen distinct variants"
        )
    return variants


def validate_guard_unit(value: object) -> str:
    if not isinstance(value, str) or GUARD_UNIT.fullmatch(value) is None:
        raise CollectionError("unexpected guard unit")
    return value


def collect(manifest: dict[str, Any], launch: dict[str, Any]) -> dict[str, Any]:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise CollectionError("unsupported results manifest schema")
    if (
        launch.get("schema") != LAUNCH_SCHEMA
        or launch.get("launched") is not True
        or launch.get("remote_mutations") is not False
        or launch.get("r2_mutations") is not False
    ):
        raise CollectionError("launch receipt does not prove a sealed local launch")
    launch_evidence = launch.get("evidence")
    if not isinstance(launch_evidence, dict):
        raise CollectionError("launch receipt evidence must be an object")
    target_slot = manifest.get("target_slot")
    variants_raw = manifest.get("variants")
    if not isinstance(target_slot, int):
        raise CollectionError("results manifest lacks target or variants")
    unit_namespace = validate_unit_namespace(manifest.get("unit_namespace"))
    variants = validate_variant_set(variants_raw, unit_namespace)
    units = {item["unit"] for item in variants}
    raw_launch_units = launch_evidence.get("units")
    if not isinstance(raw_launch_units, list):
        raise CollectionError("launch receipt lacks unit bindings")
    launch_units = {
        item.get("unit"): item.get("invocation_id")
        for item in raw_launch_units
        if isinstance(item, dict) and item.get("unit") in units
    }
    if set(launch_units) != units or not all(launch_units.values()):
        raise CollectionError("launch receipt does not bind every canary invocation")

    started = launch_evidence.get("started_at_utc")
    if not isinstance(started, str):
        raise CollectionError("launch receipt lacks start time")
    guard_unit = validate_guard_unit(manifest.get("guard_unit"))
    guard_lines = journal_lines(["-u", guard_unit, "--since", started])
    guard_samples = parse_guard_samples(guard_lines, units)
    if any(
        value["samples"] < 1 or value["maximum_worker_vmas"] is None
        for value in guard_samples.values()
    ):
        raise CollectionError("guard journal lacks a sample for one or more variants")

    collected: list[dict[str, Any]] = []
    for variant in variants:
        name = variant["name"]
        state = sample_unit(variant["unit"])
        require_terminal_success(state, launch_units[variant["unit"]])
        receipt_path = Path(variant["canary_receipt"])
        receipt, receipt_sha256 = load_json_file(
            receipt_path, f"{name} canary receipt", required_uid=1000
        )
        performance = validate_canary_receipt(
            receipt, target_slot, launch_units[variant["unit"]]
        )
        scratch = require_under(Path(variant["scratch"]), PRIVATE_ROOT, f"{name} scratch")
        archive = require_under(Path(variant["archive"]), PRIVATE_ROOT, f"{name} archive")
        archive_metadata = archive.lstat()
        if not stat.S_ISREG(archive_metadata.st_mode):
            raise CollectionError(f"{name} archive is not regular")
        scratch_stats = tree_statistics(scratch)
        accounts_stats = tree_statistics(
            find_accounts_state(scratch), collect_appendvec_slots=True
        )
        transaction_delta = performance["transaction_delta"]
        scratch_stats["physical_bytes_per_million_transactions"] = (
            scratch_stats["physical_bytes"] * 1_000_000 / transaction_delta
        )
        wave_metrics = None
        if variant["wave_metrics"]:
            wave_metrics = parse_wave_metrics(
                journal_lines(
                    [
                        f"_SYSTEMD_INVOCATION_ID={launch_units[variant['unit']]}",
                        "--grep",
                        "historical execution wave metrics",
                    ]
                )
            )
            if wave_metrics is None:
                raise CollectionError(f"{name} has no wave metrics in its journal")
        collected.append(
            {
                "name": name,
                "unit": asdict(state),
                "canary_receipt": str(receipt_path),
                "canary_receipt_sha256": receipt_sha256,
                "performance": performance,
                "guard": guard_samples[variant["unit"]],
                "scratch": scratch_stats,
                "accounts_state": accounts_stats,
                "archive": {
                    "path": str(archive),
                    "size": archive_metadata.st_size,
                    "physical_bytes": archive_metadata.st_blocks * 512,
                },
                "wave_metrics": wave_metrics,
            }
        )
    return {
        "target_slot": target_slot,
        "variants": collected,
        "selection_authorized": False,
        "next_gate": "review metrics, select one environment, then repeat full root/plugin qualification",
    }


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_noclobber(
    path: Path, payload: dict[str, Any], required_uid: int = 0
) -> None:
    if not path.is_absolute():
        raise CollectionError("result receipt path must be absolute")
    parent = path.parent.resolve(strict=True)
    metadata = parent.stat()
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise CollectionError("result receipt directory must be root-owned and owner-only")
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


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise CollectionError("performance result collection must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    manifest, manifest_sha256 = load_json_file(
        args.manifest, "results manifest", required_uid=0
    )
    collector_binding = validate_collector_binding(manifest)
    launch_path = manifest.get("launch_receipt")
    expected_launch_manifest = manifest.get("launch_manifest_sha256")
    if (
        not isinstance(launch_path, str)
        or SHA256.fullmatch(str(expected_launch_manifest)) is None
    ):
        raise CollectionError("results manifest has invalid launch receipt binding")
    launch, actual_launch_sha256 = load_json_file(
        Path(launch_path), "launch receipt", required_uid=0
    )
    if launch.get("manifest_sha256") != expected_launch_manifest:
        raise CollectionError("launch receipt does not bind the expected launch manifest")
    evidence = collect(manifest, launch)
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest": str(args.manifest),
        "manifest_sha256": manifest_sha256,
        "collector": collector_binding,
        "launch_receipt": launch_path,
        "launch_receipt_sha256": actual_launch_sha256,
        "evidence": evidence,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    write_json_noclobber(args.receipt, receipt, required_uid=0)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CollectionError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
