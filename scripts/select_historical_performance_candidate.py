#!/usr/bin/env python3
"""Select a bounded historical replay candidate from sealed result evidence."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping, Sequence


LEGACY_POLICY_SCHEMA = "jetstreamer-private-epoch202-store8-selection-policy-v1"
POLICY_SCHEMA = LEGACY_POLICY_SCHEMA
GENERIC_POLICY_SCHEMA = "jetstreamer-historical-performance-selection-policy-v2"
RESULT_SCHEMA = "jetstreamer-historical-performance-results-receipt-v1"
RECEIPT_SCHEMA = "jetstreamer-historical-performance-selection-receipt-v1"
CONTROL = "waves-control-t16"
CANDIDATE = "waves-store8-t16"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
VARIANT_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


class SelectionError(RuntimeError):
    """Selection evidence is missing, unsafe, or internally inconsistent."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_owner_only_json(path: Path, description: str) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise SelectionError(f"{description} path must be absolute")
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SelectionError(f"cannot read {description} {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise SelectionError(f"{description} has unsafe identity: {path}")
    if not isinstance(payload, dict):
        raise SelectionError(f"{description} must be a JSON object")
    return payload, sha256_file(path)


def finite_positive(value: object, context: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise SelectionError(f"{context} must be finite and positive")
    return float(value)


def nonnegative_integer(value: object, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise SelectionError(f"{context} must be a non-negative integer")
    return value


def required_mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise SelectionError(f"{context} must be an object")
    return value


def ratio(candidate: float | int, control: float | int, context: str) -> float:
    denominator = finite_positive(control, f"control {context}")
    numerator = finite_positive(candidate, f"candidate {context}")
    return numerator / denominator


def policy_variants(policy: Mapping[str, Any]) -> tuple[str, str, bool]:
    schema = policy.get("schema")
    if schema == LEGACY_POLICY_SCHEMA:
        return CONTROL, CANDIDATE, True
    if schema != GENERIC_POLICY_SCHEMA:
        raise SelectionError("unsupported selection policy schema")
    control = policy.get("control_variant")
    candidate = policy.get("candidate_variant")
    require_wave_metrics = policy.get("require_wave_metrics", False)
    if (
        not isinstance(control, str)
        or VARIANT_NAME.fullmatch(control) is None
        or not isinstance(candidate, str)
        or VARIANT_NAME.fullmatch(candidate) is None
        or control == candidate
    ):
        raise SelectionError("selection policy has invalid variant names")
    if not isinstance(require_wave_metrics, bool):
        raise SelectionError("require_wave_metrics must be boolean")
    return control, candidate, require_wave_metrics


def variant_metrics(
    raw: Mapping[str, Any],
    target_slot: int,
    expected_name: str,
    *,
    require_wave_metrics: bool,
) -> dict[str, float | int]:
    name = raw.get("name")
    if name != expected_name:
        raise SelectionError(f"unexpected variant name: {name!r}")
    unit = required_mapping(raw.get("unit"), f"{name} unit")
    if (
        unit.get("load_state") != "loaded"
        or unit.get("active_state") != "inactive"
        or unit.get("result") != "success"
        or nonnegative_integer(unit.get("main_pid"), f"{name} main_pid") != 0
        or nonnegative_integer(unit.get("restarts"), f"{name} restarts") != 0
    ):
        raise SelectionError(f"{name} is not exact zero-restart terminal success")
    performance = required_mapping(raw.get("performance"), f"{name} performance")
    final = required_mapping(performance.get("final_progress"), f"{name} final progress")
    if nonnegative_integer(final.get("slot"), f"{name} final slot") < target_slot:
        raise SelectionError(f"{name} did not reach the bounded target")
    progress_rates = required_mapping(
        performance.get("progress_rates"), f"{name} progress rates"
    )
    guard = required_mapping(raw.get("guard"), f"{name} guard")
    if nonnegative_integer(guard.get("samples"), f"{name} guard samples") < 1:
        raise SelectionError(f"{name} has no guard sample")
    scratch = required_mapping(raw.get("scratch"), f"{name} scratch")
    accounts = required_mapping(raw.get("accounts_state"), f"{name} accounts state")
    fanout = required_mapping(
        accounts.get("appendvec_store_fanout"), f"{name} AppendVec fanout"
    )
    if require_wave_metrics:
        wave_metrics = required_mapping(
            raw.get("wave_metrics"), f"{name} wave metrics"
        )
        finite_positive(wave_metrics.get("waves"), f"{name} wave count")
    return {
        "slots_per_second": finite_positive(
            progress_rates.get("slots_per_second"), f"{name} slots/s"
        ),
        "transactions_per_second": finite_positive(
            progress_rates.get("transactions_per_second"), f"{name} transactions/s"
        ),
        "account_updates_per_second": finite_positive(
            progress_rates.get("account_updates_per_second"), f"{name} updates/s"
        ),
        "effective_cpu_cores": finite_positive(
            performance.get("effective_cpu_cores"), f"{name} effective CPU"
        ),
        "physical_scratch_bytes": nonnegative_integer(
            scratch.get("physical_bytes"), f"{name} physical scratch"
        ),
        "apparent_scratch_bytes": nonnegative_integer(
            scratch.get("apparent_bytes"), f"{name} apparent scratch"
        ),
        "regular_files": nonnegative_integer(
            scratch.get("regular_files"), f"{name} regular files"
        ),
        "worker_vmas": nonnegative_integer(
            guard.get("maximum_worker_vmas"), f"{name} maximum worker VMAs"
        ),
        "recognized_appendvec_files": nonnegative_integer(
            fanout.get("recognized_appendvec_files"),
            f"{name} recognized AppendVec files",
        ),
        "mean_stores_per_slot": finite_positive(
            fanout.get("mean_stores_per_slot"), f"{name} mean stores/slot"
        ),
    }


def evaluate_selection(
    policy: Mapping[str, Any],
    results: Mapping[str, Any],
    *,
    policy_path: str,
    policy_sha256: str,
    results_path: str,
    results_sha256: str,
) -> dict[str, Any]:
    control_name, candidate_name, require_wave_metrics = policy_variants(policy)
    if results.get("schema") != RESULT_SCHEMA:
        raise SelectionError("unsupported performance result schema")
    expected_manifest_sha256 = policy.get("results_manifest_sha256")
    if (
        not isinstance(expected_manifest_sha256, str)
        or SHA256.fullmatch(expected_manifest_sha256) is None
        or results.get("manifest_sha256") != expected_manifest_sha256
    ):
        raise SelectionError("result receipt does not bind the policy's manifest")
    if results.get("remote_mutations") is not False or results.get("r2_mutations") is not False:
        raise SelectionError("result receipt does not prove a mutation-free cohort")
    evidence = required_mapping(results.get("evidence"), "result evidence")
    if evidence.get("selection_authorized") is not False:
        raise SelectionError("collector result unexpectedly authorizes selection")
    fixed = required_mapping(policy.get("fixed_environment"), "fixed environment")
    target_slot = nonnegative_integer(fixed.get("target_slot"), "target slot")
    if evidence.get("target_slot") != target_slot:
        raise SelectionError("result target does not match the selection policy")
    variants_raw = evidence.get("variants")
    if not isinstance(variants_raw, list) or len(variants_raw) != 2:
        raise SelectionError("result must contain exactly the selected control and candidate")
    variants = {
        item.get("name"): item
        for item in variants_raw
        if isinstance(item, dict)
        and item.get("name") in (control_name, candidate_name)
    }
    if set(variants) != {control_name, candidate_name}:
        raise SelectionError("result variant set is incomplete or ambiguous")
    control = variant_metrics(
        variants[control_name],
        target_slot,
        control_name,
        require_wave_metrics=require_wave_metrics,
    )
    candidate = variant_metrics(
        variants[candidate_name],
        target_slot,
        candidate_name,
        require_wave_metrics=require_wave_metrics,
    )

    throughput_floors = required_mapping(
        policy.get("candidate_throughput_floors_relative_to_control"),
        "throughput floors",
    )
    resource_ceilings = required_mapping(
        policy.get("candidate_resource_nonregression_ceilings_relative_to_control"),
        "resource ceilings",
    )
    throughput_ratios = {
        name: ratio(candidate[name], control[name], name)
        for name in (
            "slots_per_second",
            "transactions_per_second",
            "account_updates_per_second",
        )
    }
    resource_ratios = {
        name: ratio(candidate[name], control[name], name)
        for name in (
            "effective_cpu_cores",
            "physical_scratch_bytes",
            "apparent_scratch_bytes",
            "regular_files",
            "worker_vmas",
        )
    }
    throughput_gates = {
        name: value >= finite_positive(throughput_floors.get(name), f"{name} floor")
        for name, value in throughput_ratios.items()
    }
    resource_gates = {
        name: value <= finite_positive(resource_ceilings.get(name), f"{name} ceiling")
        for name, value in resource_ratios.items()
    }
    material = required_mapping(policy.get("material_storage_win"), "material win")
    minimum_reduction = finite_positive(
        material.get("minimum_relative_reduction"), "minimum storage reduction"
    )
    storage_reductions = {
        name: 1.0 - ratio(candidate[name], control[name], name)
        for name in (
            "physical_scratch_bytes",
            "regular_files",
            "recognized_appendvec_files",
            "mean_stores_per_slot",
            "worker_vmas",
        )
    }
    material_storage_win = any(
        value >= minimum_reduction for value in storage_reductions.values()
    )
    select_candidate = (
        all(throughput_gates.values())
        and all(resource_gates.values())
        and material_storage_win
    )
    selected = candidate_name if select_candidate else control_name
    return {
        "schema": RECEIPT_SCHEMA,
        "policy_schema": policy.get("schema"),
        "control_variant": control_name,
        "candidate_variant": candidate_name,
        "require_wave_metrics": require_wave_metrics,
        "selected_variant": selected,
        "candidate_selected": select_candidate,
        "target_slot": target_slot,
        "policy": policy_path,
        "policy_sha256": policy_sha256,
        "results": results_path,
        "results_sha256": results_sha256,
        "control_metrics": control,
        "candidate_metrics": candidate,
        "throughput_ratios": throughput_ratios,
        "throughput_gates": throughput_gates,
        "resource_ratios": resource_ratios,
        "resource_gates": resource_gates,
        "storage_reductions": storage_reductions,
        "material_storage_win": material_storage_win,
        "minimum_material_storage_reduction": minimum_reduction,
        "selection_authorized": True,
        "qualification_launch_authorized": False,
        "publication_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_noclobber(path: Path, payload: Mapping[str, Any]) -> None:
    if not path.is_absolute():
        raise SelectionError("selection receipt path must be absolute")
    parent = path.parent.resolve(strict=True)
    metadata = parent.stat()
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise SelectionError("selection receipt directory must be root-owned and owner-only")
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
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise SelectionError("performance selection must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    policy, policy_sha256 = load_owner_only_json(args.policy, "selection policy")
    results, results_sha256 = load_owner_only_json(args.results, "performance results")
    receipt = evaluate_selection(
        policy,
        results,
        policy_path=str(args.policy),
        policy_sha256=policy_sha256,
        results_path=str(args.results),
        results_sha256=results_sha256,
    )
    receipt["selected_at_utc"] = datetime.now(timezone.utc).isoformat()
    receipt["selector"] = {
        "path": str(Path(__file__).resolve(strict=True)),
        "sha256": sha256_file(Path(__file__).resolve(strict=True)),
    }
    for variant in results["evidence"]["variants"]:
        archive = Path(variant["archive"]["path"])
        try:
            archive_metadata = archive.lstat()
        except OSError as error:
            raise SelectionError(f"cannot re-observe diagnostic archive {archive}: {error}") from error
        if not archive.is_absolute() or not stat.S_ISREG(archive_metadata.st_mode):
            raise SelectionError(f"diagnostic archive is not an absolute regular file: {archive}")
        if Path(str(archive) + ".sha256").exists():
            raise SelectionError(f"diagnostic sidecar exists for {archive}")
    write_json_noclobber(args.receipt, receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SelectionError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
