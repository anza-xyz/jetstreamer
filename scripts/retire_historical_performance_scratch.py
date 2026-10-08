#!/usr/bin/env python3
"""Safely retire exact scratch trees after a sealed historical performance cohort."""

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


MANIFEST_SCHEMA = "jetstreamer-historical-performance-results-v1"
RESULTS_SCHEMA = "jetstreamer-historical-performance-results-receipt-v1"
PLAN_SCHEMA = "jetstreamer-historical-performance-scratch-retirement-plan-v1"
RETIREMENT_SCHEMA = "jetstreamer-historical-performance-scratch-retirement-v1"
PRIVATE_ROOT = Path("/home/ubuntu/.jetstreamer-private/performance-ab-202")
# Retained as the original cohort fixture; runtime validation derives the exact
# admitted variants from the sealed manifest and result receipt.
VARIANTS = ("singleton-t16", "singleton-t32", "waves-t16", "waves-t32")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
VARIANT_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
UNIT_NAMESPACE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
UNIT_NAME = re.compile(
    r"^horizon-perf-epoch202(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?@[a-z0-9-]+\.service$"
)


class RetirementError(RuntimeError):
    """A fail-closed performance-scratch retirement error."""


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


@dataclass(frozen=True)
class ScratchBinding:
    name: str
    unit: str
    expected_invocation_id: str
    scratch: Path
    physical_bytes: int
    apparent_bytes: int
    regular_files: int
    directories: int


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json_file(path: Path, description: str, required_uid: int = 0) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise RetirementError(f"{description} path must be absolute")
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RetirementError(f"cannot read {description} {path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != required_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RetirementError(f"{description} has unsafe identity: {path}")
    if not isinstance(payload, dict):
        raise RetirementError(f"{description} must be a JSON object")
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
        raise RetirementError(f"invalid canary unit: {unit}")
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,Result,MainPID,InvocationID,"
            "NRestarts,ExecMainCode,ExecMainStatus",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RetirementError(
            f"cannot inspect {unit}: {completed.stderr.strip() or completed.stdout.strip()}"
        )
    fields = parse_systemctl_show(completed.stdout)
    try:
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
        )
    except ValueError as error:
        raise RetirementError(f"invalid systemd state for {unit}") from error


def require_terminal_success(state: UnitState, expected_invocation_id: str) -> None:
    if (
        state.load_state != "loaded"
        or state.active_state != "inactive"
        or state.result != "success"
        or state.main_pid != 0
        or state.restarts != 0
        or state.exec_main_code not in (0, 1)
        or state.exec_main_status != 0
        or (
            expected_invocation_id
            and state.invocation_id not in ("", expected_invocation_id)
        )
    ):
        raise RetirementError(f"canary has not reached exact terminal success: {state}")


def validate_unit_namespace(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or UNIT_NAMESPACE.fullmatch(value) is None:
        raise RetirementError("invalid unit namespace")
    return value


def expected_paths(
    name: str, unit_namespace: str | None = None
) -> dict[str, Path | str]:
    root = PRIVATE_ROOT / name
    namespace = f"-{unit_namespace}" if unit_namespace is not None else ""
    return {
        "unit": f"horizon-perf-epoch202{namespace}@{name}.service",
        "canary_receipt": root / "canary-receipt.json",
        "scratch": root / "scratch",
        "archive": root / "output" / "epoch-202-through-87695515.jet",
    }


def require_nonnegative_integer(value: Any, description: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise RetirementError(f"{description} is invalid")
    return value


def validate_manifest_and_results(
    manifest_path: Path,
    manifest: dict[str, Any],
    manifest_sha256: str,
    results_path: Path,
    results: dict[str, Any],
) -> list[ScratchBinding]:
    if (
        manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("future_receipt") != str(results_path)
        or manifest.get("selection_authorized") is not False
        or manifest.get("publication_authorized") is not False
        or manifest.get("remote_mutations_authorized") is not False
        or manifest.get("r2_mutations_authorized") is not False
    ):
        raise RetirementError("results manifest does not describe a local-only sealed cohort")
    if (
        results.get("schema") != RESULTS_SCHEMA
        or results.get("manifest") != str(manifest_path)
        or results.get("manifest_sha256") != manifest_sha256
        or results.get("remote_mutations") is not False
        or results.get("r2_mutations") is not False
    ):
        raise RetirementError("results receipt does not bind the exact local-only manifest")
    evidence = results.get("evidence")
    manifest_variants = manifest.get("variants")
    if (
        not isinstance(evidence, dict)
        or evidence.get("target_slot") != manifest.get("target_slot")
        or evidence.get("selection_authorized") is not False
        or not isinstance(manifest_variants, list)
        or not isinstance(evidence.get("variants"), list)
    ):
        raise RetirementError("results receipt lacks sealed cohort evidence")
    if not all(isinstance(item, dict) for item in manifest_variants) or not all(
        isinstance(item, dict) for item in evidence["variants"]
    ):
        raise RetirementError("manifest and receipt variants must be objects")
    manifest_names = [item.get("name") for item in manifest_variants]
    evidence_names = [item.get("name") for item in evidence["variants"]]
    manifest_by_name = dict(zip(manifest_names, manifest_variants, strict=True))
    evidence_by_name = dict(zip(evidence_names, evidence["variants"], strict=True))
    if (
        not 2 <= len(manifest_variants) <= 16
        or len(evidence["variants"]) != len(manifest_variants)
        or any(
            not isinstance(name, str) or VARIANT_NAME.fullmatch(name) is None
            for name in manifest_names + evidence_names
        )
        or len(manifest_by_name) != len(manifest_variants)
        or len(evidence_by_name) != len(evidence["variants"])
        or set(manifest_by_name) != set(evidence_by_name)
    ):
        raise RetirementError(
            "manifest and receipt must bind the same two to sixteen distinct variants"
        )

    unit_namespace = validate_unit_namespace(manifest.get("unit_namespace"))
    bindings: list[ScratchBinding] = []
    for name in manifest_names:
        expected = expected_paths(name, unit_namespace)
        declared = manifest_by_name[name]
        observed = evidence_by_name[name]
        for key in ("unit", "canary_receipt", "scratch", "archive"):
            if declared.get(key) != str(expected[key]):
                raise RetirementError(f"variant {name} has unexpected manifest {key}")
        unit = observed.get("unit")
        scratch = observed.get("scratch")
        accounts_state = observed.get("accounts_state")
        archive = observed.get("archive")
        if (
            not isinstance(unit, dict)
            or unit.get("unit") != expected["unit"]
            or unit.get("result") != "success"
            or unit.get("active_state") != "inactive"
            or unit.get("main_pid") != 0
            or unit.get("restarts") != 0
            or unit.get("exec_main_status") != 0
            or observed.get("canary_receipt") != str(expected["canary_receipt"])
            or SHA256.fullmatch(str(observed.get("canary_receipt_sha256"))) is None
            or not isinstance(scratch, dict)
            or scratch.get("path") != str(expected["scratch"])
            or not isinstance(accounts_state, dict)
            or not isinstance(accounts_state.get("path"), str)
            or not isinstance(archive, dict)
            or archive.get("path") != str(expected["archive"])
        ):
            raise RetirementError(f"results receipt has incomplete evidence for {name}")
        try:
            Path(accounts_state["path"]).relative_to(Path(expected["scratch"]))
        except ValueError as error:
            raise RetirementError(f"accounts-state evidence escapes {name} scratch") from error
        invocation_id = unit.get("invocation_id")
        if not isinstance(invocation_id, str):
            raise RetirementError(f"results receipt has invalid invocation for {name}")
        bindings.append(
            ScratchBinding(
                name=name,
                unit=str(expected["unit"]),
                expected_invocation_id=invocation_id,
                scratch=Path(expected["scratch"]),
                physical_bytes=require_nonnegative_integer(
                    scratch.get("physical_bytes"), f"{name} physical bytes"
                ),
                apparent_bytes=require_nonnegative_integer(
                    scratch.get("apparent_bytes"), f"{name} apparent bytes"
                ),
                regular_files=require_nonnegative_integer(
                    scratch.get("regular_files"), f"{name} regular files"
                ),
                directories=require_nonnegative_integer(
                    scratch.get("directories"), f"{name} directories", minimum=1
                ),
            )
        )
    return bindings


def validate_retirement_plan(
    plan: dict[str, Any],
    manifest_path: Path,
    manifest_sha256: str,
    results_path: Path,
    intent_path: Path,
    completion_path: Path,
    bindings: Sequence[ScratchBinding],
) -> None:
    variants = plan.get("variants")
    expected_variants = [
        {"name": binding.name, "scratch": str(binding.scratch)}
        for binding in bindings
    ]
    executable = plan.get("retirer")
    actual_executable = Path(__file__).resolve()
    if (
        plan.get("schema") != PLAN_SCHEMA
        or plan.get("results_manifest") != str(manifest_path)
        or plan.get("results_manifest_sha256") != manifest_sha256
        or plan.get("results_receipt") != str(results_path)
        or plan.get("intent_receipt") != str(intent_path)
        or plan.get("completion_receipt") != str(completion_path)
        or variants != expected_variants
        or not isinstance(executable, dict)
        or executable.get("path") != str(actual_executable)
        or SHA256.fullmatch(str(executable.get("sha256"))) is None
        or executable.get("sha256") != sha256_file(actual_executable)
        or plan.get("preserve_diagnostic_outputs") is not True
        or plan.get("publication_authorized") is not False
        or plan.get("remote_mutations_authorized") is not False
        or plan.get("r2_mutations_authorized") is not False
    ):
        raise RetirementError("retirement plan does not bind this exact local-only cleanup")


def require_exact_scratch(path: Path, admitted_scratches: set[Path]) -> Path:
    if path not in admitted_scratches:
        raise RetirementError(f"scratch is outside the exact admitted set: {path}")
    try:
        root = PRIVATE_ROOT.resolve(strict=True)
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise RetirementError(f"scratch is unavailable: {path}: {error}") from error
    if (
        root != PRIVATE_ROOT
        or resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or os.path.ismount(path)
    ):
        raise RetirementError(f"scratch is not an exact real unmounted directory: {path}")
    return path


def _read_proc_file(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as error:
        raise RetirementError(f"cannot inspect live process evidence {path}: {error}") from error


def _read_proc_link(path: Path) -> str | None:
    try:
        return os.readlink(path).removesuffix(" (deleted)")
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as error:
        raise RetirementError(f"cannot inspect live process link {path}: {error}") from error


def process_references(scratches: Sequence[Path], proc_root: Path = Path("/proc")) -> list[str]:
    references: list[str] = []
    needles = {scratch: os.fsencode(str(scratch)) for scratch in scratches}
    try:
        processes = list(proc_root.iterdir())
    except OSError as error:
        raise RetirementError(f"cannot enumerate processes: {error}") from error
    for process in processes:
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        pid = process.name
        for label in ("cmdline", "maps"):
            content = _read_proc_file(process / label)
            if content is None:
                continue
            for scratch, needle in needles.items():
                if needle in content:
                    references.append(f"pid {pid} {label} references {scratch}")
        for label in ("cwd", "root", "exe"):
            target = _read_proc_link(process / label)
            if target is None:
                continue
            for scratch in scratches:
                try:
                    Path(target).relative_to(scratch)
                except ValueError:
                    continue
                references.append(f"pid {pid} {label} references {scratch}")
        try:
            descriptors = list((process / "fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            descriptors = []
        except OSError as error:
            raise RetirementError(f"cannot enumerate process descriptors {pid}: {error}") from error
        for descriptor in descriptors:
            target = _read_proc_link(descriptor)
            if target is None:
                continue
            for scratch in scratches:
                try:
                    Path(target).relative_to(scratch)
                except ValueError:
                    continue
                references.append(f"pid {pid} fd {descriptor.name} references {scratch}")
    return references


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def require_receipt_parent(path: Path) -> Path:
    if not path.is_absolute():
        raise RetirementError("retirement receipt path must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        metadata = parent.stat()
    except OSError as error:
        raise RetirementError(f"cannot resolve retirement receipt parent: {error}") from error
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RetirementError("retirement receipt directory must be root-owned and owner-only")
    return parent / path.name


def write_json_noclobber(path: Path, payload: dict[str, Any]) -> None:
    destination = require_receipt_parent(path)
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
    fsync_directory(destination.parent)


def delete_exact_tree(path: Path, admitted_scratches: set[Path]) -> None:
    require_exact_scratch(path, admitted_scratches)
    completed = subprocess.run(
        ["/usr/bin/find", str(path), "-xdev", "-depth", "-delete"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or path.exists() or path.is_symlink():
        raise RetirementError(
            f"scratch deletion did not complete for {path}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )


def receipt_common(
    plan_path: Path,
    plan_sha256: str,
    manifest_path: Path,
    manifest_sha256: str,
    results_path: Path,
    results_sha256: str,
    bindings: Sequence[ScratchBinding],
    units: Sequence[UnitState],
) -> dict[str, Any]:
    states = {state.unit: asdict(state) for state in units}
    return {
        "schema": RETIREMENT_SCHEMA,
        "retirement_plan": str(plan_path),
        "retirement_plan_sha256": plan_sha256,
        "manifest": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "results_receipt": str(results_path),
        "results_receipt_sha256": results_sha256,
        "variants": [
            {
                "name": binding.name,
                "unit": states[binding.unit],
                "scratch": str(binding.scratch),
                "recorded_physical_bytes": binding.physical_bytes,
                "recorded_apparent_bytes": binding.apparent_bytes,
                "recorded_regular_files": binding.regular_files,
                "recorded_directories": binding.directories,
            }
            for binding in bindings
        ],
        "preserved_artifacts": [
            "canary receipts",
            "partial diagnostic .jet archives and segment manifests",
            "launch, admission, result, guard, and retirement receipts",
            "systemd journal evidence",
            "genesis and immutable manifests",
        ],
        "remote_mutations": False,
        "r2_mutations": False,
    }


def validate_existing_receipt(
    path: Path,
    expected_status: str,
    plan_path: Path,
    plan_sha256: str,
    manifest_path: Path,
    manifest_sha256: str,
    results_path: Path,
    results_sha256: str,
    bindings: Sequence[ScratchBinding],
) -> tuple[dict[str, Any], str]:
    payload, digest = load_json_file(path, "retirement receipt")
    variants = payload.get("variants")
    expected_pairs = [(binding.name, str(binding.scratch)) for binding in bindings]
    actual_pairs = (
        [(item.get("name"), item.get("scratch")) for item in variants]
        if isinstance(variants, list) and all(isinstance(item, dict) for item in variants)
        else []
    )
    if (
        payload.get("schema") != RETIREMENT_SCHEMA
        or payload.get("status") != expected_status
        or payload.get("retirement_plan") != str(plan_path)
        or payload.get("retirement_plan_sha256") != plan_sha256
        or payload.get("manifest") != str(manifest_path)
        or payload.get("manifest_sha256") != manifest_sha256
        or payload.get("results_receipt") != str(results_path)
        or payload.get("results_receipt_sha256") != results_sha256
        or actual_pairs != expected_pairs
        or payload.get("remote_mutations") is not False
        or payload.get("r2_mutations") is not False
    ):
        raise RetirementError(f"existing retirement receipt is not the expected binding: {path}")
    return payload, digest


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retirement-plan", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--results-receipt", type=Path, required=True)
    parser.add_argument("--intent-receipt", type=Path, required=True)
    parser.add_argument("--completion-receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.intent_receipt == args.completion_receipt:
        parser.error("intent and completion receipts must differ")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise RetirementError("performance scratch retirement must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    plan, plan_sha256 = load_json_file(args.retirement_plan, "retirement plan")
    manifest, manifest_sha256 = load_json_file(args.manifest, "results manifest")
    results, results_sha256 = load_json_file(args.results_receipt, "results receipt")
    bindings = validate_manifest_and_results(
        args.manifest,
        manifest,
        manifest_sha256,
        args.results_receipt,
        results,
    )
    validate_retirement_plan(
        plan,
        args.manifest,
        manifest_sha256,
        args.results_receipt,
        args.intent_receipt,
        args.completion_receipt,
        bindings,
    )
    scratches = [binding.scratch for binding in bindings]
    admitted_scratches = set(scratches)

    intent_exists = args.intent_receipt.exists() or args.intent_receipt.is_symlink()
    completion_exists = args.completion_receipt.exists() or args.completion_receipt.is_symlink()
    if completion_exists and not intent_exists:
        raise RetirementError("completion receipt exists without its deletion intent")

    units = [sample_unit(binding.unit) for binding in bindings]
    for binding, unit in zip(bindings, units, strict=True):
        require_terminal_success(unit, binding.expected_invocation_id)
    references = process_references(scratches)
    if references:
        raise RetirementError("live process still references performance scratch: " + "; ".join(references))

    if intent_exists:
        _, intent_sha256 = validate_existing_receipt(
            args.intent_receipt,
            "deletion-intent-fsynced",
            args.retirement_plan,
            plan_sha256,
            args.manifest,
            manifest_sha256,
            args.results_receipt,
            results_sha256,
            bindings,
        )
    else:
        for scratch in scratches:
            require_exact_scratch(scratch, admitted_scratches)
        common = receipt_common(
            args.retirement_plan,
            plan_sha256,
            args.manifest,
            manifest_sha256,
            args.results_receipt,
            results_sha256,
            bindings,
            units,
        )
        intent = {
            **common,
            "status": "deletion-intent-fsynced",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "available_bytes_before": available_bytes(PRIVATE_ROOT),
        }
        write_json_noclobber(args.intent_receipt, intent)
        intent_sha256 = sha256_file(args.intent_receipt)

    if completion_exists:
        completion, _ = validate_existing_receipt(
            args.completion_receipt,
            "deleted",
            args.retirement_plan,
            plan_sha256,
            args.manifest,
            manifest_sha256,
            args.results_receipt,
            results_sha256,
            bindings,
        )
        if completion.get("intent_receipt_sha256") != intent_sha256:
            raise RetirementError("completion receipt does not bind the deletion intent")
        if any(path.exists() or path.is_symlink() for path in scratches):
            raise RetirementError("completion receipt exists while a bound scratch tree remains")
        print(json.dumps(completion, sort_keys=True), flush=True)
        return 0

    intent, _ = validate_existing_receipt(
        args.intent_receipt,
        "deletion-intent-fsynced",
        args.retirement_plan,
        plan_sha256,
        args.manifest,
        manifest_sha256,
        args.results_receipt,
        results_sha256,
        bindings,
    )
    for binding in bindings:
        if binding.scratch.exists() or binding.scratch.is_symlink():
            delete_exact_tree(binding.scratch, admitted_scratches)
    if any(path.exists() or path.is_symlink() for path in scratches):
        raise RetirementError("one or more exact scratch trees remain after deletion")
    completion = {
        **receipt_common(
            args.retirement_plan,
            plan_sha256,
            args.manifest,
            manifest_sha256,
            args.results_receipt,
            results_sha256,
            bindings,
            units,
        ),
        "status": "deleted",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "intent_receipt": str(args.intent_receipt),
        "intent_receipt_sha256": intent_sha256,
        "available_bytes_before": intent.get("available_bytes_before"),
        "available_bytes_after": available_bytes(PRIVATE_ROOT),
    }
    write_json_noclobber(args.completion_receipt, completion)
    print(json.dumps(completion, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RetirementError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
