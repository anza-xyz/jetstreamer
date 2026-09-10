#!/usr/bin/env python3
"""Read-only GCS snapshot inventory preflight for mainnet epochs 1 through 100.

Each epoch with a root checkpoint is an independent verification cohort. A run
of epochs without a root checkpoint is joined to the first later epoch with a
root checkpoint, provided every epoch uses the same runtime. Such a cohort
starts from a root snapshot in the epoch immediately before the cohort. Hourly
objects remain eligible only as transport bootstraps for single-epoch cohorts;
they are never trust anchors or replay checkpoints.

The two fixture options consume the unmodified output of ``gcloud storage ls
--json``.  Supplying either fixture requires supplying both, and suppresses all
gcloud execution.

Run ``scripts/preflight_gcs_snapshots.py`` for live inventory, or run
``scripts/preflight_gcs_snapshots.py --inventory-root-json ROOT.json
--inventory-hourly-json HOURLY.json`` for an offline preflight.  The command
prints one JSON report and does not write files or modify cloud resources.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


BUCKET_NAME = "mainnet-beta-ledger-us-ny5"
BUCKET_URI = f"gs://{BUCKET_NAME}"
ROOT_PATTERN = f"{BUCKET_URI}/*/snapshot-*"
HOURLY_PATTERN = f"{BUCKET_URI}/*/hourly/snapshot-*"
GCLOUD_ACCOUNT = "sam.johnson@anza.xyz"
BILLING_PROJECT = "principal-lane-200702"
LOCAL_ROOT = Path("/home/sol/horizon")
FIRST_EPOCH = 1
LAST_EPOCH = 100
EPOCH_SLOTS = 432_000
UINT64_MAX = (1 << 64) - 1
SCHEMA = "jetstreamer-gcs-snapshot-preflight-v2"
EPOCH_12_BOOTSTRAP_SLOT = 5_183_736
EPOCH_12_BOOTSTRAP_ACCOUNTS_HASH = (
    "BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR"
)
RUNTIME_ROUTES = (
    (1, 1, "solana-v1.0.7-to-v1.0.8", (".tar.bz2",)),
    (2, 7, "solana-v1.0.8", (".tar.bz2",)),
    (8, 8, "solana-v1.0.13", (".tar.bz2",)),
    (9, 10, "solana-v1.0.14", (".tar.bz2",)),
    (11, 11, "solana-v1.0.14", (".tar.bz2",)),
    (12, 29, "solana-v1.0.23", (".tar.bz2",)),
    (30, 60, "solana-v1.1.23", (".tar.bz2",)),
    (61, 91, "solana-v1.2.32", (".tar.bz2", ".tar.zst")),
    (92, 100, "solana-v1.3.19", (".tar.bz2", ".tar.zst")),
)
SNAPSHOT_ARCHIVE_EXTENSIONS = (".tar.zst", ".tar.lz4", ".tar.bz2")

_DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_SNAPSHOT_BASENAME_RE = re.compile(
    r"snapshot-(?P<slot>0|[1-9][0-9]*)-"
    r"(?P<identity>[1-9A-HJ-NP-Za-km-z]{32,44})"
    r"(?P<extension>\.tar\.(?:zst|lz4|bz2))\Z"
)
_SNAPSHOT_PATH_RE = re.compile(
    r"(?P<anchor>0|[1-9][0-9]*)/"
    r"(?:(?P<hourly>hourly)/)?"
    r"snapshot-(?P<slot>0|[1-9][0-9]*)-"
    r"(?P<identity>[1-9A-HJ-NP-Za-km-z]{32,44})"
    r"(?P<extension>\.tar\.(?:zst|lz4|bz2))\Z"
)
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_VALUES = {character: index for index, character in enumerate(_BASE58_ALPHABET)}


class PreflightError(Exception):
    """An inventory cannot be used safely."""


class _DuplicateJsonKey(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class SnapshotObject:
    source: str
    uri: str
    versioned_uri: str
    object_name: str
    filename: str
    accounts_hash: str
    anchor_slot: int
    slot: int
    extension: str
    size: int
    generation: int
    metageneration: int
    crc32c: str


@dataclasses.dataclass(frozen=True)
class EpochPlan:
    epoch: int
    runtime: str
    accepted_extensions: Tuple[str, ...]
    bootstrap: SnapshotObject
    checkpoints: Tuple[SnapshotObject, ...]
    cohort_first_epoch: int
    cohort_last_epoch: int


@dataclasses.dataclass(frozen=True)
class VerificationCohort:
    first_epoch: int
    last_epoch: int
    runtime: str
    accepted_extensions: Tuple[str, ...]
    bootstrap: SnapshotObject
    checkpoints: Tuple[SnapshotObject, ...]


def _object_pairs_no_duplicates(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _required_string(mapping: Mapping[str, Any], key: str, context: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise PreflightError(f"{context}: {key} must be a non-empty string")
    return value


def _canonical_u64(value: Any, field: str, context: str, *, nonzero: bool = False) -> int:
    # GCS represents uint64 metadata fields as decimal JSON strings.
    if not isinstance(value, str) or _DECIMAL_RE.fullmatch(value) is None:
        raise PreflightError(f"{context}: {field} must be a canonical decimal string")
    parsed = int(value)
    if parsed > UINT64_MAX or (nonzero and parsed == 0):
        qualifier = "non-zero " if nonzero else ""
        raise PreflightError(f"{context}: {field} is not a {qualifier}uint64")
    return parsed


def _path_u64(value: str, field: str, context: str) -> int:
    parsed = int(value)
    if parsed > UINT64_MAX:
        raise PreflightError(f"{context}: {field} exceeds uint64")
    return parsed


def _optional_path_u64(value: str) -> Optional[int]:
    if _DECIMAL_RE.fullmatch(value) is None:
        return None
    parsed = int(value)
    return parsed if parsed <= UINT64_MAX else None


def _base58_decodes_to_32_bytes(value: str) -> bool:
    number = 0
    try:
        for character in value:
            number = number * 58 + _BASE58_VALUES[character]
    except KeyError:
        return False
    encoded = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    decoded = b"\0" * (len(value) - len(value.lstrip("1"))) + encoded
    return len(decoded) == 32


def _validate_crc32c(value: str, context: str) -> None:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise PreflightError(f"{context}: crc32c is not canonical base64") from error
    if len(decoded) != 4 or base64.b64encode(decoded).decode("ascii") != value:
        raise PreflightError(f"{context}: crc32c must encode exactly four bytes")


def _parse_snapshot_object(
    raw: Any,
    source: str,
    index: int,
    relevant_slots: Tuple[int, int],
) -> Optional[SnapshotObject]:
    context = f"{source} inventory entry {index}"
    if not isinstance(raw, dict):
        raise PreflightError(f"{context}: entry must be a JSON object")
    if raw.get("type") != "cloud_object":
        raise PreflightError(f"{context}: type must be 'cloud_object'")
    versioned_uri = _required_string(raw, "url", context)
    metadata = raw.get("metadata")
    if not isinstance(metadata, dict):
        raise PreflightError(f"{context}: metadata must be a JSON object")
    if metadata.get("kind") != "storage#object":
        raise PreflightError(f"{context}: metadata.kind must be 'storage#object'")

    bucket = _required_string(metadata, "bucket", context)
    object_name = _required_string(metadata, "name", context)
    if bucket != BUCKET_NAME:
        raise PreflightError(f"{context}: unexpected bucket {bucket!r}")
    filename = object_name.rsplit("/", 1)[-1]
    if not filename.endswith(SNAPSHOT_ARCHIVE_EXTENSIONS):
        return None
    filename_match = _SNAPSHOT_BASENAME_RE.fullmatch(filename)
    if filename_match is None or not _base58_decodes_to_32_bytes(
        filename_match.group("identity")
    ):
        raise PreflightError(f"{context}: invalid snapshot object path {object_name!r}")
    filename_slot = _path_u64(filename_match.group("slot"), "snapshot slot", context)
    anchor_text = object_name.split("/", 1)[0]
    anchor_slot = _optional_path_u64(anchor_text)
    relevant_start, relevant_end = relevant_slots
    below_range = filename_slot < relevant_start and (
        anchor_slot is None or anchor_slot < relevant_start
    )
    above_range = filename_slot > relevant_end and (
        anchor_slot is None or anchor_slot > relevant_end
    )
    if below_range or above_range:
        return None
    generation_text = _required_string(metadata, "generation", context)
    generation = _canonical_u64(generation_text, "metadata.generation", context, nonzero=True)
    metageneration = _canonical_u64(
        metadata.get("metageneration"), "metadata.metageneration", context, nonzero=True
    )
    size = _canonical_u64(metadata.get("size"), "metadata.size", context, nonzero=True)
    crc32c = _required_string(metadata, "crc32c", context)
    _validate_crc32c(crc32c, context)

    uri = f"{BUCKET_URI}/{object_name}"
    expected_versioned_uri = f"{uri}#{generation_text}"
    if versioned_uri != expected_versioned_uri:
        raise PreflightError(
            f"{context}: url {versioned_uri!r} does not match metadata identity "
            f"{expected_versioned_uri!r}"
        )
    object_id = _required_string(metadata, "id", context)
    if object_id != f"{BUCKET_NAME}/{object_name}/{generation_text}":
        raise PreflightError(f"{context}: metadata.id does not match name and generation")

    match = _SNAPSHOT_PATH_RE.fullmatch(object_name)
    if match is None:
        raise PreflightError(f"{context}: invalid snapshot object path {object_name!r}")
    location = "hourly" if match.group("hourly") else "root"
    if location != source:
        raise PreflightError(f"{context}: path belongs to the {location} inventory")
    anchor_slot = _path_u64(match.group("anchor"), "anchor slot", context)
    snapshot_slot = _path_u64(match.group("slot"), "snapshot slot", context)
    if location == "root" and anchor_slot != snapshot_slot:
        raise PreflightError(f"{context}: root directory slot does not match filename slot")
    if location == "hourly" and anchor_slot > snapshot_slot:
        raise PreflightError(f"{context}: hourly anchor is later than snapshot slot")
    identity = match.group("identity")
    if not _base58_decodes_to_32_bytes(identity):
        raise PreflightError(f"{context}: snapshot identity is not a 32-byte base58 hash")

    return SnapshotObject(
        source=source,
        uri=uri,
        versioned_uri=versioned_uri,
        object_name=object_name,
        filename=object_name.rsplit("/", 1)[-1],
        accounts_hash=identity,
        anchor_slot=anchor_slot,
        slot=snapshot_slot,
        extension=match.group("extension"),
        size=size,
        generation=generation,
        metageneration=metageneration,
        crc32c=crc32c,
    )


def requested_slot_range(
    first_epoch: int = FIRST_EPOCH, last_epoch: int = LAST_EPOCH
) -> Tuple[int, int]:
    if first_epoch < FIRST_EPOCH or last_epoch > LAST_EPOCH or first_epoch > last_epoch:
        raise PreflightError(
            f"requested epoch range {first_epoch}-{last_epoch} is outside "
            f"{FIRST_EPOCH}-{LAST_EPOCH}"
        )
    return (first_epoch - 1) * EPOCH_SLOTS, (last_epoch + 1) * EPOCH_SLOTS - 1


def parse_inventory_json(
    text: str,
    source: str,
    relevant_slots: Optional[Tuple[int, int]] = None,
) -> Tuple[SnapshotObject, ...]:
    """Parse one raw gcloud JSON listing and validate every returned object."""
    if source not in ("root", "hourly"):
        raise ValueError("source must be 'root' or 'hourly'")
    try:
        raw = json.loads(
            text,
            object_pairs_hook=_object_pairs_no_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, _DuplicateJsonKey, ValueError) as error:
        raise PreflightError(f"{source} inventory is not valid strict JSON: {error}") from error
    if not isinstance(raw, list):
        raise PreflightError(f"{source} inventory must be a JSON list")

    if relevant_slots is None:
        relevant_slots = requested_slot_range()
    if relevant_slots[0] > relevant_slots[1]:
        raise ValueError("relevant slot range must not be empty")

    objects = [
        parsed
        for index, item in enumerate(raw)
        if (parsed := _parse_snapshot_object(item, source, index, relevant_slots)) is not None
    ]
    seen: Dict[str, SnapshotObject] = {}
    for item in objects:
        previous = seen.get(item.object_name)
        if previous is not None:
            if previous == item:
                continue
            raise PreflightError(
                f"{source} inventory has conflicting metadata for object {item.object_name!r}"
            )
        seen[item.object_name] = item
    return tuple(
        sorted(seen.values(), key=lambda item: (item.slot, item.object_name, item.generation))
    )


def runtime_route(epoch: int) -> Tuple[str, Tuple[str, ...]]:
    for first, last, runtime, extensions in RUNTIME_ROUTES:
        if first <= epoch <= last:
            return runtime, extensions
    raise PreflightError(f"epoch {epoch} is outside supported range {FIRST_EPOCH}-{LAST_EPOCH}")


def accepted_extensions(epoch: int) -> Tuple[str, ...]:
    return runtime_route(epoch)[1]


def epoch_slot_range(epoch: int) -> Tuple[int, int]:
    start = epoch * EPOCH_SLOTS
    return start, start + EPOCH_SLOTS - 1


def _unique_newest(
    candidates: Sequence[SnapshotObject], description: str
) -> SnapshotObject:
    if not candidates:
        raise PreflightError(f"no compatible {description}")
    newest_slot = max(item.slot for item in candidates)
    newest = sorted(
        (item for item in candidates if item.slot == newest_slot),
        key=lambda item: item.versioned_uri,
    )
    if len(newest) != 1:
        choices = ", ".join(item.versioned_uri for item in newest)
        raise PreflightError(
            f"newest compatible {description} slot {newest_slot} is ambiguous: {choices}"
        )
    return newest[0]


def _select_bootstrap(
    epoch: int,
    candidates: Sequence[SnapshotObject],
    *,
    root_only: bool,
) -> SnapshotObject:
    _, extensions = runtime_route(epoch)
    prior_start, prior_end = epoch_slot_range(epoch - 1)
    eligible = [
        item
        for item in candidates
        if prior_start <= item.slot <= prior_end
        and item.extension in extensions
        and (not root_only or item.source == "root")
    ]
    if epoch == 12:
        eligible = [
            item
            for item in eligible
            if item.slot == EPOCH_12_BOOTSTRAP_SLOT
            and item.accounts_hash == EPOCH_12_BOOTSTRAP_ACCOUNTS_HASH
        ]
    source = "root bootstrap" if root_only else "bootstrap"
    try:
        return _unique_newest(eligible, source)
    except PreflightError as error:
        if epoch == 12:
            raise PreflightError(
                "epoch 12: required canonical bootstrap "
                f"snapshot-{EPOCH_12_BOOTSTRAP_SLOT}-"
                f"{EPOCH_12_BOOTSTRAP_ACCOUNTS_HASH}.tar.bz2 is absent or ambiguous"
            ) from error
        raise PreflightError(
            f"epoch {epoch}: {error} in prior-epoch slots {prior_start}..={prior_end}"
        ) from error


def _root_checkpoints_through(
    root_objects: Sequence[SnapshotObject],
    bootstrap_slot: int,
    end_slot_inclusive: int,
    extensions: Tuple[str, ...],
    context: str,
) -> Tuple[SnapshotObject, ...]:
    groups: Dict[int, List[SnapshotObject]] = {}
    for item in root_objects:
        if bootstrap_slot < item.slot <= end_slot_inclusive and item.extension in extensions:
            groups.setdefault(item.slot, []).append(item)
    checkpoints: List[SnapshotObject] = []
    for slot_number in sorted(groups):
        choices = sorted(groups[slot_number], key=lambda item: item.versioned_uri)
        if len(choices) != 1:
            detail = ", ".join(item.versioned_uri for item in choices)
            raise PreflightError(
                f"{context}: root checkpoint slot {slot_number} is ambiguous: {detail}"
            )
        checkpoints.append(choices[0])
    return tuple(checkpoints)


def build_verification_cohorts(
    root_objects: Sequence[SnapshotObject],
    hourly_objects: Sequence[SnapshotObject],
    first_epoch: int = FIRST_EPOCH,
    last_epoch: int = LAST_EPOCH,
) -> Tuple[VerificationCohort, ...]:
    """Plan the shortest fail-closed cohorts that end at a root checkpoint."""
    requested_slot_range(first_epoch, last_epoch)
    all_bootstraps = tuple(root_objects) + tuple(hourly_objects)
    cohorts: List[VerificationCohort] = []
    epoch = first_epoch
    while epoch <= last_epoch:
        runtime, extensions = runtime_route(epoch)
        bootstrap = _select_bootstrap(epoch, all_bootstraps, root_only=False)
        _, epoch_end = epoch_slot_range(epoch)
        checkpoints = _root_checkpoints_through(
            root_objects,
            bootstrap.slot,
            epoch_end,
            extensions,
            f"epoch {epoch}",
        )
        if checkpoints:
            cohorts.append(
                VerificationCohort(
                    epoch,
                    epoch,
                    runtime,
                    extensions,
                    bootstrap,
                    checkpoints,
                )
            )
            epoch += 1
            continue

        # A checkpoint-free epoch must be replayed continuously from a root
        # in its predecessor epoch through the first later root. Replaying the
        # later epoch from an hourly object would leave the earlier archives
        # outside the verified state transition.
        bootstrap = _select_bootstrap(epoch, root_objects, root_only=True)
        cohort_end = epoch
        cohort_checkpoints: Tuple[SnapshotObject, ...] = ()
        while cohort_end <= last_epoch:
            if cohort_end != epoch:
                next_runtime, next_extensions = runtime_route(cohort_end)
                if next_runtime != runtime or next_extensions != extensions:
                    raise PreflightError(
                        f"epoch {epoch}: root-checkpoint gap crosses runtime boundary at epoch "
                        f"{cohort_end} ({runtime} to {next_runtime})"
                    )
            _, candidate_end = epoch_slot_range(cohort_end)
            cohort_checkpoints = _root_checkpoints_through(
                root_objects,
                bootstrap.slot,
                candidate_end,
                extensions,
                f"epochs {epoch}-{cohort_end}",
            )
            final_start, _ = epoch_slot_range(cohort_end)
            if any(item.slot >= final_start for item in cohort_checkpoints):
                break
            if cohort_end == last_epoch:
                cohort_checkpoints = ()
                break
            cohort_end += 1
        if not cohort_checkpoints:
            raise PreflightError(
                f"epoch {epoch}: no compatible unique root checkpoint after trusted root "
                f"bootstrap slot {bootstrap.slot} through requested epoch {last_epoch}"
            )
        cohorts.append(
            VerificationCohort(
                epoch,
                cohort_end,
                runtime,
                extensions,
                bootstrap,
                cohort_checkpoints,
            )
        )
        epoch = cohort_end + 1
    return tuple(cohorts)


def build_epoch_plans(
    root_objects: Sequence[SnapshotObject],
    hourly_objects: Sequence[SnapshotObject],
    first_epoch: int = FIRST_EPOCH,
    last_epoch: int = LAST_EPOCH,
) -> Tuple[EpochPlan, ...]:
    """Expand root-aware verification cohorts into per-epoch work records."""
    plans: List[EpochPlan] = []
    for cohort in build_verification_cohorts(
        root_objects, hourly_objects, first_epoch, last_epoch
    ):
        for epoch in range(cohort.first_epoch, cohort.last_epoch + 1):
            epoch_start, epoch_end = epoch_slot_range(epoch)
            checkpoints = tuple(
                item
                for item in cohort.checkpoints
                if epoch_start <= item.slot <= epoch_end
            )
            plans.append(
                EpochPlan(
                    epoch,
                    cohort.runtime,
                    cohort.accepted_extensions,
                    cohort.bootstrap,
                    checkpoints,
                    cohort.first_epoch,
                    cohort.last_epoch,
                )
            )
    return tuple(plans)


def _manifest_object(item: SnapshotObject) -> Dict[str, Any]:
    return {
        "accounts_hash": item.accounts_hash,
        "anchor_slot": item.anchor_slot,
        "crc32c": item.crc32c,
        "extension": item.extension,
        "generation": item.generation,
        "size": item.size,
        "slot": item.slot,
        "source": item.source,
        "uri": item.uri,
        "versioned_uri": item.versioned_uri,
    }


def build_manifest(plans: Sequence[EpochPlan]) -> Dict[str, Any]:
    cohort_records: List[Dict[str, Any]] = []
    for plan in plans:
        key = (plan.cohort_first_epoch, plan.cohort_last_epoch)
        if cohort_records and (
            cohort_records[-1]["first_epoch"], cohort_records[-1]["last_epoch"]
        ) == key:
            cohort_records[-1]["root_checkpoints"].extend(
                _manifest_object(item) for item in plan.checkpoints
            )
            continue
        cohort_records.append(
            {
                "accepted_extensions": list(plan.accepted_extensions),
                "bootstrap": _manifest_object(plan.bootstrap),
                "first_epoch": plan.cohort_first_epoch,
                "last_epoch": plan.cohort_last_epoch,
                "publication_gate": "all-archives-validated-and-final-root-verified",
                "root_checkpoints": [
                    _manifest_object(item) for item in plan.checkpoints
                ],
                "runtime": plan.runtime,
            }
        )
    return {
        "account": GCLOUD_ACCOUNT,
        "billing_project": BILLING_PROJECT,
        "bucket": BUCKET_URI,
        "epoch_slots": EPOCH_SLOTS,
        "first_epoch": plans[0].epoch if plans else None,
        "inventory_patterns": {"hourly": HOURLY_PATTERN, "root": ROOT_PATTERN},
        "last_epoch": plans[-1].epoch if plans else None,
        "runtime_routes": [
            {
                "accepted_extensions": list(extensions),
                "first_epoch": first,
                "last_epoch": last,
                "runtime": runtime,
            }
            for first, last, runtime, extensions in RUNTIME_ROUTES
        ],
        "selection_policy": {
            "bootstrap_sources": ["root", "hourly"],
            "bootstrap_window": "prior-epoch",
            "checkpoint_gap_bootstrap_source": "root",
            "checkpoint_gap_runtime_boundary": "reject",
            "checkpoint_source": "root",
            "checkpoint_window": "after-bootstrap-through-cohort-end",
            "newest_slot_must_be_unique": True,
            "publication": "withhold-complete-cohort-until-final-root",
        },
        "verification_cohorts": cohort_records,
        "epochs": [
            {
                "accepted_extensions": list(plan.accepted_extensions),
                "bootstrap": _manifest_object(plan.bootstrap),
                "bootstrap_window": {
                    "end_inclusive": epoch_slot_range(plan.cohort_first_epoch - 1)[1],
                    "start": epoch_slot_range(plan.cohort_first_epoch - 1)[0],
                },
                "epoch": plan.epoch,
                "runtime_state_source": (
                    f"{plan.bootstrap.source}-bootstrap"
                    if plan.epoch == plan.cohort_first_epoch
                    else "carried-from-previous-epoch"
                ),
                "verification_cohort": {
                    "first_epoch": plan.cohort_first_epoch,
                    "last_epoch": plan.cohort_last_epoch,
                },
                "post_bootstrap_root_checkpoints": [
                    _manifest_object(item) for item in plan.checkpoints
                ],
                "runtime": plan.runtime,
            }
            for plan in plans
        ],
        "schema": SCHEMA,
    }


def manifest_fingerprint(manifest: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        manifest, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def selected_bootstraps(plans: Sequence[EpochPlan]) -> Tuple[SnapshotObject, ...]:
    by_identity: Dict[Tuple[str, int], SnapshotObject] = {}
    filenames: Dict[str, SnapshotObject] = {}
    for plan in plans:
        item = plan.bootstrap
        by_identity[(item.uri, item.generation)] = item
        previous = filenames.get(item.filename)
        if previous is not None and (previous.uri, previous.generation) != (
            item.uri,
            item.generation,
        ):
            raise PreflightError(
                f"selected objects collide at local filename {item.filename!r}: "
                f"{previous.versioned_uri}, {item.versioned_uri}"
            )
        filenames[item.filename] = item
    return tuple(sorted(by_identity.values(), key=lambda item: item.versioned_uri))


def build_storage_report(
    plans: Sequence[EpochPlan], local_root: Path = LOCAL_ROOT, free_bytes: Optional[int] = None
) -> Dict[str, Any]:
    try:
        root_metadata = local_root.lstat()
    except OSError as error:
        raise PreflightError(f"cannot inspect local root {local_root}: {error}") from error
    if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
        raise PreflightError(f"local root is not a real directory: {local_root}")

    selected = selected_bootstraps(plans)
    selected_bootstrap_bytes = sum(item.size for item in selected)
    present_bootstrap_bytes = 0
    missing: List[Dict[str, Any]] = []
    conflicts = 0
    for item in selected:
        local_path = local_root / item.filename
        try:
            metadata = local_path.lstat()
        except FileNotFoundError:
            missing.append(
                {
                    "expected_bytes": item.size,
                    "local_path": str(local_path),
                    "reason": "absent",
                    "versioned_uri": item.versioned_uri,
                }
            )
            continue
        except OSError as error:
            raise PreflightError(
                f"cannot inspect selected local path {local_path}: {error}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            conflicts += 1
            reason = "not-a-regular-file"
        elif metadata.st_size != item.size:
            conflicts += 1
            reason = f"size-mismatch:{metadata.st_size}"
        else:
            present_bootstrap_bytes += item.size
            continue
        missing.append(
            {
                "expected_bytes": item.size,
                "local_path": str(local_path),
                "reason": reason,
                "versioned_uri": item.versioned_uri,
            }
        )

    missing_bootstrap_bytes = selected_bootstrap_bytes - present_bootstrap_bytes
    if free_bytes is None:
        try:
            free_bytes = shutil.disk_usage(local_root).free
        except OSError as error:
            raise PreflightError(f"cannot read free space for {local_root}: {error}") from error
    if not isinstance(free_bytes, int) or isinstance(free_bytes, bool) or free_bytes < 0:
        raise ValueError("free_bytes must be a non-negative integer")
    enough_space = free_bytes >= missing_bootstrap_bytes
    return {
        "conflicting_local_paths": conflicts,
        "enough_free_bytes_for_missing_bootstraps": enough_space,
        "free_bytes": free_bytes,
        "local_root": str(local_root),
        "missing_bootstrap_objects": missing,
        "missing_local_bootstrap_bytes": missing_bootstrap_bytes,
        "present_bootstrap_bytes": present_bootstrap_bytes,
        "preflight_ok": enough_space and conflicts == 0,
        "selected_bootstrap_bytes": selected_bootstrap_bytes,
        "selected_bootstrap_objects": len(selected),
    }


def gcloud_inventory_command(pattern: str) -> List[str]:
    if pattern not in (ROOT_PATTERN, HOURLY_PATTERN):
        raise ValueError("unexpected inventory pattern")
    return [
        "gcloud",
        "storage",
        "ls",
        "--json",
        "--quiet",
        f"--account={GCLOUD_ACCOUNT}",
        f"--billing-project={BILLING_PROJECT}",
        pattern,
    ]


def run_gcloud_inventory(pattern: str) -> str:
    command = gcloud_inventory_command(pattern)
    environment = os.environ.copy()
    environment["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="strict",
            env=environment,
        )
    except (OSError, UnicodeError) as error:
        raise PreflightError(f"failed to run {' '.join(command)}: {error}") from error
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "no stderr"
        raise PreflightError(
            f"gcloud inventory command failed with status {completed.returncode}: "
            f"{' '.join(command)}: {detail}"
        )
    return completed.stdout


def _read_fixture(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise PreflightError(f"cannot read {label} inventory fixture {path}: {error}") from error


def load_inventory_texts(
    root_fixture: Optional[Path], hourly_fixture: Optional[Path]
) -> Tuple[str, str]:
    if (root_fixture is None) != (hourly_fixture is None):
        raise PreflightError(
            "--inventory-root-json and --inventory-hourly-json must be supplied together"
        )
    if root_fixture is not None and hourly_fixture is not None:
        return _read_fixture(root_fixture, "root"), _read_fixture(hourly_fixture, "hourly")
    # Keep these as distinct invocations.  The path shape is part of the trust
    # boundary between canonical checkpoints and bootstrap-only hourly objects.
    return run_gcloud_inventory(ROOT_PATTERN), run_gcloud_inventory(HOURLY_PATTERN)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory-root-json",
        type=Path,
        help="raw JSON output from the root gcloud listing (requires the hourly fixture)",
    )
    parser.add_argument(
        "--inventory-hourly-json",
        type=Path,
        help="raw JSON output from the hourly gcloud listing (requires the root fixture)",
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=LOCAL_ROOT,
        help=f"directory checked for selected archives (default: {LOCAL_ROOT})",
    )
    parser.add_argument("--first-epoch", type=int, default=FIRST_EPOCH)
    parser.add_argument("--last-epoch", type=int, default=LAST_EPOCH)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        relevant_slots = requested_slot_range(arguments.first_epoch, arguments.last_epoch)
        root_text, hourly_text = load_inventory_texts(
            arguments.inventory_root_json, arguments.inventory_hourly_json
        )
        root_objects = parse_inventory_json(root_text, "root", relevant_slots)
        hourly_objects = parse_inventory_json(hourly_text, "hourly", relevant_slots)
        plans = build_epoch_plans(
            root_objects,
            hourly_objects,
            arguments.first_epoch,
            arguments.last_epoch,
        )
        manifest = build_manifest(plans)
        fingerprint = manifest_fingerprint(manifest)
        storage = build_storage_report(plans, arguments.local_root)
        report = {
            "free_bytes": storage["free_bytes"],
            "inventory_objects": {"hourly": len(hourly_objects), "root": len(root_objects)},
            "manifest": manifest,
            "manifest_fingerprint": fingerprint,
            "missing_local_bootstrap_bytes": storage["missing_local_bootstrap_bytes"],
            "selected_bootstrap_bytes": storage["selected_bootstrap_bytes"],
            "storage": storage,
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if storage["preflight_ok"] else 2
    except PreflightError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
