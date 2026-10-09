#!/usr/bin/env python3
"""Read-only GCS snapshot inventory preflight for supported mainnet epochs.

By default, each epoch with a root checkpoint is an independent verification
cohort. ``--target-cohort-epochs`` may deliberately coalesce up to four
consecutive epochs that use the same runtime, preserving every intermediate
root checkpoint while amortizing bootstrap and AccountsDb/cache setup. If the
preferred endpoint has no root, the cohort ends at the latest earlier root in
that target window. Only a target window with no root is extended to the first
later epoch with one, provided every epoch uses the same runtime. Every
multi-epoch cohort starts from a root snapshot in the epoch immediately before
the cohort. ``--publish-through-epoch`` may make the final part of the planned
range an isolated verification tail: those epochs participate in the terminal
root proof but remain private and are never publication candidates.
Hourly objects remain eligible only as transport bootstraps for single-epoch
cohorts; they are never trust anchors or replay checkpoints.

The two fixture options consume the unmodified output of ``gcloud storage ls
--json``.  Supplying either fixture requires supplying both, and suppresses all
gcloud execution.  A previously sealed manifest report may instead be supplied
with its independently recorded fingerprint.  That mode strictly validates the
old report, extracts only its generation-bound snapshot identities, and replans
them under the current schema without contacting GCS.  The complete source
report must remain structurally and cryptographically valid, while current
runtime-route agreement is required only for the requested subrange.  An
unrelated compatibility correction outside that subrange therefore cannot
invalidate otherwise immutable snapshot evidence.

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
MAX_SUPPORTED_EPOCH = 301
MAX_TARGET_COHORT_EPOCHS = 4
MAX_SOURCE_MANIFEST_BYTES = 64 * 1024 * 1024
EPOCH_SLOTS = 432_000
UINT64_MAX = (1 << 64) - 1
SCHEMA = "jetstreamer-gcs-snapshot-preflight-v4"
SOURCE_MANIFEST_SCHEMAS = frozenset(
    {
        "jetstreamer-gcs-snapshot-preflight-v2",
        "jetstreamer-gcs-snapshot-preflight-v3",
        SCHEMA,
    }
)
EPOCH_12_BOOTSTRAP_SLOT = 5_183_736
EPOCH_12_BOOTSTRAP_ACCOUNTS_HASH = (
    "BUqwiSm2GgH9ByKrBDF6epXHYK9RRh3vyZDKtUqtMXfR"
)
EPOCH_154_BOOTSTRAP_SLOT = 66_527_778
EPOCH_154_BOOTSTRAP_ACCOUNTS_HASH = (
    "CocT95ybNYZNtjH1kWmybzknzb1D17up3Rbt5MT9iCxY"
)
RUNTIME_ROUTES = (
    (1, 1, "solana-v1.0.7-to-v1.0.8", (".tar.bz2",)),
    (2, 7, "solana-v1.0.8", (".tar.bz2",)),
    (8, 8, "solana-v1.0.13", (".tar.bz2",)),
    (9, 10, "solana-v1.0.14", (".tar.bz2",)),
    (11, 11, "solana-v1.0.14", (".tar.bz2",)),
    (12, 29, "solana-v1.0.23", (".tar.bz2",)),
    (30, 30, "solana-v1.1.15", (".tar.bz2",)),
    (31, 60, "solana-v1.1.23", (".tar.bz2",)),
    (61, 66, "solana-v1.2.32", (".tar.bz2", ".tar.zst")),
    (
        67,
        68,
        "solana-v1.2.32-mainnet-epoch68-transition",
        (".tar.bz2", ".tar.zst"),
    ),
    (69, 91, "solana-v1.2.32", (".tar.bz2", ".tar.zst")),
    (92, 100, "solana-v1.3.19", (".tar.bz2", ".tar.zst")),
    (101, 128, "solana-v1.3.23", (".tar.bz2", ".tar.zst")),
    (129, 130, "solana-v1.4.17", (".tar.bz2", ".tar.zst")),
    (131, 134, "solana-v1.4.19", (".tar.bz2", ".tar.zst")),
    (135, 147, "solana-v1.4.25", (".tar.bz2", ".tar.zst")),
    (148, 149, "solana-v1.5.5", (".tar.bz2", ".tar.zst")),
    (150, 153, "solana-v1.5.6", (".tar.bz2", ".tar.zst")),
    (154, 154, "solana-v1.5.8", (".tar.bz2", ".tar.zst")),
    (155, 173, "solana-v1.5.6", (".tar.bz2", ".tar.zst")),
    (174, 200, "solana-v1.6.15", (".tar.bz2", ".tar.zst")),
    (201, 201, "solana-v1.6.16", (".tar.bz2", ".tar.zst")),
    (214, 215, "solana-v1.6.17", (".tar.bz2", ".tar.zst")),
    (216, 232, "solana-v1.6.20", (".tar.bz2", ".tar.zst")),
    (233, 265, "solana-v1.7.15", (".tar.bz2", ".tar.zst")),
    (266, 301, "solana-v1.8.11", (".tar.bz2", ".tar.zst")),
)
ROOT_COHORT_HISTORICAL_RUNTIMES = frozenset(
    {
        "solana-v1.0.8",
        "solana-v1.0.13",
        "solana-v1.0.14",
        "solana-v1.0.23",
        "solana-v1.1.15",
        "solana-v1.1.23",
        "solana-v1.2.32-mainnet-epoch68-transition",
        "solana-v1.2.32",
        "solana-v1.3.19",
        "solana-v1.3.23",
        "solana-v1.4.17",
        "solana-v1.4.19",
        "solana-v1.4.25",
        "solana-v1.5.5",
        "solana-v1.5.19",
        "solana-v1.5.6",
        "solana-v1.5.8",
        "solana-v1.6.15",
        "solana-v1.6.16",
        "solana-v1.6.17",
        "solana-v1.6.20",
        "solana-v1.7.15",
        "solana-v1.8.11",
    }
)
SNAPSHOT_ARCHIVE_EXTENSIONS = (".tar.zst", ".tar.lz4", ".tar.bz2")

_DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_FINGERPRINT_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
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
    md5_hash: Optional[str]


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


def _optional_md5_hash(metadata: Mapping[str, Any], context: str) -> Optional[str]:
    value = metadata.get("md5Hash")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise PreflightError(f"{context}: md5Hash must be a non-empty string")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise PreflightError(f"{context}: md5Hash is not canonical base64") from error
    if len(decoded) != 16 or base64.b64encode(decoded).decode("ascii") != value:
        raise PreflightError(f"{context}: md5Hash must encode exactly 16 bytes")
    return value


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
    md5_hash = _optional_md5_hash(metadata, context)

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
        md5_hash=md5_hash,
    )


def requested_slot_range(
    first_epoch: int = FIRST_EPOCH, last_epoch: int = LAST_EPOCH
) -> Tuple[int, int]:
    if (
        first_epoch < FIRST_EPOCH
        or last_epoch > MAX_SUPPORTED_EPOCH
        or first_epoch > last_epoch
    ):
        raise PreflightError(
            f"requested epoch range {first_epoch}-{last_epoch} is outside "
            f"{FIRST_EPOCH}-{MAX_SUPPORTED_EPOCH}"
        )
    return (first_epoch - 1) * EPOCH_SLOTS, (last_epoch + 1) * EPOCH_SLOTS - 1


def parse_inventory_json(
    text: str,
    source: str,
    relevant_slots: Optional[Tuple[int, int]] = None,
    exclusions: Optional[List[Dict[str, Any]]] = None,
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

    def same_snapshot_payload(left: SnapshotObject, right: SnapshotObject) -> bool:
        return (
            left.filename == right.filename
            and left.accounts_hash == right.accounts_hash
            and left.extension == right.extension
            and left.size == right.size
            and left.crc32c == right.crc32c
            and left.md5_hash is not None
            and left.md5_hash == right.md5_hash
        )

    def record_exclusion(
        item: SnapshotObject, reason: str, retained: Optional[SnapshotObject]
    ) -> None:
        if exclusions is None:
            return
        exclusions.append(
            {
                "crc32c": item.crc32c,
                "generation": item.generation,
                "md5_hash": item.md5_hash,
                "object_name": item.object_name,
                "reason": reason,
                "retained_object_name": retained.object_name if retained else None,
                "size": item.size,
                "source": item.source,
            }
        )

    if source == "root":
        for item in tuple(seen.values()):
            if item.anchor_slot == item.slot:
                continue
            canonical_name = f"{item.slot}/{item.filename}"
            canonical = seen.get(canonical_name)
            if canonical is None:
                raise PreflightError(
                    f"root inventory object {item.object_name!r} has a directory slot "
                    "that does not match its filename slot and no canonical copy exists"
                )
            if not same_snapshot_payload(item, canonical):
                raise PreflightError(
                    f"root inventory object {item.object_name!r} has a directory slot "
                    "that does not match its filename slot and differs from its canonical copy"
                )
            record_exclusion(item, "digest-identical-misrooted-root-alias", canonical)
            del seen[item.object_name]
    else:
        for item in tuple(seen.values()):
            if item.anchor_slot <= item.slot:
                continue
            valid_copies = (
                candidate
                for candidate in seen.values()
                if candidate.object_name != item.object_name
                and candidate.anchor_slot <= candidate.slot
                and same_snapshot_payload(item, candidate)
            )
            valid_copy = next(valid_copies, None)
            if valid_copy is not None:
                record_exclusion(
                    item, "digest-identical-late-anchor-hourly-alias", valid_copy
                )
                del seen[item.object_name]
                continue
            record_exclusion(item, "invalid-late-anchor-hourly-quarantine", None)
            del seen[item.object_name]
    return tuple(
        sorted(seen.values(), key=lambda item: (item.slot, item.object_name, item.generation))
    )


def _manifest_u64(
    value: Any, field: str, context: str, *, nonzero: bool = False
) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise PreflightError(f"{context}: {field} must be a JSON integer")
    if value < 0 or value > UINT64_MAX or (nonzero and value == 0):
        qualifier = "non-zero " if nonzero else ""
        raise PreflightError(f"{context}: {field} is not a {qualifier}uint64")
    return value


def _validate_manifest_md5(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise PreflightError(f"{context}: md5_hash must be a non-empty string")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise PreflightError(f"{context}: md5_hash is not canonical base64") from error
    if len(decoded) != 16 or base64.b64encode(decoded).decode("ascii") != value:
        raise PreflightError(f"{context}: md5_hash must encode exactly 16 bytes")
    return value


def _parse_manifest_snapshot(
    raw: Any, schema: str, context: str
) -> SnapshotObject:
    if not isinstance(raw, dict):
        raise PreflightError(f"{context}: snapshot must be a JSON object")
    common_keys = {
        "accounts_hash",
        "anchor_slot",
        "crc32c",
        "extension",
        "generation",
        "size",
        "slot",
        "source",
        "uri",
        "versioned_uri",
    }
    expected_keys = common_keys if schema.endswith("-v2") else common_keys | {"md5_hash"}
    if set(raw) != expected_keys:
        missing = sorted(expected_keys - set(raw))
        unexpected = sorted(set(raw) - expected_keys)
        raise PreflightError(
            f"{context}: snapshot fields differ from {schema}: "
            f"missing={missing}, unexpected={unexpected}"
        )

    source = _required_string(raw, "source", context)
    if source not in ("root", "hourly"):
        raise PreflightError(f"{context}: source must be 'root' or 'hourly'")
    accounts_hash = _required_string(raw, "accounts_hash", context)
    if not _base58_decodes_to_32_bytes(accounts_hash):
        raise PreflightError(f"{context}: accounts_hash is not a 32-byte base58 hash")
    extension = _required_string(raw, "extension", context)
    if extension not in SNAPSHOT_ARCHIVE_EXTENSIONS:
        raise PreflightError(f"{context}: unsupported snapshot extension {extension!r}")
    anchor_slot = _manifest_u64(raw.get("anchor_slot"), "anchor_slot", context)
    slot = _manifest_u64(raw.get("slot"), "slot", context)
    size = _manifest_u64(raw.get("size"), "size", context, nonzero=True)
    generation = _manifest_u64(
        raw.get("generation"), "generation", context, nonzero=True
    )
    crc32c = _required_string(raw, "crc32c", context)
    _validate_crc32c(crc32c, context)

    if schema.endswith("-v2"):
        md5_hash = None
    elif schema.endswith("-v3"):
        md5_hash = _validate_manifest_md5(raw.get("md5_hash"), context)
    else:
        md5_value = raw.get("md5_hash")
        md5_hash = (
            None
            if md5_value is None
            else _validate_manifest_md5(md5_value, context)
        )

    if source == "root" and anchor_slot != slot:
        raise PreflightError(f"{context}: root snapshot anchor_slot must equal slot")
    if source == "hourly" and anchor_slot > slot:
        raise PreflightError(f"{context}: hourly snapshot anchor_slot exceeds slot")
    filename = f"snapshot-{slot}-{accounts_hash}{extension}"
    middle = "hourly/" if source == "hourly" else ""
    object_name = f"{anchor_slot}/{middle}{filename}"
    uri = _required_string(raw, "uri", context)
    expected_uri = f"{BUCKET_URI}/{object_name}"
    if uri != expected_uri:
        raise PreflightError(
            f"{context}: uri {uri!r} does not match snapshot identity {expected_uri!r}"
        )
    versioned_uri = _required_string(raw, "versioned_uri", context)
    expected_versioned_uri = f"{uri}#{generation}"
    if versioned_uri != expected_versioned_uri:
        raise PreflightError(
            f"{context}: versioned_uri {versioned_uri!r} does not match "
            f"snapshot identity {expected_versioned_uri!r}"
        )
    return SnapshotObject(
        source=source,
        uri=uri,
        versioned_uri=versioned_uri,
        object_name=object_name,
        filename=filename,
        accounts_hash=accounts_hash,
        anchor_slot=anchor_slot,
        slot=slot,
        extension=extension,
        size=size,
        generation=generation,
        # Manifest schemas intentionally omit mutable object metadata.  This
        # sentinel never enters the regenerated report.
        metageneration=0,
        crc32c=crc32c,
        md5_hash=md5_hash,
    )


def _strict_json_file(path: Path, maximum_bytes: int, description: str) -> Any:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise PreflightError(f"cannot open {description} {path}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise PreflightError(f"{description} is not a regular file: {path}")
        if before.st_size > maximum_bytes:
            raise PreflightError(
                f"{description} exceeds {maximum_bytes} bytes: {path}"
            )
        chunks: List[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise PreflightError(f"short read from {description} {path}")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise PreflightError(f"{description} grew while it was read: {path}")
        after = os.fstat(descriptor)
        def identity(item: os.stat_result) -> Tuple[int, ...]:
            return (
                item.st_dev,
                item.st_ino,
                item.st_mode,
                item.st_uid,
                item.st_gid,
                item.st_nlink,
                item.st_size,
                item.st_mtime_ns,
                item.st_ctime_ns,
            )

        if identity(before) != identity(after):
            raise PreflightError(f"{description} changed while it was read: {path}")
        try:
            return json.loads(
                b"".join(chunks).decode("utf-8"),
                object_pairs_hook=_object_pairs_no_duplicates,
                parse_constant=_reject_json_constant,
            )
        except (
            UnicodeError,
            json.JSONDecodeError,
            _DuplicateJsonKey,
            ValueError,
        ) as error:
            raise PreflightError(
                f"{description} is not valid strict JSON: {error}"
            ) from error
    finally:
        os.close(descriptor)


def load_sealed_manifest_objects(
    path: Path,
    expected_fingerprint: str,
    first_epoch: int,
    last_epoch: int,
) -> Tuple[Tuple[SnapshotObject, ...], Tuple[SnapshotObject, ...], Dict[str, Any]]:
    """Recover immutable inventory evidence from a fingerprint-bound report."""
    requested_slot_range(first_epoch, last_epoch)
    if _FINGERPRINT_RE.fullmatch(expected_fingerprint) is None:
        raise PreflightError(
            "source manifest fingerprint must be sha256 plus 64 lowercase hex digits"
        )
    report = _strict_json_file(path, MAX_SOURCE_MANIFEST_BYTES, "source manifest report")
    if not isinstance(report, dict):
        raise PreflightError("source manifest report must be a JSON object")
    manifest = report.get("manifest")
    embedded_fingerprint = report.get("manifest_fingerprint")
    if not isinstance(manifest, dict) or not isinstance(embedded_fingerprint, str):
        raise PreflightError(
            "source manifest report lacks manifest or manifest_fingerprint"
        )
    computed_fingerprint = manifest_fingerprint(manifest)
    if (
        embedded_fingerprint != computed_fingerprint
        or expected_fingerprint != computed_fingerprint
    ):
        raise PreflightError(
            "source manifest fingerprint mismatch: "
            f"expected {expected_fingerprint}, embedded {embedded_fingerprint}, "
            f"computed {computed_fingerprint}"
        )
    schema = manifest.get("schema")
    if schema not in SOURCE_MANIFEST_SCHEMAS:
        raise PreflightError(f"unsupported source manifest schema {schema!r}")
    required_identity = {
        "account": GCLOUD_ACCOUNT,
        "billing_project": BILLING_PROJECT,
        "bucket": BUCKET_URI,
        "epoch_slots": EPOCH_SLOTS,
        "inventory_patterns": {"hourly": HOURLY_PATTERN, "root": ROOT_PATTERN},
    }
    for field, expected in required_identity.items():
        if manifest.get(field) != expected:
            raise PreflightError(
                f"source manifest {field} does not match current immutable identity"
            )
    manifest_first = manifest.get("first_epoch")
    manifest_last = manifest.get("last_epoch")
    if (
        not isinstance(manifest_first, int)
        or isinstance(manifest_first, bool)
        or not isinstance(manifest_last, int)
        or isinstance(manifest_last, bool)
        or manifest_first < FIRST_EPOCH
        or manifest_last < manifest_first
        or manifest_last > MAX_SUPPORTED_EPOCH
    ):
        raise PreflightError("source manifest has an invalid epoch range")
    if first_epoch < manifest_first or last_epoch > manifest_last:
        raise PreflightError(
            f"requested epoch range {first_epoch}-{last_epoch} is not covered by "
            f"source manifest range {manifest_first}-{manifest_last}"
        )

    raw_cohorts = manifest.get("verification_cohorts")
    raw_epochs = manifest.get("epochs")
    if not isinstance(raw_cohorts, list) or not isinstance(raw_epochs, list):
        raise PreflightError("source manifest cohorts and epochs must be JSON lists")
    objects_by_identity: Dict[Tuple[str, int], SnapshotObject] = {}
    objects_by_name: Dict[str, SnapshotObject] = {}

    def record(item: SnapshotObject, context: str) -> None:
        identity_key = (item.uri, item.generation)
        previous = objects_by_identity.get(identity_key)
        if previous is not None and previous != item:
            raise PreflightError(f"{context}: conflicting repeated snapshot identity")
        named = objects_by_name.get(item.object_name)
        if named is not None and named != item:
            raise PreflightError(
                f"{context}: conflicting metadata for {item.object_name!r}"
            )
        objects_by_identity[identity_key] = item
        objects_by_name[item.object_name] = item

    cohort_by_epoch: Dict[
        int,
        Tuple[
            int,
            int,
            SnapshotObject,
            Tuple[SnapshotObject, ...],
            str,
            Tuple[str, ...],
        ],
    ] = {}
    next_epoch = manifest_first
    for index, raw_cohort in enumerate(raw_cohorts):
        context = f"source cohort {index}"
        if not isinstance(raw_cohort, dict):
            raise PreflightError(f"{context}: cohort must be a JSON object")
        cohort_first = raw_cohort.get("first_epoch")
        cohort_last = raw_cohort.get("last_epoch")
        if (
            not isinstance(cohort_first, int)
            or isinstance(cohort_first, bool)
            or not isinstance(cohort_last, int)
            or isinstance(cohort_last, bool)
            or cohort_first != next_epoch
            or cohort_last < cohort_first
            or cohort_last > manifest_last
        ):
            raise PreflightError(f"{context}: cohorts must exactly and contiguously cover range")
        runtime = raw_cohort.get("runtime")
        extensions = raw_cohort.get("accepted_extensions")
        if (
            not isinstance(runtime, str)
            or not runtime
            or not isinstance(extensions, list)
            or not extensions
            or any(
                not isinstance(extension, str)
                or extension not in SNAPSHOT_ARCHIVE_EXTENSIONS
                for extension in extensions
            )
            or len(set(extensions)) != len(extensions)
        ):
            raise PreflightError(f"{context}: runtime or extensions are malformed")
        source_extensions = tuple(extensions)
        for epoch in range(cohort_first, cohort_last + 1):
            if first_epoch <= epoch <= last_epoch:
                expected_runtime, expected_extensions = runtime_route(epoch)
                if runtime != expected_runtime or source_extensions != expected_extensions:
                    raise PreflightError(
                        f"{context}: runtime or extensions disagree with selected "
                        f"epoch {epoch} route"
                    )
        if raw_cohort.get("publication_gate") != (
            "all-archives-validated-and-final-root-verified"
        ):
            raise PreflightError(f"{context}: publication gate is incompatible")
        bootstrap = _parse_manifest_snapshot(
            raw_cohort.get("bootstrap"), schema, f"{context} bootstrap"
        )
        if cohort_last > cohort_first and bootstrap.source != "root":
            raise PreflightError(f"{context}: multi-epoch bootstrap must be a root")
        prior_start, prior_end = epoch_slot_range(cohort_first - 1)
        if not prior_start <= bootstrap.slot <= prior_end:
            raise PreflightError(f"{context}: bootstrap is outside the prior epoch")
        record(bootstrap, f"{context} bootstrap")
        raw_checkpoints = raw_cohort.get("root_checkpoints")
        if not isinstance(raw_checkpoints, list) or not raw_checkpoints:
            raise PreflightError(f"{context}: root_checkpoints must be a non-empty list")
        checkpoints: List[SnapshotObject] = []
        last_slot = bootstrap.slot
        _, cohort_end_slot = epoch_slot_range(cohort_last)
        for checkpoint_index, raw_checkpoint in enumerate(raw_checkpoints):
            checkpoint_context = f"{context} root checkpoint {checkpoint_index}"
            checkpoint = _parse_manifest_snapshot(
                raw_checkpoint, schema, checkpoint_context
            )
            if (
                checkpoint.source != "root"
                or checkpoint.slot <= last_slot
                or checkpoint.slot > cohort_end_slot
            ):
                raise PreflightError(
                    f"{checkpoint_context}: root checkpoint ordering/range is invalid"
                )
            last_slot = checkpoint.slot
            checkpoints.append(checkpoint)
            record(checkpoint, checkpoint_context)
        final_start, _ = epoch_slot_range(cohort_last)
        if checkpoints[-1].slot < final_start:
            raise PreflightError(f"{context}: terminal checkpoint is before final epoch")
        cohort_tuple = (
            cohort_first,
            cohort_last,
            bootstrap,
            tuple(checkpoints),
            runtime,
            source_extensions,
        )
        for epoch in range(cohort_first, cohort_last + 1):
            cohort_by_epoch[epoch] = cohort_tuple
        next_epoch = cohort_last + 1
    if next_epoch != manifest_last + 1:
        raise PreflightError("source cohorts do not cover the complete manifest range")

    seen_epochs: set[int] = set()
    for index, raw_epoch in enumerate(raw_epochs):
        context = f"source epoch record {index}"
        if not isinstance(raw_epoch, dict):
            raise PreflightError(f"{context}: epoch record must be a JSON object")
        epoch = raw_epoch.get("epoch")
        if (
            not isinstance(epoch, int)
            or isinstance(epoch, bool)
            or epoch not in cohort_by_epoch
            or epoch in seen_epochs
        ):
            raise PreflightError(f"{context}: epoch identity is invalid or duplicated")
        seen_epochs.add(epoch)
        (
            cohort_first,
            cohort_last,
            bootstrap,
            checkpoints,
            source_runtime,
            source_extensions,
        ) = cohort_by_epoch[epoch]
        if (
            raw_epoch.get("runtime") != source_runtime
            or raw_epoch.get("accepted_extensions") != list(source_extensions)
            or raw_epoch.get("verification_cohort")
            != {"first_epoch": cohort_first, "last_epoch": cohort_last}
        ):
            raise PreflightError(
                f"{context}: epoch route disagrees with its source cohort or its "
                "cohort binding is invalid"
            )
        if first_epoch <= epoch <= last_epoch:
            expected_runtime, expected_extensions = runtime_route(epoch)
            if (
                source_runtime != expected_runtime
                or source_extensions != expected_extensions
            ):
                raise PreflightError(
                    f"{context}: source route disagrees with selected epoch {epoch} route"
                )
        expected_window = {
            "start": epoch_slot_range(cohort_first - 1)[0],
            "end_inclusive": epoch_slot_range(cohort_first - 1)[1],
        }
        if raw_epoch.get("bootstrap_window") != expected_window:
            raise PreflightError(f"{context}: bootstrap window is invalid")
        epoch_bootstrap = _parse_manifest_snapshot(
            raw_epoch.get("bootstrap"), schema, f"{context} bootstrap"
        )
        if epoch_bootstrap != bootstrap:
            raise PreflightError(f"{context}: bootstrap disagrees with its cohort")
        record(epoch_bootstrap, f"{context} bootstrap")
        raw_epoch_checkpoints = raw_epoch.get("post_bootstrap_root_checkpoints")
        if not isinstance(raw_epoch_checkpoints, list):
            raise PreflightError(f"{context}: checkpoint list is invalid")
        epoch_start, epoch_end = epoch_slot_range(epoch)
        expected_checkpoints = tuple(
            item for item in checkpoints if epoch_start <= item.slot <= epoch_end
        )
        parsed_checkpoints = tuple(
            _parse_manifest_snapshot(
                item, schema, f"{context} root checkpoint {checkpoint_index}"
            )
            for checkpoint_index, item in enumerate(raw_epoch_checkpoints)
        )
        if parsed_checkpoints != expected_checkpoints:
            raise PreflightError(f"{context}: checkpoints disagree with its cohort")
        for checkpoint in parsed_checkpoints:
            record(checkpoint, f"{context} root checkpoint")
        expected_state_source = (
            f"{bootstrap.source}-bootstrap"
            if epoch == cohort_first
            else "carried-from-previous-epoch"
        )
        if raw_epoch.get("runtime_state_source") != expected_state_source:
            raise PreflightError(f"{context}: runtime state source is invalid")
    if seen_epochs != set(range(manifest_first, manifest_last + 1)):
        raise PreflightError("source epoch records do not cover the complete manifest range")

    root_objects = tuple(
        sorted(
            (item for item in objects_by_identity.values() if item.source == "root"),
            key=lambda item: (item.slot, item.object_name, item.generation),
        )
    )
    hourly_objects = tuple(
        sorted(
            (item for item in objects_by_identity.values() if item.source == "hourly"),
            key=lambda item: (item.slot, item.object_name, item.generation),
        )
    )
    provenance = {
        "manifest_fingerprint": computed_fingerprint,
        "schema": schema,
        "first_epoch": manifest_first,
        "last_epoch": manifest_last,
        "selected_first_epoch": first_epoch,
        "selected_last_epoch": last_epoch,
    }
    return root_objects, hourly_objects, provenance


def runtime_route(epoch: int) -> Tuple[str, Tuple[str, ...]]:
    for first, last, runtime, extensions in RUNTIME_ROUTES:
        if first <= epoch <= last:
            return runtime, extensions
    raise PreflightError(
        f"epoch {epoch} is outside supported range {FIRST_EPOCH}-{MAX_SUPPORTED_EPOCH}"
    )


def accepted_extensions(epoch: int) -> Tuple[str, ...]:
    return runtime_route(epoch)[1]


def root_cohort_runtime_is_historical(runtime: str) -> bool:
    """Return whether a runtime is an isolated, pinned Solana worker."""
    return runtime in ROOT_COHORT_HISTORICAL_RUNTIMES


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
    # Historical snapshots were sometimes copied byte-for-byte from an hourly
    # path into the canonical root path. Treat those as one candidate only
    # when both GCS content digests and every snapshot identity field agree.
    if len(newest) > 1:
        identities = {
            (
                item.accounts_hash,
                item.extension,
                item.size,
                item.crc32c,
                item.md5_hash,
            )
            for item in newest
        }
        if len(identities) == 1 and newest[0].md5_hash is not None:
            root_copies = [item for item in newest if item.source == "root"]
            if len(root_copies) == 1:
                return root_copies[0]
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
    elif epoch == 154:
        eligible = [
            item
            for item in eligible
            if item.slot == EPOCH_154_BOOTSTRAP_SLOT
            and item.accounts_hash == EPOCH_154_BOOTSTRAP_ACCOUNTS_HASH
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
        if epoch == 154:
            raise PreflightError(
                "epoch 154: required canonical bootstrap "
                f"snapshot-{EPOCH_154_BOOTSTRAP_SLOT}-"
                f"{EPOCH_154_BOOTSTRAP_ACCOUNTS_HASH} is absent or ambiguous"
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
    target_cohort_epochs: int = 1,
) -> Tuple[VerificationCohort, ...]:
    """Plan root-verifiable cohorts, optionally coalescing same-runtime epochs."""
    requested_slot_range(first_epoch, last_epoch)
    if not 1 <= target_cohort_epochs <= MAX_TARGET_COHORT_EPOCHS:
        raise ValueError(
            f"target_cohort_epochs must be between 1 and {MAX_TARGET_COHORT_EPOCHS}"
        )
    all_bootstraps = tuple(root_objects) + tuple(hourly_objects)
    cohorts: List[VerificationCohort] = []
    epoch = first_epoch
    while epoch <= last_epoch:
        runtime, extensions = runtime_route(epoch)
        preferred_end = epoch
        for candidate_end in range(
            epoch + 1,
            min(last_epoch, epoch + target_cohort_epochs - 1) + 1,
        ):
            next_runtime, next_extensions = runtime_route(candidate_end)
            if next_runtime != runtime or next_extensions != extensions:
                break
            preferred_end = candidate_end

        bootstrap = _select_bootstrap(
            epoch,
            root_objects if preferred_end > epoch else all_bootstraps,
            root_only=preferred_end > epoch,
        )

        # A requested multi-epoch cohort or a checkpoint-free epoch must be
        # replayed continuously from a root in its predecessor epoch through a
        # root in the final epoch. Replaying later members from hourly objects
        # would leave their archives outside the verified state transition.
        if preferred_end > epoch and not root_cohort_runtime_is_historical(runtime):
            raise PreflightError(
                f"epoch {epoch}: multi-epoch cohorts require an isolated historical "
                f"Solana runtime, got {runtime}"
            )
        cohort_end = preferred_end
        cohort_checkpoints: Tuple[SnapshotObject, ...] = ()
        if preferred_end > epoch:
            _, preferred_end_slot = epoch_slot_range(preferred_end)
            preferred_checkpoints = _root_checkpoints_through(
                root_objects,
                bootstrap.slot,
                preferred_end_slot,
                extensions,
                f"epochs {epoch}-{preferred_end}",
            )
            if preferred_checkpoints:
                # Keep the requested cohort a soft maximum whenever the target
                # window contains a safe terminal root. Ending at the latest
                # such root avoids turning a four-epoch preference into a much
                # larger restart and publication unit merely because the
                # preferred final epoch itself has no checkpoint.
                cohort_end = max(item.slot // EPOCH_SLOTS for item in preferred_checkpoints)
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
            if cohort_end == epoch:
                if not root_cohort_runtime_is_historical(runtime):
                    raise PreflightError(
                        f"epoch {epoch}: root-checkpoint gaps require an isolated historical "
                        f"Solana runtime, got {runtime}"
                    )
                bootstrap = _select_bootstrap(epoch, root_objects, root_only=True)
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
    target_cohort_epochs: int = 1,
) -> Tuple[EpochPlan, ...]:
    """Expand root-aware verification cohorts into per-epoch work records."""
    plans: List[EpochPlan] = []
    for cohort in build_verification_cohorts(
        root_objects,
        hourly_objects,
        first_epoch,
        last_epoch,
        target_cohort_epochs,
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
    # GCS composite objects intentionally have no MD5 metadata.  Keep the key
    # explicit so v4 distinguishes a composite object from an incomplete
    # manifest; generation, size, and CRC32C remain mandatory and fingerprinted.
    return {
        "accounts_hash": item.accounts_hash,
        "anchor_slot": item.anchor_slot,
        "crc32c": item.crc32c,
        "extension": item.extension,
        "generation": item.generation,
        "md5_hash": item.md5_hash,
        "size": item.size,
        "slot": item.slot,
        "source": item.source,
        "uri": item.uri,
        "versioned_uri": item.versioned_uri,
    }


def build_manifest(
    plans: Sequence[EpochPlan],
    target_cohort_epochs: int = 1,
    publish_through_epoch: Optional[int] = None,
) -> Dict[str, Any]:
    if not plans:
        if publish_through_epoch is not None:
            raise PreflightError("cannot bind a publication boundary to an empty plan")
    else:
        first_epoch = plans[0].epoch
        verification_last_epoch = plans[-1].epoch
        if publish_through_epoch is None:
            publish_through_epoch = verification_last_epoch
        if not first_epoch <= publish_through_epoch <= verification_last_epoch:
            raise PreflightError(
                f"publication boundary {publish_through_epoch} is outside planned epochs "
                f"{first_epoch}-{verification_last_epoch}"
            )
        final_cohort_first = plans[-1].cohort_first_epoch
        if publish_through_epoch < final_cohort_first:
            raise PreflightError(
                f"verification tail must be contained in the final sealed cohort "
                f"{final_cohort_first}-{verification_last_epoch}"
            )
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
                "publish_through_epoch": min(
                    plan.cohort_last_epoch, publish_through_epoch
                ),
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
        "publication_boundary_epoch": publish_through_epoch,
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
            "verification_tail": "private-no-sidecar-no-r2",
            "target_cohort_epochs": target_cohort_epochs,
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
                "publication_scope": (
                    "requested"
                    if plan.epoch <= publish_through_epoch
                    else "verification-tail-private"
                ),
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
        "--source-manifest-report",
        type=Path,
        help=(
            "fingerprint-bound prior v2/v3/v4 report used as immutable offline "
            "snapshot evidence"
        ),
    )
    parser.add_argument(
        "--source-manifest-fingerprint",
        help=(
            "independently recorded sha256 fingerprint required with "
            "--source-manifest-report"
        ),
    )
    parser.add_argument(
        "--local-root",
        type=Path,
        default=LOCAL_ROOT,
        help=f"directory checked for selected archives (default: {LOCAL_ROOT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="create the JSON report at this path instead of writing it to stdout",
    )
    parser.add_argument("--first-epoch", type=int, default=FIRST_EPOCH)
    parser.add_argument("--last-epoch", type=int, default=LAST_EPOCH)
    parser.add_argument(
        "--publish-through-epoch",
        type=int,
        help=(
            "last publishable epoch when --last-epoch includes a private verification tail "
            "(default: --last-epoch)"
        ),
    )
    parser.add_argument(
        "--target-cohort-epochs",
        type=int,
        choices=range(1, MAX_TARGET_COHORT_EPOCHS + 1),
        default=1,
        help=(
            "prefer continuous same-runtime verification cohorts of this many epochs "
            f"(default: 1; maximum: {MAX_TARGET_COHORT_EPOCHS})"
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        if arguments.publish_through_epoch is not None and not (
            arguments.first_epoch
            <= arguments.publish_through_epoch
            <= arguments.last_epoch
        ):
            raise PreflightError(
                f"publication boundary {arguments.publish_through_epoch} is outside requested "
                f"verification epochs {arguments.first_epoch}-{arguments.last_epoch}"
            )
        relevant_slots = requested_slot_range(arguments.first_epoch, arguments.last_epoch)
        inventory_exclusions: List[Dict[str, Any]] = []
        source_provenance: Optional[Dict[str, Any]] = None
        source_mode = arguments.source_manifest_report is not None
        if source_mode:
            if (
                arguments.inventory_root_json is not None
                or arguments.inventory_hourly_json is not None
            ):
                raise PreflightError(
                    "--source-manifest-report cannot be combined with inventory fixtures"
                )
            if arguments.source_manifest_fingerprint is None:
                raise PreflightError(
                    "--source-manifest-fingerprint is required with "
                    "--source-manifest-report"
                )
            root_objects, hourly_objects, source_provenance = (
                load_sealed_manifest_objects(
                    arguments.source_manifest_report,
                    arguments.source_manifest_fingerprint,
                    arguments.first_epoch,
                    arguments.last_epoch,
                )
            )
        else:
            if arguments.source_manifest_fingerprint is not None:
                raise PreflightError(
                    "--source-manifest-fingerprint requires --source-manifest-report"
                )
            root_text, hourly_text = load_inventory_texts(
                arguments.inventory_root_json, arguments.inventory_hourly_json
            )
            root_objects = parse_inventory_json(
                root_text, "root", relevant_slots, inventory_exclusions
            )
            hourly_objects = parse_inventory_json(
                hourly_text, "hourly", relevant_slots, inventory_exclusions
            )
        plans = build_epoch_plans(
            root_objects,
            hourly_objects,
            arguments.first_epoch,
            arguments.last_epoch,
            arguments.target_cohort_epochs,
        )
        manifest = build_manifest(
            plans,
            arguments.target_cohort_epochs,
            arguments.publish_through_epoch,
        )
        fingerprint = manifest_fingerprint(manifest)
        storage = build_storage_report(plans, arguments.local_root)
        report = {
            "free_bytes": storage["free_bytes"],
            "inventory_objects": {"hourly": len(hourly_objects), "root": len(root_objects)},
            "inventory_exclusions": inventory_exclusions,
            "manifest": manifest,
            "manifest_fingerprint": fingerprint,
            "missing_local_bootstrap_bytes": storage["missing_local_bootstrap_bytes"],
            "selected_bootstrap_bytes": storage["selected_bootstrap_bytes"],
            "storage": storage,
        }
        if source_provenance is not None:
            report["replanned_from_source_manifest"] = source_provenance
        encoded_report = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if arguments.output is None:
            sys.stdout.write(encoded_report)
        else:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(arguments.output, flags, 0o600)
            except OSError as error:
                raise PreflightError(
                    f"cannot create preflight report {arguments.output}: {error}"
                ) from error
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                    output.write(encoded_report)
                    output.flush()
                    os.fsync(output.fileno())
            except (OSError, UnicodeError) as error:
                try:
                    arguments.output.unlink()
                except OSError:
                    pass
                raise PreflightError(
                    f"cannot write preflight report {arguments.output}: {error}"
                ) from error
        return 0 if storage["preflight_ok"] else 2
    except PreflightError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
