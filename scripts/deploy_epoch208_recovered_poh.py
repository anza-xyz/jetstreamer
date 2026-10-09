#!/usr/bin/env python3
"""Deploy a sealed epoch-208 recovered-PoH file for an unprivileged worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OUTPUT_SCHEMA = "jetstreamer-epoch208-decoded-gap-boundaries-v1"
RECEIPT_SCHEMA = "jetstreamer-epoch208-recovered-gap-boundaries-v1"
DEPLOYMENT_SCHEMA = "jetstreamer-private-epoch208-recovered-poh-deployment-v1"
MAX_BOUNDARY_BYTES = 1024 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024


class DeploymentError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--boundaries", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    parser.add_argument("--destination", type=absolute_path, required=True)
    parser.add_argument("--expected-source-uid", type=int, default=0)
    parser.add_argument("--expected-destination-parent-uid", type=int, default=0)
    parser.add_argument("--expected-gap-runs", type=int, default=25)
    parser.add_argument("--expected-skipped-slots", type=int, default=71)
    parser.add_argument("--expected-total-boundaries", type=int, default=96)
    parser.add_argument("--ticks-per-slot", type=int, default=64)
    return parser.parse_args()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)


def read_sealed(path: Path, maximum_bytes: int, expected_uid: int) -> bytes:
    if not path.is_absolute():
        raise DeploymentError(f"sealed input is not absolute: {path}")
    try:
        if path.resolve(strict=True) != path:
            raise DeploymentError(f"sealed input path is not canonical: {path}")
    except OSError as error:
        raise DeploymentError(f"cannot resolve sealed input {path}: {error}") from error
    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise DeploymentError(f"cannot open sealed input {path}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise DeploymentError(f"sealed input is not a regular file: {path}")
        if before.st_uid != expected_uid:
            raise DeploymentError(
                f"sealed input {path} has uid {before.st_uid}, expected {expected_uid}"
            )
        if before.st_mode & 0o022:
            raise DeploymentError(
                f"sealed input is group/other writable: {path} mode {before.st_mode & 0o7777:o}"
            )
        if before.st_size <= 0 or before.st_size > maximum_bytes:
            raise DeploymentError(
                f"sealed input {path} has {before.st_size} bytes, expected 1..={maximum_bytes}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        current = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise DeploymentError(f"cannot restat sealed input {path}: {error}") from error
    if not stat.S_ISREG(current.st_mode) or not (
        file_identity(before) == file_identity(after) == file_identity(current)
    ):
        raise DeploymentError(f"sealed input changed while reading: {path}")
    data = b"".join(chunks)
    if len(data) != before.st_size:
        raise DeploymentError(f"sealed input length changed while reading: {path}")
    return data


def decode_object(data: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except json.JSONDecodeError as error:
        raise DeploymentError(f"{label} is not valid JSON: {error}") from error
    if not isinstance(value, dict):
        raise DeploymentError(f"{label} is not a JSON object")
    return value


def require_int(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise DeploymentError(f"missing integer {key!r}")
    return value


def require_string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise DeploymentError(f"missing nonempty string {key!r}")
    return value


def validate(
    args: argparse.Namespace,
    boundary_bytes: bytes,
    receipt_bytes: bytes,
) -> tuple[dict[str, Any], dict[str, Any]]:
    boundaries = decode_object(boundary_bytes, "boundary file")
    receipt = decode_object(receipt_bytes, "collector receipt")
    if boundaries.get("schema") != OUTPUT_SCHEMA:
        raise DeploymentError("boundary file has the wrong schema")
    if receipt.get("schema") != RECEIPT_SCHEMA:
        raise DeploymentError("collector receipt has the wrong schema")
    if receipt.get("output") != str(args.boundaries):
        raise DeploymentError("collector receipt does not name the exact boundary path")
    digest = sha256_bytes(boundary_bytes)
    if receipt.get("output_sha256") != digest:
        raise DeploymentError("collector receipt does not bind the boundary SHA-256")
    expected_scalars = {
        "gap_runs": args.expected_gap_runs,
        "skipped_slots": args.expected_skipped_slots,
        "total_boundaries": args.expected_total_boundaries,
        "hidden_intermediate_boundaries": args.expected_skipped_slots,
        "ticks_per_slot": args.ticks_per_slot,
    }
    for key, expected in expected_scalars.items():
        if receipt.get(key) != expected:
            raise DeploymentError(f"collector receipt field {key!r} does not match")
    for key in (
        "canonical_final_blockhash_matches",
        "hidden_intermediate_boundaries_distinct",
    ):
        if receipt.get(key) is not True:
            raise DeploymentError(f"collector receipt field {key!r} is not true")
    for key in (
        "publication_authorized",
        "runtime_route_promotion_authorized",
        "remote_mutations",
        "r2_mutations",
    ):
        if receipt.get(key) is not False:
            raise DeploymentError(f"collector receipt field {key!r} is not false")

    output_slots = boundaries.get("slots")
    receipt_slots = receipt.get("slots")
    if not isinstance(output_slots, list) or not isinstance(receipt_slots, list):
        raise DeploymentError("boundary file or receipt has no slot array")
    if len(output_slots) != args.expected_gap_runs or len(receipt_slots) != len(output_slots):
        raise DeploymentError("boundary file and receipt do not contain every gap run")
    seen_slots: set[int] = set()
    total_boundaries = 0
    hidden_boundaries = 0
    previous_slot = -1
    for output_slot, receipt_slot in zip(output_slots, receipt_slots):
        if not isinstance(output_slot, dict) or not isinstance(receipt_slot, dict):
            raise DeploymentError("boundary file or receipt contains a non-object slot")
        slot = require_int(output_slot, "slot")
        if slot <= previous_slot or slot in seen_slots:
            raise DeploymentError("boundary slots are duplicated or not strictly increasing")
        if receipt_slot.get("post_gap_block") != slot:
            raise DeploymentError(f"collector receipt slot does not match output slot {slot}")
        boundary_items = output_slot.get("boundaries")
        ordinals = receipt_slot.get("boundary_tick_ordinals")
        if not isinstance(boundary_items, list) or not boundary_items:
            raise DeploymentError(f"output slot {slot} has no boundaries")
        expected_count = require_int(receipt_slot, "boundaries")
        if len(boundary_items) != expected_count or not isinstance(ordinals, list):
            raise DeploymentError(f"boundary count does not match for slot {slot}")
        expected_ordinals = [
            args.ticks_per_slot * index for index in range(1, expected_count + 1)
        ]
        actual_ordinals = [
            item.get("tick_ordinal") if isinstance(item, dict) else None
            for item in boundary_items
        ]
        if ordinals != expected_ordinals or actual_ordinals != expected_ordinals:
            raise DeploymentError(f"boundary ordinals do not match for slot {slot}")
        hashes = [require_string(item, "hash") for item in boundary_items]
        hidden = receipt_slot.get("hidden_boundary_hashes")
        final_hash = require_string(receipt_slot, "canonical_final_blockhash")
        if not isinstance(hidden, list) or not all(isinstance(value, str) for value in hidden):
            raise DeploymentError(f"hidden boundary hashes are invalid for slot {slot}")
        if hashes[:-1] != hidden or hashes[-1] != final_hash:
            raise DeploymentError(f"receipt hashes do not match output slot {slot}")
        if output_slot.get("final_entry_hash") != final_hash:
            raise DeploymentError(f"final entry hash does not match for slot {slot}")
        if receipt_slot.get("canonical_final_blockhash_match") is not True:
            raise DeploymentError(f"canonical final hash is not attested for slot {slot}")
        if len(set(hashes)) != len(hashes):
            raise DeploymentError(f"boundary hashes repeat within slot {slot}")
        skipped = require_int(receipt_slot, "skipped")
        if skipped != len(hidden) or expected_count != skipped + 1:
            raise DeploymentError(f"gap arithmetic does not match for slot {slot}")
        total_boundaries += expected_count
        hidden_boundaries += skipped
        seen_slots.add(slot)
        previous_slot = slot
    if total_boundaries != args.expected_total_boundaries:
        raise DeploymentError("decoded total boundary count does not match")
    if hidden_boundaries != args.expected_skipped_slots:
        raise DeploymentError("decoded hidden boundary count does not match")
    return boundaries, receipt


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_file(path: Path, data: bytes, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def deploy(args: argparse.Namespace) -> dict[str, Any]:
    if args.expected_source_uid < 0 or args.expected_destination_parent_uid < 0:
        raise DeploymentError("expected uids must not be negative")
    if min(
        args.expected_gap_runs,
        args.expected_skipped_slots,
        args.expected_total_boundaries,
        args.ticks_per_slot,
    ) <= 0:
        raise DeploymentError("expected counts and ticks per slot must be positive")
    if args.destination.exists() or args.destination.is_symlink():
        raise DeploymentError(f"destination already exists: {args.destination}")
    if args.destination.resolve(strict=False) != args.destination:
        raise DeploymentError(f"destination path is not canonical: {args.destination}")
    parent = args.destination.parent
    try:
        parent_metadata = parent.lstat()
    except OSError as error:
        raise DeploymentError(f"cannot stat destination parent {parent}: {error}") from error
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != args.expected_destination_parent_uid
        or parent_metadata.st_mode & 0o022
    ):
        raise DeploymentError("destination parent is not a trusted non-writable directory")

    boundary_bytes = read_sealed(args.boundaries, MAX_BOUNDARY_BYTES, args.expected_source_uid)
    receipt_bytes = read_sealed(args.receipt, MAX_RECEIPT_BYTES, args.expected_source_uid)
    validate(args, boundary_bytes, receipt_bytes)
    boundary_sha256 = sha256_bytes(boundary_bytes)
    receipt_sha256 = sha256_bytes(receipt_bytes)
    deployed_boundary = args.destination / "all-gap-boundaries.json"
    manifest = {
        "schema": DEPLOYMENT_SCHEMA,
        "recorded_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_boundaries": str(args.boundaries),
        "source_boundaries_sha256": boundary_sha256,
        "source_receipt": str(args.receipt),
        "source_receipt_sha256": receipt_sha256,
        "deployed_boundaries": str(deployed_boundary),
        "gap_runs": args.expected_gap_runs,
        "skipped_slots": args.expected_skipped_slots,
        "total_boundaries": args.expected_total_boundaries,
        "ticks_per_slot": args.ticks_per_slot,
        "runtime_environment": {
            "JETSTREAMER_HISTORICAL_RECOVERED_POH_BOUNDARIES": str(deployed_boundary),
            "JETSTREAMER_HISTORICAL_RECOVERED_POH_SHA256": boundary_sha256,
        },
        "runtime_route_promotion_authorized": False,
        "publication_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    sums = (
        f"{boundary_sha256}  all-gap-boundaries.json\n"
        f"{receipt_sha256}  collector-receipt.json\n"
        f"{sha256_bytes(manifest_bytes)}  deployment-manifest.json\n"
    ).encode()

    stage = Path(tempfile.mkdtemp(prefix=f".{args.destination.name}.", dir=parent))
    try:
        os.chmod(stage, 0o700)
        write_file(stage / "all-gap-boundaries.json", boundary_bytes, 0o444)
        write_file(stage / "collector-receipt.json", receipt_bytes, 0o444)
        write_file(stage / "deployment-manifest.json", manifest_bytes, 0o444)
        write_file(stage / "SHA256SUMS", sums, 0o444)
        fsync_directory(stage)
        os.chmod(stage, 0o555)
        if args.destination.exists() or args.destination.is_symlink():
            raise DeploymentError(f"destination appeared during deployment: {args.destination}")
        os.rename(stage, args.destination)
        fsync_directory(parent)
    except Exception:
        if stage.exists():
            os.chmod(stage, 0o700)
            shutil.rmtree(stage)
        raise
    return {
        "outcome": "deployed",
        "destination": str(args.destination),
        "boundaries_sha256": boundary_sha256,
        "receipt_sha256": receipt_sha256,
        "publication_authorized": False,
        "r2_mutations": False,
    }


def main() -> int:
    args = parse_args()
    try:
        result = deploy(args)
    except DeploymentError as error:
        print(f"deployment failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
