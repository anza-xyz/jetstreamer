#!/usr/bin/env python3
"""Collect and validate recovered epoch-208 PoH boundaries after an SST scan."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


SCHEMA = "jetstreamer-epoch208-recovered-poh-boundaries-v1"


class CollectionError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scan-state", type=absolute_path, required=True)
    parser.add_argument("--decoder", type=absolute_path, required=True)
    parser.add_argument("--decoder-sha256", required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--expected-slot", type=int, required=True)
    parser.add_argument("--ticks-per-slot", type=int, required=True)
    parser.add_argument("--expected-boundary-count", type=int, required=True)
    parser.add_argument("--expected-final-blockhash", required=True)
    parser.add_argument("--output", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    return parser.parse_args()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_json_bytes(path: Path) -> tuple[bytes, Any]:
    try:
        data = path.read_bytes()
        return data, json.loads(data)
    except (OSError, json.JSONDecodeError) as error:
        raise CollectionError(f"cannot read JSON {path}: {error}") from error


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def install_no_clobber(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise CollectionError(f"refusing to overwrite different artifact: {path}")
        fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def require_string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise CollectionError(f"missing nonempty string {key!r}")
    return value


def collect(args: argparse.Namespace) -> str:
    scan_bytes, scan = read_json_bytes(args.scan_state)
    if not isinstance(scan, dict):
        raise CollectionError("scan state is not a JSON object")
    if scan.get("schema") != "jetstreamer-streaming-rocksdb-prefix-scan-v1":
        raise CollectionError("scan state has the wrong schema")
    status = require_string(scan, "status")
    if status == "running":
        return "deferred-running"
    if status != "complete":
        raise CollectionError(f"scan state is terminal but not complete: {status!r}")
    if scan.get("source") != args.expected_source:
        raise CollectionError("scan source does not match the immutable expected source")
    target_prefixes = scan.get("target_prefixes")
    expected_prefix = f"{args.expected_slot:016X}"
    if not isinstance(target_prefixes, list) or expected_prefix not in target_prefixes:
        raise CollectionError(f"scan did not include expected slot prefix {expected_prefix}")
    if args.ticks_per_slot <= 0 or args.expected_boundary_count <= 1:
        raise CollectionError("tick and boundary expectations must be positive")
    if sha256_file(args.decoder) != args.decoder_sha256:
        raise CollectionError("decoder SHA-256 does not match the sealed expectation")
    if args.receipt.exists():
        if not args.output.exists():
            raise CollectionError("receipt exists but recovered-boundary output is missing")
        _, existing = read_json_bytes(args.receipt)
        if not isinstance(existing, dict) or existing.get("schema") != SCHEMA:
            raise CollectionError("existing receipt has the wrong schema")
        expected_existing = {
            "scan_state": str(args.scan_state),
            "scan_state_sha256": sha256_bytes(scan_bytes),
            "scan_source": args.expected_source,
            "decoder": str(args.decoder),
            "decoder_sha256": args.decoder_sha256,
            "output": str(args.output),
            "output_sha256": sha256_file(args.output),
            "slot": args.expected_slot,
            "ticks_per_slot": args.ticks_per_slot,
            "final_boundary_hash": args.expected_final_blockhash,
            "canonical_final_blockhash_match": True,
            "intermediate_differs_from_final": True,
            "publication_authorized": False,
            "runtime_route_promotion_authorized": False,
            "remote_mutations": False,
            "r2_mutations": False,
        }
        for key, value in expected_existing.items():
            if existing.get(key) != value:
                raise CollectionError(f"existing receipt field {key!r} does not match")
        if existing.get("intermediate_boundary_hash") == args.expected_final_blockhash:
            raise CollectionError("existing receipt repeats the visible final blockhash")
        return "already-collected"

    result = subprocess.run(
        [str(args.decoder), str(args.scan_state), str(args.ticks_per_slot)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise CollectionError(
            f"decoder exited {result.returncode}: {result.stderr.decode(errors='replace')}"
        )
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise CollectionError(f"decoder output is not JSON: {error}") from error
    if not isinstance(report, list):
        raise CollectionError("decoder report is not a JSON array")
    selected = [item for item in report if isinstance(item, dict) and item.get("slot") == args.expected_slot]
    if len(selected) != 1:
        raise CollectionError(
            f"expected exactly one report for slot {args.expected_slot}, found {len(selected)}"
        )
    slot = selected[0]
    boundaries = slot.get("boundaries")
    if not isinstance(boundaries, list) or len(boundaries) != args.expected_boundary_count:
        raise CollectionError(
            f"expected {args.expected_boundary_count} boundaries, found "
            f"{len(boundaries) if isinstance(boundaries, list) else 'non-list'}"
        )
    expected_ordinals = [args.ticks_per_slot * index for index in range(1, args.expected_boundary_count + 1)]
    actual_ordinals = [item.get("tick_ordinal") if isinstance(item, dict) else None for item in boundaries]
    if actual_ordinals != expected_ordinals:
        raise CollectionError(
            f"boundary tick ordinals {actual_ordinals!r} do not match {expected_ordinals!r}"
        )
    boundary_hashes = [require_string(item, "hash") for item in boundaries]
    if boundary_hashes[-1] != args.expected_final_blockhash:
        raise CollectionError("recovered final boundary does not match canonical final blockhash")
    if slot.get("final_entry_hash") != args.expected_final_blockhash:
        raise CollectionError("decoded final entry does not match canonical final blockhash")
    if boundary_hashes[0] == args.expected_final_blockhash:
        raise CollectionError("intermediate boundary incorrectly repeats the visible final blockhash")

    normalized_output = json.dumps(report, indent=2, sort_keys=True).encode() + b"\n"
    output_sha256 = sha256_bytes(normalized_output)
    receipt = {
        "schema": SCHEMA,
        "recorded_at_unix_seconds": time.time(),
        "scan_state": str(args.scan_state),
        "scan_state_sha256": sha256_bytes(scan_bytes),
        "scan_source": args.expected_source,
        "decoder": str(args.decoder),
        "decoder_sha256": args.decoder_sha256,
        "output": str(args.output),
        "output_sha256": output_sha256,
        "slot": args.expected_slot,
        "ticks_per_slot": args.ticks_per_slot,
        "boundary_tick_ordinals": actual_ordinals,
        "intermediate_boundary_hash": boundary_hashes[0],
        "final_boundary_hash": boundary_hashes[-1],
        "canonical_final_blockhash_match": True,
        "intermediate_differs_from_final": True,
        "publication_authorized": False,
        "runtime_route_promotion_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    normalized_receipt = json.dumps(receipt, indent=2, sort_keys=True).encode() + b"\n"
    install_no_clobber(args.output, normalized_output)
    install_no_clobber(args.receipt, normalized_receipt)
    return "collected"


def main() -> int:
    args = parse_args()
    try:
        outcome = collect(args)
    except CollectionError as error:
        print(f"collection failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"outcome": outcome}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
