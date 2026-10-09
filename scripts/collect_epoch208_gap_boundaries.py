#!/usr/bin/env python3
"""Validate all recovered epoch-208 skipped-slot PoH boundaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


RECEIPT_SCHEMA = "jetstreamer-epoch208-recovered-gap-boundaries-v1"
OUTPUT_SCHEMA = "jetstreamer-epoch208-decoded-gap-boundaries-v1"
SCAN_SCHEMA = "jetstreamer-streaming-rocksdb-prefix-scan-v1"
AUDIT_SCHEMA = "jetstreamer-private-epoch208-full-skipped-slot-audit-v1"


class CollectionError(RuntimeError):
    pass


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def sha256_value(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError("SHA-256 must be 64 lowercase hexadecimal digits")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scan-state", action="append", type=absolute_path, required=True)
    parser.add_argument("--gap-audit", type=absolute_path, required=True)
    parser.add_argument("--gap-audit-sha256", type=sha256_value, required=True)
    parser.add_argument("--block-metadata", type=absolute_path, required=True)
    parser.add_argument("--block-metadata-sha256", type=sha256_value, required=True)
    parser.add_argument("--decoder", type=absolute_path, required=True)
    parser.add_argument("--decoder-sha256", type=sha256_value, required=True)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--expected-source")
    source_group.add_argument(
        "--expected-scan-source",
        action="append",
        nargs=2,
        metavar=("ABSOLUTE_SCAN_STATE", "GENERATION_BOUND_GS_URI"),
    )
    parser.add_argument("--ticks-per-slot", type=int, required=True)
    parser.add_argument("--output", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    return parser.parse_args()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise CollectionError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def read_json_bytes(path: Path) -> tuple[bytes, Any]:
    try:
        data = path.read_bytes()
        return data, json.loads(data)
    except (OSError, json.JSONDecodeError) as error:
        raise CollectionError(f"cannot read JSON {path}: {error}") from error


def require_string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise CollectionError(f"missing nonempty string {key!r}")
    return value


def require_int(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise CollectionError(f"missing integer {key!r}")
    return value


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


def load_gaps(args: argparse.Namespace) -> tuple[bytes, list[dict[str, Any]]]:
    audit_bytes, audit = read_json_bytes(args.gap_audit)
    if sha256_bytes(audit_bytes) != args.gap_audit_sha256:
        raise CollectionError("gap-audit SHA-256 does not match the sealed expectation")
    if not isinstance(audit, dict) or audit.get("schema") != AUDIT_SCHEMA:
        raise CollectionError("gap audit has the wrong schema")
    gaps = audit.get("gaps")
    if not isinstance(gaps, list) or not gaps:
        raise CollectionError("gap audit has no gaps")
    normalized = []
    seen_slots: set[int] = set()
    for item in gaps:
        if not isinstance(item, dict):
            raise CollectionError("gap audit contains a non-object gap")
        previous = require_int(item, "previous_present")
        slot = require_int(item, "post_gap_block")
        skipped = require_int(item, "skipped")
        boundaries = require_int(item, "boundaries")
        prefix = require_string(item, "key_prefix")
        if slot in seen_slots:
            raise CollectionError(f"duplicate post-gap slot {slot}")
        if skipped <= 0 or slot - previous - 1 != skipped:
            raise CollectionError(f"gap arithmetic is invalid for post-gap slot {slot}")
        if boundaries != skipped + 1:
            raise CollectionError(f"boundary count is invalid for post-gap slot {slot}")
        if prefix != f"{slot:016X}":
            raise CollectionError(f"key prefix is invalid for post-gap slot {slot}")
        seen_slots.add(slot)
        normalized.append(
            {
                "previous_present": previous,
                "post_gap_block": slot,
                "skipped": skipped,
                "boundaries": boundaries,
                "key_prefix": prefix,
            }
        )
    if normalized != sorted(normalized, key=lambda item: item["post_gap_block"]):
        raise CollectionError("gap audit is not in canonical slot order")
    summary = audit.get("summary")
    if not isinstance(summary, dict):
        raise CollectionError("gap audit has no summary")
    expected_summary = {
        "gap_runs": len(normalized),
        "skipped_slots": sum(item["skipped"] for item in normalized),
        "post_gap_blocks_requiring_original_entries": len(normalized),
        "total_boundaries_in_post_gap_blocks": sum(
            item["boundaries"] for item in normalized
        ),
        "hidden_intermediate_boundaries": sum(item["skipped"] for item in normalized),
    }
    for key, expected in expected_summary.items():
        if summary.get(key) != expected:
            raise CollectionError(f"gap-audit summary field {key!r} does not match")
    return audit_bytes, normalized


def load_blockhashes(
    args: argparse.Namespace, gaps: list[dict[str, Any]]
) -> tuple[bytes, dict[int, str]]:
    metadata_bytes, metadata = read_json_bytes(args.block_metadata)
    if sha256_bytes(metadata_bytes) != args.block_metadata_sha256:
        raise CollectionError("block-metadata SHA-256 does not match the sealed expectation")
    if not isinstance(metadata, list):
        raise CollectionError("block metadata is not a JSON array")
    expected_slots = {item["post_gap_block"] for item in gaps}
    by_slot: dict[int, str] = {}
    gap_by_slot = {item["post_gap_block"]: item for item in gaps}
    for response in metadata:
        if not isinstance(response, dict) or "error" in response:
            raise CollectionError("block metadata contains an RPC error or non-object")
        slot = response.get("id")
        result = response.get("result")
        if not isinstance(slot, int) or not isinstance(result, dict):
            raise CollectionError("block metadata response has invalid id/result")
        if slot in by_slot:
            raise CollectionError(f"block metadata repeats slot {slot}")
        blockhash = require_string(result, "blockhash")
        require_string(result, "previousBlockhash")
        parent = require_int(result, "parentSlot")
        gap = gap_by_slot.get(slot)
        if gap is None:
            raise CollectionError(f"block metadata contains unexpected slot {slot}")
        if parent != gap["previous_present"]:
            raise CollectionError(f"RPC parent slot does not match gap audit for slot {slot}")
        by_slot[slot] = blockhash
    if set(by_slot) != expected_slots:
        missing = sorted(expected_slots - set(by_slot))
        raise CollectionError(f"block metadata does not exactly cover gaps; missing={missing}")
    return metadata_bytes, by_slot


def load_scan_states(
    args: argparse.Namespace, gaps: list[dict[str, Any]]
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    expected_sources: dict[Path, str] | None = None
    if args.expected_scan_source is not None:
        expected_sources = {}
        for raw_path, source in args.expected_scan_source:
            path = Path(raw_path)
            if not path.is_absolute():
                raise CollectionError(
                    f"expected-scan-source path must be absolute: {raw_path}"
                )
            if path in expected_sources:
                raise CollectionError(
                    f"expected-scan-source path is duplicated: {path}"
                )
            if re.fullmatch(r"gs://.+#[0-9]+", source) is None:
                raise CollectionError(
                    f"expected scan source is not generation-bound: {source}"
                )
            expected_sources[path] = source
        if set(expected_sources) != set(args.scan_state):
            missing = sorted(str(path) for path in set(args.scan_state) - set(expected_sources))
            unexpected = sorted(
                str(path) for path in set(expected_sources) - set(args.scan_state)
            )
            raise CollectionError(
                "expected-scan-source bindings do not exactly match scan-state paths; "
                f"missing={missing}, unexpected={unexpected}"
            )

    bindings = []
    scans = []
    for path in args.scan_state:
        data, scan = read_json_bytes(path)
        if not isinstance(scan, dict) or scan.get("schema") != SCAN_SCHEMA:
            raise CollectionError(f"scan state has the wrong schema: {path}")
        expected_source = (
            expected_sources[path] if expected_sources is not None else args.expected_source
        )
        if scan.get("source") != expected_source:
            raise CollectionError(f"scan source does not match for {path}")
        status = require_string(scan, "status")
        if status not in {"running", "complete"}:
            raise CollectionError(f"scan state is terminal but not complete: {path}: {status!r}")
        prefixes = scan.get("target_prefixes")
        if (
            not isinstance(prefixes, list)
            or not prefixes
            or not all(isinstance(prefix, str) for prefix in prefixes)
            or len(prefixes) != len(set(prefixes))
        ):
            raise CollectionError(f"scan target prefixes are invalid: {path}")
        binding = {"path": str(path), "sha256": sha256_bytes(data)}
        if expected_sources is not None:
            binding["source"] = expected_source
        bindings.append(binding)
        scans.append(scan)
    for gap in gaps:
        coverage = sum(gap["key_prefix"] in scan["target_prefixes"] for scan in scans)
        if coverage != 1:
            raise CollectionError(
                f"post-gap slot {gap['post_gap_block']} has scan coverage {coverage}, expected 1"
            )
    return bindings, scans


def decode_reports(args: argparse.Namespace) -> list[dict[str, Any]]:
    reports = []
    for path in args.scan_state:
        result = subprocess.run(
            [str(args.decoder), str(path), str(args.ticks_per_slot)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode:
            raise CollectionError(
                f"decoder exited {result.returncode} for {path}: "
                f"{result.stderr.decode(errors='replace')}"
            )
        try:
            decoded = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CollectionError(f"decoder output is not JSON for {path}: {error}") from error
        if not isinstance(decoded, list) or not all(isinstance(item, dict) for item in decoded):
            raise CollectionError(f"decoder report is not an array of objects for {path}")
        reports.extend(decoded)
    return reports


def validate_reports(
    args: argparse.Namespace,
    gaps: list[dict[str, Any]],
    blockhashes: dict[int, str],
    reports: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected_reports = []
    receipt_slots = []
    for gap in gaps:
        slot_number = gap["post_gap_block"]
        selected = [item for item in reports if item.get("slot") == slot_number]
        if len(selected) != 1:
            raise CollectionError(
                f"expected exactly one decoded report for slot {slot_number}, found {len(selected)}"
            )
        slot = selected[0]
        boundaries = slot.get("boundaries")
        if not isinstance(boundaries, list) or len(boundaries) != gap["boundaries"]:
            raise CollectionError(
                f"slot {slot_number} expected {gap['boundaries']} boundaries, found "
                f"{len(boundaries) if isinstance(boundaries, list) else 'non-list'}"
            )
        expected_ordinals = [
            args.ticks_per_slot * index for index in range(1, gap["boundaries"] + 1)
        ]
        actual_ordinals = [
            item.get("tick_ordinal") if isinstance(item, dict) else None
            for item in boundaries
        ]
        if actual_ordinals != expected_ordinals:
            raise CollectionError(
                f"slot {slot_number} boundary ordinals {actual_ordinals!r} do not match "
                f"{expected_ordinals!r}"
            )
        hashes = [require_string(item, "hash") for item in boundaries]
        final_hash = blockhashes[slot_number]
        if hashes[-1] != final_hash or slot.get("final_entry_hash") != final_hash:
            raise CollectionError(
                f"slot {slot_number} recovered final boundary does not match canonical blockhash"
            )
        hidden = hashes[:-1]
        if len(hidden) != gap["skipped"]:
            raise CollectionError(f"slot {slot_number} hidden-boundary count does not match gap")
        if final_hash in hidden or len(set(hashes)) != len(hashes):
            raise CollectionError(
                f"slot {slot_number} repeats a PoH boundary hash across the gap"
            )
        selected_reports.append(slot)
        receipt_slots.append(
            {
                **gap,
                "boundary_tick_ordinals": actual_ordinals,
                "hidden_boundary_hashes": hidden,
                "canonical_final_blockhash": final_hash,
                "canonical_final_blockhash_match": True,
            }
        )
    return selected_reports, receipt_slots


def collect(args: argparse.Namespace) -> str:
    if args.ticks_per_slot <= 0:
        raise CollectionError("ticks per slot must be positive")
    if len(set(args.scan_state)) != len(args.scan_state):
        raise CollectionError("scan-state paths must be unique")
    audit_bytes, gaps = load_gaps(args)
    metadata_bytes, blockhashes = load_blockhashes(args, gaps)
    scan_bindings, scans = load_scan_states(args, gaps)
    if any(scan["status"] == "running" for scan in scans):
        return "deferred-running"
    if sha256_file(args.decoder) != args.decoder_sha256:
        raise CollectionError("decoder SHA-256 does not match the sealed expectation")

    static_receipt = {
        "schema": RECEIPT_SCHEMA,
        "gap_audit": str(args.gap_audit),
        "gap_audit_sha256": sha256_bytes(audit_bytes),
        "block_metadata": str(args.block_metadata),
        "block_metadata_sha256": sha256_bytes(metadata_bytes),
        "scan_states": scan_bindings,
        "decoder": str(args.decoder),
        "decoder_sha256": args.decoder_sha256,
        "ticks_per_slot": args.ticks_per_slot,
        "output": str(args.output),
        "gap_runs": len(gaps),
        "skipped_slots": sum(item["skipped"] for item in gaps),
        "total_boundaries": sum(item["boundaries"] for item in gaps),
        "hidden_intermediate_boundaries": sum(item["skipped"] for item in gaps),
        "canonical_final_blockhash_matches": True,
        "hidden_intermediate_boundaries_distinct": True,
        "publication_authorized": False,
        "runtime_route_promotion_authorized": False,
        "remote_mutations": False,
        "r2_mutations": False,
    }
    if args.expected_scan_source is None:
        static_receipt["scan_source"] = args.expected_source
    else:
        static_receipt["scan_sources"] = [
            {"path": binding["path"], "source": binding["source"]}
            for binding in scan_bindings
        ]
    if args.receipt.exists():
        if not args.output.exists():
            raise CollectionError("receipt exists but decoded-boundary output is missing")
        _, existing = read_json_bytes(args.receipt)
        if not isinstance(existing, dict):
            raise CollectionError("existing receipt is not a JSON object")
        for key, value in static_receipt.items():
            if existing.get(key) != value:
                raise CollectionError(f"existing receipt field {key!r} does not match")
        if existing.get("output_sha256") != sha256_file(args.output):
            raise CollectionError("existing receipt does not bind the current output")
        return "already-collected"

    reports = decode_reports(args)
    selected_reports, receipt_slots = validate_reports(args, gaps, blockhashes, reports)
    output = {"schema": OUTPUT_SCHEMA, "slots": selected_reports}
    output_bytes = json.dumps(output, indent=2, sort_keys=True).encode() + b"\n"
    receipt = {
        **static_receipt,
        "recorded_at_unix_seconds": time.time(),
        "output_sha256": sha256_bytes(output_bytes),
        "slots": receipt_slots,
    }
    receipt_bytes = json.dumps(receipt, indent=2, sort_keys=True).encode() + b"\n"
    install_no_clobber(args.output, output_bytes)
    install_no_clobber(args.receipt, receipt_bytes)
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
