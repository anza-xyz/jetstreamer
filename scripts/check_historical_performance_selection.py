#!/usr/bin/env python3
"""Fail closed unless a sealed performance receipt selected one exact variant."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping, Sequence


RECEIPT_SCHEMA = "jetstreamer-historical-performance-selection-receipt-v1"
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SelectionGateError(RuntimeError):
    """The selection evidence does not authorize the expected environment."""


def sha256_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_root_receipt(file_path: Path) -> tuple[dict[str, Any], str]:
    if not file_path.is_absolute():
        raise SelectionGateError("selection receipt path must be absolute")
    try:
        metadata = file_path.lstat()
        payload = json.loads(file_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SelectionGateError(f"cannot read selection receipt {file_path}: {error}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise SelectionGateError(f"selection receipt has unsafe identity: {file_path}")
    if not isinstance(payload, dict):
        raise SelectionGateError("selection receipt must be a JSON object")
    return payload, sha256_file(file_path)


def require_sha256(value: str, label: str) -> str:
    if SHA256.fullmatch(value) is None:
        raise SelectionGateError(f"{label} must be a lowercase SHA-256")
    return value


def validate_selection(
    receipt: Mapping[str, Any],
    *,
    expected_variant: str,
    expected_policy_sha256: str,
    expected_selector_sha256: str,
) -> None:
    if receipt.get("schema") != RECEIPT_SCHEMA:
        raise SelectionGateError("unsupported selection receipt schema")
    if not expected_variant or receipt.get("selected_variant") != expected_variant:
        raise SelectionGateError("selection receipt chose a different variant")
    if receipt.get("selection_authorized") is not True:
        raise SelectionGateError("selection receipt does not authorize selection")
    if receipt.get("qualification_launch_authorized") is not False:
        raise SelectionGateError("selection receipt unexpectedly authorizes qualification launch")
    if receipt.get("publication_authorized") is not False:
        raise SelectionGateError("selection receipt unexpectedly authorizes publication")
    if receipt.get("remote_mutations") is not False or receipt.get("r2_mutations") is not False:
        raise SelectionGateError("selection receipt does not prove mutation-free evidence")
    if receipt.get("policy_sha256") != require_sha256(
        expected_policy_sha256, "expected policy digest"
    ):
        raise SelectionGateError("selection receipt policy digest mismatch")
    selector = receipt.get("selector")
    if not isinstance(selector, dict):
        raise SelectionGateError("selection receipt lacks selector evidence")
    if selector.get("sha256") != require_sha256(
        expected_selector_sha256, "expected selector digest"
    ):
        raise SelectionGateError("selection receipt selector digest mismatch")
    candidate_selected = receipt.get("candidate_selected")
    if not isinstance(candidate_selected, bool):
        raise SelectionGateError("selection receipt has invalid candidate decision")
    if candidate_selected != (expected_variant == "waves-store8-t16"):
        raise SelectionGateError("selection receipt variant and candidate decision disagree")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--expected-variant", required=True)
    parser.add_argument("--expected-policy-sha256", required=True)
    parser.add_argument("--expected-selector-sha256", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise SelectionGateError("performance selection gate must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    receipt, receipt_sha256 = load_root_receipt(args.receipt)
    validate_selection(
        receipt,
        expected_variant=args.expected_variant,
        expected_policy_sha256=args.expected_policy_sha256,
        expected_selector_sha256=args.expected_selector_sha256,
    )
    print(
        json.dumps(
            {
                "receipt": str(args.receipt),
                "receipt_sha256": receipt_sha256,
                "selected_variant": args.expected_variant,
                "qualification_launch_authorized": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SelectionGateError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
