#!/usr/bin/env python3
"""Cross-check Horizon publication receipts without requiring local archives."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import stat
import sys


SHA256 = re.compile(r"[0-9a-f]{64}\Z")
R2_SCHEMA = "jetstreamer-horizon-r2-receipt-v1"


def absolute_directory(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("directory must be absolute")
    return path


def sha256(value: str) -> str:
    if SHA256.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("expected a lowercase SHA-256 digest")
    return value


def regular_file(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1


def read_words(path: Path, count: int) -> list[str] | None:
    try:
        if not regular_file(path):
            return None
        words = path.read_text(encoding="ascii").split()
    except (OSError, UnicodeDecodeError):
        return None
    if len(words) != count or not all(SHA256.fullmatch(word) for word in words):
        return None
    return words


def read_gate(
    directory: Path,
    epoch: int,
    suffix: str,
    *,
    verifier_sha256: str | None = None,
    script_sha256: str | None = None,
) -> str | None:
    words = read_words(directory / f"epoch-{epoch}.{suffix}.ok", 3)
    if words is None:
        return None
    if verifier_sha256 is not None and words[1] != verifier_sha256:
        return None
    if script_sha256 is not None and words[2] != script_sha256:
        return None
    return words[0]


def read_r2(directory: Path, epoch: int) -> str | None:
    path = directory / f"epoch-{epoch}.r2.json"
    try:
        if not regular_file(path):
            return None
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None
    digest = receipt.get("archive_sha256")
    valid = (
        receipt.get("schema") == R2_SCHEMA
        and receipt.get("epoch") == epoch
        and receipt.get("archive_key") == f"epoch-{epoch}.jet"
        and receipt.get("checksum_key") == f"epoch-{epoch}.jet.sha256"
        and isinstance(receipt.get("archive_length"), int)
        and receipt["archive_length"] > 0
        and isinstance(digest, str)
        and SHA256.fullmatch(digest) is not None
        and (
            receipt.get("remote_sha256_readback") is True
            or (
                isinstance(receipt.get("r2_composite_sha256"), str)
                and bool(receipt["r2_composite_sha256"])
            )
        )
    )
    return digest if valid else None


def read_boundary(directory: Path, left: int, right: int) -> tuple[str, str] | None:
    words = read_words(directory / f"boundary-{left}-{right}.ok", 4)
    return (words[0], words[1]) if words is not None else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("first_epoch", type=int)
    parser.add_argument("last_epoch", type=int)
    parser.add_argument("--full-receipts", required=True, type=absolute_directory)
    parser.add_argument("--plugin-receipts", required=True, type=absolute_directory)
    parser.add_argument("--plugin-pipeline-sha256", required=True, type=sha256)
    parser.add_argument("--plugin-verifier-script-sha256", required=True, type=sha256)
    parser.add_argument("--boundary-receipts", required=True, type=absolute_directory)
    parser.add_argument("--r2-receipts", required=True, type=absolute_directory)
    parser.add_argument(
        "--require-outer-boundaries",
        action="store_true",
        help="also require predecessor->first and last->successor receipts",
    )
    args = parser.parse_args()
    if args.first_epoch < 0 or args.last_epoch < args.first_epoch:
        parser.error("invalid inclusive epoch range")
    return args


def audit(args: argparse.Namespace) -> list[str]:
    errors: list[str] = []
    digests: dict[int, str] = {}
    for epoch in range(args.first_epoch, args.last_epoch + 1):
        full = read_gate(args.full_receipts, epoch, "full")
        plugin = read_gate(
            args.plugin_receipts,
            epoch,
            "plugin",
            verifier_sha256=args.plugin_pipeline_sha256,
            script_sha256=args.plugin_verifier_script_sha256,
        )
        r2 = read_r2(args.r2_receipts, epoch)
        missing = [
            name
            for name, value in (("full", full), ("plugin", plugin), ("r2", r2))
            if value is None
        ]
        if missing:
            errors.append(f"epoch {epoch}: missing/invalid {','.join(missing)} receipt")
            continue
        assert full is not None and plugin is not None and r2 is not None
        if not full == plugin == r2:
            errors.append(
                f"epoch {epoch}: receipt digest mismatch full={full} plugin={plugin} r2={r2}"
            )
            continue
        digests[epoch] = full

    boundary_first = args.first_epoch - int(
        args.require_outer_boundaries and args.first_epoch > 0
    )
    boundary_last = args.last_epoch + int(args.require_outer_boundaries)
    if args.require_outer_boundaries:
        for neighbor in (boundary_first, boundary_last):
            if neighbor not in digests:
                digest = read_r2(args.r2_receipts, neighbor)
                if digest is None:
                    errors.append(
                        f"epoch {neighbor}: missing/invalid R2 receipt for outer boundary"
                    )
                else:
                    digests[neighbor] = digest

    for left in range(boundary_first, boundary_last):
        right = left + 1
        boundary = read_boundary(args.boundary_receipts, left, right)
        if boundary is None:
            errors.append(f"boundary {left}-{right}: missing/invalid receipt")
            continue
        expected_left = digests.get(left)
        expected_right = digests.get(right)
        if expected_left is None or expected_right is None:
            continue
        if boundary != (expected_left, expected_right):
            errors.append(
                f"boundary {left}-{right}: digest mismatch "
                f"receipt={boundary[0]},{boundary[1]} "
                f"expected={expected_left},{expected_right}"
            )
    return errors


def main() -> int:
    args = parse_args()
    errors = audit(args)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(f"Horizon receipt audit incomplete: {len(errors)} issue(s)", file=sys.stderr)
        return 1
    print(
        f"Horizon receipt audit complete for epochs "
        f"{args.first_epoch}-{args.last_epoch}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
