#!/usr/bin/env python3
"""Progressively upload complete Horizon epoch pairs to R2.

This is deliberately a thin orchestrator around ``jetstreamer-r2``. The Rust
binary owns hashing, multipart integrity, remote readback, and receipt writes.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


SIDECAR_PATTERN = re.compile(r"([0-9a-f]{64})  epoch-([0-9]+)\.jet\n\Z")


def absolute_directory(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_dir():
        raise argparse.ArgumentTypeError(f"not an absolute directory: {value}")
    return path


def absolute_file(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_file():
        raise argparse.ArgumentTypeError(f"not an absolute file: {value}")
    return path


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def regular_file(path: Path) -> os.stat_result | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError(f"expected a regular file: {path}")
    return metadata


def local_pair(directory: Path, epoch: int) -> tuple[Path, Path, str, os.stat_result] | None:
    archive = directory / f"epoch-{epoch}.jet"
    sidecar = directory / f"epoch-{epoch}.jet.sha256"
    archive_metadata = regular_file(archive)
    sidecar_metadata = regular_file(sidecar)
    if archive_metadata is None and sidecar_metadata is None:
        return None
    if archive_metadata is None or sidecar_metadata is None:
        return None
    sidecar_bytes = sidecar.read_bytes()
    try:
        sidecar_text = sidecar_bytes.decode("ascii")
    except UnicodeDecodeError as error:
        raise RuntimeError(f"non-ASCII checksum sidecar: {sidecar}") from error
    match = SIDECAR_PATTERN.fullmatch(sidecar_text)
    if match is None or int(match.group(2)) != epoch:
        raise RuntimeError(f"non-canonical checksum sidecar: {sidecar}")
    return archive, sidecar, match.group(1), archive_metadata


def receipt_matches(
    receipt_path: Path,
    epoch: int,
    digest: str,
    archive_metadata: os.stat_result,
) -> bool:
    try:
        receipt_metadata = regular_file(receipt_path)
        if receipt_metadata is None:
            return False
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        return (
            receipt.get("schema") == "jetstreamer-horizon-r2-receipt-v1"
            and receipt.get("epoch") == epoch
            and receipt.get("archive_key") == f"epoch-{epoch}.jet"
            and receipt.get("checksum_key") == f"epoch-{epoch}.jet.sha256"
            and receipt.get("archive_sha256") == digest
            and receipt.get("archive_length") == archive_metadata.st_size
            and receipt.get("verified_unix_seconds", -1) >= archive_metadata.st_mtime
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return False


def sync_epoch(args: argparse.Namespace, epoch: int) -> None:
    command = [
        str(args.uploader),
        "sync",
        str(args.horizon_directory),
        "--epochs",
        f"{epoch}-{epoch}",
        "--receipt-directory",
        str(args.receipt_directory),
        "--legacy-part-size-mib",
        str(args.legacy_part_size_mib),
        "--concurrency",
        str(args.concurrency),
    ]
    print(f"epoch {epoch}: progressive R2 sync started", flush=True)
    subprocess.run(command, check=True)
    print(f"epoch {epoch}: progressive R2 sync complete", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("uploader", type=absolute_file)
    parser.add_argument("horizon_directory", type=absolute_directory)
    parser.add_argument("receipt_directory", type=absolute_directory)
    parser.add_argument("first_epoch", type=int)
    parser.add_argument("last_epoch", type=int)
    parser.add_argument("--poll-seconds", type=positive_integer, default=300)
    parser.add_argument("--concurrency", type=positive_integer, default=4)
    parser.add_argument("--legacy-part-size-mib", type=positive_integer, default=5)
    args = parser.parse_args()
    if args.first_epoch < 0 or args.last_epoch < args.first_epoch:
        parser.error("invalid inclusive epoch range")
    return args


def main() -> int:
    args = parse_args()
    while True:
        incomplete = 0
        for epoch in range(args.first_epoch, args.last_epoch + 1):
            pair = local_pair(args.horizon_directory, epoch)
            receipt_path = args.receipt_directory / f"epoch-{epoch}.r2.json"
            if pair is None:
                if regular_file(receipt_path) is None:
                    incomplete += 1
                continue
            _archive, _sidecar, digest, archive_metadata = pair
            if receipt_matches(receipt_path, epoch, digest, archive_metadata):
                continue
            sync_epoch(args, epoch)
        if incomplete == 0:
            print(
                f"epochs {args.first_epoch}-{args.last_epoch}: every epoch has an R2 receipt",
                flush=True,
            )
            return 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)
