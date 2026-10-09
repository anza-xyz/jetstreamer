#!/usr/bin/env python3
"""Retire durable R2-backed Horizon pairs after a filesystem floor is crossed."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Sequence

import sync_horizon_r2_progressive as progressive


ARCHIVE_NAME = re.compile(r"epoch-([0-9]+)\.jet\Z")
SIDECAR_NAME = re.compile(r"epoch-([0-9]+)\.jet\.sha256\Z")


class RetirementError(RuntimeError):
    """An emergency local-retirement invariant was not satisfied."""


def available_bytes(path: Path) -> int:
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize


def local_epochs(directory: Path, first_epoch: int, last_epoch: int) -> tuple[int, ...]:
    archives: set[int] = set()
    sidecars: set[int] = set()
    for entry in directory.iterdir():
        metadata = entry.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise RetirementError(f"non-regular public Horizon entry: {entry}")
        archive = ARCHIVE_NAME.fullmatch(entry.name)
        sidecar = SIDECAR_NAME.fullmatch(entry.name)
        if archive is None and sidecar is None:
            raise RetirementError(f"unexpected public Horizon entry: {entry}")
        epoch = int((archive or sidecar).group(1))
        if not first_epoch <= epoch <= last_epoch:
            raise RetirementError(f"public Horizon epoch is outside the authorized range: {epoch}")
        (archives if archive is not None else sidecars).add(epoch)
    if archives != sidecars:
        raise RetirementError(
            "public Horizon namespace contains an incomplete local pair: "
            f"archives={sorted(archives)} sidecars={sorted(sidecars)}"
        )
    for epoch in sorted(archives):
        if progressive.local_pair(directory, epoch) is None:
            raise RetirementError(f"epoch {epoch} is not one canonical local pair")
    return tuple(sorted(archives))


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("uploader", type=progressive.absolute_file)
    parser.add_argument("horizon_directory", type=progressive.absolute_directory)
    parser.add_argument("receipt_directory", type=progressive.absolute_directory)
    parser.add_argument("--filesystem", type=progressive.absolute_directory, required=True)
    parser.add_argument("--minimum-free-bytes", type=progressive.positive_integer, required=True)
    parser.add_argument("--first-epoch", type=int, required=True)
    parser.add_argument("--last-epoch", type=int, required=True)
    parser.add_argument("--local-mutation-lock", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.first_epoch < 0 or args.last_epoch < args.first_epoch:
        parser.error("invalid authorized epoch range")
    if not args.local_mutation_lock.is_absolute():
        parser.error("--local-mutation-lock must be absolute")
    if args.local_mutation_lock.parent.resolve(strict=True) != args.receipt_directory:
        parser.error("--local-mutation-lock must be directly inside the receipt directory")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    observed = available_bytes(args.filesystem)
    if observed >= args.minimum_free_bytes:
        print(
            json.dumps(
                {
                    "available_bytes": observed,
                    "minimum_free_bytes": args.minimum_free_bytes,
                    "retired_epochs": [],
                    "trip": False,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0

    with progressive.local_mutation_lock(args.local_mutation_lock):
        observed = available_bytes(args.filesystem)
        if observed >= args.minimum_free_bytes:
            print(
                json.dumps(
                    {
                        "available_bytes": observed,
                        "minimum_free_bytes": args.minimum_free_bytes,
                        "retired_epochs": [],
                        "trip": False,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            return 0
        epochs = local_epochs(
            args.horizon_directory, args.first_epoch, args.last_epoch
        )
        if not epochs:
            raise RetirementError(
                "filesystem is below the reserve but no complete local pair is "
                "available for retirement"
            )
        retired: list[int] = []
        for epoch in epochs:
            command = [
                str(args.uploader),
                "retire-local",
                str(args.horizon_directory),
                "--epochs",
                f"{epoch}-{epoch}",
                "--receipt-directory",
                str(args.receipt_directory),
            ]
            subprocess.run(command, check=True)
            retired.append(epoch)
            if available_bytes(args.filesystem) >= args.minimum_free_bytes:
                break
        remaining = set(
            local_epochs(args.horizon_directory, args.first_epoch, args.last_epoch)
        )
        not_retired = set(retired) & remaining
        if not_retired:
            raise RetirementError(
                f"verified uploader left authorized local epochs present: {sorted(not_retired)}"
            )

    final_available = available_bytes(args.filesystem)
    print(
        json.dumps(
            {
                "available_bytes_before": observed,
                "available_bytes_after": final_available,
                "floor_recovered": final_available >= args.minimum_free_bytes,
                "minimum_free_bytes": args.minimum_free_bytes,
                "retired_epochs": retired,
                "remote_mutations": False,
                "r2_mutations": False,
                "trip": True,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if final_available < args.minimum_free_bytes:
        raise RetirementError(
            "all available local pairs were retired but the filesystem remains "
            "below the reserve"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RetirementError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1) from error
