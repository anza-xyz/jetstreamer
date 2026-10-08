#!/usr/bin/env python3
"""Progressively upload complete Horizon epoch pairs to R2.

This is deliberately a thin orchestrator around ``jetstreamer-r2``. The Rust
binary owns hashing, multipart integrity, remote readback, receipt writes, and
optional local deletion. This process delegates upload only after the full
archive and current-plugin receipts bind the same archive digest. Local
retirement additionally requires both adjacent-boundary receipts. Keeping those
gates separate lets disjoint producers publish complete archives through R2 so
their neighbors can be restored for boundary verification without weakening the
local-deletion gate.
"""

from __future__ import annotations

import argparse
from collections.abc import Collection
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


SIDECAR_PATTERN = re.compile(r"([0-9a-f]{64})  epoch-([0-9]+)\.jet\n\Z")
GATE_RECEIPT_PATTERN = re.compile(
    r"([0-9a-f]{64}) ([0-9a-f]{64}) ([0-9a-f]{64})\n\Z"
)


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


def sha256(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise argparse.ArgumentTypeError("expected a lowercase SHA-256 digest")
    return value


def sha256_allowlist(value: str) -> tuple[str, ...]:
    values = value.split(",")
    if not values or any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in values):
        raise argparse.ArgumentTypeError(
            "expected a comma-separated lowercase SHA-256 allowlist"
        )
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("duplicate SHA-256 allowlist entry")
    return tuple(values)


def digest_allowed(expected: str | Collection[str] | None, actual: str) -> bool:
    if expected is None:
        return True
    if isinstance(expected, str):
        return actual == expected
    return actual in expected


def epoch_range(value: str) -> tuple[int, int]:
    parts = value.split("-", 1)
    try:
        first = int(parts[0])
        last = int(parts[-1])
    except ValueError as error:
        raise argparse.ArgumentTypeError("invalid epoch range") from error
    if first < 0 or last < first:
        raise argparse.ArgumentTypeError("invalid epoch range")
    return first, last


def regular_file(path: Path) -> os.stat_result | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError(f"expected a regular file: {path}")
    return metadata


@contextmanager
def local_mutation_lock(path: Path | None):
    if path is None:
        yield
        return
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise RuntimeError(f"unsafe local-mutation lock: {path}")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        current = path.lstat()
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise RuntimeError(f"local-mutation lock namespace changed: {path}")
        yield
    finally:
        os.close(descriptor)


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
            and (
                receipt.get("remote_sha256_readback") is True
                or isinstance(receipt.get("r2_composite_sha256"), str)
            )
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return False


def gate_receipt_matches(
    directory: Path,
    epoch: int,
    suffix: str,
    digest: str,
    *,
    verifier_sha256: str | Collection[str] | None = None,
    script_sha256: str | None = None,
) -> bool:
    path = directory / f"epoch-{epoch}.{suffix}.ok"
    try:
        if regular_file(path) is None:
            return False
        match = GATE_RECEIPT_PATTERN.fullmatch(path.read_text(encoding="ascii"))
        return (
            match is not None
            and match.group(1) == digest
            and digest_allowed(verifier_sha256, match.group(2))
            and (script_sha256 is None or match.group(3) == script_sha256)
        )
    except (OSError, UnicodeDecodeError):
        return False


def boundary_receipt_matches(
    directory: Path,
    left_epoch: int,
    right_epoch: int,
    digest: str,
    *,
    digest_field: int,
    verifier_sha256: str | Collection[str],
    script_sha256: str,
) -> bool:
    path = directory / f"boundary-{left_epoch}-{right_epoch}.ok"
    try:
        if regular_file(path) is None:
            return False
        fields = path.read_text(encoding="ascii").split()
        return (
            len(fields) == 4
            and all(re.fullmatch(r"[0-9a-f]{64}", field) for field in fields)
            and fields[digest_field] == digest
            and digest_allowed(verifier_sha256, fields[2])
            and fields[3] == script_sha256
        )
    except (OSError, UnicodeDecodeError):
        return False


def publication_allowed(args: argparse.Namespace, epoch: int, digest: str) -> bool:
    return (
        gate_receipt_matches(
            args.full_receipt_directory,
            epoch,
            "full",
            digest,
            verifier_sha256=args.full_verifier_sha256,
            script_sha256=args.full_verifier_script_sha256,
        )
        and gate_receipt_matches(
            args.plugin_receipt_directory,
            epoch,
            "plugin",
            digest,
            verifier_sha256=args.plugin_pipeline_sha256,
            script_sha256=args.plugin_verifier_script_sha256,
        )
    )


def adjacent_boundaries_allowed(
    args: argparse.Namespace, epoch: int, digest: str
) -> bool:
    return (
        (
            epoch == 0
            or boundary_receipt_matches(
                args.boundary_receipt_directory,
                epoch - 1,
                epoch,
                digest,
                digest_field=1,
                verifier_sha256=args.boundary_verifier_sha256,
                script_sha256=args.boundary_verifier_script_sha256,
            )
        )
        and boundary_receipt_matches(
            args.boundary_receipt_directory,
            epoch,
            epoch + 1,
            digest,
            digest_field=0,
            verifier_sha256=args.boundary_verifier_sha256,
            script_sha256=args.boundary_verifier_script_sha256,
        )
    )


def retirement_allowed(args: argparse.Namespace, epoch: int, digest: str) -> bool:
    return (
        args.delete_local
        and not any(first <= epoch <= last for first, last in args.defer_epochs)
        and publication_allowed(args, epoch, digest)
        and adjacent_boundaries_allowed(args, epoch, digest)
    )


def sync_epoch(args: argparse.Namespace, epoch: int, *, delete_local: bool = False) -> None:
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
    if delete_local:
        command.append("--delete-local")
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
    parser.add_argument("--delete-local", action="store_true")
    parser.add_argument("--local-mutation-lock", type=Path)
    parser.add_argument("--full-receipt-directory", type=absolute_directory)
    parser.add_argument("--full-verifier-sha256", type=sha256_allowlist)
    parser.add_argument("--full-verifier-script-sha256", type=sha256)
    parser.add_argument("--plugin-receipt-directory", type=absolute_directory)
    parser.add_argument("--plugin-pipeline-sha256", type=sha256_allowlist)
    parser.add_argument("--plugin-verifier-script-sha256", type=sha256)
    parser.add_argument("--boundary-receipt-directory", type=absolute_directory)
    parser.add_argument("--boundary-verifier-sha256", type=sha256_allowlist)
    parser.add_argument("--boundary-verifier-script-sha256", type=sha256)
    parser.add_argument(
        "--defer-epochs", type=epoch_range, action="append", default=[]
    )
    args = parser.parse_args()
    if args.first_epoch < 0 or args.last_epoch < args.first_epoch:
        parser.error("invalid inclusive epoch range")
    if (
        args.full_receipt_directory is None
        or args.full_verifier_sha256 is None
        or args.full_verifier_script_sha256 is None
        or args.plugin_receipt_directory is None
        or args.plugin_pipeline_sha256 is None
        or args.plugin_verifier_script_sha256 is None
        or args.boundary_receipt_directory is None
        or args.boundary_verifier_sha256 is None
        or args.boundary_verifier_script_sha256 is None
    ):
        parser.error(
            "upload requires --full-receipt-directory, "
            "--full-verifier-sha256, --full-verifier-script-sha256 and "
            "--plugin-receipt-directory, --plugin-pipeline-sha256, "
            "--plugin-verifier-script-sha256, --boundary-receipt-directory, "
            "--boundary-verifier-sha256 and --boundary-verifier-script-sha256"
        )
    if args.local_mutation_lock is not None:
        if not args.local_mutation_lock.is_absolute():
            parser.error("--local-mutation-lock must be absolute")
        if args.local_mutation_lock.parent.resolve(strict=True) != args.receipt_directory:
            parser.error("--local-mutation-lock must be directly inside the receipt directory")
    return args


def main() -> int:
    args = parse_args()
    while True:
        incomplete = 0
        for epoch in range(args.first_epoch, args.last_epoch + 1):
            with local_mutation_lock(args.local_mutation_lock):
                pair = local_pair(args.horizon_directory, epoch)
                receipt_path = args.receipt_directory / f"epoch-{epoch}.r2.json"
                if pair is None:
                    if regular_file(receipt_path) is None:
                        incomplete += 1
                    continue
                _archive, _sidecar, digest, archive_metadata = pair
                publish = publication_allowed(args, epoch, digest)
                retire = retirement_allowed(args, epoch, digest)
                if receipt_matches(receipt_path, epoch, digest, archive_metadata):
                    if retire:
                        sync_epoch(args, epoch, delete_local=True)
                    continue
                if not publish:
                    incomplete += 1
                    print(
                        f"epoch {epoch}: waiting for exact full and plugin receipts",
                        flush=True,
                    )
                    continue
                sync_epoch(args, epoch, delete_local=retire)
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
