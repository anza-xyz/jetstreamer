#!/usr/bin/env python3
"""Run a sealed qualification validator and durably publish its JSON receipt."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Sequence


SHA256 = re.compile(r"^[0-9a-f]{64}$")
EPOCH_208_CONFLICT_SLOT = 89856107
EPOCH_208_CONFLICT_ACCOUNT = "FJwFtQFEyKEA4M6ZTrosTRPJphEpDA9ckUeMq9pRJdd4"
EPOCH_208_CONFLICT_FIRST = (
    "7,2jFfi2JubVwgEZd11pQZkX3kBHr9M4CjH5jbeULzJ4JijrfNwS8amU2ipd1Y3BZEf"
    "gWYQ143eVseceSn46TkPL1y,848104700000"
)
EPOCH_208_CONFLICT_SECOND = (
    "8,3oKU6ZkBjX9SP6njWLoiSChvP87y73Y26GBuj6TL8X62nhZ1FABjDfYT3AZc4eS54f"
    "ppWgFzyPCrSGHKFxDbuyov,848104695000"
)
EPOCH_208_CONFLICT = (
    EPOCH_208_CONFLICT_SLOT,
    EPOCH_208_CONFLICT_ACCOUNT,
    EPOCH_208_CONFLICT_FIRST,
    EPOCH_208_CONFLICT_SECOND,
)


class ValidationLaunchError(RuntimeError):
    pass


@dataclass(frozen=True)
class ValidationRequest:
    validator: Path
    validator_sha256: str
    archive: Path
    receipt: Path
    epoch: int
    output_start: int
    bootstrap: int
    terminal: int
    runtime: str
    worker_sha256: str
    private_root: Path
    conflict_slot: int | None = None
    conflict_account: str | None = None
    conflict_first: str | None = None
    conflict_second: str | None = None


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"path must be absolute: {value}")
    return path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def require_canonical_regular(
    path: Path,
    label: str,
    *,
    expected_uid: int | None = None,
    executable: bool = False,
) -> os.stat_result:
    if not path.is_absolute():
        raise ValidationLaunchError(f"{label} path must be absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ValidationLaunchError(f"cannot inspect {label} {path}: {error}") from error
    if resolved != path or not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValidationLaunchError(f"{label} has an unsafe identity: {path}")
    if expected_uid is not None and metadata.st_uid != expected_uid:
        raise ValidationLaunchError(
            f"{label} has uid {metadata.st_uid}, expected {expected_uid}: {path}"
        )
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ValidationLaunchError(f"{label} is group/other writable: {path}")
    if executable and not metadata.st_mode & stat.S_IXUSR:
        raise ValidationLaunchError(f"{label} is not owner-executable: {path}")
    return metadata


def require_descendant(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValidationLaunchError(f"{label} is outside private root {root}: {path}") from error


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def validator_arguments(request: ValidationRequest) -> list[str]:
    arguments = [
        str(request.validator),
        str(request.archive),
        f"--private-root={request.private_root}",
        f"--expected-epoch={request.epoch}",
        f"--expected-output-start-slot={request.output_start}",
        f"--expected-bootstrap-slot={request.bootstrap}",
        f"--expected-terminal-slot={request.terminal}",
        f"--expected-runtime-profile={request.runtime}",
        f"--expected-worker-sha256={request.worker_sha256}",
    ]
    conflict = (
        request.conflict_slot,
        request.conflict_account,
        request.conflict_first,
        request.conflict_second,
    )
    if request.epoch == 208 and not all(value is not None for value in conflict):
        raise ValidationLaunchError("epoch 208 requires the canonical conflict expectations")
    if request.epoch == 208 and conflict != EPOCH_208_CONFLICT:
        raise ValidationLaunchError("epoch 208 conflict expectations are not canonical")
    if any(value is not None for value in conflict):
        if not all(value is not None for value in conflict):
            raise ValidationLaunchError("conflict expectations must be supplied atomically")
        if request.conflict_slot is None or not (
            request.output_start <= request.conflict_slot <= request.terminal
        ):
            raise ValidationLaunchError("conflict slot is outside the qualification output")
        if not all(
            isinstance(value, str) and value
            for value in (
                request.conflict_account,
                request.conflict_first,
                request.conflict_second,
            )
        ):
            raise ValidationLaunchError("conflict string expectations must not be empty")
        arguments.extend(
            [
                f"--expected-conflict-slot={request.conflict_slot}",
                f"--expected-conflict-account={request.conflict_account}",
                f"--expected-conflict-first={request.conflict_first}",
                f"--expected-conflict-second={request.conflict_second}",
            ]
        )
    return arguments


def validate_request(request: ValidationRequest, deployment_uid: int) -> os.stat_result:
    if not SHA256.fullmatch(request.validator_sha256):
        raise ValidationLaunchError("validator SHA-256 is not canonical lowercase hex")
    if not SHA256.fullmatch(request.worker_sha256):
        raise ValidationLaunchError("worker SHA-256 is not canonical lowercase hex")
    if min(request.epoch, request.output_start, request.bootstrap, request.terminal) < 0:
        raise ValidationLaunchError("numeric expectations must not be negative")
    if request.output_start > request.terminal or request.bootstrap >= request.output_start:
        raise ValidationLaunchError("qualification slot expectations are inconsistent")
    if not request.runtime:
        raise ValidationLaunchError("runtime expectation must not be empty")

    root = request.private_root.resolve(strict=True)
    if root != request.private_root:
        raise ValidationLaunchError("private root path must be canonical")
    root_metadata = root.stat()
    if not stat.S_ISDIR(root_metadata.st_mode) or root_metadata.st_mode & 0o022:
        raise ValidationLaunchError("private root is not a trusted directory")
    archive_metadata = require_canonical_regular(request.archive, "archive")
    require_descendant(request.archive, root, "archive")

    validator_metadata = require_canonical_regular(
        request.validator,
        "validator",
        expected_uid=deployment_uid,
        executable=True,
    )
    if sha256_file(request.validator) != request.validator_sha256:
        raise ValidationLaunchError("validator SHA-256 mismatch")
    if identity(validator_metadata) != identity(request.validator.stat()):
        raise ValidationLaunchError("validator changed while hashing")

    if request.receipt.exists() or request.receipt.is_symlink():
        raise ValidationLaunchError(f"receipt already exists: {request.receipt}")
    try:
        receipt_parent = request.receipt.parent.resolve(strict=True)
        parent_metadata = receipt_parent.stat()
    except OSError as error:
        raise ValidationLaunchError(f"cannot inspect receipt directory: {error}") from error
    if receipt_parent != request.receipt.parent:
        raise ValidationLaunchError("receipt directory is not canonical")
    require_descendant(request.receipt, root, "receipt")
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != deployment_uid
        or stat.S_IMODE(parent_metadata.st_mode) & 0o077
    ):
        raise ValidationLaunchError("receipt directory must be deployment-owned and owner-only")
    validator_arguments(request)
    return archive_metadata


def run_validation(request: ValidationRequest, deployment_uid: int = 0) -> dict[str, object]:
    archive_before = validate_request(request, deployment_uid)
    receipt_parent = request.receipt.parent
    temporary_fd, temporary_name = tempfile.mkstemp(
        prefix=f".{request.receipt.name}.", suffix=".tmp", dir=receipt_parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        os.fchmod(temporary_fd, 0o600)
        completed = subprocess.run(
            validator_arguments(request),
            stdout=temporary_fd,
            stderr=subprocess.PIPE,
            text=True,
            close_fds=True,
        )
        if completed.returncode != 0:
            raise ValidationLaunchError(
                "independent validator failed: "
                + (completed.stderr.strip() or f"exit {completed.returncode}")
            )
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = -1

        try:
            payload = json.loads(temporary.read_bytes())
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationLaunchError(f"validator output is not valid JSON: {error}") from error
        if not isinstance(payload, dict):
            raise ValidationLaunchError("validator output must be a JSON object")

        archive_after = request.archive.stat(follow_symlinks=False)
        if not stat.S_ISREG(archive_after.st_mode) or identity(archive_before) != identity(archive_after):
            raise ValidationLaunchError("archive changed during independent validation")
        if request.receipt.exists() or request.receipt.is_symlink():
            raise ValidationLaunchError(f"receipt appeared during validation: {request.receipt}")
        try:
            os.link(temporary, request.receipt, follow_symlinks=False)
        except FileExistsError as error:
            raise ValidationLaunchError(f"receipt appeared during validation: {request.receipt}") from error
        fsync_directory(receipt_parent)
        published = True
        temporary.unlink()
        fsync_directory(receipt_parent)
        return payload
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if not published and temporary.exists():
            temporary.unlink()
            fsync_directory(receipt_parent)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validator", type=absolute_path, required=True)
    parser.add_argument("--validator-sha256", required=True)
    parser.add_argument("--archive", type=absolute_path, required=True)
    parser.add_argument("--receipt", type=absolute_path, required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--output-start", type=int, required=True)
    parser.add_argument("--bootstrap", type=int, required=True)
    parser.add_argument("--terminal", type=int, required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--worker-sha256", required=True)
    parser.add_argument("--private-root", type=absolute_path, required=True)
    parser.add_argument("--expected-conflict-slot", type=int)
    parser.add_argument("--expected-conflict-account")
    parser.add_argument("--expected-conflict-first")
    parser.add_argument("--expected-conflict-second")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise ValidationLaunchError("independent validator launcher must run as root")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    request = ValidationRequest(
        validator=args.validator,
        validator_sha256=args.validator_sha256,
        archive=args.archive,
        receipt=args.receipt,
        epoch=args.epoch,
        output_start=args.output_start,
        bootstrap=args.bootstrap,
        terminal=args.terminal,
        runtime=args.runtime,
        worker_sha256=args.worker_sha256,
        private_root=args.private_root,
        conflict_slot=args.expected_conflict_slot,
        conflict_account=args.expected_conflict_account,
        conflict_first=args.expected_conflict_first,
        conflict_second=args.expected_conflict_second,
    )
    payload = run_validation(request)
    print(json.dumps(payload, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationLaunchError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
