#!/usr/bin/env python3
"""Fail closed when an upload wrapper's pinned hashes do not match deployed files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path


SHA256_RE = r"[0-9a-f]{64}"
OPTION_RE = re.compile(r"^[a-z][a-z0-9-]*-sha256$")
RECEIPT_DIRECTORY_OPTION_RE = re.compile(
    r"^(?:full|plugin|boundary)-receipt-directory$"
)
WRAPPER_HASH_RE = re.compile(
    rf"(?m)^\s*--(?P<option>[a-z][a-z0-9-]*-sha256)="
    rf"(?P<allowlist>{SHA256_RE}(?:,{SHA256_RE})*)\s*(?:\\)?\s*$"
)
WRAPPER_RECEIPT_DIRECTORY_RE = re.compile(
    r"(?m)^\s*--(?P<option>(?:full|plugin|boundary)-receipt-directory)="
    r"(?P<path>/[^\s\\]+)\s*(?:\\)?\s*$"
)


class AuditError(RuntimeError):
    """A fail-closed wrapper audit error."""


@dataclass(frozen=True)
class FileDigest:
    path: str
    sha256: str
    size: int
    device: int
    inode: int
    mode: int
    uid: int
    gid: int


def digest_regular_file(
    path: Path, *, capture_bytes: bool = False
) -> tuple[FileDigest, bytes | None]:
    if not path.is_absolute():
        raise AuditError(f"path must be absolute: {path}")

    try:
        before = path.lstat()
    except OSError as exc:
        raise AuditError(f"cannot stat {path}: {exc}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise AuditError(f"path must be a regular file, not a symlink: {path}")

    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AuditError(f"cannot open {path}: {exc}") from exc

    digest = hashlib.sha256()
    captured = bytearray() if capture_bytes else None
    try:
        opened = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise AuditError(f"path identity changed while opening: {path}")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            if captured is not None:
                captured.extend(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)

    identity_before = (
        opened.st_dev,
        opened.st_ino,
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise AuditError(f"file changed while hashing: {path}")

    return (
        FileDigest(
            path=str(path),
            sha256=digest.hexdigest(),
            size=after.st_size,
            device=after.st_dev,
            inode=after.st_ino,
            mode=stat.S_IMODE(after.st_mode),
            uid=after.st_uid,
            gid=after.st_gid,
        ),
        bytes(captured) if captured is not None else None,
    )


def parse_binding(value: str) -> tuple[str, Path]:
    option, separator, raw_path = value.partition("=")
    if not separator or not OPTION_RE.fullmatch(option) or not raw_path:
        raise argparse.ArgumentTypeError(
            "binding must be OPTION-SHA256=/absolute/path"
        )
    path = Path(raw_path)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(f"binding path must be absolute: {raw_path}")
    return option, path


def parse_receipt_state_directory(value: str) -> tuple[str, Path]:
    option, separator, raw_path = value.partition("=")
    if (
        not separator
        or not RECEIPT_DIRECTORY_OPTION_RE.fullmatch(option)
        or not raw_path
    ):
        raise argparse.ArgumentTypeError(
            "receipt-state-directory must be "
            "{full,plugin,boundary}-receipt-directory=/absolute/state-directory"
        )
    path = Path(raw_path)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError(
            f"receipt state directory must be absolute: {raw_path}"
        )
    if os.path.normpath(raw_path) != raw_path:
        raise argparse.ArgumentTypeError(
            f"receipt state directory must be normalized: {raw_path}"
        )
    return option, path


def audit_wrapper(
    wrapper: Path,
    bindings: list[tuple[str, Path]],
    receipt_state_directories: list[tuple[str, Path]],
) -> dict[str, object]:
    auditor_digest, _ = digest_regular_file(Path(__file__).resolve())
    wrapper_digest, wrapper_bytes = digest_regular_file(wrapper, capture_bytes=True)
    assert wrapper_bytes is not None
    try:
        wrapper_text = wrapper_bytes.decode("utf-8")
    except UnicodeError as exc:
        raise AuditError(f"cannot read wrapper as UTF-8: {wrapper}: {exc}") from exc

    configured: dict[str, list[str]] = {}
    for match in WRAPPER_HASH_RE.finditer(wrapper_text):
        option = match.group("option")
        if option in configured:
            raise AuditError(f"wrapper repeats --{option}")
        configured[option] = match.group("allowlist").split(",")

    supplied: dict[str, Path] = {}
    for option, path in bindings:
        if option in supplied:
            raise AuditError(f"binding repeats {option}")
        supplied[option] = path

    configured_options = set(configured)
    supplied_options = set(supplied)
    if configured_options != supplied_options:
        missing = sorted(configured_options - supplied_options)
        unexpected = sorted(supplied_options - configured_options)
        raise AuditError(
            "binding set does not match wrapper hash options: "
            f"missing={missing} unexpected={unexpected}"
        )
    if not configured:
        raise AuditError("wrapper contains no pinned --*-sha256 options")

    configured_receipt_directories: dict[str, str] = {}
    for match in WRAPPER_RECEIPT_DIRECTORY_RE.finditer(wrapper_text):
        option = match.group("option")
        if option in configured_receipt_directories:
            raise AuditError(f"wrapper repeats --{option}")
        configured_path = match.group("path")
        if os.path.normpath(configured_path) != configured_path:
            raise AuditError(
                f"wrapper --{option} path must be normalized: {configured_path}"
            )
        configured_receipt_directories[option] = configured_path

    supplied_receipt_state_directories: dict[str, Path] = {}
    for option, path in receipt_state_directories:
        if option in supplied_receipt_state_directories:
            raise AuditError(f"receipt state binding repeats {option}")
        supplied_receipt_state_directories[option] = path

    configured_receipt_options = set(configured_receipt_directories)
    supplied_receipt_options = set(supplied_receipt_state_directories)
    if configured_receipt_options != supplied_receipt_options:
        missing = sorted(configured_receipt_options - supplied_receipt_options)
        unexpected = sorted(supplied_receipt_options - configured_receipt_options)
        raise AuditError(
            "receipt state binding set does not match wrapper receipt-directory "
            f"options: missing={missing} unexpected={unexpected}"
        )

    audited_receipt_directories = []
    for option in sorted(configured_receipt_directories):
        state_directory = supplied_receipt_state_directories[option]
        expected_receipt_directory = state_directory / "receipts"
        configured_receipt_directory = Path(configured_receipt_directories[option])
        if configured_receipt_directory != expected_receipt_directory:
            raise AuditError(
                f"--{option} must point at the verifier receipt child: "
                f"configured={configured_receipt_directory} "
                f"expected={expected_receipt_directory}"
            )
        audited_receipt_directories.append(
            {
                "option": option,
                "state_directory": str(state_directory),
                "configured_receipt_directory": str(configured_receipt_directory),
            }
        )

    audited_bindings = []
    for option in sorted(configured):
        file_digest, _ = digest_regular_file(supplied[option])
        allowlist = configured[option]
        if file_digest.sha256 not in allowlist:
            raise AuditError(
                f"--{option} does not admit {file_digest.path}: "
                f"actual={file_digest.sha256} configured={','.join(allowlist)}"
            )
        audited_bindings.append(
            {
                "option": option,
                "configured_sha256_allowlist": allowlist,
                "file": {
                    "path": file_digest.path,
                    "sha256": file_digest.sha256,
                    "size": file_digest.size,
                    "device": file_digest.device,
                    "inode": file_digest.inode,
                    "mode": f"{file_digest.mode:04o}",
                    "uid": file_digest.uid,
                    "gid": file_digest.gid,
                },
            }
        )

    return {
        "schema": "jetstreamer-horizon-upload-wrapper-audit-v1",
        "status": "pass",
        "auditor": {
            "path": auditor_digest.path,
            "sha256": auditor_digest.sha256,
            "size": auditor_digest.size,
            "device": auditor_digest.device,
            "inode": auditor_digest.inode,
            "mode": f"{auditor_digest.mode:04o}",
            "uid": auditor_digest.uid,
            "gid": auditor_digest.gid,
        },
        "wrapper": {
            "path": wrapper_digest.path,
            "sha256": wrapper_digest.sha256,
            "size": wrapper_digest.size,
            "device": wrapper_digest.device,
            "inode": wrapper_digest.inode,
            "mode": f"{wrapper_digest.mode:04o}",
            "uid": wrapper_digest.uid,
            "gid": wrapper_digest.gid,
        },
        "bindings": audited_bindings,
        "receipt_directories": audited_receipt_directories,
    }


def write_receipt_no_clobber(output: Path, payload: bytes) -> None:
    if not output.is_absolute():
        raise AuditError(f"output path must be absolute: {output}")
    parent = output.parent
    try:
        parent_stat = parent.lstat()
    except OSError as exc:
        raise AuditError(f"cannot stat output directory {parent}: {exc}") from exc
    if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
        raise AuditError(f"output parent must be a directory, not a symlink: {parent}")

    temporary = parent / f".{output.name}.tmp.{os.getpid()}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = None
    linked = False
    try:
        descriptor = os.open(temporary, flags, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AuditError(f"short write while creating receipt: {temporary}")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.link(temporary, output, follow_symlinks=False)
        linked = True
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError as exc:
        raise AuditError(f"refusing to overwrite existing receipt: {output}") from exc
    except OSError as exc:
        raise AuditError(f"cannot create receipt {output}: {exc}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        if linked:
            directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wrapper", type=Path)
    parser.add_argument(
        "--binding",
        action="append",
        default=[],
        type=parse_binding,
        metavar="OPTION-SHA256=/ABSOLUTE/PATH",
        help="bind one wrapper --*-sha256 option to the exact deployed file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="atomically create and fsync this absolute JSON receipt path",
    )
    parser.add_argument(
        "--receipt-state-directory",
        action="append",
        default=[],
        type=parse_receipt_state_directory,
        metavar="OPTION=/ABSOLUTE/STATE-DIRECTORY",
        help=(
            "bind one wrapper --*-receipt-directory option to the verifier "
            "state directory whose receipts/ child it must name"
        ),
    )
    args = parser.parse_args()

    try:
        result = audit_wrapper(
            args.wrapper, args.binding, args.receipt_state_directory
        )
    except AuditError as exc:
        print(f"audit failed: {exc}", file=sys.stderr)
        return 1
    payload = (json.dumps(result, sort_keys=True) + "\n").encode("utf-8")
    if args.output is not None:
        try:
            write_receipt_no_clobber(args.output, payload)
        except AuditError as exc:
            print(f"audit failed: {exc}", file=sys.stderr)
            return 1
        print(args.output)
    else:
        sys.stdout.buffer.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
