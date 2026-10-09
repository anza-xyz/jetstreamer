#!/usr/bin/env python3
"""Stream a bzip2 RocksDB tar and recover SST records by hexadecimal key prefix.

The scanner keeps at most one SST on disk, records every matching internal
version, and never needs to restore the complete RocksDB backup.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path


MATCH_RE = re.compile(
    r"^'(?P<key>[0-9A-F]+)' seq:(?P<sequence>[0-9]+), "
    r"type:(?P<record_type>[0-9]+) => (?P<value>[0-9A-F]*)$"
)
GENERATION_RE = re.compile(r"^gs://.+#[0-9]+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--gcs-source", help="Generation-bound gs:// URL")
    source.add_argument("--archive-file", type=Path, help="Local .tar.bz2 for testing")
    parser.add_argument("--target-prefix", action="append", required=True)
    parser.add_argument("--work-directory", type=Path, required=True)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--max-sst-bytes", type=int, default=2 * 1024**3)
    parser.add_argument("--max-retained-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--max-matches", type=int, default=100000)
    parser.add_argument("--sst-dump", default="sst_dump")
    parser.add_argument("--gcloud", default="gcloud")
    parser.add_argument(
        "--candidate-column-family",
        action="append",
        default=[],
        help="Scan only these families; SSTs with unknown family are always scanned",
    )
    parser.add_argument("--progress-every-ssts", type=int, default=100)
    return parser.parse_args()


def normalize_prefix(value: str) -> str:
    value = value.removeprefix("0x").upper()
    if not value or len(value) % 2 or not re.fullmatch(r"[0-9A-F]+", value):
        raise ValueError(f"invalid even-length hexadecimal prefix: {value!r}")
    return value


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_state(path: Path, state: dict) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(state, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    fsync_directory(path.parent)


def column_family(sst_dump: str, path: Path) -> tuple[str | None, str]:
    result = subprocess.run(
        [sst_dump, f"--file={path}", "--command=identify", "--show_properties"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    match = re.search(r"^\s*column family name:\s*(\S+)\s*$", result.stdout, re.MULTILINE)
    return (match.group(1) if match else None, result.stdout)


def scan_prefix(sst_dump: str, path: Path, prefix: str) -> list[dict]:
    result = subprocess.run(
        [
            sst_dump,
            f"--file={path}",
            "--command=scan",
            f"--prefix=0x{prefix}",
            "--input_key_hex",
            "--output_hex",
            "--read_num=1000000",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"sst_dump scan failed for {path}:\n{result.stdout}")
    matches = []
    for line in result.stdout.splitlines():
        parsed = MATCH_RE.fullmatch(line)
        if parsed and parsed.group("key").startswith(prefix):
            item = parsed.groupdict()
            item["sequence"] = int(item["sequence"])
            item["record_type"] = int(item["record_type"])
            matches.append(item)
    return matches


def main() -> int:
    args = parse_args()
    prefixes = [normalize_prefix(value) for value in args.target_prefix]
    if args.gcs_source and not GENERATION_RE.fullmatch(args.gcs_source):
        raise SystemExit("--gcs-source must include an immutable #generation suffix")
    if args.max_sst_bytes <= 0 or args.max_retained_bytes <= 0 or args.max_matches <= 0:
        raise SystemExit("size and match bounds must be positive")

    def interrupted(signum: int, _frame: object) -> None:
        raise InterruptedError(f"received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)

    args.work_directory.mkdir(parents=True, exist_ok=True)
    args.result_directory.mkdir(parents=True, exist_ok=True)
    matched_directory = args.result_directory / "matched-ssts"
    matched_directory.mkdir(exist_ok=True)
    state_path = args.result_directory / "scan-state.json"
    stderr_path = args.result_directory / "gcloud-stderr.log"
    candidates = set(args.candidate_column_family)
    source_description = args.gcs_source or str(args.archive_file.resolve())
    state = {
        "schema": "jetstreamer-streaming-rocksdb-prefix-scan-v1",
        "source": source_description,
        "target_prefixes": prefixes,
        "candidate_column_families": sorted(candidates),
        "started_unix_seconds": time.time(),
        "status": "running",
        "tar_members": 0,
        "sst_members": 0,
        "sst_bytes_streamed": 0,
        "sst_members_scanned": 0,
        "sst_members_skipped_by_family": 0,
        "retained_sst_bytes": 0,
        "matches": [],
    }
    write_state(state_path, state)

    process: subprocess.Popen | None = None
    source_stream = None
    stderr_file = None
    temporary_path: Path | None = None
    try:
        if args.gcs_source:
            stderr_file = stderr_path.open("wb")
            process = subprocess.Popen(
                [args.gcloud, "storage", "cat", args.gcs_source],
                stdout=subprocess.PIPE,
                stderr=stderr_file,
            )
            assert process.stdout is not None
            source_stream = process.stdout
        else:
            source_stream = args.archive_file.open("rb")

        with tarfile.open(fileobj=source_stream, mode="r|bz2") as archive:
            for member in archive:
                state["tar_members"] += 1
                if not member.isfile() or not member.name.endswith(".sst"):
                    continue
                state["sst_members"] += 1
                state["sst_bytes_streamed"] += member.size
                if member.size > args.max_sst_bytes:
                    raise RuntimeError(
                        f"SST {member.name!r} is {member.size} bytes, above the "
                        f"configured {args.max_sst_bytes}-byte bound"
                    )
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise RuntimeError(f"tar did not expose regular member {member.name!r}")
                with tempfile.NamedTemporaryFile(
                    dir=args.work_directory, prefix="streamed-sst-", suffix=".sst", delete=False
                ) as temporary:
                    temporary_path = Path(temporary.name)
                    shutil.copyfileobj(extracted, temporary, length=1024 * 1024)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                if temporary_path.stat().st_size != member.size:
                    raise RuntimeError(f"short extraction for {member.name!r}")

                family, identify_output = column_family(args.sst_dump, temporary_path)
                should_scan = family is None or not candidates or family in candidates
                member_matches = []
                if should_scan:
                    state["sst_members_scanned"] += 1
                    for prefix in prefixes:
                        for match in scan_prefix(args.sst_dump, temporary_path, prefix):
                            match.update(
                                {
                                    "prefix": prefix,
                                    "tar_member": member.name,
                                    "column_family": family,
                                    "sst_size_bytes": member.size,
                                }
                            )
                            member_matches.append(match)
                    if member_matches:
                        if len(state["matches"]) + len(member_matches) > args.max_matches:
                            raise RuntimeError(
                                f"match count would exceed configured bound {args.max_matches}"
                            )
                        if state["retained_sst_bytes"] + member.size > args.max_retained_bytes:
                            raise RuntimeError(
                                "matched SST retention would exceed configured "
                                f"{args.max_retained_bytes}-byte bound"
                            )
                        destination = matched_directory / Path(member.name).name
                        if destination.exists():
                            raise RuntimeError(f"duplicate matched SST basename: {destination.name}")
                        shutil.copy2(temporary_path, destination)
                        with destination.open("rb") as saved:
                            os.fsync(saved.fileno())
                        fsync_directory(matched_directory)
                        state["retained_sst_bytes"] += member.size
                        state["matches"].extend(member_matches)
                else:
                    state["sst_members_skipped_by_family"] += 1

                temporary_path.unlink()
                temporary_path = None
                if member_matches or state["sst_members"] % args.progress_every_ssts == 0:
                    state["last_tar_member"] = member.name
                    state["last_column_family"] = family
                    if family is None:
                        state["last_identify_output"] = identify_output[-4000:]
                    state["updated_unix_seconds"] = time.time()
                    write_state(state_path, state)
                    print(
                        json.dumps(
                            {
                                "sst_members": state["sst_members"],
                                "sst_bytes_streamed": state["sst_bytes_streamed"],
                                "matches": len(state["matches"]),
                                "last_tar_member": member.name,
                                "column_family": family,
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )

        if process:
            process.stdout.close()
            return_code = process.wait()
            if return_code:
                raise RuntimeError(f"gcloud storage cat exited with status {return_code}")
        state["status"] = "complete"
        state["completed_unix_seconds"] = time.time()
        write_state(state_path, state)
        return 0
    except BaseException as error:
        state["status"] = "failed"
        state["failed_unix_seconds"] = time.time()
        state["error"] = f"{type(error).__name__}: {error}"
        write_state(state_path, state)
        raise
    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()
        if source_stream:
            source_stream.close()
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if stderr_file:
            stderr_file.flush()
            os.fsync(stderr_file.fileno())
            stderr_file.close()
            fsync_directory(args.result_directory)


if __name__ == "__main__":
    sys.exit(main())
