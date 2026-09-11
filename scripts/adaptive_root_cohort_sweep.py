#!/usr/bin/env python3
"""Schedule sealed historical root-checkpoint cohorts in bounded systemd lanes.

The manifest cohort is the atomic work item. Producer services write only to
pre-provisioned owner-only lane directories. A completed lane is imported into
the public destination only by jetstreamer-node's
``--recover-staged-cohort-only`` path, one cohort at a time.

Planning is the default. Pass ``--execute`` only from a detached, root-owned
service after reviewing the printed plan. This controller never copies cloud
credentials, removes a failed attempt, or repairs a partial public namespace.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import grp
import re
import secrets
import shlex
import signal
import stat
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence


GIB = 1024**3
MANIFEST_SCHEMA = "jetstreamer-gcs-snapshot-preflight-v2"
RECEIPT_SCHEMA = "jetstreamer-root-cohort-publication-receipt-v2"
ROOT_CHECKPOINT_CONTEXT_SCHEMA = "jetstreamer-root-checkpoint-gate-context-v1"
PUBLICATION_GATE = "all-archives-validated-and-final-root-verified"
STATE_SCHEMA = "jetstreamer-adaptive-root-cohort-sweep-v3"
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_RECEIPT_BYTES = 32 * 1024 * 1024
DEFAULT_FIRST_EPOCH = 22
DEFAULT_LAST_EPOCH = 100
DEFAULT_INITIAL_CONCURRENCY = 2
DEFAULT_TARGET_CONCURRENCY = 6
DEFAULT_MAX_CONCURRENCY = 8
DEFAULT_SETTLE_SECONDS = 600
DEFAULT_POLL_SECONDS = 15
DEFAULT_MEMORY_HIGH_GIB = 48
DEFAULT_MEMORY_MAX_GIB = 64
DEFAULT_MEMORY_RESERVE_GIB = 64
DEFAULT_PROTECTED_MEMORY_GIB = 350
DEFAULT_CPUS_PER_LANE = 8
DEFAULT_CPU_QUOTA_PERCENT = 1000
DEFAULT_ACCOUNT = "sam.johnson@anza.xyz"
DEFAULT_PROJECT = "principal-lane-200702"
MAX_U64 = (1 << 64) - 1
MAX_RECEIPT_EPOCHS = 10_000
CANONICAL_GENESIS_SHA256 = (
    "133f7eaefcd59466f3b291aadd1b0d3522432072cf5b539445218c6c125ea945"
)
ARCHIVE_BATCH_MARKERS = (
    ".jetstreamer-archive-batch",
    ".jetstreamer-archive-batch-outcome",
)
FINGERPRINT_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SAFE_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
SYSTEMD_UNIT_RE = re.compile(r"[A-Za-z0-9_.@:-]{1,240}\.service\Z")
RUNTIME_WORKERS = {
    "solana-v1.0.8": ("V1_0_8", "jetstreamer-historical-worker-v1-0-8"),
    "solana-v1.0.13": ("V1_0_13", "jetstreamer-historical-worker-v1-0-13"),
    "solana-v1.0.14": ("V1_0_14", "jetstreamer-historical-worker-v1-0-14"),
    "solana-v1.0.23": ("V1_0_23", "jetstreamer-historical-worker-v1-0-23"),
    "solana-v1.1.23": ("V1_1_23", "jetstreamer-historical-worker-v1-1-23"),
    "solana-v1.2.32": ("V1_2_32", "jetstreamer-historical-worker-v1-2-32"),
    "solana-v1.3.19": ("V1_3_19", "jetstreamer-historical-worker-v1-3-19"),
}
SENSITIVE_PATHS = (
    "/home/sol/.ssh",
    "/home/sol/workspace",
    "/home/sol/.gnupg",
    "/home/sol/.codex",
    "/home/sol/.claude",
    "/home/sol/.claude.json",
    "/home/sol/.copilot",
    "/home/sol/.cargo",
    "/home/sol/.rustup",
    "/home/sol/.gsutil",
    "/home/sol/identity",
    "/home/sol/.bash_history",
    "/home/sol/.zsh_history",
    "/home/sol/.config",
)


class SweepError(RuntimeError):
    """The controller cannot continue without weakening an invariant."""


class DuplicateJsonKey(ValueError):
    pass


@dataclasses.dataclass(frozen=True, order=True)
class Cohort:
    first_epoch: int
    last_epoch: int
    runtime: str

    @property
    def label(self) -> str:
        if self.first_epoch == self.last_epoch:
            return str(self.first_epoch)
        return f"{self.first_epoch}-{self.last_epoch}"

    @property
    def epochs(self) -> tuple[int, ...]:
        return tuple(range(self.first_epoch, self.last_epoch + 1))

    def overlaps(self, first: int, last: int) -> bool:
        return self.first_epoch <= last and first <= self.last_epoch


@dataclasses.dataclass(frozen=True)
class Lane:
    name: str
    root: Path

    @property
    def output(self) -> Path:
        return self.root / "output"

    @property
    def private(self) -> Path:
        return self.root / "private"

    @property
    def cloud_config(self) -> Path:
        return self.root / "config" / "gcloud"


@dataclasses.dataclass(frozen=True)
class ProducerProcess:
    pid: int
    start_time: int
    cohort: Cohort
    output: Path
    manifest: Path
    fingerprint: str
    unit: str | None
    argv: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class EpochClaim:
    pid: int
    start_time: int
    first_epoch: int
    last_epoch: int
    output: Path
    executable_argument: Path
    unit: str | None

    def overlaps(self, cohort: Cohort) -> bool:
        return self.first_epoch <= cohort.last_epoch and cohort.first_epoch <= self.last_epoch


@dataclasses.dataclass(frozen=True)
class UnitStatus:
    exists: bool
    active: bool
    succeeded: bool
    result: str
    main_status: int | None
    memory_current: int | None
    memory_peak: int | None
    sub_state: str = ""

    @property
    def exited_successfully(self) -> bool:
        return (
            self.exists
            and self.active
            and self.sub_state == "exited"
            and self.result == "success"
            and self.main_status == 0
        )


@dataclasses.dataclass(frozen=True)
class DirectoryIdentity:
    device: int
    inode: int

    def as_json(self) -> dict[str, int]:
        return {"device": self.device, "inode": self.inode}


class BoundDirectory:
    """Keep a directory inode open and reject path replacement."""

    def __init__(self, path: Path):
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        self.path = path
        try:
            self.descriptor = os.open(path, flags)
        except OSError as error:
            raise SweepError(f"failed to bind directory {path}: {error}") from error
        try:
            info = os.fstat(self.descriptor)
            if not stat.S_ISDIR(info.st_mode):
                raise SweepError(f"bound path is not a directory: {path}")
            self.identity = DirectoryIdentity(info.st_dev, info.st_ino)
            self.mode = info.st_mode
            self.uid = info.st_uid
            self.gid = info.st_gid
            self.revalidate()
        except BaseException:
            os.close(self.descriptor)
            raise

    def revalidate(self) -> None:
        try:
            descriptor_info = os.fstat(self.descriptor)
            path_info = self.path.lstat()
            canonical = self.path.resolve(strict=True)
        except OSError as error:
            raise SweepError(f"bound directory became unavailable: {self.path}: {error}") from error
        stable = (
            descriptor_info.st_dev,
            descriptor_info.st_ino,
            descriptor_info.st_mode,
            descriptor_info.st_uid,
            descriptor_info.st_gid,
        )
        observed = (
            path_info.st_dev,
            path_info.st_ino,
            path_info.st_mode,
            path_info.st_uid,
            path_info.st_gid,
        )
        expected = (
            self.identity.device,
            self.identity.inode,
            self.mode,
            self.uid,
            self.gid,
        )
        if (
            canonical != self.path
            or not stat.S_ISDIR(descriptor_info.st_mode)
            or not stat.S_ISDIR(path_info.st_mode)
            or stable != expected
            or observed != expected
        ):
            raise SweepError(f"bound directory identity changed: {self.path}")

    def close(self) -> None:
        os.close(self.descriptor)


def _pairs_no_duplicates(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJsonKey(key)
        result[key] = value
    return result


def strict_json(data: bytes, source: Path) -> Any:
    try:
        return json.loads(
            data,
            object_pairs_hook=_pairs_no_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite number {value}")
            ),
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        DuplicateJsonKey,
        ValueError,
        RecursionError,
    ) as error:
        raise SweepError(f"invalid JSON in {source}: {error}") from error


def _validate_canonical_json_value(value: Any) -> None:
    if value is None or isinstance(value, (bool, str)):
        if isinstance(value, str) and not value.isascii():
            raise SweepError("manifest contains a non-ASCII string")
        return
    if isinstance(value, int) and not isinstance(value, bool):
        return
    if isinstance(value, list):
        for item in value:
            _validate_canonical_json_value(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise SweepError("manifest contains a non-ASCII object key")
            _validate_canonical_json_value(item)
        return
    raise SweepError("manifest contains a non-integer JSON number")


def canonical_json(value: Any) -> bytes:
    _validate_canonical_json_value(value)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("ascii")


def read_regular_nofollow_with_identity(
    path: Path, maximum: int
) -> tuple[bytes, dict[str, int]]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise SweepError(f"failed to open {path}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise SweepError(f"expected a singly linked regular file: {path}")
        if before.st_size <= 0 or before.st_size > maximum:
            raise SweepError(f"file size for {path} is outside 1..={maximum}")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise SweepError(f"short read from {path}")
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            raise SweepError(f"file changed while it was read: {path}")
        identity = file_identity_from_stat(after)
        if file_identity(path) != identity:
            raise SweepError(f"file namespace changed while it was read: {path}")
        return b"".join(chunks), identity
    finally:
        os.close(descriptor)


def read_regular_nofollow(path: Path, maximum: int) -> bytes:
    return read_regular_nofollow_with_identity(path, maximum)[0]


def _stat_identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def load_cohorts(
    manifest_path: Path,
    expected_fingerprint: str,
    first_epoch: int,
    last_epoch: int,
) -> tuple[Cohort, ...]:
    if not FINGERPRINT_RE.fullmatch(expected_fingerprint):
        raise SweepError("manifest fingerprint must be sha256 plus 64 lowercase hex digits")
    report = strict_json(
        read_regular_nofollow(manifest_path, MAX_MANIFEST_BYTES), manifest_path
    )
    if not isinstance(report, dict):
        raise SweepError("manifest report must be a JSON object")
    body = report.get("manifest")
    embedded = report.get("manifest_fingerprint")
    if not isinstance(body, dict) or not isinstance(embedded, str):
        raise SweepError("manifest report lacks manifest or manifest_fingerprint")
    computed = "sha256:" + hashlib.sha256(canonical_json(body)).hexdigest()
    if embedded != computed or expected_fingerprint != computed:
        raise SweepError(
            f"manifest fingerprint mismatch: expected {expected_fingerprint}, "
            f"embedded {embedded}, computed {computed}"
        )
    if body.get("schema") != MANIFEST_SCHEMA or body.get("epoch_slots") != 432_000:
        raise SweepError("manifest schema or epoch slot count is incompatible")
    manifest_first = body.get("first_epoch")
    manifest_last = body.get("last_epoch")
    raw_cohorts = body.get("verification_cohorts")
    if (
        not isinstance(manifest_first, int)
        or isinstance(manifest_first, bool)
        or not isinstance(manifest_last, int)
        or isinstance(manifest_last, bool)
        or not isinstance(raw_cohorts, list)
        or manifest_first < 1
        or manifest_last < manifest_first
    ):
        raise SweepError("manifest has invalid epoch bounds or cohort list")
    if first_epoch < manifest_first or last_epoch > manifest_last or first_epoch > last_epoch:
        raise SweepError("requested epoch range is outside the sealed manifest")

    cohorts: list[Cohort] = []
    expected_next = manifest_first
    for index, raw in enumerate(raw_cohorts):
        if not isinstance(raw, dict):
            raise SweepError(f"manifest cohort {index} is not an object")
        start = raw.get("first_epoch")
        end = raw.get("last_epoch")
        runtime = raw.get("runtime")
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
            or start != expected_next
            or end < start
            or not isinstance(runtime, str)
            or not runtime
            or raw.get("publication_gate") != PUBLICATION_GATE
            or not isinstance(raw.get("root_checkpoints"), list)
            or not raw["root_checkpoints"]
        ):
            raise SweepError(f"manifest cohort {index} is invalid or noncontiguous")
        cohorts.append(Cohort(start, end, runtime))
        expected_next = end + 1
    if expected_next != manifest_last + 1:
        raise SweepError("manifest cohorts do not cover the declared epoch range")

    selected = tuple(
        cohort for cohort in cohorts if cohort.overlaps(first_epoch, last_epoch)
    )
    if (
        not selected
        or selected[0].first_epoch != first_epoch
        or selected[-1].last_epoch != last_epoch
    ):
        raise SweepError("requested bounds split a sealed verification cohort")
    unsupported = [item.runtime for item in selected if item.runtime not in RUNTIME_WORKERS]
    if unsupported:
        raise SweepError(f"selected cohort runtime is unsupported: {unsupported[0]}")
    return selected


def parse_lane(value: str) -> Lane:
    name, separator, raw_path = value.partition("=")
    if not separator or not SAFE_NAME_RE.fullmatch(name):
        raise argparse.ArgumentTypeError("lane must be NAME=/absolute/path with a safe name")
    path = Path(raw_path)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("lane path must be absolute")
    return Lane(name, path)


def epoch_range(value: str) -> tuple[int, int] | None:
    match = re.fullmatch(r"([0-9]+)(?:-([0-9]+))?", value)
    if match is None:
        return None
    first = int(match.group(1))
    last = int(match.group(2) or match.group(1))
    return (first, last) if 0 <= first <= last else None


def parse_epoch_range(value: str) -> tuple[int, int]:
    parsed = epoch_range(value)
    if parsed is None:
        raise argparse.ArgumentTypeError("expected EPOCH or FIRST-LAST")
    return parsed


def parse_proc_stat_start_time(data: str) -> int:
    command_end = data.rfind(")")
    if command_end < 0:
        raise ValueError("process stat lacks a closing command delimiter")
    fields_after_command = data[command_end + 2 :].split()
    # Field 3 (state) is index 0 here; field 22 is therefore index 19.
    if len(fields_after_command) <= 19:
        raise ValueError("process stat is truncated")
    return int(fields_after_command[19])


def proc_start_time(pid: int) -> int:
    return parse_proc_stat_start_time(Path(f"/proc/{pid}/stat").read_text())


def process_systemd_unit(pid: int) -> str | None:
    try:
        for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
            _hierarchy, separator, path = line.partition("::")
            if separator:
                unit = Path(path).name
                if SYSTEMD_UNIT_RE.fullmatch(unit):
                    return unit
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass
    return None


def discover_producers(
    *, expected_node: Path | None = None, expected_uid: int | None = None
) -> tuple[ProducerProcess, ...]:
    found: list[ProducerProcess] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            process_info = entry.stat()
            if expected_uid is not None and process_info.st_uid != expected_uid:
                continue
            argv = (entry / "cmdline").read_bytes().split(b"\0")
            args = [item.decode("utf-8") for item in argv if item]
            if (
                len(args) < 3
                or Path(args[0]).name != "jetstreamer-node"
                or args.count("--root-checkpoint-cohort") != 1
                or args.count("--verify") != 1
                or "--no-verify" in args
                or "--recover-staged-cohort-only" in args
            ):
                continue
            if expected_node is not None:
                if args[0] != str(expected_node):
                    continue
                expected_node_info = expected_node.stat()
                try:
                    executable_info = (entry / "exe").stat()
                except PermissionError:
                    # A hardened, non-dumpable service may hide this link even
                    # from a capability-limited root controller. Adoption also
                    # requires an exact root-owned systemd ExecStart and MainPID.
                    executable_info = None
                if executable_info is not None:
                    if (
                        not stat.S_ISREG(executable_info.st_mode)
                        or (executable_info.st_dev, executable_info.st_ino)
                        != (expected_node_info.st_dev, expected_node_info.st_ino)
                    ):
                        continue
            bounds = epoch_range(args[1])
            if bounds is None:
                continue
            manifest_args = [
                item.split("=", 1)[1]
                for item in args
                if item.startswith("--cohort-manifest=")
            ]
            fingerprints = [
                item.split("=", 1)[1]
                for item in args
                if item.startswith("--cohort-manifest-fingerprint=")
            ]
            if len(manifest_args) != 1 or len(fingerprints) != 1:
                continue
            output = Path(args[2]).resolve(strict=True)
            manifest = Path(manifest_args[0]).resolve(strict=True)
            found.append(
                ProducerProcess(
                    pid=pid,
                    start_time=proc_start_time(pid),
                    cohort=Cohort(bounds[0], bounds[1], ""),
                    output=output,
                    manifest=manifest,
                    fingerprint=fingerprints[0],
                    unit=process_systemd_unit(pid),
                    argv=tuple(args),
                )
            )
        except (
            FileNotFoundError,
            PermissionError,
            ProcessLookupError,
            UnicodeDecodeError,
            ValueError,
            OSError,
        ):
            continue
    return tuple(found)


def discover_epoch_claims(*, expected_uid: int) -> tuple[EpochClaim, ...]:
    claims: list[EpochClaim] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            if entry.stat().st_uid != expected_uid:
                continue
            args = [
                item.decode("utf-8")
                for item in (entry / "cmdline").read_bytes().split(b"\0")
                if item
            ]
            if len(args) < 3 or Path(args[0]).name != "jetstreamer-node":
                continue
            bounds = epoch_range(args[1])
            if bounds is None:
                continue
            output = Path(args[2]).resolve(strict=True)
            claims.append(
                EpochClaim(
                    pid,
                    proc_start_time(pid),
                    bounds[0],
                    bounds[1],
                    output,
                    Path(args[0]),
                    process_systemd_unit(pid),
                )
            )
        except (
            FileNotFoundError,
            PermissionError,
            ProcessLookupError,
            UnicodeDecodeError,
            ValueError,
            OSError,
        ):
            continue
    return tuple(claims)


def require_disjoint_epoch_claims(claims: Sequence[EpochClaim]) -> None:
    for index, claim in enumerate(claims):
        for other in claims[index + 1 :]:
            overlaps = (
                claim.first_epoch <= other.last_epoch
                and other.first_epoch <= claim.last_epoch
            )
            if overlaps or claim.output == other.output:
                raise SweepError(
                    f"jetstreamer PIDs {claim.pid} and {other.pid} have "
                    "overlapping epoch or output claims"
                )


def process_is_same(pid: int, start_time: int) -> bool:
    try:
        return proc_start_time(pid) == start_time
    except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
        return False


def require_private_network_namespace() -> None:
    interfaces = {item.name for item in Path("/sys/class/net").iterdir()}
    if interfaces != {"lo"}:
        raise SweepError(
            "--execute requires PrivateNetwork=yes and a loopback-only controller namespace"
        )


def _path_arg(path: Path) -> str:
    return str(path)


def producer_environment(
    cohort: Cohort,
    deploy: Path,
    lane: Lane,
    account: str,
    project: str,
) -> tuple[str, ...]:
    worker_env, worker_name = RUNTIME_WORKERS[cohort.runtime]
    worker = deploy / worker_name
    return (
        "HOME=/home/sol",
        "USER=sol",
        "LOGNAME=sol",
        "LANG=C.UTF-8",
        "PATH=/usr/bin:/bin",
        f"XDG_CONFIG_HOME={lane.root / 'config'}",
        f"CLOUDSDK_CONFIG={lane.cloud_config}",
        "CLOUDSDK_CORE_DISABLE_PROMPTS=1",
        "CLOUDSDK_CORE_DISABLE_FILE_LOGGING=1",
        f"CLOUDSDK_CORE_PROJECT={project}",
        f"CLOUDSDK_CORE_ACCOUNT={account}",
        "JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1",
        f"JETSTREAMER_HISTORICAL_WORKER_{worker_env}={worker}",
        "JETSTREAMER_HISTORICAL_POH_THREADS=10",
        "RAYON_NUM_THREADS=10",
        "JETSTREAMER_ARCHIVE_BACKEND=http",
        "JETSTREAMER_HTTP_BASE_URL=https://files.old-faithful.net/",
        "JETSTREAMER_COMPACT_INDEX_BASE_URL=https://files.old-faithful.net/",
        "JETSTREAMER_NETWORK_CAPACITY_MB=100000",
        "JETSTREAMER_THREADS=12",
        "JETSTREAMER_RIPGET_THREADS=12",
        "JETSTREAMER_CLEAR_ACCOUNTS_ON_START=true",
        "JETSTREAMER_VERIFY_SNAPSHOTS=1",
        "JETSTREAMER_SKIP_SNAPSHOT_VERIFY=0",
        "JETSTREAMER_LOAD_FROM_DIR=0",
        "JETSTREAMER_ENFORCE_ARCHIVE_HASH=1",
        "JETSTREAMER_PRUNE_EPOCH_SNAPSHOTS=0",
        f"JETSTREAMER_PRIVATE_RUN_ROOT={lane.private}",
        "RUST_LOG=info,solana_metrics=off,solana_runtime::bank=warn,solana_runtime::accounts_db=warn",
        "RUST_BACKTRACE=1",
    )


def build_producer_command(
    *,
    cohort: Cohort,
    lane: Lane,
    deploy: Path,
    manifest: Path,
    fingerprint: str,
    unit: str,
    account: str = DEFAULT_ACCOUNT,
    project: str = DEFAULT_PROJECT,
    memory_high_gib: int = DEFAULT_MEMORY_HIGH_GIB,
    memory_max_gib: int = DEFAULT_MEMORY_MAX_GIB,
    cpu_quota_percent: int = DEFAULT_CPU_QUOTA_PERCENT,
) -> list[str]:
    _, worker_name = RUNTIME_WORKERS[cohort.runtime]
    node = deploy / "jetstreamer-node"
    worker = deploy / worker_name
    whole_lane_paths = f"{lane.root}"
    properties = (
        "Type=exec",
        "KillMode=control-group",
        "TimeoutStartSec=infinity",
        "TimeoutStopSec=2min",
        f"MemoryHigh={memory_high_gib * GIB}",
        f"MemoryMax={memory_max_gib * GIB}",
        "MemorySwapMax=0",
        f"CPUQuota={cpu_quota_percent}%",
        "TasksMax=1024",
        "Nice=5",
        "IOSchedulingClass=best-effort",
        "IOSchedulingPriority=4",
        "LimitNOFILE=1048576",
        "OOMPolicy=stop",
        "UMask=0077",
        "NoNewPrivileges=yes",
        "PrivateTmp=yes",
        "PrivateDevices=yes",
        "PrivateIPC=yes",
        "TemporaryFileSystem=/dev/shm:rw,nosuid,nodev,noexec,mode=1777,size=64M",
        "ProtectSystem=strict",
        "ProtectHome=read-only",
        "ProtectKernelTunables=yes",
        "ProtectKernelModules=yes",
        "ProtectKernelLogs=yes",
        "ProtectControlGroups=yes",
        "ProtectClock=yes",
        "ProtectHostname=yes",
        "RestrictNamespaces=yes",
        "RestrictRealtime=yes",
        "RestrictSUIDSGID=yes",
        "LockPersonality=yes",
        "SystemCallArchitectures=native",
        "CapabilityBoundingSet=",
        "AmbientCapabilities=",
        "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
        "NoExecPaths=/home/sol",
        f"ExecPaths={node} {worker} {whole_lane_paths}",
        f"ReadWritePaths={whole_lane_paths}",
        f"BindReadOnlyPaths={deploy}:{deploy}:rbind",
        "InaccessiblePaths=" + " ".join(f"-{item}" for item in SENSITIVE_PATHS),
        "User=sol",
        "Group=horizon",
        f"WorkingDirectory={deploy}",
    )
    command = [
        "/usr/bin/systemd-run",
        f"--unit={unit}",
        "--collect",
        f"--description=Sealed adaptive root cohort {cohort.label} in {lane.name}",
    ]
    command.extend(f"--property={item}" for item in properties)
    command.extend(
        f"--setenv={item}"
        for item in producer_environment(cohort, deploy, lane, account, project)
    )
    command.extend(
        [
            _path_arg(node),
            cohort.label,
            _path_arg(lane.output),
            "--verify",
            "--root-checkpoint-cohort",
            f"--cohort-manifest={manifest}",
            f"--cohort-manifest-fingerprint={fingerprint}",
        ]
    )
    return command


def build_import_command(
    *,
    cohort: Cohort,
    receipt: Path,
    deploy: Path,
    manifest: Path,
    fingerprint: str,
    public_dir: Path,
    public_private_root: Path,
    unit: str,
    memory_high_gib: int = DEFAULT_MEMORY_HIGH_GIB,
    memory_max_gib: int = DEFAULT_MEMORY_MAX_GIB,
    cpu_quota_percent: int = DEFAULT_CPU_QUOTA_PERCENT,
) -> list[str]:
    worker_env, worker_name = RUNTIME_WORKERS[cohort.runtime]
    node = deploy / "jetstreamer-node"
    worker = deploy / worker_name
    environment = (
        "HOME=/home/sol",
        "USER=sol",
        "LOGNAME=sol",
        "LANG=C.UTF-8",
        "PATH=/usr/bin:/bin",
        "JETSTREAMER_ALLOW_CANDIDATE_RUNTIME=1",
        f"JETSTREAMER_HISTORICAL_WORKER_{worker_env}={worker}",
        "RAYON_NUM_THREADS=10",
        "JETSTREAMER_ENFORCE_ARCHIVE_HASH=1",
        f"JETSTREAMER_PRIVATE_RUN_ROOT={public_private_root}",
        "RUST_LOG=info,solana_metrics=off",
        "RUST_BACKTRACE=1",
    )
    properties = (
        "Type=exec",
        "RemainAfterExit=yes",
        "KillMode=control-group",
        "TimeoutStartSec=infinity",
        "TimeoutStopSec=2min",
        f"MemoryHigh={memory_high_gib * GIB}",
        f"MemoryMax={memory_max_gib * GIB}",
        "MemorySwapMax=0",
        f"CPUQuota={cpu_quota_percent}%",
        "TasksMax=1024",
        "Nice=5",
        "LimitNOFILE=1048576",
        "OOMPolicy=stop",
        "UMask=0077",
        "NoNewPrivileges=yes",
        "PrivateNetwork=yes",
        "PrivateTmp=yes",
        "PrivateDevices=yes",
        "PrivateIPC=yes",
        "ProtectSystem=full",
        "ProtectHome=no",
        "ProtectKernelTunables=yes",
        "ProtectKernelModules=yes",
        "ProtectKernelLogs=yes",
        "ProtectControlGroups=yes",
        "ProtectClock=yes",
        "ProtectHostname=yes",
        "RestrictNamespaces=yes",
        "RestrictRealtime=yes",
        "RestrictSUIDSGID=yes",
        "LockPersonality=yes",
        "SystemCallArchitectures=native",
        "CapabilityBoundingSet=",
        "AmbientCapabilities=",
        "RestrictAddressFamilies=AF_UNIX",
        "NoExecPaths=/home/sol",
        f"ExecPaths={node}",
        "User=sol",
        "Group=horizon",
        f"WorkingDirectory={deploy}",
        "InaccessiblePaths=" + " ".join(f"-{item}" for item in SENSITIVE_PATHS),
    )
    command = [
        "/usr/bin/systemd-run",
        f"--unit={unit}",
        f"--description=Serialized import for sealed root cohort {cohort.label}",
    ]
    command.extend(f"--property={item}" for item in properties)
    command.extend(f"--setenv={item}" for item in environment)
    command.extend(
        [
        _path_arg(node),
        cohort.label,
        _path_arg(public_dir),
        "--verify",
        "--recover-staged-cohort-only",
        f"--cohort-manifest={manifest}",
        f"--cohort-manifest-fingerprint={fingerprint}",
        f"--source-cohort-receipt={receipt}",
        ]
    )
    return command


def parse_meminfo(data: str) -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in data.splitlines():
        match = re.fullmatch(r"(MemTotal|MemAvailable):\s+([0-9]+)\s+kB", line)
        if match:
            values[match.group(1)] = int(match.group(2)) * 1024
    if set(values) != {"MemTotal", "MemAvailable"}:
        raise SweepError("/proc/meminfo lacks MemTotal or MemAvailable")
    return values["MemTotal"], values["MemAvailable"]


def admission_capacity(
    *,
    memory_total: int,
    memory_available: int,
    logical_cpus: int,
    active: int,
    elapsed_seconds: float,
    lane_count: int,
    initial: int,
    target: int,
    maximum: int,
    settle_seconds: int,
    memory_max: int,
    memory_reserve: int,
    protected_memory: int,
    cpus_per_lane: int,
    memory_current: Sequence[int | None],
    memory_peak: Sequence[int | None],
    memory_high: int,
) -> int:
    if min(memory_total, memory_available, logical_cpus, lane_count, memory_max) <= 0:
        return min(active, 1)
    static_memory = max(0, memory_total - protected_memory - memory_reserve) // memory_max
    live_additional = max(0, memory_available - memory_reserve) // memory_max
    live_memory = active + live_additional
    cpu = max(1, logical_cpus // max(1, cpus_per_lane))
    # One observation window can qualify at most one additional lane. The
    # controller persists the new limit and starts a fresh window.
    ramp_steps = min(1, int(elapsed_seconds // max(1, settle_seconds)))
    ramp = min(target, initial + ramp_steps)
    configured = min(maximum, lane_count, ramp)
    capacity = min(configured, static_memory, live_memory, cpu)
    if active >= initial:
        if any(value is None for value in memory_current):
            return min(active, capacity)
        if any(value is not None and value >= memory_high for value in memory_current):
            return min(active, capacity)
        if any(value is not None and value >= memory_high for value in memory_peak):
            return min(active, capacity)
    return capacity


def parse_systemd_show(text: str) -> UnitStatus:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            fields[key] = value

    def number(name: str) -> int | None:
        value = fields.get(name, "")
        return int(value) if value.isdigit() else None

    exists = fields.get("LoadState") == "loaded"
    active = fields.get("ActiveState") in {"activating", "active", "reloading"}
    result = fields.get("Result", "")
    sub_state = fields.get("SubState", "")
    main_status = number("ExecMainStatus")
    succeeded = exists and not active and result == "success" and main_status == 0
    return UnitStatus(
        exists,
        active,
        succeeded,
        result,
        main_status,
        number("MemoryCurrent"),
        number("MemoryPeak"),
        sub_state,
    )


def unit_status(unit: str) -> UnitStatus:
    result = subprocess.run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "--no-pager",
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=MemoryCurrent",
            "--property=MemoryPeak",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if re.search(r"(?m)^LoadState=not-found$", result.stdout):
            return parse_systemd_show(result.stdout)
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic"
        raise SweepError(f"failed to query systemd unit {unit}: {detail}")
    return parse_systemd_show(result.stdout)


def require_unused_unit_name(unit: str) -> None:
    """Reject a retained unit before durable state claims a new launch."""
    if unit_status(unit).exists:
        raise SweepError(
            f"refusing to launch {unit}: a systemd unit with that name already exists"
        )


def systemd_properties(unit: str, names: Sequence[str]) -> dict[str, str] | None:
    command = ["/usr/bin/systemctl", "show", unit, "--no-pager"]
    command.extend(f"--property={name}" for name in names)
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    properties: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    return properties


def parse_systemd_timespan_usec(value: str) -> int | None:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(us|ms|s|min|h)", value)
    if match is None:
        return None
    multipliers = {
        "us": Decimal(1),
        "ms": Decimal(1_000),
        "s": Decimal(1_000_000),
        "min": Decimal(60_000_000),
        "h": Decimal(3_600_000_000),
    }
    try:
        result = Decimal(match.group(1)) * multipliers[match.group(2)]
    except InvalidOperation:
        return None
    return int(result) if result == result.to_integral_value() else None


def unit_is_hardened_for_adoption(
    unit: str,
    process: ProducerProcess,
    cohort: Cohort,
    lane: Lane,
    deploy: Path,
    memory_high_gib: int = DEFAULT_MEMORY_HIGH_GIB,
    memory_max_gib: int = DEFAULT_MEMORY_MAX_GIB,
    cpu_quota_percent: int = DEFAULT_CPU_QUOTA_PERCENT,
    account: str = DEFAULT_ACCOUNT,
    project: str = DEFAULT_PROJECT,
) -> bool:
    properties = systemd_properties(
        unit,
        (
            "MainPID",
            "ControlGroup",
            "ExecStart",
            "Type",
            "RemainAfterExit",
            "KillMode",
            "TimeoutStartUSec",
            "TimeoutStopUSec",
            "MemoryHigh",
            "MemoryMax",
            "MemorySwapMax",
            "CPUQuotaPerSecUSec",
            "TasksMax",
            "Nice",
            "IOSchedulingClass",
            "IOSchedulingPriority",
            "LimitNOFILE",
            "OOMPolicy",
            "UMask",
            "User",
            "Group",
            "WorkingDirectory",
            "Environment",
            "ReadWritePaths",
            "ExecPaths",
            "NoExecPaths",
            "InaccessiblePaths",
            "PrivateTmp",
            "PrivateDevices",
            "PrivateIPC",
            "TemporaryFileSystem",
            "ProtectHome",
            "ProtectSystem",
            "ProtectKernelTunables",
            "ProtectKernelModules",
            "ProtectKernelLogs",
            "ProtectControlGroups",
            "ProtectClock",
            "ProtectHostname",
            "NoNewPrivileges",
            "RestrictNamespaces",
            "RestrictRealtime",
            "RestrictSUIDSGID",
            "LockPersonality",
            "SystemCallArchitectures",
            "RestrictAddressFamilies",
            "CapabilityBoundingSet",
            "AmbientCapabilities",
            "BindReadOnlyPaths",
        ),
    )
    if properties is None:
        return False
    _, worker_name = RUNTIME_WORKERS[cohort.runtime]
    exact = {
        "MainPID": str(process.pid),
        "ControlGroup": f"/system.slice/{unit}",
        "Type": "exec",
        "RemainAfterExit": "no",
        "KillMode": "control-group",
        "TimeoutStartUSec": "infinity",
        "TimeoutStopUSec": "2min",
        "MemoryHigh": str(memory_high_gib * GIB),
        "MemoryMax": str(memory_max_gib * GIB),
        "MemorySwapMax": "0",
        "TasksMax": "1024",
        "Nice": "5",
        "IOSchedulingClass": "2",
        "IOSchedulingPriority": "4",
        "LimitNOFILE": "1048576",
        "OOMPolicy": "stop",
        "UMask": "0077",
        "User": "sol",
        "Group": "horizon",
        "WorkingDirectory": str(deploy),
        "ReadWritePaths": str(lane.root),
        "ExecPaths": f"{deploy / 'jetstreamer-node'} {deploy / worker_name} {lane.root}",
        "NoExecPaths": "/home/sol",
        "InaccessiblePaths": " ".join(f"-{item}" for item in SENSITIVE_PATHS),
        "PrivateTmp": "yes",
        "PrivateDevices": "yes",
        "PrivateIPC": "yes",
        "TemporaryFileSystem": "/dev/shm:rw,nosuid,nodev,noexec,mode=1777,size=64M",
        "ProtectHome": "read-only",
        "ProtectSystem": "strict",
        "ProtectKernelTunables": "yes",
        "ProtectKernelModules": "yes",
        "ProtectKernelLogs": "yes",
        "ProtectControlGroups": "yes",
        "ProtectClock": "yes",
        "ProtectHostname": "yes",
        "NoNewPrivileges": "yes",
        "RestrictNamespaces": "yes",
        "RestrictRealtime": "yes",
        "RestrictSUIDSGID": "yes",
        "LockPersonality": "yes",
        "SystemCallArchitectures": "native",
        "CapabilityBoundingSet": "",
        "AmbientCapabilities": "",
        "BindReadOnlyPaths": f"{deploy}:{deploy}:rbind",
    }
    if any(properties.get(key) != value for key, value in exact.items()):
        return False
    if parse_systemd_timespan_usec(properties.get("CPUQuotaPerSecUSec", "")) != (
        cpu_quota_percent * 10_000
    ):
        return False
    try:
        configured_environment = set(shlex.split(properties.get("Environment", "")))
    except ValueError:
        return False
    if configured_environment != set(
        producer_environment(cohort, deploy, lane, account, project)
    ):
        return False
    node = deploy / "jetstreamer-node"
    if re.match(
        rf"^\{{ path={re.escape(str(node))} ; argv\[\]={re.escape(str(node))} ",
        properties.get("ExecStart", ""),
    ) is None:
        return False
    return set(properties.get("RestrictAddressFamilies", "").split()) == {
        "AF_UNIX",
        "AF_INET",
        "AF_INET6",
    }


def checksum_file(path: Path) -> str:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise SweepError(f"archive is not a singly linked regular file: {path}")
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            raise SweepError(f"archive changed while hashing: {path}")
        if file_identity(path) != file_identity_from_stat(after):
            raise SweepError(f"archive namespace changed while hashing: {path}")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def public_archive_namespace_complete(
    public_dir: Path,
    cohort: Cohort,
    expected_uid: int,
    expected_gid: int,
    rehash: bool = True,
) -> bool:
    present = 0
    for epoch in cohort.epochs:
        archive = public_dir / f"epoch-{epoch}.jet"
        checksum = public_dir / f"epoch-{epoch}.jet.sha256"
        legacy_manifest = public_dir / f"epoch-{epoch}.jet.segment.json"
        try:
            legacy_manifest.lstat()
        except FileNotFoundError:
            pass
        else:
            raise SweepError(
                f"unexpected legacy manifest in public namespace: {legacy_manifest}"
            )
        try:
            archive_info = archive.lstat()
        except FileNotFoundError:
            archive_info = None
        try:
            checksum_info = checksum.lstat()
        except FileNotFoundError:
            checksum_info = None
        archive_exists = archive_info is not None
        checksum_exists = checksum_info is not None
        if archive_exists != checksum_exists:
            raise SweepError(f"partial public namespace for epoch {epoch}")
        if not archive_exists:
            continue
        assert archive_info is not None and checksum_info is not None
        for path, info in ((archive, archive_info), (checksum, checksum_info)):
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != expected_uid
                or info.st_gid != expected_gid
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o440
                or stat.S_IMODE(info.st_mode) & 0o7000
                or path.resolve(strict=True) != path
            ):
                raise SweepError(f"unsafe public archive namespace member: {path}")
        present += 1
        sidecar = read_regular_nofollow(checksum, 512).decode("ascii", "strict")
        match = re.fullmatch(rf"([0-9a-f]{{64}})  epoch-{epoch}\.jet\n", sidecar)
        if match is None:
            raise SweepError(f"noncanonical checksum sidecar for epoch {epoch}")
        if rehash and checksum_file(archive) != match.group(1):
            raise SweepError(f"public checksum mismatch for epoch {epoch}")
    if present not in {0, len(cohort.epochs)}:
        raise SweepError(f"partial public cohort namespace for {cohort.label}")
    return present == len(cohort.epochs)


def public_recovery_marker_present(public_dir: Path, expected_uid: int) -> bool:
    present: list[Path] = []
    for name in ARCHIVE_BATCH_MARKERS:
        path = public_dir / name
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != expected_uid
            or stat.S_IMODE(info.st_mode) & 0o077
            or stat.S_IMODE(info.st_mode) & 0o7000
            or path.resolve(strict=True) != path
        ):
            raise SweepError(f"unsafe public archive batch marker: {path}")
        present.append(path)
    if len(present) > 1:
        raise SweepError("public destination contains both archive batch markers")
    return bool(present)


def decode_receipt_path(encoded: Any) -> Path | None:
    if not isinstance(encoded, str):
        return None
    try:
        decoded = base64.b64decode(encoded, validate=True)
        if base64.b64encode(decoded).decode("ascii") != encoded or b"\0" in decoded:
            return None
        return Path(os.fsdecode(decoded))
    except (ValueError, UnicodeDecodeError):
        return None


def is_u64(value: Any) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= MAX_U64
    )


def receipt_destination(
    data: Mapping[str, Any],
) -> tuple[Path, DirectoryIdentity] | None:
    destination = data.get("destination")
    if (
        not isinstance(destination, dict)
        or set(destination) != {"path_base64", "device", "inode"}
        or not is_u64(destination.get("device"))
        or not is_u64(destination.get("inode"))
    ):
        return None
    path = decode_receipt_path(destination.get("path_base64"))
    if path is None:
        return None
    return path, DirectoryIdentity(destination["device"], destination["inode"])


def directory_identity(path: Path) -> DirectoryIdentity:
    try:
        info = path.lstat()
        canonical = path.resolve(strict=True)
    except OSError as error:
        raise SweepError(f"failed to inspect directory identity for {path}: {error}") from error
    if canonical != path or not stat.S_ISDIR(info.st_mode):
        raise SweepError(f"expected a canonical real directory: {path}")
    return DirectoryIdentity(info.st_dev, info.st_ino)


def canonical_receipt_epochs(value: Any) -> tuple[int, ...] | None:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > MAX_RECEIPT_EPOCHS
        or any(
            not isinstance(epoch, int)
            or isinstance(epoch, bool)
            or epoch < 0
            or epoch > MAX_U64
            for epoch in value
        )
    ):
        return None
    first = value[0]
    if any(epoch != first + offset for offset, epoch in enumerate(value)):
        return None
    return tuple(value)


def file_identity(path: Path) -> dict[str, int]:
    try:
        info = path.lstat()
    except OSError as error:
        raise SweepError(f"failed to inspect file identity for {path}: {error}") from error
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise SweepError(f"expected a singly linked regular file: {path}")
    return file_identity_from_stat(info)


def file_identity_from_stat(info: os.stat_result) -> dict[str, int]:
    seconds_per_nanosecond = 1_000_000_000
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "mode": info.st_mode,
        "uid": info.st_uid,
        "gid": info.st_gid,
        "link_count": info.st_nlink,
        "length": info.st_size,
        "modified_seconds": info.st_mtime_ns // seconds_per_nanosecond,
        "modified_nanoseconds": info.st_mtime_ns % seconds_per_nanosecond,
        "changed_seconds": info.st_ctime_ns // seconds_per_nanosecond,
        "changed_nanoseconds": info.st_ctime_ns % seconds_per_nanosecond,
    }


def identity_matches(path: Path, raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        return raw == file_identity(path)
    except SweepError:
        return False


def identity_matches_across_rename(path: Path, raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        observed = file_identity(path)
    except SweepError:
        return False
    if raw.keys() != observed.keys():
        return False
    return all(
        raw[key] == value
        for key, value in observed.items()
        if key not in {"changed_seconds", "changed_nanoseconds"}
    )


def is_digest_array(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) == 32
        and all(isinstance(byte, int) and not isinstance(byte, bool) and 0 <= byte <= 255 for byte in value)
    )


def is_root_checkpoint_summary(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "slot",
        "bank_hash",
        "accounts_hash",
        "last_blockhash",
        "capitalization",
        "transaction_count",
        "tick_height",
        "slot_complete",
        "write_count",
        "next_write_version",
    }:
        return False
    return (
        all(
            is_u64(value.get(key))
            for key in (
                "slot",
                "capitalization",
                "transaction_count",
                "tick_height",
                "write_count",
                "next_write_version",
            )
        )
        and value.get("slot_complete") is True
        and all(
            is_digest_array(value.get(key))
            for key in ("bank_hash", "accounts_hash", "last_blockhash")
        )
    )


def root_checkpoint_gate_is_valid(gate: Any, epochs: tuple[int, ...]) -> bool:
    if not isinstance(gate, dict) or set(gate) != {
        "kind",
        "version",
        "verified_manifest_checkpoints",
        "members",
    }:
        return False
    checkpoints = gate.get("verified_manifest_checkpoints")
    members = gate.get("members")
    if (
        gate.get("kind") != "sealed-root-checkpoint-cohort"
        or gate.get("version") != 1
        or not isinstance(checkpoints, list)
        or not checkpoints
        or len(checkpoints) > MAX_RECEIPT_EPOCHS
        or not isinstance(members, list)
        or len(members) != len(epochs)
    ):
        return False
    checkpoint_slots: list[int] = []
    for checkpoint in checkpoints:
        if (
            not isinstance(checkpoint, dict)
            or set(checkpoint) != {"slot", "accounts_hash"}
            or not is_u64(checkpoint.get("slot"))
            or not is_digest_array(checkpoint.get("accounts_hash"))
        ):
            return False
        checkpoint_slots.append(checkpoint["slot"])
    if any(left >= right for left, right in zip(checkpoint_slots, checkpoint_slots[1:])):
        return False
    for expected_epoch, member in zip(epochs, members):
        if (
            not isinstance(member, dict)
            or set(member) != {"epoch", "bootstrap", "terminal"}
            or member.get("epoch") != expected_epoch
            or not is_root_checkpoint_summary(member.get("bootstrap"))
            or not is_root_checkpoint_summary(member.get("terminal"))
            or member["terminal"]["slot"] < member["bootstrap"]["slot"]
        ):
            return False
    return True


def decode_bound_receipt_context(
    data: Mapping[str, Any],
    epochs: tuple[int, ...],
    fingerprint: str,
    source: Path,
) -> tuple[bytes, Mapping[str, Any]] | None:
    if FINGERPRINT_RE.fullmatch(fingerprint) is None:
        return None
    context = data.get("publication_context_base64")
    gate = data.get("root_checkpoint_gate")
    if not isinstance(context, str) or not root_checkpoint_gate_is_valid(gate, epochs):
        return None
    try:
        decoded = base64.b64decode(context, validate=True)
    except ValueError:
        return None
    if (
        not decoded
        or len(decoded) > 4 * 1024 * 1024
        or base64.b64encode(decoded).decode("ascii") != context
    ):
        return None
    try:
        bound = strict_json(decoded, source)
    except SweepError:
        return None
    if (
        not isinstance(bound, dict)
        or set(bound) != {"schema", "manifest_fingerprint", "epochs", "gate"}
        or bound.get("schema") != ROOT_CHECKPOINT_CONTEXT_SCHEMA
        or bound.get("manifest_fingerprint")
        != list(bytes.fromhex(fingerprint.removeprefix("sha256:")))
        or canonical_receipt_epochs(bound.get("epochs")) != epochs
        or bound.get("gate") != gate
    ):
        return None
    assert isinstance(gate, dict)
    return decoded, gate


def discover_committed_receipts(
    destination: Path,
    receipt_root: Path,
    fingerprint: str,
    expected_uid: int,
    *,
    allow_rename_ctime: bool = False,
    expected_destination_identity: DirectoryIdentity | None = None,
) -> dict[Cohort, Path]:
    if expected_destination_identity is None:
        expected_destination_identity = directory_identity(destination)
    candidates: dict[Cohort, list[Path]] = {}
    pattern = "destination-*/evidence/archive-batches/destination-*/batch-*.json"
    for path in receipt_root.glob(pattern):
        try:
            receipt_info = path.lstat()
            if (
                not stat.S_ISREG(receipt_info.st_mode)
                or receipt_info.st_uid != expected_uid
                or receipt_info.st_nlink != 1
                or stat.S_IMODE(receipt_info.st_mode) & 0o077
                or stat.S_IMODE(receipt_info.st_mode) & 0o7000
                or path.resolve(strict=True) != path
            ):
                continue
            data = strict_json(read_regular_nofollow(path, MAX_RECEIPT_BYTES), path)
        except SweepError:
            continue
        if (
            not isinstance(data, dict)
            or set(data)
            != {
                "schema",
                "transaction_id",
                "manifest_fingerprint",
                "destination",
                "epochs",
                "root_checkpoint_gate",
                "publication_context_base64",
                "outcome",
            }
            or data.get("schema") != RECEIPT_SCHEMA
            or data.get("manifest_fingerprint") != fingerprint
            or receipt_destination(data)
            != (destination, expected_destination_identity)
        ):
            continue
        transaction_id = data.get("transaction_id")
        if (
            not isinstance(transaction_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", transaction_id) is None
            or path.name != f"batch-{transaction_id}.json"
        ):
            continue
        epochs = canonical_receipt_epochs(data.get("epochs"))
        outcome = data.get("outcome")
        outcome_members = outcome.get("members") if isinstance(outcome, dict) else None
        if (
            epochs is None
            or not isinstance(outcome, dict)
            or set(outcome) != {"kind", "members"}
            or outcome.get("kind") != "committed"
            or not isinstance(outcome_members, list)
            or len(outcome_members) != len(epochs)
            or any(not isinstance(item, dict) for item in outcome_members)
            or decode_bound_receipt_context(data, epochs, fingerprint, path) is None
        ):
            continue
        members = outcome_members
        assert isinstance(members, list)
        if tuple(item.get("epoch") for item in members) != epochs:
            continue
        archive_identity_matches = (
            identity_matches_across_rename if allow_rename_ctime else identity_matches
        )
        members_are_live = True
        for epoch, member in zip(epochs, members):
            archive = destination / f"epoch-{epoch}.jet"
            checksum = destination / f"epoch-{epoch}.jet.sha256"
            if (
                not isinstance(member, dict)
                or set(member)
                != {
                    "epoch",
                    "archive_path_base64",
                    "manifest_path_base64",
                    "checksum_path_base64",
                    "archive_sha256",
                    "initial_archive",
                    "initial_manifest",
                    "initial_checksum",
                    "committed_archive",
                    "committed_manifest",
                    "committed_checksum",
                }
                or decode_receipt_path(member.get("archive_path_base64")) != archive
                or decode_receipt_path(member.get("checksum_path_base64")) != checksum
                or member.get("manifest_path_base64") is not None
                or member.get("initial_archive") is not None
                or member.get("initial_manifest") is not None
                or member.get("initial_checksum") is not None
                or not archive_identity_matches(
                    archive, member.get("committed_archive")
                )
                or member.get("committed_manifest") is not None
                or not identity_matches(
                    checksum, member.get("committed_checksum")
                )
            ):
                members_are_live = False
                break
            digest = member.get("archive_sha256")
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                members_are_live = False
                break
            try:
                sidecar = read_regular_nofollow(checksum, 512).decode("ascii", "strict")
            except (SweepError, UnicodeDecodeError):
                members_are_live = False
                break
            if sidecar != f"{digest}  epoch-{epoch}.jet\n":
                members_are_live = False
                break
        if not members_are_live:
            continue
        cohort = Cohort(epochs[0], epochs[-1], "")
        candidates.setdefault(cohort, []).append(path)
    ambiguous = {cohort: paths for cohort, paths in candidates.items() if len(paths) != 1}
    if ambiguous:
        labels = ", ".join(cohort.label for cohort in sorted(ambiguous))
        raise SweepError(f"destination {destination} has ambiguous live receipts for {labels}")
    return {cohort: paths[0] for cohort, paths in candidates.items()}


def capture_committed_receipt_evidence(
    destination: Path,
    receipt_root: Path,
    cohort: Cohort,
    fingerprint: str,
    expected_uid: int,
    *,
    allow_rename_ctime: bool,
    expected_destination_identity: DirectoryIdentity,
    rehash_archives: bool,
) -> dict[str, Any]:
    receipts = discover_committed_receipts(
        destination,
        receipt_root,
        fingerprint,
        expected_uid,
        allow_rename_ctime=allow_rename_ctime,
        expected_destination_identity=expected_destination_identity,
    )
    receipt = receipts.get(Cohort(cohort.first_epoch, cohort.last_epoch, ""))
    if receipt is None:
        raise SweepError(f"cohort {cohort.label} has no unique live committed receipt")
    receipt_bytes, receipt_identity = read_regular_nofollow_with_identity(
        receipt, MAX_RECEIPT_BYTES
    )
    data = strict_json(receipt_bytes, receipt)
    if not isinstance(data, dict):
        raise SweepError(f"committed receipt is not an object: {receipt}")
    epochs = canonical_receipt_epochs(data.get("epochs"))
    if epochs != cohort.epochs:
        raise SweepError(f"committed receipt changed cohort while binding: {receipt}")
    context_and_gate = decode_bound_receipt_context(data, epochs, fingerprint, receipt)
    if context_and_gate is None:
        raise SweepError(f"committed receipt has invalid gate context: {receipt}")
    context, gate = context_and_gate
    outcome = data.get("outcome")
    members = outcome.get("members") if isinstance(outcome, dict) else None
    if not isinstance(members, list) or len(members) != len(epochs):
        raise SweepError(f"committed receipt members changed while binding: {receipt}")
    digests: list[dict[str, Any]] = []
    for epoch, member in zip(epochs, members):
        if not isinstance(member, dict):
            raise SweepError(f"committed receipt member {epoch} is malformed")
        archive = destination / f"epoch-{epoch}.jet"
        checksum = destination / f"epoch-{epoch}.jet.sha256"
        archive_before = file_identity(archive)
        checksum_before = file_identity(checksum)
        archive_matches = (
            identity_matches_across_rename
            if allow_rename_ctime
            else identity_matches
        )
        if (
            not archive_matches(archive, member.get("committed_archive"))
            or not identity_matches(checksum, member.get("committed_checksum"))
        ):
            raise SweepError(f"committed receipt identity changed for epoch {epoch}")
        digest = member.get("archive_sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise SweepError(f"committed receipt digest is malformed for epoch {epoch}")
        sidecar = read_regular_nofollow(checksum, 512).decode("ascii", "strict")
        if sidecar != f"{digest}  epoch-{epoch}.jet\n":
            raise SweepError(f"committed receipt checksum changed for epoch {epoch}")
        if rehash_archives and checksum_file(archive) != digest:
            raise SweepError(f"committed receipt archive hash changed for epoch {epoch}")
        if file_identity(archive) != archive_before or file_identity(checksum) != checksum_before:
            raise SweepError(f"committed receipt member changed while hashing epoch {epoch}")
        digests.append({"epoch": epoch, "archive_sha256": digest})
    if (
        not identity_matches(receipt, receipt_identity)
        or read_regular_nofollow(receipt, MAX_RECEIPT_BYTES) != receipt_bytes
    ):
        raise SweepError(f"committed receipt changed while its evidence was captured: {receipt}")
    return {
        "receipt": str(receipt),
        "receipt_identity": receipt_identity,
        "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "destination_identity": expected_destination_identity.as_json(),
        "epochs": list(epochs),
        "publication_context_sha256": hashlib.sha256(context).hexdigest(),
        "root_checkpoint_gate_sha256": hashlib.sha256(canonical_json(gate)).hexdigest(),
        "members": digests,
    }


def receipt_semantics(evidence: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        evidence.get("epochs"),
        evidence.get("publication_context_sha256"),
        evidence.get("root_checkpoint_gate_sha256"),
        evidence.get("members"),
    )


def verify_bound_receipt_file(evidence: Mapping[str, Any]) -> None:
    receipt = Path(str(evidence.get("receipt", "")))
    if (
        not identity_matches(receipt, evidence.get("receipt_identity"))
        or checksum_file(receipt) != evidence.get("receipt_sha256")
        or not identity_matches(receipt, evidence.get("receipt_identity"))
    ):
        raise SweepError(f"bound source receipt changed after importer launch: {receipt}")


def discover_live_receipts(
    lane: Lane,
    fingerprint: str,
    expected_uid: int,
    expected_destination_identity: DirectoryIdentity | None = None,
) -> dict[Cohort, Path]:
    return discover_committed_receipts(
        lane.output,
        lane.private,
        fingerprint,
        expected_uid,
        allow_rename_ctime=True,
        expected_destination_identity=expected_destination_identity,
    )


def public_cohort_complete(
    public_dir: Path,
    public_private_root: Path,
    cohort: Cohort,
    fingerprint: str,
    expected_uid: int,
    expected_gid: int,
    *,
    rehash: bool = False,
    expected_destination_identity: DirectoryIdentity | None = None,
) -> bool:
    namespace_complete = public_archive_namespace_complete(
        public_dir, cohort, expected_uid, expected_gid, rehash=rehash
    )
    receipts = discover_committed_receipts(
        public_dir,
        public_private_root,
        fingerprint,
        expected_uid,
        expected_destination_identity=expected_destination_identity,
    )
    receipt = receipts.get(Cohort(cohort.first_epoch, cohort.last_epoch, ""))
    if namespace_complete and receipt is None:
        raise SweepError(
            f"public cohort {cohort.label} has no identity-bound committed v2 receipt"
        )
    if receipt is not None and not namespace_complete:
        raise SweepError(
            f"public receipt for cohort {cohort.label} does not bind a complete namespace"
        )
    return namespace_complete


def validate_real_directory(
    path: Path,
    *,
    uid: int | None = None,
    gid: int | None = None,
    mode: int | None = None,
    owner_only: bool = False,
) -> Path:
    try:
        info = path.lstat()
        canonical = path.resolve(strict=True)
    except OSError as error:
        raise SweepError(f"failed to inspect directory {path}: {error}") from error
    if canonical != path or not stat.S_ISDIR(info.st_mode):
        raise SweepError(f"directory is not an absolute canonical real directory: {path}")
    if uid is not None and info.st_uid != uid:
        raise SweepError(f"directory has unexpected owner: {path}")
    if gid is not None and info.st_gid != gid:
        raise SweepError(f"directory has unexpected group: {path}")
    if mode is not None and stat.S_IMODE(info.st_mode) != mode:
        raise SweepError(f"directory has unexpected mode: {path}")
    if owner_only and (stat.S_IMODE(info.st_mode) & 0o077 or stat.S_IMODE(info.st_mode) & 0o7000):
        raise SweepError(f"directory must be owner-only with no special bits: {path}")
    return canonical


def validate_lane(
    lane: Lane, sol_uid: int, horizon_gid: int, public_device: int
) -> Lane:
    root = validate_real_directory(lane.root, uid=sol_uid, mode=0o700)
    output = validate_real_directory(
        root / "output", uid=sol_uid, gid=horizon_gid, mode=0o700
    )
    validate_real_directory(root / "private", uid=sol_uid, mode=0o700)
    validate_real_directory(root / "config", uid=sol_uid, mode=0o700)
    validate_real_directory(root / "config" / "gcloud", uid=sol_uid, mode=0o700)
    if output.stat().st_dev != public_device:
        raise SweepError(f"lane {lane.name} output and public destination are on different filesystems")
    genesis = output / "genesis.tar.bz2"
    info = genesis.lstat()
    if (
        genesis.resolve(strict=True) != genesis
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != sol_uid
        or info.st_gid != horizon_gid
        or stat.S_IMODE(info.st_mode) != 0o600
        or checksum_file(genesis) != CANONICAL_GENESIS_SHA256
    ):
        raise SweepError(f"lane {lane.name} lacks a safe canonical genesis archive")
    return Lane(lane.name, root)


def require_root_controlled_ancestry(path: Path, description: str) -> None:
    current = path
    while True:
        info = current.lstat()
        if (
            current.resolve(strict=True) != current
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != 0
            or stat.S_IMODE(info.st_mode) & 0o022
        ):
            raise SweepError(
                f"{description} ancestry must be canonical, root-owned, and nonwritable: {current}"
            )
        if current == current.parent:
            return
        current = current.parent


def verify_deployment(
    deploy: Path,
    manifest: Path,
    cohorts: Sequence[Cohort],
) -> Path:
    deploy = validate_real_directory(deploy)
    if deploy.stat().st_uid != 0 or stat.S_IMODE(deploy.stat().st_mode) & 0o022:
        raise SweepError("deployment must be root-owned and not group/world writable")
    require_root_controlled_ancestry(deploy.parent, "deployment")
    sums_path = deploy / "SHA256SUMS"
    sums_text = read_regular_nofollow(sums_path, 64 * 1024).decode("ascii", "strict")
    sums: dict[str, str] = {}
    for line in sums_text.splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9._-]+)", line)
        if match is None or match.group(2) in sums:
            raise SweepError("deployment SHA256SUMS is noncanonical")
        sums[match.group(2)] = match.group(1)
    required = {"jetstreamer-node", manifest.name}
    required.update(RUNTIME_WORKERS[cohort.runtime][1] for cohort in cohorts)
    for name in sorted(required):
        path = deploy / name
        info = path.lstat()
        if (
            path.resolve(strict=True) != path
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) & 0o022
            or stat.S_IMODE(info.st_mode) & 0o7000
        ):
            raise SweepError(
                f"deployment member must be a singly linked root-owned nonwritable file: {name}"
            )
        if name not in sums or checksum_file(path) != sums[name]:
            raise SweepError(f"deployment checksum mismatch for {name}")
    if manifest.parent != deploy:
        raise SweepError("sealed manifest must be a direct deployment member")
    return deploy


def verify_controller_source(path: Path, expected_sha256: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise SweepError("--controller-sha256 must contain 64 lowercase hex digits")
    info = path.lstat()
    if (
        path.resolve(strict=True) != path
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != 0
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) & 0o022
        or stat.S_IMODE(info.st_mode) & 0o7000
    ):
        raise SweepError(
            "--execute requires a singly linked root-owned nonwritable controller source"
        )
    if checksum_file(path) != expected_sha256:
        raise SweepError("controller source SHA-256 does not match --controller-sha256")
    require_root_controlled_ancestry(path.parent, "controller source")


def atomic_write_state(path: Path, state: Mapping[str, Any]) -> None:
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    )
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    try:
        try:
            payload = json.dumps(state, sort_keys=True, indent=2).encode("utf-8") + b"\n"
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise SweepError(f"short write to controller state {temporary}")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def controller_configuration_sha256(
    args: argparse.Namespace,
    cohorts: Sequence[Cohort],
    adopted_cohorts: Sequence[Cohort],
    lanes: Sequence[Lane],
) -> str:
    configuration = {
        "schema": STATE_SCHEMA,
        "manifest_fingerprint": args.manifest_fingerprint,
        "manifest": str(args.manifest),
        "deploy_dir": str(args.deploy_dir),
        "deploy_sums_sha256": checksum_file(args.deploy_dir / "SHA256SUMS"),
        "public_dir": str(args.public_dir),
        "public_dir_identity": directory_identity(args.public_dir).as_json(),
        "public_private_root": str(args.public_private_root),
        "public_private_root_identity": directory_identity(
            args.public_private_root
        ).as_json(),
        "state_dir": str(args.state_dir),
        "state_dir_identity": directory_identity(args.state_dir).as_json(),
        "scheduled_cohorts": [dataclasses.asdict(cohort) for cohort in cohorts],
        "adopted_cohorts": [dataclasses.asdict(cohort) for cohort in adopted_cohorts],
        "lanes": [
            {
                "name": lane.name,
                "root": str(lane.root),
                "identities": {
                    "root": directory_identity(lane.root).as_json(),
                    "output": directory_identity(lane.output).as_json(),
                    "private": directory_identity(lane.private).as_json(),
                    "config": directory_identity(lane.root / "config").as_json(),
                    "gcloud": directory_identity(lane.cloud_config).as_json(),
                },
            }
            for lane in sorted(lanes, key=lambda item: item.name)
        ],
        "initial_concurrency": args.initial_concurrency,
        "target_concurrency": args.target_concurrency,
        "max_concurrency": args.max_concurrency,
        "settle_seconds": args.settle_seconds,
        "poll_seconds": args.poll_seconds,
        "memory_high_gib": args.memory_high_gib,
        "memory_max_gib": args.memory_max_gib,
        "memory_reserve_gib": args.memory_reserve_gib,
        "protected_memory_gib": args.protected_memory_gib,
        "cpus_per_lane": args.cpus_per_lane,
        "cpu_quota_percent": args.cpu_quota_percent,
        "gcloud_account": args.gcloud_account,
        "gcloud_project": args.gcloud_project,
        "controller_id": args.controller_id,
        "controller_sha256": args.controller_sha256,
    }
    return hashlib.sha256(canonical_json(configuration)).hexdigest()


def load_state(
    path: Path,
    fingerprint: str,
    public_dir: Path,
    configuration_sha256: str,
    initial_concurrency: int,
) -> dict[str, Any]:
    try:
        path.lstat()
    except FileNotFoundError:
        return {
            "schema": STATE_SCHEMA,
            "manifest_fingerprint": fingerprint,
            "public_dir": str(public_dir),
            "configuration_sha256": configuration_sha256,
            "ramp_started_unix": time.time(),
            "last_ramp_unix": time.time(),
            "ramp_limit": initial_concurrency,
            "sequence": 0,
            "attempts": {},
            "assignments": {},
            "completed": {},
            "import_owner": None,
        }
    data = strict_json(read_regular_nofollow(path, 1024 * 1024), path)
    if (
        not isinstance(data, dict)
        or data.get("schema") != STATE_SCHEMA
        or data.get("manifest_fingerprint") != fingerprint
        or data.get("public_dir") != str(public_dir)
        or data.get("configuration_sha256") != configuration_sha256
        or not isinstance(data.get("assignments"), dict)
        or not isinstance(data.get("attempts"), dict)
        or not isinstance(data.get("completed"), dict)
        or "import_owner" not in data
        or not isinstance(data.get("sequence"), int)
        or isinstance(data.get("sequence"), bool)
        or data["sequence"] < 0
    ):
        raise SweepError("controller state does not match this sealed sweep")
    return data


def receipt_evidence_is_valid(value: Any, cohort: Cohort) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "receipt",
        "receipt_identity",
        "receipt_sha256",
        "destination_identity",
        "epochs",
        "publication_context_sha256",
        "root_checkpoint_gate_sha256",
        "members",
    }:
        return False
    receipt = value.get("receipt")
    identity = value.get("receipt_identity")
    destination_identity = value.get("destination_identity")
    members = value.get("members")
    digest_fields = (
        value.get("receipt_sha256"),
        value.get("publication_context_sha256"),
        value.get("root_checkpoint_gate_sha256"),
    )
    return (
        isinstance(receipt, str)
        and Path(receipt).is_absolute()
        and isinstance(identity, dict)
        and set(identity)
        == {
            "device",
            "inode",
            "mode",
            "uid",
            "gid",
            "link_count",
            "length",
            "modified_seconds",
            "modified_nanoseconds",
            "changed_seconds",
            "changed_nanoseconds",
        }
        and all(isinstance(item, int) and not isinstance(item, bool) for item in identity.values())
        and isinstance(destination_identity, dict)
        and set(destination_identity) == {"device", "inode"}
        and all(is_u64(destination_identity.get(key)) for key in ("device", "inode"))
        and value.get("epochs") == list(cohort.epochs)
        and all(
            isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            for digest in digest_fields
        )
        and isinstance(members, list)
        and members
        == [
            {
                "epoch": epoch,
                "archive_sha256": members[index].get("archive_sha256")
                if index < len(members) and isinstance(members[index], dict)
                else None,
            }
            for index, epoch in enumerate(cohort.epochs)
        ]
        and all(
            isinstance(member.get("archive_sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", member["archive_sha256"]) is not None
            for member in members
        )
    )


def validate_state_assignments(
    state: Mapping[str, Any],
    lanes: Mapping[str, Lane],
    cohorts: Mapping[tuple[int, int], Cohort],
    scheduled_bounds: set[tuple[int, int]],
) -> None:
    valid_phases = {
        "launching",
        "producer",
        "staged",
        "importing",
        "failed",
        "import_failed",
    }
    assignments = state.get("assignments")
    attempts = state.get("attempts", {})
    completed = state.get("completed")
    import_owner = state.get("import_owner")
    if (
        not isinstance(assignments, dict)
        or not isinstance(attempts, dict)
        or not isinstance(completed, dict)
        or (import_owner is not None and not isinstance(import_owner, str))
    ):
        raise SweepError("controller state assignments, attempts, or completion data are malformed")
    ramp_limit = state.get("ramp_limit")
    last_ramp = state.get("last_ramp_unix")
    ramp_started = state.get("ramp_started_unix")
    if (
        not isinstance(ramp_limit, int)
        or isinstance(ramp_limit, bool)
        or ramp_limit <= 0
        or ramp_limit > len(lanes)
        or not isinstance(last_ramp, (int, float))
        or isinstance(last_ramp, bool)
        or not math.isfinite(last_ramp)
        or last_ramp <= 0
        or not isinstance(ramp_started, (int, float))
        or isinstance(ramp_started, bool)
        or not math.isfinite(ramp_started)
        or ramp_started <= 0
    ):
        raise SweepError("controller state has invalid ramp metadata")
    seen: set[tuple[int, int]] = set()
    for lane_name, raw in assignments.items():
        if lane_name not in lanes or not isinstance(raw, dict):
            raise SweepError("controller state names an unknown lane or malformed assignment")
        first_epoch = raw.get("first_epoch")
        last_epoch = raw.get("last_epoch")
        if (
            not isinstance(first_epoch, int)
            or isinstance(first_epoch, bool)
            or not isinstance(last_epoch, int)
            or isinstance(last_epoch, bool)
        ):
            raise SweepError("controller state assignment has invalid epoch bounds")
        cohort = assignment_cohort(raw, cohorts)
        bounds = (cohort.first_epoch, cohort.last_epoch)
        if bounds in seen or raw.get("phase") not in valid_phases:
            raise SweepError("controller state duplicates a cohort or has an invalid phase")
        if not isinstance(raw.get("adopt_only"), bool):
            raise SweepError("controller state assignment lacks an adopt-only boolean")
        if raw["adopt_only"] != (bounds not in scheduled_bounds):
            raise SweepError("controller state assignment has inconsistent adopt-only state")
        seen.add(bounds)
        unit = raw.get("unit")
        if unit is not None and (
            not isinstance(unit, str) or SYSTEMD_UNIT_RE.fullmatch(unit) is None
        ):
            raise SweepError("controller state has an invalid producer unit")
        import_unit = raw.get("import_unit")
        if import_unit is not None and (
            not isinstance(import_unit, str)
            or SYSTEMD_UNIT_RE.fullmatch(import_unit) is None
        ):
            raise SweepError("controller state has an invalid importer unit")
        if raw.get("phase") in {"staged", "importing", "import_failed"}:
            receipt = raw.get("receipt")
            if not isinstance(receipt, str) or not Path(receipt).is_absolute():
                raise SweepError("controller state has an invalid source receipt path")
        source_evidence = raw.get("source_evidence")
        if source_evidence is not None and not receipt_evidence_is_valid(
            source_evidence, cohort
        ):
            raise SweepError("controller state has invalid source receipt evidence")
        if source_evidence is not None:
            try:
                Path(source_evidence["receipt"]).relative_to(lanes[lane_name].private)
            except ValueError as error:
                raise SweepError(
                    "controller state source receipt escaped its lane private root"
                ) from error
            if (
                source_evidence["receipt"] != raw.get("receipt")
                or source_evidence["destination_identity"]
                != directory_identity(lanes[lane_name].output).as_json()
            ):
                raise SweepError(
                    "controller state source receipt evidence changed lane binding"
                )
        if raw.get("phase") == "importing" and source_evidence is None:
            raise SweepError("controller state importer lacks bound source receipt evidence")
        if raw.get("phase") in {"launching", "producer"} and unit is None:
            raise SweepError("controller state producer assignment lacks a unit")
        if raw.get("phase") == "importing" and import_unit is None:
            raise SweepError("controller state importing assignment lacks an importer unit")
        for key in (
            "pid",
            "pid_start_time",
            "attempt",
            "import_recovery_attempts",
            "source_recovery_attempts",
        ):
            value = raw.get(key)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise SweepError(f"controller state assignment has invalid {key}")
        if (raw.get("pid") is None) != (raw.get("pid_start_time") is None):
            raise SweepError("controller state has an incomplete process identity")
        source_recovery = raw.get("source_recovery")
        if source_recovery is not None and not isinstance(source_recovery, bool):
            raise SweepError("controller state has invalid source recovery state")
    if any(
        not isinstance(key, str)
        or key not in {cohort.label for cohort in cohorts.values()}
        or not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        for key, value in attempts.items()
    ):
        raise SweepError("controller state has invalid attempt counters")
    if import_owner is not None:
        owner = assignments.get(import_owner)
        if (
            not isinstance(owner, dict)
            or owner.get("phase") not in {"staged", "importing", "import_failed"}
        ):
            raise SweepError("controller state import owner is not a recoverable assignment")
    importing_lanes = [
        lane_name
        for lane_name, raw in assignments.items()
        if isinstance(raw, dict) and raw.get("phase") == "importing"
    ]
    if len(importing_lanes) > 1 or (
        importing_lanes and importing_lanes[0] != import_owner
    ):
        raise SweepError("controller state does not serialize importer ownership")
    for label, record in completed.items():
        if not isinstance(label, str) or not isinstance(record, dict):
            raise SweepError("controller state has malformed completion attestation")
        bounds = (record.get("first_epoch"), record.get("last_epoch"))
        if any(not isinstance(value, int) or isinstance(value, bool) for value in bounds):
            raise SweepError("controller state completion has invalid epoch bounds")
        cohort = cohorts.get(bounds)
        recorded_unix = record.get("recorded_unix")
        if (
            cohort is None
            or cohort.label != label
            or bounds in seen
            or not receipt_evidence_is_valid(
                {key: value for key, value in record.items() if key not in {
                    "first_epoch", "last_epoch", "recorded_unix"
                }},
                cohort,
            )
            or not isinstance(recorded_unix, (int, float))
            or isinstance(recorded_unix, bool)
            or not math.isfinite(recorded_unix)
            or recorded_unix <= 0
        ):
            raise SweepError("controller state has invalid completion attestation")


def public_output_files(lane: Lane) -> tuple[Path, ...]:
    return tuple(sorted(lane.output.glob("epoch-*.jet*")))


def assignment_cohort(raw: Mapping[str, Any], by_bounds: Mapping[tuple[int, int], Cohort]) -> Cohort:
    bounds = (raw.get("first_epoch"), raw.get("last_epoch"))
    if bounds not in by_bounds:
        raise SweepError(f"state contains an unknown cohort {bounds}")
    return by_bounds[bounds]


def assignment_process_alive(raw: Mapping[str, Any]) -> bool:
    pid = raw.get("pid")
    start_time = raw.get("pid_start_time")
    return isinstance(pid, int) and isinstance(start_time, int) and process_is_same(pid, start_time)


class Controller:
    def __init__(
        self,
        args: argparse.Namespace,
        cohorts: tuple[Cohort, ...],
        adopted_cohorts: tuple[Cohort, ...],
        lanes: tuple[Lane, ...],
    ):
        self.args = args
        self.cohorts = cohorts
        self.adopted_cohorts = adopted_cohorts
        self.managed_cohorts = tuple(
            sorted((*cohorts, *adopted_cohorts), key=lambda item: item.first_epoch)
        )
        self.scheduled_bounds = {
            (item.first_epoch, item.last_epoch) for item in cohorts
        }
        self.by_bounds = {
            (item.first_epoch, item.last_epoch): item
            for item in self.managed_cohorts
        }
        self.lanes = {lane.name: lane for lane in lanes}
        self.sol_uid = pwd.getpwnam("sol").pw_uid
        self.horizon_gid = grp.getgrnam("horizon").gr_gid
        self.node = args.deploy_dir / "jetstreamer-node"
        self.stopping = False
        binding_paths = {
            args.public_dir,
            args.public_private_root,
            args.state_dir,
            *(
                path
                for lane in lanes
                for path in (
                    lane.root,
                    lane.output,
                    lane.private,
                    lane.root / "config",
                    lane.cloud_config,
                )
            ),
        }
        self.directory_bindings: dict[Path, BoundDirectory] = {}
        try:
            for path in sorted(binding_paths):
                self.directory_bindings[path] = BoundDirectory(path)
        except BaseException:
            self.close()
            raise
        self.revalidate_operational_directories()
        self.public_destination_identity = self.directory_bindings[
            args.public_dir
        ].identity
        self.lane_output_identities = {
            lane.name: self.directory_bindings[lane.output].identity for lane in lanes
        }
        self.state_path = args.state_dir / "state.json"
        self.configuration_sha256 = controller_configuration_sha256(
            args, cohorts, adopted_cohorts, lanes
        )
        self.state = load_state(
            self.state_path,
            args.manifest_fingerprint,
            args.public_dir,
            self.configuration_sha256,
            args.initial_concurrency,
        )
        validate_state_assignments(
            self.state, self.lanes, self.by_bounds, self.scheduled_bounds
        )
        self.revalidate_operational_directories()
        if args.retry_failed:
            for lane_name, raw in list(self.state["assignments"].items()):
                if not isinstance(raw, dict):
                    continue
                if raw.get("phase") == "failed":
                    cohort = assignment_cohort(raw, self.by_bounds)
                    self.state["attempts"][cohort.label] = 0
                    del self.state["assignments"][lane_name]
                elif raw.get("phase") == "import_failed":
                    raw["phase"] = "staged"
                    raw["import_recovery_attempts"] = 0
                    for key in (
                        "failure",
                        "import_result",
                        "import_status",
                    ):
                        raw.pop(key, None)
            self.save()

    def revalidate_operational_directories(self) -> None:
        for binding in getattr(self, "directory_bindings", {}).values():
            binding.revalidate()

    def close(self) -> None:
        for binding in getattr(self, "directory_bindings", {}).values():
            binding.close()
        self.directory_bindings = {}

    def public_identity(self) -> DirectoryIdentity:
        if hasattr(self, "public_destination_identity"):
            return self.public_destination_identity
        return directory_identity(self.args.public_dir)

    def lane_output_identity(self, lane_name: str) -> DirectoryIdentity:
        identities = getattr(self, "lane_output_identities", {})
        if lane_name in identities:
            return identities[lane_name]
        return directory_identity(self.lanes[lane_name].output)

    def save(self) -> None:
        atomic_write_state(self.state_path, self.state)

    def public_complete(self, cohort: Cohort, *, rehash: bool = False) -> bool:
        self.revalidate_operational_directories()
        complete = public_cohort_complete(
            self.args.public_dir,
            self.args.public_private_root,
            cohort,
            self.args.manifest_fingerprint,
            self.sol_uid,
            self.horizon_gid,
            rehash=rehash,
            expected_destination_identity=self.public_identity(),
        )
        self.revalidate_operational_directories()
        return complete

    def public_state_after_import(self, cohort: Cohort) -> tuple[bool, bool]:
        self.revalidate_operational_directories()
        recovery_pending = public_recovery_marker_present(
            self.args.public_dir, self.sol_uid
        )
        try:
            complete = self.public_complete(cohort, rehash=False)
        except SweepError:
            if not recovery_pending:
                raise
            complete = False
        self.revalidate_operational_directories()
        return complete and not recovery_pending, recovery_pending

    def capture_completion_attestation(
        self, cohort: Cohort, source_evidence: Mapping[str, Any]
    ) -> dict[str, Any]:
        self.revalidate_operational_directories()
        if public_recovery_marker_present(self.args.public_dir, self.sol_uid):
            raise SweepError("cannot attest a public cohort while a batch marker remains")
        if not self.public_complete(cohort, rehash=False):
            raise SweepError(f"cannot attest incomplete public cohort {cohort.label}")
        public_evidence = capture_committed_receipt_evidence(
            self.args.public_dir,
            self.args.public_private_root,
            cohort,
            self.args.manifest_fingerprint,
            self.sol_uid,
            allow_rename_ctime=False,
            expected_destination_identity=self.public_identity(),
            rehash_archives=True,
        )
        receipt = Path(public_evidence["receipt"])
        receipt.relative_to(self.args.public_private_root)
        verify_bound_receipt_file(source_evidence)
        if receipt_semantics(public_evidence) != receipt_semantics(source_evidence):
            raise SweepError(
                f"public receipt evidence differs from the bound source cohort {cohort.label}"
            )
        self.revalidate_operational_directories()
        return {
            "first_epoch": cohort.first_epoch,
            "last_epoch": cohort.last_epoch,
            **public_evidence,
            "recorded_unix": time.time(),
        }

    def trusted_public_complete(self, cohort: Cohort, *, final: bool = False) -> bool:
        self.revalidate_operational_directories()
        record = self.state["completed"].get(cohort.label)
        if record is None:
            namespace_complete = public_archive_namespace_complete(
                self.args.public_dir,
                cohort,
                self.sol_uid,
                self.horizon_gid,
                rehash=False,
            )
            if namespace_complete:
                raise SweepError(
                    f"public cohort {cohort.label} exists without a root controller attestation"
                )
            self.revalidate_operational_directories()
            return False
        if not isinstance(record, dict):
            raise SweepError(f"completion attestation for cohort {cohort.label} is malformed")
        receipt = Path(record.get("receipt", ""))
        try:
            receipt.relative_to(self.args.public_private_root)
        except ValueError as error:
            raise SweepError(
                f"completion receipt for cohort {cohort.label} escaped its private root"
            ) from error
        if (
            record.get("first_epoch") != cohort.first_epoch
            or record.get("last_epoch") != cohort.last_epoch
            or record.get("destination_identity") != self.public_identity().as_json()
            or not identity_matches(receipt, record.get("receipt_identity"))
            or checksum_file(receipt) != record.get("receipt_sha256")
        ):
            raise SweepError(f"completion attestation changed for cohort {cohort.label}")
        receipts = discover_committed_receipts(
            self.args.public_dir,
            self.args.public_private_root,
            self.args.manifest_fingerprint,
            self.sol_uid,
            expected_destination_identity=self.public_identity(),
        )
        if receipts.get(Cohort(cohort.first_epoch, cohort.last_epoch, "")) != receipt:
            raise SweepError(f"completion receipt no longer binds cohort {cohort.label}")
        if not self.public_complete(cohort, rehash=final):
            raise SweepError(f"attested public cohort {cohort.label} is incomplete")
        self.revalidate_operational_directories()
        return True

    def finalize_import(
        self,
        lane_name: str,
        cohort: Cohort,
        source_evidence: Mapping[str, Any],
    ) -> None:
        self.state["completed"][cohort.label] = self.capture_completion_attestation(
            cohort, source_evidence
        )
        del self.state["assignments"][lane_name]
        if self.state.get("import_owner") == lane_name:
            self.state["import_owner"] = None
        self.save()

    def require_live_source_receipt(
        self, lane_name: str, cohort: Cohort, receipt: Path
    ) -> Path:
        try:
            expected = receipt.resolve(strict=True)
            info = receipt.lstat()
            expected.relative_to(self.lanes[lane_name].private)
        except (OSError, ValueError) as error:
            raise SweepError(
                f"source receipt for cohort {cohort.label} is unavailable: {error}"
            ) from error
        if (
            expected != receipt
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != self.sol_uid
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) & 0o077
            or stat.S_IMODE(info.st_mode) & 0o7000
        ):
            raise SweepError(f"source receipt for cohort {cohort.label} is unsafe")
        if public_recovery_marker_present(self.args.public_dir, self.sol_uid):
            # A committed-but-unacknowledged public transaction may already
            # have moved the source archive identities. The importer verifies
            # the retained receipt and the durable destination journal.
            return expected
        receipts = discover_live_receipts(
            self.lanes[lane_name],
            self.args.manifest_fingerprint,
            self.sol_uid,
            self.lane_output_identity(lane_name),
        )
        live = receipts.get(Cohort(cohort.first_epoch, cohort.last_epoch, ""))
        if live is None or live != expected:
            raise SweepError(
                f"source receipt for cohort {cohort.label} is no longer the one "
                f"bound to {self.lanes[lane_name].output}"
            )
        return live

    def require_hardened_process(
        self, process: ProducerProcess, cohort: Cohort, lane: Lane
    ) -> None:
        expected_argv = (
            str(self.node),
            cohort.label,
            str(lane.output),
            "--verify",
            "--root-checkpoint-cohort",
            f"--cohort-manifest={self.args.manifest}",
            f"--cohort-manifest-fingerprint={self.args.manifest_fingerprint}",
        )
        if process.argv != expected_argv:
            raise SweepError(
                f"matching producer PID {process.pid} for cohort {cohort.label} "
                "has unexpected arguments"
            )
        if process.unit is None or not unit_is_hardened_for_adoption(
            process.unit,
            process,
            cohort,
            lane,
            self.args.deploy_dir,
            self.args.memory_high_gib,
            self.args.memory_max_gib,
            self.args.cpu_quota_percent,
            self.args.gcloud_account,
            self.args.gcloud_project,
        ):
            raise SweepError(
                f"matching producer PID {process.pid} for cohort {cohort.label} "
                "is not owned by an approved hardened systemd unit"
            )

    def reconcile_active_assignment(
        self, lane_name: str, raw: dict[str, Any], cohort: Cohort
    ) -> bool:
        unit = raw.get("unit")
        if not isinstance(unit, str):
            return False
        status = unit_status(unit)
        if not status.active:
            return False
        candidates = [
            process
            for process in discover_producers(
                expected_node=self.node, expected_uid=self.sol_uid
            )
            if process.unit == unit
        ]
        if len(candidates) != 1:
            if not unit_status(unit).active:
                return False
            raise SweepError(
                f"active producer unit {unit} does not contain exactly one sealed node"
            )
        process = candidates[0]
        lane = self.lanes[lane_name]
        if (
            process.cohort.first_epoch != cohort.first_epoch
            or process.cohort.last_epoch != cohort.last_epoch
            or process.output != lane.output
        ):
            raise SweepError(f"active producer unit {unit} changed its cohort or lane claim")
        try:
            self.require_hardened_process(process, cohort, lane)
        except SweepError:
            if not unit_status(unit).active:
                return False
            raise
        stored_pid = raw.get("pid")
        stored_start = raw.get("pid_start_time")
        if stored_pid is not None and (
            stored_pid != process.pid or stored_start != process.start_time
        ):
            raise SweepError(f"active producer unit {unit} changed process identity")
        if stored_pid is None:
            raw["pid"] = process.pid
            raw["pid_start_time"] = process.start_time
            self.save()
        return True

    def adopt_live_producers(self) -> None:
        assigned = self.state["assignments"]
        changed = False
        assigned_bounds = {
            (raw["first_epoch"], raw["last_epoch"])
            for raw in assigned.values()
            if isinstance(raw, dict)
        }
        lane_by_output = {lane.output: lane for lane in self.lanes.values()}
        for process in discover_producers(expected_node=self.node, expected_uid=self.sol_uid):
            bounds = (process.cohort.first_epoch, process.cohort.last_epoch)
            lane = lane_by_output.get(process.output)
            cohort = self.by_bounds.get(bounds)
            if (
                lane is None
                or cohort is None
                or bounds in assigned_bounds
                or process.manifest != self.args.manifest
                or process.fingerprint != self.args.manifest_fingerprint
            ):
                continue
            if lane.name in assigned:
                raise SweepError(f"two producers claim lane {lane.name}")
            self.require_hardened_process(process, cohort, lane)
            assigned[lane.name] = {
                "phase": "producer",
                "first_epoch": cohort.first_epoch,
                "last_epoch": cohort.last_epoch,
                "pid": process.pid,
                "pid_start_time": process.start_time,
                "unit": process.unit,
                "adopt_only": bounds not in self.scheduled_bounds,
            }
            self.state["attempts"].setdefault(cohort.label, 1)
            assigned_bounds.add(bounds)
            print(f"adopted live producer PID {process.pid} for cohort {cohort.label} in {lane.name}")
            self.save()
            changed = True
        if changed:
            self.save()

    def adopt_receipts(self) -> None:
        assigned = self.state["assignments"]
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        assigned_bounds = {
            (raw["first_epoch"], raw["last_epoch"])
            for raw in assigned.values()
            if isinstance(raw, dict)
        }
        for lane in self.lanes.values():
            if lane.name in assigned or any(claim.output == lane.output for claim in claims):
                continue
            receipts = discover_live_receipts(
                lane,
                self.args.manifest_fingerprint,
                self.sol_uid,
                self.lane_output_identity(lane.name),
            )
            matched = [
                (self.by_bounds[(raw.first_epoch, raw.last_epoch)], path)
                for raw, path in receipts.items()
                if (raw.first_epoch, raw.last_epoch) in self.by_bounds
                and (raw.first_epoch, raw.last_epoch) not in assigned_bounds
                and not any(claim.overlaps(raw) for claim in claims)
            ]
            if len(matched) > 1:
                raise SweepError(f"lane {lane.name} contains more than one live sealed cohort")
            if matched:
                cohort, receipt = matched[0]
                assigned[lane.name] = {
                    "phase": "staged",
                    "first_epoch": cohort.first_epoch,
                    "last_epoch": cohort.last_epoch,
                    "receipt": str(receipt),
                    "unit": None,
                    "adopt_only": (
                        cohort.first_epoch,
                        cohort.last_epoch,
                    )
                    not in self.scheduled_bounds,
                }
                assigned_bounds.add((cohort.first_epoch, cohort.last_epoch))
                print(f"adopted staged cohort {cohort.label} from {lane.name}")
                self.save()

    def receipt_location_for_cohort(
        self, cohort: Cohort
    ) -> tuple[Lane, Path] | None:
        matches: list[tuple[Lane, Path]] = []
        key = Cohort(cohort.first_epoch, cohort.last_epoch, "")
        for lane in self.lanes.values():
            receipt = discover_live_receipts(
                lane,
                self.args.manifest_fingerprint,
                self.sol_uid,
                self.lane_output_identity(lane.name),
            ).get(key)
            if receipt is not None:
                matches.append((lane, receipt))
        if len(matches) > 1:
            raise SweepError(
                f"more than one lane contains a live receipt for cohort {cohort.label}"
            )
        return matches[0] if matches else None

    def adopt_receipt_instead_of_launch(
        self,
        lane_name: str,
        raw: dict[str, Any],
        cohort: Cohort,
    ) -> bool:
        found = self.receipt_location_for_cohort(cohort)
        if found is None:
            return False
        lane, receipt = found
        assigned = self.state["assignments"]
        if lane.name != lane_name:
            if lane.name in assigned:
                raise SweepError(
                    f"receipt for cohort {cohort.label} appeared in occupied {lane.name}"
                )
            if assigned.get(lane_name) is not raw:
                raise SweepError("launch assignment changed during receipt handoff")
            del assigned[lane_name]
            assigned[lane.name] = raw
        raw.update(
            {
                "phase": "staged",
                "receipt": str(receipt),
                "unit": None,
                "adopt_only": (
                    cohort.first_epoch,
                    cohort.last_epoch,
                )
                not in self.scheduled_bounds,
            }
        )
        for key in ("source_recovery", "source_recovery_attempts", "pid", "pid_start_time"):
            raw.pop(key, None)
        self.save()
        print(
            f"adopted receipt for cohort {cohort.label} from {lane.name} before launch",
            flush=True,
        )
        return True

    def require_no_orphan_lane_markers(self) -> None:
        assigned = self.state["assignments"]
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        suspect = [
            lane
            for lane in self.lanes.values()
            if lane.name not in assigned
            and not any(claim.output == lane.output for claim in claims)
            and public_recovery_marker_present(lane.output, self.sol_uid)
        ]
        if not suspect:
            return
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        orphaned = [
            lane.name
            for lane in suspect
            if not any(claim.output == lane.output for claim in claims)
            and public_recovery_marker_present(lane.output, self.sol_uid)
        ]
        if orphaned:
            raise SweepError(
                "unassigned lane publication marker requires preserved operator recovery: "
                + ", ".join(sorted(orphaned))
            )

    def launch_producer(
        self,
        lane_name: str,
        raw: dict[str, Any],
        cohort: Cohort,
        *,
        source_recovery: bool,
    ) -> bool:
        lane = self.lanes[lane_name]
        self.revalidate_operational_directories()
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        if any(
            claim.overlaps(cohort) or claim.output == lane.output for claim in claims
        ):
            raise SweepError(
                f"cannot launch cohort {cohort.label}; a live process already claims its epoch or lane"
            )
        if not source_recovery and self.adopt_receipt_instead_of_launch(
            lane_name, raw, cohort
        ):
            return False
        sequence = int(self.state.get("sequence", 0)) + 1
        attempts = int(self.state["attempts"].get(cohort.label, 0)) + 1
        if source_recovery:
            recovery_attempts = int(raw.get("source_recovery_attempts", 0)) + 1
            if recovery_attempts > 3:
                raw["phase"] = "failed"
                raw["failure"] = "source publication recovery exceeded three passes"
                self.save()
                return False
            unit_kind = "source-recovery"
        else:
            unit_kind = "sweep"
        unit = (
            f"jetstreamer-root-{unit_kind}-{self.args.controller_id}-"
            f"e{cohort.first_epoch}-{cohort.last_epoch}-{lane.name}-{sequence}.service"
        )
        require_unused_unit_name(unit)
        self.state["sequence"] = sequence
        if source_recovery:
            raw["source_recovery_attempts"] = recovery_attempts
        else:
            self.state["attempts"][cohort.label] = attempts
            raw["attempt"] = attempts
            raw.pop("source_recovery_attempts", None)
        raw["phase"] = "launching"
        raw["unit"] = unit
        raw["source_recovery"] = source_recovery
        raw.pop("pid", None)
        raw.pop("pid_start_time", None)
        self.save()
        verify_deployment(self.args.deploy_dir, self.args.manifest, (cohort,))
        command = build_producer_command(
            cohort=cohort,
            lane=lane,
            deploy=self.args.deploy_dir,
            manifest=self.args.manifest,
            fingerprint=self.args.manifest_fingerprint,
            unit=unit,
            account=self.args.gcloud_account,
            project=self.args.gcloud_project,
            memory_high_gib=self.args.memory_high_gib,
            memory_max_gib=self.args.memory_max_gib,
            cpu_quota_percent=self.args.cpu_quota_percent,
        )
        self.revalidate_operational_directories()
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            status = unit_status(unit)
            if status.exists:
                raw["launch_warning"] = result.stderr.strip() or result.stdout.strip()
                self.save()
                print(
                    f"systemd-run reported an error for producer {unit}, but the "
                    "unit exists; retaining it for authoritative reconciliation",
                    file=sys.stderr,
                    flush=True,
                )
                return True
            raw["phase"] = "failed"
            raw["failure"] = (
                f"failed to create producer {unit}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
            self.save()
            print(raw["failure"], file=sys.stderr, flush=True)
            return False
        raw["phase"] = "producer"
        self.state["last_ramp_unix"] = time.time()
        self.save()
        action = "source recovery" if source_recovery else "cohort"
        print(
            f"started {action} {cohort.label} in {lane.name} as {unit}",
            flush=True,
        )
        return True

    def retire_import_unit(self, unit: str, status: UnitStatus) -> None:
        if not status.exists:
            return
        if status.active and status.sub_state != "exited":
            raise SweepError(f"refusing to stop live importer unit {unit}")
        result = subprocess.run(
            ["/usr/bin/systemctl", "stop", unit],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 and unit_status(unit).exists:
            raise SweepError(
                f"failed to retire importer unit {unit}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    def refresh_assignments(self) -> None:
        assigned = self.state["assignments"]
        for lane_name, raw in list(assigned.items()):
            cohort = assignment_cohort(raw, self.by_bounds)
            phase = raw.get("phase")
            if phase == "import_failed":
                if cohort.label in self.state["completed"] and self.trusted_public_complete(
                    cohort
                ):
                    print(f"cohort {cohort.label} is public; releasing {lane_name}")
                    del assigned[lane_name]
                    self.save()
                continue
            if phase == "staged":
                continue
            if phase == "importing":
                unit = raw.get("import_unit")
                status = unit_status(unit) if isinstance(unit, str) else UnitStatus(
                    False, False, False, "not-found", None, None, None
                )
                if status.active and status.sub_state != "exited":
                    continue
                complete, recovery_pending = self.public_state_after_import(cohort)
                proven_success = status.exited_successfully or status.succeeded
                raw["import_result"] = status.result
                raw["import_status"] = status.main_status
                if complete and proven_success:
                    source_evidence = raw.get("source_evidence")
                    if not receipt_evidence_is_valid(source_evidence, cohort):
                        raise SweepError(
                            f"successful importer for cohort {cohort.label} lacks "
                            "bound source receipt evidence"
                        )
                    self.finalize_import(lane_name, cohort, source_evidence)
                    # Persist the root-owned completion attestation before
                    # unloading the retained successful unit. If the
                    # controller dies between these operations, an exited
                    # transient unit leaks, but the import remains durably
                    # proven and no valid public output is quarantined.
                    self.retire_import_unit(unit, status)
                    print(
                        f"recovered successful import state for cohort {cohort.label}",
                        flush=True,
                    )
                    continue
                attempts = int(raw.get("import_recovery_attempts", 0)) + 1
                raw["import_recovery_attempts"] = attempts
                if complete:
                    raw["phase"] = "import_failed"
                    raw["failure"] = (
                        "public cohort exists after controller restart without a "
                        "provably successful importer"
                    )
                    self.state["import_owner"] = None
                elif status.exists and not proven_success and not recovery_pending:
                    raw["phase"] = "import_failed"
                    raw["failure"] = (
                        f"importer failed with result {status.result!r} and "
                        f"status {status.main_status!r}"
                    )
                    self.state["import_owner"] = None
                elif attempts > 3:
                    raw["phase"] = "import_failed"
                    raw["failure"] = "import recovery exceeded three passes"
                    if not recovery_pending:
                        self.state["import_owner"] = None
                else:
                    raw["phase"] = "staged"
                self.save()
                # Preserve the unit's authoritative exit result until the
                # recovery decision and attempt counter are durable.
                self.retire_import_unit(unit, status)
                continue
            if phase not in {"producer", "launching"}:
                continue
            if self.reconcile_active_assignment(lane_name, raw, cohort):
                continue
            lane = self.lanes[lane_name]
            if public_recovery_marker_present(lane.output, self.sol_uid):
                self.launch_producer(
                    lane_name, raw, cohort, source_recovery=True
                )
                continue
            receipts = discover_live_receipts(
                lane,
                self.args.manifest_fingerprint,
                self.sol_uid,
                self.lane_output_identity(lane_name),
            )
            receipt = receipts.get(Cohort(cohort.first_epoch, cohort.last_epoch, ""))
            if receipt is None:
                if raw.get("source_recovery") is True and not public_output_files(lane):
                    raw.pop("source_recovery", None)
                    self.launch_producer(
                        lane_name, raw, cohort, source_recovery=False
                    )
                    continue
                raw["phase"] = "failed"
                raw["failure"] = "producer ended without one live v2 receipt"
                unit = raw.get("unit")
                if isinstance(unit, str):
                    status = unit_status(unit)
                    raw["unit_result"] = status.result
                    raw["unit_status"] = status.main_status
                self.save()
                print(
                    f"producer failed without a receipt for cohort {cohort.label}; "
                    f"quarantined {lane_name} and preserved all private data",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            raw["phase"] = "staged"
            raw["receipt"] = str(receipt)
            raw.pop("source_recovery", None)
            raw.pop("source_recovery_attempts", None)
            self.save()

    def import_one(self) -> bool:
        assignments = self.state["assignments"]
        owner = self.state.get("import_owner")
        recovery_pending = public_recovery_marker_present(
            self.args.public_dir, self.sol_uid
        )
        if recovery_pending and owner is None:
            raise SweepError("public archive batch marker has no root-state import owner")
        if owner is not None:
            raw = assignments.get(owner)
            if not isinstance(raw, dict):
                raise SweepError("serialized importer owner no longer has an assignment")
            if raw.get("phase") == "import_failed":
                return False
            if raw.get("phase") != "staged":
                return False
            candidates = [(owner, raw)]
        else:
            candidates = [
                (lane_name, raw)
                for lane_name, raw in assignments.items()
                if isinstance(raw, dict) and raw.get("phase") == "staged"
            ]
        if not candidates:
            return False

        lane_name, raw = candidates[0]
        cohort = assignment_cohort(raw, self.by_bounds)
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        if any(
            claim.overlaps(cohort)
            or claim.output == self.lanes[lane_name].output
            for claim in claims
        ):
            return False
        if public_recovery_marker_present(
            self.lanes[lane_name].output, self.sol_uid
        ):
            raw["phase"] = "producer"
            if self.state.get("import_owner") == lane_name:
                self.state["import_owner"] = None
            self.launch_producer(
                lane_name, raw, cohort, source_recovery=True
            )
            return True
        receipt = self.require_live_source_receipt(
            lane_name, cohort, Path(raw["receipt"])
        )
        self.revalidate_operational_directories()
        if recovery_pending:
            source_evidence = raw.get("source_evidence")
            if not receipt_evidence_is_valid(source_evidence, cohort):
                raise SweepError(
                    f"public recovery for cohort {cohort.label} lacks prebound source evidence"
                )
            verify_bound_receipt_file(source_evidence)
        else:
            source_evidence = capture_committed_receipt_evidence(
                self.lanes[lane_name].output,
                self.lanes[lane_name].private,
                cohort,
                self.args.manifest_fingerprint,
                self.sol_uid,
                allow_rename_ctime=True,
                expected_destination_identity=self.lane_output_identity(lane_name),
                rehash_archives=True,
            )
        verify_deployment(self.args.deploy_dir, self.args.manifest, (cohort,))
        sequence = int(self.state.get("sequence", 0)) + 1
        unit = (
            f"jetstreamer-root-import-{self.args.controller_id}-"
            f"e{cohort.first_epoch}-{cohort.last_epoch}-{sequence}.service"
        )
        require_unused_unit_name(unit)
        self.state["sequence"] = sequence
        raw["source_evidence"] = source_evidence
        command = build_import_command(
            cohort=cohort,
            receipt=receipt,
            deploy=self.args.deploy_dir,
            manifest=self.args.manifest,
            fingerprint=self.args.manifest_fingerprint,
            public_dir=self.args.public_dir,
            public_private_root=self.args.public_private_root,
            unit=unit,
            memory_high_gib=self.args.memory_high_gib,
            memory_max_gib=self.args.memory_max_gib,
            cpu_quota_percent=self.args.cpu_quota_percent,
        )
        print(f"importing validated cohort {cohort.label} from {lane_name}", flush=True)
        raw["phase"] = "importing"
        raw["import_unit"] = unit
        self.state["import_owner"] = lane_name
        self.save()
        self.revalidate_operational_directories()
        verify_bound_receipt_file(source_evidence)
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            status = unit_status(unit)
            if status.exists:
                raw["import_launch_warning"] = (
                    result.stderr.strip() or result.stdout.strip()
                )
                self.save()
                print(
                    f"systemd-run reported an error for importer {unit}, but the "
                    "unit exists; retaining it for authoritative reconciliation",
                    file=sys.stderr,
                    flush=True,
                )
                return True
            raw["phase"] = "import_failed"
            raw["failure"] = (
                f"failed to create importer {unit}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
            self.state["import_owner"] = None
            self.save()
            print(raw["failure"], file=sys.stderr, flush=True)
            return False
        return True

    def all_live_producer_statuses(
        self, claims: Sequence[EpochClaim]
    ) -> list[UnitStatus]:
        statuses: list[UnitStatus] = []
        for claim in claims:
            if claim.unit is None:
                statuses.append(UnitStatus(True, True, False, "external", None, None, None))
                continue
            status = unit_status(claim.unit)
            statuses.append(
                status
                if status.active
                else UnitStatus(True, True, False, "external", None, None, None)
            )
        return statuses

    def desired_capacity(self, claims: Sequence[EpochClaim]) -> int:
        total, available = parse_meminfo(Path("/proc/meminfo").read_text())
        statuses = self.all_live_producer_statuses(claims)
        now = time.time()
        current_limit = max(
            self.args.initial_concurrency,
            min(self.args.target_concurrency, int(self.state["ramp_limit"])),
        )
        elapsed = max(0.0, now - float(self.state["last_ramp_unix"]))
        next_limit = min(self.args.target_concurrency, current_limit + 1)
        capacity = admission_capacity(
            memory_total=total,
            memory_available=available,
            logical_cpus=os.cpu_count() or 0,
            active=len(statuses),
            elapsed_seconds=elapsed,
            lane_count=len(self.lanes),
            initial=current_limit,
            target=next_limit,
            maximum=self.args.max_concurrency,
            settle_seconds=self.args.settle_seconds,
            memory_max=self.args.memory_max_gib * GIB,
            memory_reserve=self.args.memory_reserve_gib * GIB,
            protected_memory=self.args.protected_memory_gib * GIB,
            cpus_per_lane=self.args.cpus_per_lane,
            memory_current=[status.memory_current for status in statuses],
            memory_peak=[status.memory_peak for status in statuses],
            memory_high=self.args.memory_high_gib * GIB,
        )
        if next_limit > current_limit and capacity >= next_limit:
            self.state["ramp_limit"] = next_limit
            self.state["last_ramp_unix"] = now
            self.save()
        return capacity

    def schedule(self) -> int:
        if self.state.get("import_owner") is not None or public_recovery_marker_present(
            self.args.public_dir, self.sol_uid
        ):
            return 0
        assigned = self.state["assignments"]
        claims = discover_epoch_claims(expected_uid=self.sol_uid)
        require_disjoint_epoch_claims(claims)
        active = len(claims)
        capacity = self.desired_capacity(claims)
        if active >= capacity:
            return 0
        claimed = {
            (raw["first_epoch"], raw["last_epoch"])
            for raw in assigned.values()
            if isinstance(raw, dict)
        }
        pending = [
            cohort
            for cohort in self.managed_cohorts
            if (cohort.first_epoch, cohort.last_epoch) not in claimed
            and not any(claim.overlaps(cohort) for claim in claims)
            and not self.trusted_public_complete(cohort)
        ]
        free_lanes = [
            lane
            for lane in self.lanes.values()
            if lane.name not in assigned
            and not public_output_files(lane)
            and not public_recovery_marker_present(lane.output, self.sol_uid)
            and all(claim.output != lane.output for claim in claims)
        ]
        started = 0
        for cohort, lane in zip(pending, free_lanes):
            if active + started >= capacity:
                break
            current_claims = discover_epoch_claims(expected_uid=self.sol_uid)
            require_disjoint_epoch_claims(current_claims)
            if (
                len(current_claims) >= capacity
                or any(
                    claim.overlaps(cohort) or claim.output == lane.output
                    for claim in current_claims
                )
            ):
                break
            raw = {
                "phase": "failed",
                "first_epoch": cohort.first_epoch,
                "last_epoch": cohort.last_epoch,
                "adopt_only": False,
            }
            if (cohort.first_epoch, cohort.last_epoch) not in self.scheduled_bounds:
                raw["adopt_only"] = True
            assigned[lane.name] = raw
            if self.launch_producer(
                lane.name, raw, cohort, source_recovery=False
            ):
                started += 1
        return started

    def all_complete(self) -> bool:
        if (
            self.state["assignments"]
            or self.state.get("import_owner") is not None
            or public_recovery_marker_present(self.args.public_dir, self.sol_uid)
        ):
            return False
        return all(
            self.trusted_public_complete(cohort, final=True)
            for cohort in self.managed_cohorts
        )

    def run(self) -> None:
        self.revalidate_operational_directories()
        require_disjoint_epoch_claims(
            discover_epoch_claims(expected_uid=self.sol_uid)
        )
        self.adopt_live_producers()
        self.adopt_receipts()
        self.require_no_orphan_lane_markers()
        while not self.stopping:
            self.revalidate_operational_directories()
            self.adopt_live_producers()
            self.adopt_receipts()
            self.require_no_orphan_lane_markers()
            self.refresh_assignments()
            if self.import_one():
                continue
            if self.all_complete():
                print(
                    f"all sealed cohorts {self.args.first_epoch}-{self.args.last_epoch} are public",
                    flush=True,
                )
                return
            self.schedule()
            time.sleep(self.args.poll_seconds)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-fingerprint", required=True)
    parser.add_argument("--public-dir", type=Path, default=Path("/home/sol/horizon"))
    parser.add_argument("--public-private-root", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--lane", action="append", type=parse_lane, required=True)
    parser.add_argument("--first-epoch", type=int, default=DEFAULT_FIRST_EPOCH)
    parser.add_argument("--last-epoch", type=int, default=DEFAULT_LAST_EPOCH)
    parser.add_argument(
        "--adopt-cohort",
        action="append",
        type=parse_epoch_range,
        default=[],
        help=(
            "also manage one exact manifest cohort; adopt live work or a receipt first, "
            "and schedule a clean fallback if neither exists"
        ),
    )
    parser.add_argument("--initial-concurrency", type=int, default=DEFAULT_INITIAL_CONCURRENCY)
    parser.add_argument("--target-concurrency", type=int, default=DEFAULT_TARGET_CONCURRENCY)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--settle-seconds", type=int, default=DEFAULT_SETTLE_SECONDS)
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--memory-high-gib", type=int, default=DEFAULT_MEMORY_HIGH_GIB)
    parser.add_argument("--memory-max-gib", type=int, default=DEFAULT_MEMORY_MAX_GIB)
    parser.add_argument("--memory-reserve-gib", type=int, default=DEFAULT_MEMORY_RESERVE_GIB)
    parser.add_argument("--protected-memory-gib", type=int, default=DEFAULT_PROTECTED_MEMORY_GIB)
    parser.add_argument("--cpus-per-lane", type=int, default=DEFAULT_CPUS_PER_LANE)
    parser.add_argument("--cpu-quota-percent", type=int, default=DEFAULT_CPU_QUOTA_PERCENT)
    parser.add_argument("--gcloud-account", default=DEFAULT_ACCOUNT)
    parser.add_argument("--gcloud-project", default=DEFAULT_PROJECT)
    parser.add_argument("--controller-id", default="historical-v1")
    parser.add_argument("--controller-sha256")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="after operator review, preserve failed data and admit a fresh attempt",
    )
    return parser


def validate_options(args: argparse.Namespace) -> None:
    if not SAFE_NAME_RE.fullmatch(args.controller_id):
        raise SweepError("controller-id must contain only lowercase letters, digits, and hyphens")
    numbers = (
        args.first_epoch,
        args.last_epoch,
        args.initial_concurrency,
        args.target_concurrency,
        args.max_concurrency,
        args.settle_seconds,
        args.poll_seconds,
        args.memory_high_gib,
        args.memory_max_gib,
        args.memory_reserve_gib,
        args.protected_memory_gib,
        args.cpus_per_lane,
        args.cpu_quota_percent,
    )
    if any(value <= 0 for value in numbers):
        raise SweepError("epoch bounds, concurrency, intervals, and resource limits must be positive")
    if args.first_epoch > args.last_epoch:
        raise SweepError("first epoch exceeds last epoch")
    if not (
        args.initial_concurrency <= args.target_concurrency <= args.max_concurrency <= 8
    ):
        raise SweepError("concurrency must satisfy initial <= target <= max <= 8")
    if args.memory_high_gib >= args.memory_max_gib:
        raise SweepError("memory-high-gib must be lower than memory-max-gib")
    if args.execute and not args.controller_sha256:
        raise SweepError("--execute requires --controller-sha256")
    names = [lane.name for lane in args.lane]
    roots = [lane.root for lane in args.lane]
    if len(names) != len(set(names)) or len(roots) != len(set(roots)):
        raise SweepError("lane names and paths must be unique")
    for name in (args.gcloud_account, args.gcloud_project):
        if not name or any(character.isspace() or character == "\0" for character in name):
            raise SweepError("gcloud account and project must be nonempty single tokens")


def prepare(
    args: argparse.Namespace,
) -> tuple[tuple[Cohort, ...], tuple[Cohort, ...], tuple[Lane, ...]]:
    validate_options(args)
    for attribute in (
        "deploy_dir",
        "manifest",
        "public_dir",
        "public_private_root",
        "state_dir",
    ):
        path = getattr(args, attribute)
        if not path.is_absolute():
            raise SweepError(f"{attribute.replace('_', '-')} must be absolute")
        setattr(args, attribute, path.resolve(strict=True))
    operational_paths = [
        ("deployment", args.deploy_dir),
        ("public destination", args.public_dir),
        ("public receipt root", args.public_private_root),
        ("controller state", args.state_dir),
        *((f"lane {lane.name}", lane.root.resolve(strict=True)) for lane in args.lane),
    ]
    for index, (label, path) in enumerate(operational_paths):
        for other_label, other_path in operational_paths[index + 1 :]:
            if path == other_path or path in other_path.parents or other_path in path.parents:
                raise SweepError(
                    f"operational paths overlap: {label} {path} and "
                    f"{other_label} {other_path}"
                )
    cohorts = load_cohorts(
        args.manifest, args.manifest_fingerprint, args.first_epoch, args.last_epoch
    )
    adopted: list[Cohort] = []
    for first, last in args.adopt_cohort:
        selected = load_cohorts(
            args.manifest, args.manifest_fingerprint, first, last
        )
        if len(selected) != 1:
            raise SweepError(f"--adopt-cohort={first}-{last} must name one exact cohort")
        adopted.append(selected[0])
    all_ranges = [
        (item.first_epoch, item.last_epoch, "scheduled") for item in cohorts
    ] + [
        (item.first_epoch, item.last_epoch, "adopted") for item in adopted
    ]
    for index, (first, last, kind) in enumerate(all_ranges):
        for other_first, other_last, other_kind in all_ranges[index + 1 :]:
            if first <= other_last and other_first <= last:
                raise SweepError(
                    f"{kind} cohort {first}-{last} overlaps {other_kind} "
                    f"cohort {other_first}-{other_last}"
                )
    verify_deployment(args.deploy_dir, args.manifest, (*cohorts, *adopted))
    if args.execute:
        verify_controller_source(
            Path(__file__).resolve(strict=True), args.controller_sha256
        )
    sol_uid = pwd.getpwnam("sol").pw_uid
    horizon_gid = grp.getgrnam("horizon").gr_gid
    public = validate_real_directory(
        args.public_dir, uid=sol_uid, gid=horizon_gid, mode=0o3770
    )
    state_dir = validate_real_directory(args.state_dir, uid=0, gid=0, mode=0o700)
    require_root_controlled_ancestry(state_dir, "controller state")
    public_private = validate_real_directory(
        args.public_private_root, uid=sol_uid, gid=horizon_gid, mode=0o700
    )
    if public.stat().st_dev != public_private.stat().st_dev:
        raise SweepError("public private root must be on the public destination filesystem")
    lanes = tuple(
        validate_lane(lane, sol_uid, horizon_gid, public.stat().st_dev)
        for lane in args.lane
    )
    if args.initial_concurrency > len(lanes):
        raise SweepError("initial concurrency exceeds the number of pre-provisioned lanes")
    if state_dir.stat().st_dev != public.stat().st_dev:
        # State need not share a filesystem with archives, but keeping it in
        # the same private operational tree avoids surprising mount lifetime.
        print("warning: controller state is on a different filesystem", file=sys.stderr)
    return cohorts, tuple(adopted), lanes


def print_plan(
    args: argparse.Namespace,
    cohorts: Sequence[Cohort],
    adopted_cohorts: Sequence[Cohort],
    lanes: Sequence[Lane],
) -> None:
    sol_uid = pwd.getpwnam("sol").pw_uid
    completed = [
        cohort.label
        for cohort in cohorts
        if public_cohort_complete(
            args.public_dir,
            args.public_private_root,
            cohort,
            args.manifest_fingerprint,
            sol_uid,
            grp.getgrnam("horizon").gr_gid,
        )
    ]
    managed = (*cohorts, *adopted_cohorts)
    live = [
        {
            "pid": claim.pid,
            "cohort": (
                str(claim.first_epoch)
                if claim.first_epoch == claim.last_epoch
                else f"{claim.first_epoch}-{claim.last_epoch}"
            ),
            "output": str(claim.output),
            "unit": claim.unit,
        }
        for claim in discover_epoch_claims(expected_uid=sol_uid)
        if any(
            claim.first_epoch <= cohort.last_epoch
            and cohort.first_epoch <= claim.last_epoch
            for cohort in managed
        )
    ]
    report = {
        "manifest_fingerprint": args.manifest_fingerprint,
        "range": [args.first_epoch, args.last_epoch],
        "cohorts": [item.label for item in cohorts],
        "cohort_count": len(cohorts),
        "adopt_only_cohorts": [item.label for item in adopted_cohorts],
        "lanes": [str(item.root) for item in lanes],
        "initial_concurrency": args.initial_concurrency,
        "target_concurrency": args.target_concurrency,
        "max_concurrency": args.max_concurrency,
        "completed": completed,
        "live_epoch_claims": live,
        "public_write_path": "jetstreamer-node --recover-staged-cohort-only only",
    }
    print(json.dumps(report, sort_keys=True, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        cohorts, adopted_cohorts, lanes = prepare(args)
        print_plan(args, cohorts, adopted_cohorts, lanes)
        if not args.execute:
            print("planning only; pass --execute from a detached root-owned service to run")
            return 0
        if os.geteuid() != 0:
            raise SweepError("--execute requires root so producer cgroups can be created")
        require_private_network_namespace()
        lock_path = args.state_dir / "controller.lock"
        lock_descriptor = os.open(
            lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(lock_descriptor)
            raise SweepError("another controller holds the state lock") from error
        controller = Controller(args, cohorts, adopted_cohorts, lanes)

        def stop(_signum: int, _frame: Any) -> None:
            controller.stopping = True

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            controller.run()
        finally:
            controller.close()
            os.close(lock_descriptor)
        return 0
    except (SweepError, OSError, KeyError, UnicodeDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
