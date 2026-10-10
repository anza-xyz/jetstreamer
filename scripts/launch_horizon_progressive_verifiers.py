#!/usr/bin/env python3
"""Start sealed progressive verifiers and uploader at the first archive pair."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
from pathlib import Path


UNIT_RE = re.compile(r"^[A-Za-z0-9_.@:-]+\.(?:service|path)$")
INVOCATION_RE = re.compile(r"^[0-9a-f]{32}$")
SIDECAR_RE = re.compile(rb"^([0-9a-f]{64})  (epoch-[0-9]+\.jet)\n$")
SHOW_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "InvocationID",
    "NRestarts",
    "Result",
    "ExecMainStatus",
)
PATH_SHOW_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "InvocationID",
    "Result",
)


class LaunchError(RuntimeError):
    """A fail-closed progressive-verifier launch error."""


def unit_name(value: str) -> str:
    if not UNIT_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"invalid systemd unit name: {value}")
    return value


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or os.path.normpath(value) != value:
        raise argparse.ArgumentTypeError(f"path must be absolute and normalized: {value}")
    return path


def invocation_id(value: str) -> str:
    if not INVOCATION_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"invalid systemd invocation ID: {value}")
    return value


def regular_file_identity(path: Path) -> dict[str, int | str]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise LaunchError(f"cannot stat {path}: {exc}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise LaunchError(f"path must be a regular file, not a symlink: {path}")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise LaunchError(f"cannot open {path}: {exc}") from exc
    try:
        opened = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
        raise LaunchError(f"path identity changed while opening: {path}")
    return {
        "path": str(path),
        "device": opened.st_dev,
        "inode": opened.st_ino,
        "size": opened.st_size,
        "mtime_ns": opened.st_mtime_ns,
        "mode": f"{stat.S_IMODE(opened.st_mode):04o}",
        "uid": opened.st_uid,
        "gid": opened.st_gid,
    }


def read_small_regular_file(
    path: Path, *, maximum_bytes: int
) -> tuple[bytes, dict[str, int | str]]:
    """Read a bounded regular file through the descriptor whose identity we record."""
    try:
        before = path.lstat()
    except OSError as exc:
        raise LaunchError(f"cannot stat {path}: {exc}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise LaunchError(f"path must be a regular file, not a symlink: {path}")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise LaunchError(f"cannot open {path}: {exc}") from exc
    try:
        opened = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise LaunchError(f"path identity changed while opening: {path}")
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 65536))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    data = b"".join(chunks)
    if len(data) > maximum_bytes:
        raise LaunchError(f"file exceeds {maximum_bytes} bytes: {path}")
    if (
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    ) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise LaunchError(f"file changed while reading: {path}")
    identity: dict[str, int | str] = {
        "path": str(path),
        "device": after.st_dev,
        "inode": after.st_ino,
        "size": after.st_size,
        "mtime_ns": after.st_mtime_ns,
        "mode": f"{stat.S_IMODE(after.st_mode):04o}",
        "uid": after.st_uid,
        "gid": after.st_gid,
    }
    return data, identity


def observe_archive_pair(archive: Path, sidecar: Path) -> dict[str, object]:
    archive_identity = regular_file_identity(archive)
    if archive_identity["size"] <= 0:
        raise LaunchError(f"archive is empty: {archive}")
    sidecar_bytes, sidecar_identity = read_small_regular_file(
        sidecar, maximum_bytes=256
    )
    match = SIDECAR_RE.fullmatch(sidecar_bytes)
    if match is None:
        raise LaunchError(f"sidecar is not canonical coreutils SHA-256 text: {sidecar}")
    sidecar_name = match.group(2).decode("ascii")
    if sidecar_name != archive.name or sidecar != archive.with_suffix(
        archive.suffix + ".sha256"
    ):
        raise LaunchError(
            f"sidecar does not name the exact archive: archive={archive} sidecar={sidecar}"
        )
    return {
        "archive": archive_identity,
        "sidecar": sidecar_identity,
        "declared_archive_sha256": match.group(1).decode("ascii"),
    }


def systemctl_show(systemctl: Path, unit: str) -> dict[str, str | int]:
    properties = PATH_SHOW_PROPERTIES if unit.endswith(".path") else SHOW_PROPERTIES
    command = [
        str(systemctl),
        "show",
        unit,
        "--no-pager",
        f"--property={','.join(properties)}",
    ]
    try:
        completed = subprocess.run(
            command, check=True, capture_output=True, text=True
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise LaunchError(f"cannot inspect {unit}: {exc}") from exc
    values: dict[str, str | int] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in properties:
            values[key] = value
    missing = [key for key in properties if key not in values]
    if missing:
        raise LaunchError(f"systemctl output for {unit} lacks {missing}")
    try:
        if not unit.endswith(".path"):
            values["NRestarts"] = int(str(values["NRestarts"]))
            values["ExecMainStatus"] = int(str(values["ExecMainStatus"]))
    except ValueError as exc:
        raise LaunchError(f"systemctl returned non-integer counters for {unit}") from exc
    values["unit"] = unit
    return values


def require_producer(sample: dict[str, str | int], expected_invocation: str) -> None:
    if (
        sample["LoadState"] != "loaded"
        or sample["ActiveState"] != "active"
        or sample["SubState"] != "running"
        or sample["InvocationID"] != expected_invocation
        or sample["NRestarts"] != 0
    ):
        raise LaunchError(f"producer identity/state mismatch: {sample}")


def require_path_watcher(sample: dict[str, str | int]) -> None:
    if (
        sample["LoadState"] != "loaded"
        or sample["ActiveState"] != "active"
        or sample["SubState"] not in {"waiting", "running"}
        or not INVOCATION_RE.fullmatch(str(sample["InvocationID"]))
    ):
        raise LaunchError(f"path watcher is not authenticated and active: {sample}")


def path_watcher_is_inactive(sample: dict[str, str | int]) -> bool:
    return (
        sample["LoadState"] in {"loaded", "not-found"}
        and sample["ActiveState"] == "inactive"
        and sample["SubState"] == "dead"
    )


def stop_path_watcher(
    systemctl: Path, unit: str, timeout_seconds: float
) -> dict[str, str | int]:
    sample = systemctl_show(systemctl, unit)
    if not path_watcher_is_inactive(sample):
        try:
            subprocess.run([str(systemctl), "stop", unit], check=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise LaunchError(f"cannot stop path watcher {unit}: {exc}") from exc
    deadline = time.monotonic() + timeout_seconds
    while True:
        sample = systemctl_show(systemctl, unit)
        if path_watcher_is_inactive(sample):
            return sample
        if time.monotonic() >= deadline:
            raise LaunchError(f"path watcher did not stop: {sample}")
        time.sleep(0.1)


def service_is_started(sample: dict[str, str | int]) -> bool:
    if sample["LoadState"] != "loaded" or sample["NRestarts"] != 0:
        return False
    if sample["ActiveState"] in {"activating", "active"}:
        return bool(sample["InvocationID"])
    return (
        sample["ActiveState"] == "inactive"
        and sample["SubState"] == "dead"
        and sample["Result"] == "success"
        and sample["ExecMainStatus"] == 0
        and bool(sample["InvocationID"])
    )


def service_started_after(
    sample: dict[str, str | int], prior: dict[str, object]
) -> bool:
    prior_invocation = prior.get("InvocationID")
    return (
        isinstance(prior_invocation, str)
        and service_is_started(sample)
        and sample["InvocationID"] != prior_invocation
    )


def write_json_no_clobber(path: Path, value: dict[str, object]) -> None:
    parent = path.parent
    try:
        parent_stat = parent.lstat()
    except OSError as exc:
        raise LaunchError(f"cannot stat receipt directory {parent}: {exc}") from exc
    if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
        raise LaunchError(f"receipt parent must be a directory, not a symlink: {parent}")
    payload = (json.dumps(value, sort_keys=True) + "\n").encode("utf-8")
    temporary = parent / f".{path.name}.tmp.{os.getpid()}"
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
                raise LaunchError(f"short write for receipt {temporary}")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.link(temporary, path, follow_symlinks=False)
        linked = True
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError as exc:
        raise LaunchError(f"refusing to overwrite receipt: {path}") from exc
    except OSError as exc:
        raise LaunchError(f"cannot create receipt {path}: {exc}") from exc
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


def read_json(path: Path) -> dict[str, object] | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LaunchError(f"cannot stat receipt {path}: {exc}") from exc
    payload, _ = read_small_regular_file(path, maximum_bytes=1024 * 1024)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LaunchError(f"cannot parse receipt {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LaunchError(f"receipt is not a JSON object: {path}")
    return value


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=absolute_path)
    parser.add_argument("--sidecar", required=True, type=absolute_path)
    parser.add_argument("--producer-unit", required=True, type=unit_name)
    parser.add_argument(
        "--expected-producer-invocation-id", required=True, type=invocation_id
    )
    parser.add_argument("--full-unit", required=True, type=unit_name)
    parser.add_argument("--plugin-unit", required=True, type=unit_name)
    parser.add_argument("--upload-unit", type=unit_name)
    parser.add_argument("--path-unit", required=True, type=unit_name)
    parser.add_argument("--intent-receipt", required=True, type=absolute_path)
    parser.add_argument("--completion-receipt", required=True, type=absolute_path)
    parser.add_argument("--systemctl", type=absolute_path, default=Path("/usr/bin/systemctl"))
    parser.add_argument("--start-timeout-seconds", type=float, default=10.0)
    args = parser.parse_args()
    launched_units = [args.full_unit, args.plugin_unit]
    if args.upload_unit is not None:
        launched_units.append(args.upload_unit)
    if len(set(launched_units)) != len(launched_units):
        parser.error("full, plugin and upload units must be distinct")
    if args.start_timeout_seconds <= 0 or args.start_timeout_seconds > 60:
        parser.error("start timeout must be greater than zero and at most 60 seconds")

    launcher_invocation = os.environ.get("INVOCATION_ID", "")
    if not INVOCATION_RE.fullmatch(launcher_invocation):
        raise LaunchError("INVOCATION_ID is missing or invalid")

    pair = observe_archive_pair(args.archive, args.sidecar)
    binding = {
        "archive": str(args.archive),
        "sidecar": str(args.sidecar),
        "producer_unit": args.producer_unit,
        "expected_producer_invocation_id": args.expected_producer_invocation_id,
        "full_unit": args.full_unit,
        "plugin_unit": args.plugin_unit,
        "upload_unit": args.upload_unit,
        "path_unit": args.path_unit,
    }
    completion = read_json(args.completion_receipt)
    if completion is not None:
        if (
            completion.get("schema")
            != "jetstreamer-horizon-progressive-verifier-launch-completion-v1"
            or completion.get("binding") != binding
            or completion.get("archive_pair") != pair
        ):
            raise LaunchError("existing completion receipt does not match this launch")
        stop_path_watcher(
            args.systemctl, args.path_unit, args.start_timeout_seconds
        )
        print(json.dumps(completion, sort_keys=True))
        return 0

    producer = systemctl_show(args.systemctl, args.producer_unit)
    require_producer(producer, args.expected_producer_invocation_id)
    watcher = systemctl_show(args.systemctl, args.path_unit)
    require_path_watcher(watcher)

    full_before = systemctl_show(args.systemctl, args.full_unit)
    plugin_before = systemctl_show(args.systemctl, args.plugin_unit)
    upload_before = (
        systemctl_show(args.systemctl, args.upload_unit)
        if args.upload_unit is not None
        else None
    )
    intent = read_json(args.intent_receipt)
    if intent is None:
        intent = {
            "schema": "jetstreamer-horizon-progressive-verifier-launch-intent-v1",
            "observed_at_utc": utc_now(),
            "launcher_invocation_id": launcher_invocation,
            "binding": binding,
            "archive_pair": pair,
            "producer": producer,
            "path_watcher": watcher,
            "full_before": full_before,
            "plugin_before": plugin_before,
            "upload_before": upload_before,
        }
        write_json_no_clobber(args.intent_receipt, intent)
    elif (
        intent.get("schema")
        != "jetstreamer-horizon-progressive-verifier-launch-intent-v1"
        or intent.get("binding") != binding
        or intent.get("archive_pair") != pair
    ):
        raise LaunchError("existing intent receipt does not match this launch")

    intent_full_before = intent.get("full_before")
    intent_plugin_before = intent.get("plugin_before")
    if not isinstance(intent_full_before, dict) or not isinstance(
        intent_plugin_before, dict
    ):
        raise LaunchError("intent receipt lacks verifier pre-launch state")
    intent_upload_before = intent.get("upload_before")
    if args.upload_unit is not None and not isinstance(intent_upload_before, dict):
        raise LaunchError("intent receipt lacks uploader pre-launch state")
    full_started = service_started_after(full_before, intent_full_before)
    plugin_started = service_started_after(plugin_before, intent_plugin_before)
    upload_started = args.upload_unit is None or service_started_after(
        upload_before, intent_upload_before
    )
    if not (full_started and plugin_started and upload_started):
        try:
            subprocess.run(
                [
                    str(args.systemctl),
                    "start",
                    "--no-block",
                    *launched_units,
                ],
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise LaunchError(f"cannot queue verifier/upload units: {exc}") from exc

    deadline = time.monotonic() + args.start_timeout_seconds
    while True:
        full_after = systemctl_show(args.systemctl, args.full_unit)
        plugin_after = systemctl_show(args.systemctl, args.plugin_unit)
        upload_after = (
            systemctl_show(args.systemctl, args.upload_unit)
            if args.upload_unit is not None
            else None
        )
        if service_started_after(
            full_after, intent_full_before
        ) and service_started_after(plugin_after, intent_plugin_before) and (
            args.upload_unit is None
            or service_started_after(upload_after, intent_upload_before)
        ):
            break
        if time.monotonic() >= deadline:
            raise LaunchError(
                "verifier/upload units did not enter an authenticated started state: "
                f"full={full_after} plugin={plugin_after} upload={upload_after}"
            )
        time.sleep(0.1)

    if observe_archive_pair(args.archive, args.sidecar) != pair:
        raise LaunchError("archive pair changed during verifier launch")
    completion = {
        "schema": "jetstreamer-horizon-progressive-verifier-launch-completion-v1",
        "observed_at_utc": utc_now(),
        "launcher_invocation_id": launcher_invocation,
        "resumed_from_launcher_invocation_id": intent.get("launcher_invocation_id"),
        "binding": binding,
        "archive_pair": pair,
        "producer": producer,
        "path_watcher": watcher,
        "full_after": full_after,
        "plugin_after": plugin_after,
        "upload_after": upload_after,
    }
    write_json_no_clobber(args.completion_receipt, completion)
    stop_path_watcher(args.systemctl, args.path_unit, args.start_timeout_seconds)
    print(json.dumps(completion, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LaunchError as error:
        print(f"launch failed: {error}", file=sys.stderr)
        raise SystemExit(1)
