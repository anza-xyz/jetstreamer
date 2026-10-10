#!/usr/bin/python3
"""Pause historical schedulers around the final public-import commit window.

Producer and importer units are independent transient services.  This guard
keeps schedulers from admitting another public importer while the active one
finishes its last archive validation and commits its batch.  Controller
commands are read directly from systemd's live D-Bus state and authenticated
against their sealed script digest before any controller is stopped or later
restarted.

If the operator-created pause sentinel exists, controllers stay fail-closed
after the importer exits and must be resumed deliberately.
"""

import hashlib
import json
import os
import re
import stat
import subprocess
import time


POLL_SECONDS = 10
PAUSE_AT_ARCHIVE_FRACTION = 0.85
MIN_TRACKED_ARCHIVE_BYTES = 1 << 30
MAX_CONTROLLERS = 64
PAUSE_SENTINEL = "/home/sol/.jetstreamer-private/runtime/controllers-paused-for-credentials"
PAUSE_STATE_DIRECTORY = "/var/lib/jetstreamer-horizon-import-guard-v1"
PAUSE_STATE_NAME = "paused-controllers.json"
PAUSE_STATE_SCHEMA = "jetstreamer-horizon-import-guard-pause-v1"
MAX_PAUSE_STATE_BYTES = 256 * 1024
SERVICE_UNIT_RE = re.compile(r"[A-Za-z0-9_.@-]+\.service")
INVOCATION_ID_RE = re.compile(r"[0-9a-f]{32}")
MAX_DISCOVERY_RETRIES = 64


class ControllerNotLiveError(RuntimeError):
    """A controller disappeared between discovery and command capture."""


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, capture_output=True, text=True)


def unit_properties(unit: str) -> dict[str, str]:
    result = run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=MainPID",
            "--property=Transient",
            "--property=FragmentPath",
            "--property=InvocationID",
            "--property=NRestarts",
            "--no-pager",
        ]
    )
    properties = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    return properties


def running_units() -> set[str]:
    result = run(
        [
            "/usr/bin/systemctl",
            "list-units",
            "--type=service",
            "--state=running",
            "--no-legend",
            "--plain",
        ]
    )
    return {line.split()[0] for line in result.stdout.splitlines() if line.split()}


def is_controller_unit(unit: str) -> bool:
    return (
        SERVICE_UNIT_RE.fullmatch(unit) is not None
        and unit.startswith("jetstreamer-")
        and "controller" in unit
    )


def controller_units(units: set[str]) -> tuple[str, ...]:
    """Return every running Horizon scheduler controller.

    Controller unit names are operational identifiers and change whenever a
    scheduler is replaced.  Do not maintain an allowlist here.  The command
    for each discovered unit is independently authenticated by
    ``command_from_systemd`` before the unit can be stopped or restarted.
    """

    controllers = tuple(
        sorted(
            unit
            for unit in units
            if is_controller_unit(unit)
        )
    )
    if len(controllers) > MAX_CONTROLLERS:
        raise RuntimeError(
            f"refusing to manage {len(controllers)} controller units"
        )
    return controllers


def controllers_deliberately_paused() -> bool:
    try:
        descriptor = os.open(
            PAUSE_SENTINEL, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        )
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or info.st_gid != 0
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise RuntimeError("controller pause sentinel has unsafe identity")
        payload = os.read(descriptor, 4097)
        if not payload or len(payload) > 4096 or not payload.endswith(b"\n"):
            raise RuntimeError("controller pause sentinel has invalid contents")
    finally:
        os.close(descriptor)
    return True


def busctl_json(command: list[str], description: str) -> dict[str, object]:
    result = run(command)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"invalid busctl JSON for {description}") from error
    if not isinstance(payload, dict) or set(payload) != {"type", "data"}:
        raise RuntimeError(f"unexpected busctl response for {description}")
    return payload


def validate_controller_command(unit: str, command: list[str]) -> list[str]:
    if len(command) < 3 or command[0] != "/usr/bin/python3":
        raise RuntimeError(f"unsafe controller command for {unit}")
    script = command[1]
    if not script.startswith("/usr/local/lib/jetstreamer/adaptive-root-cohort-sweep-"):
        raise RuntimeError(f"unexpected controller script for {unit}: {script}")
    state_arguments = [value for value in command if value.startswith("--state-dir=")]
    digest_arguments = [
        value for value in command if value.startswith("--controller-sha256=")
    ]
    if len(state_arguments) != 1 or len(digest_arguments) != 1 or "--execute" not in command:
        raise RuntimeError(f"controller command lacks sealed execution arguments for {unit}")
    state_directory = state_arguments[0].partition("=")[2]
    if not state_directory.startswith("/var/lib/jetstreamer-root-sweep-"):
        raise RuntimeError(f"unsafe controller state directory for {unit}")
    expected_digest = digest_arguments[0].partition("=")[2]
    with open(script, "rb", buffering=0) as script_file:
        actual_digest = hashlib.file_digest(script_file, "sha256").hexdigest()
    if actual_digest != expected_digest:
        raise RuntimeError(f"controller script digest changed for {unit}")
    validated = list(command)
    if "--retry-failed" not in validated:
        validated.append("--retry-failed")
    return validated


def validate_controller_unit_definition(
    unit: str, properties: dict[str, str]
) -> str:
    """Return the safely resumable systemd unit kind."""

    if properties.get("LoadState") != "loaded":
        raise RuntimeError(f"controller unit is not loaded: {unit}")
    fragment = properties.get("FragmentPath", "")
    if (
        properties.get("Transient") == "yes"
        and fragment.startswith("/run/systemd/transient/")
        and os.path.basename(fragment) == unit
    ):
        return "transient"
    expected_fragment = f"/etc/systemd/system/{unit}"
    if properties.get("Transient") != "no" or fragment != expected_fragment:
        raise RuntimeError(f"controller has an unsafe systemd definition: {unit}")
    try:
        info = os.lstat(expected_fragment)
    except OSError as error:
        raise RuntimeError(
            f"cannot inspect persistent controller definition: {unit}"
        ) from error
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != 0
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise RuntimeError(f"persistent controller definition is unsafe: {unit}")
    return "persistent"


def controller_command_definition(
    unit: str, properties: dict[str, str]
) -> list[str]:
    validate_controller_unit_definition(unit, properties)

    object_reply = busctl_json(
        [
            "/usr/bin/busctl",
            "--json=short",
            "call",
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1",
            "org.freedesktop.systemd1.Manager",
            "GetUnit",
            "s",
            unit,
        ],
        f"systemd object path for {unit}",
    )
    object_data = object_reply.get("data")
    if (
        object_reply.get("type") != "o"
        or not isinstance(object_data, list)
        or len(object_data) != 1
        or not isinstance(object_data[0], str)
        or not object_data[0].startswith("/org/freedesktop/systemd1/unit/")
    ):
        raise RuntimeError(f"unexpected systemd object path for {unit}")

    exec_reply = busctl_json(
        [
            "/usr/bin/busctl",
            "--json=short",
            "get-property",
            "org.freedesktop.systemd1",
            object_data[0],
            "org.freedesktop.systemd1.Service",
            "ExecStart",
        ],
        f"ExecStart for {unit}",
    )
    exec_data = exec_reply.get("data")
    if (
        exec_reply.get("type") != "a(sasbttttuii)"
        or not isinstance(exec_data, list)
        or len(exec_data) != 1
        or not isinstance(exec_data[0], list)
        or len(exec_data[0]) != 10
    ):
        raise RuntimeError(f"unexpected ExecStart shape for {unit}")
    executable, command, ignore_errors, *metadata = exec_data[0]
    if (
        executable != "/usr/bin/python3"
        or not isinstance(command, list)
        or not all(isinstance(value, str) for value in command)
        or len(command) < 3
        or command[0] != executable
        or ignore_errors is not False
        or not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in metadata
        )
    ):
        raise RuntimeError(f"unsafe controller command for {unit}")
    return validate_controller_command(unit, command)


def command_from_systemd(unit: str) -> list[str]:
    properties = unit_properties(unit)
    if (
        properties.get("LoadState") != "loaded"
        or properties.get("ActiveState") != "active"
        or properties.get("SubState") != "running"
    ):
        raise ControllerNotLiveError(
            f"controller is not a live service: {unit}"
        )
    return controller_command_definition(unit, properties)


def open_pause_state_directory() -> int:
    descriptor = os.open(
        PAUSE_STATE_DIRECTORY,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
    )
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_gid != os.getegid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        os.close(descriptor)
        raise RuntimeError("pause-state directory has unsafe identity")
    return descriptor


def load_pause_state() -> dict[str, object] | None:
    directory = open_pause_state_directory()
    try:
        try:
            descriptor = os.open(
                PAUSE_STATE_NAME,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=directory,
            )
        except FileNotFoundError:
            return None
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_gid != os.getegid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or info.st_size <= 0
                or info.st_size > MAX_PAUSE_STATE_BYTES
            ):
                raise RuntimeError("pause state has unsafe identity")
            chunks = []
            remaining = MAX_PAUSE_STATE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
    finally:
        os.close(directory)
    if not payload.endswith(b"\n") or len(payload) > MAX_PAUSE_STATE_BYTES:
        raise RuntimeError("pause state has invalid framing")
    try:
        state = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("pause state is invalid JSON") from error
    if not isinstance(state, dict) or set(state) != {
        "schema",
        "importer",
        "importer_invocation_id",
        "controllers",
    }:
        raise RuntimeError("pause state has an invalid schema")
    importer = state.get("importer")
    invocation = state.get("importer_invocation_id")
    raw_controllers = state.get("controllers")
    if (
        state.get("schema") != PAUSE_STATE_SCHEMA
        or not isinstance(importer, str)
        or SERVICE_UNIT_RE.fullmatch(importer) is None
        or not importer.startswith("jetstreamer-root-import-")
        or not isinstance(invocation, str)
        or INVOCATION_ID_RE.fullmatch(invocation) is None
        or not isinstance(raw_controllers, dict)
        or not raw_controllers
        or len(raw_controllers) > MAX_CONTROLLERS
    ):
        raise RuntimeError("pause state has invalid importer or controller data")
    controllers: dict[str, list[str]] = {}
    for unit, raw_command in raw_controllers.items():
        if (
            not isinstance(unit, str)
            or not is_controller_unit(unit)
            or not isinstance(raw_command, list)
            or not all(isinstance(value, str) for value in raw_command)
        ):
            raise RuntimeError("pause state has an invalid controller command")
        controllers[unit] = validate_controller_command(unit, raw_command)
    state["controllers"] = controllers
    return state


def persist_pause_state(
    importer: str,
    invocation: str,
    controllers: dict[str, list[str]],
) -> None:
    if (
        SERVICE_UNIT_RE.fullmatch(importer) is None
        or not importer.startswith("jetstreamer-root-import-")
        or INVOCATION_ID_RE.fullmatch(invocation) is None
        or not controllers
        or len(controllers) > MAX_CONTROLLERS
        or any(not is_controller_unit(unit) for unit in controllers)
    ):
        raise RuntimeError("refusing to persist invalid pause state")
    validated_controllers = {
        unit: validate_controller_command(unit, command)
        for unit, command in controllers.items()
    }
    existing = load_pause_state()
    if existing is not None:
        if (
            existing["importer"] != importer
            or existing["importer_invocation_id"] != invocation
            or any(
                validated_controllers.get(unit) != command
                for unit, command in existing["controllers"].items()
            )
        ):
            raise RuntimeError("existing pause state conflicts with this importer")
    state = {
        "schema": PAUSE_STATE_SCHEMA,
        "importer": importer,
        "importer_invocation_id": invocation,
        "controllers": dict(sorted(validated_controllers.items())),
    }
    payload = json.dumps(state, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if len(payload) > MAX_PAUSE_STATE_BYTES:
        raise RuntimeError("pause state exceeds its size bound")
    directory = open_pause_state_directory()
    temporary = f".{PAUSE_STATE_NAME}.tmp-{os.getpid()}-{time.time_ns()}"
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise RuntimeError("short write to pause state")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(
            temporary,
            PAUSE_STATE_NAME,
            src_dir_fd=directory,
            dst_dir_fd=directory,
        )
        os.fsync(directory)
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(directory)


def remove_pause_state() -> None:
    directory = open_pause_state_directory()
    try:
        os.unlink(PAUSE_STATE_NAME, dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(directory)


def start_controller(unit: str, command: list[str]) -> None:
    state_directory = next(
        value.partition("=")[2] for value in command if value.startswith("--state-dir=")
    )
    properties = unit_properties(unit)
    if properties.get("LoadState") == "loaded" and properties.get("Transient") == "no":
        kind = validate_controller_unit_definition(unit, properties)
        if (
            kind != "persistent"
            or properties.get("ActiveState") != "inactive"
            or properties.get("SubState") != "dead"
            or properties.get("MainPID") != "0"
            or properties.get("Result") != "success"
            or controller_command_definition(unit, properties) != command
        ):
            raise RuntimeError(
                f"persistent controller changed while paused: {unit}"
            )
        result = run(["/usr/bin/systemctl", "start", unit], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"failed to restore {unit}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        if command_from_systemd(unit) != command:
            raise RuntimeError(
                f"persistent controller command changed after resume: {unit}"
            )
        return
    if (
        properties.get("LoadState") == "loaded"
        and properties.get("ActiveState") == "failed"
        and properties.get("MainPID") == "0"
        and properties.get("Transient") == "yes"
        and properties.get("FragmentPath", "").startswith("/run/systemd/transient/")
    ):
        run(["/usr/bin/systemctl", "reset-failed", unit])
        properties = unit_properties(unit)
    if properties.get("LoadState") != "not-found":
        raise RuntimeError(f"refusing to reuse loaded controller unit {unit}: {properties}")
    unit_name = unit.removesuffix(".service")
    invocation = [
        "/usr/bin/systemd-run",
        f"--unit={unit_name}",
        f"--description=Resume verified Horizon controller {unit_name}",
        "--property=Restart=no",
        "--property=KillMode=control-group",
        "--property=TimeoutStartSec=infinity",
        "--property=TimeoutStopSec=2min",
        "--property=Nice=10",
        "--property=IOSchedulingClass=idle",
        "--property=IOSchedulingPriority=4",
        "--property=TasksMax=256",
        "--property=UMask=0077",
        "--property=OOMPolicy=stop",
        "--property=NoNewPrivileges=yes",
        "--property=PrivateNetwork=yes",
        "--property=PrivateTmp=yes",
        "--property=PrivateDevices=yes",
        "--property=PrivateIPC=yes",
        "--property=ProtectSystem=strict",
        "--property=ProtectHome=read-only",
        "--property=ProtectKernelTunables=yes",
        "--property=ProtectKernelModules=yes",
        "--property=ProtectKernelLogs=yes",
        "--property=ProtectControlGroups=yes",
        "--property=ProtectClock=yes",
        "--property=ProtectHostname=yes",
        "--property=RestrictNamespaces=cgroup",
        "--property=RestrictRealtime=yes",
        "--property=RestrictSUIDSGID=yes",
        "--property=LockPersonality=yes",
        "--property=SystemCallArchitectures=native",
        "--property=CapabilityBoundingSet=cap_dac_override",
        "--property=RestrictAddressFamilies=AF_UNIX",
        f"--property=ReadWritePaths={state_directory}",
        "--setenv=PYTHONDONTWRITEBYTECODE=1",
        *command,
    ]
    result = run(invocation, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to restore {unit}: {result.stderr.strip() or result.stdout.strip()}"
        )


def wait_for_importer(unit: str) -> dict[str, str]:
    while True:
        properties = unit_properties(unit)
        if (
            properties.get("ActiveState") in {"inactive", "failed"}
            or properties.get("SubState") == "exited"
        ):
            return properties
        time.sleep(POLL_SECONDS)


def paused_importer_is_terminal(
    importer: str,
    invocation: str,
    properties: dict[str, str],
) -> bool:
    importer_disappeared = (
        properties.get("LoadState") == "not-found"
        and properties.get("ActiveState") == "inactive"
        and not properties.get("InvocationID")
    )
    if importer_disappeared:
        other_importers = sorted(
            unit
            for unit in running_units()
            if unit.startswith("jetstreamer-root-import-")
        )
        if other_importers:
            raise RuntimeError(
                f"paused importer disappeared while another importer is live: "
                f"{other_importers}"
            )
        return True
    if (
        properties.get("InvocationID") != invocation
        or properties.get("NRestarts") != "0"
    ):
        raise RuntimeError("paused importer invocation changed")
    return (
        properties.get("ActiveState") in {"inactive", "failed"}
        or properties.get("SubState") == "exited"
    )


def importer_archive_progress(unit: str) -> tuple[int, int] | None:
    properties = unit_properties(unit)
    try:
        pid = int(properties.get("MainPID", "0"))
    except ValueError:
        return None
    if pid <= 0:
        return None
    candidates = []
    try:
        descriptors = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return None
    for descriptor in descriptors:
        path = f"/proc/{pid}/fd/{descriptor}"
        try:
            target = os.readlink(path)
            size = os.stat(path).st_size
            with open(f"/proc/{pid}/fdinfo/{descriptor}", encoding="ascii") as fdinfo:
                position_line = next(
                    line for line in fdinfo if line.startswith("pos:\t")
                )
            position = int(position_line.partition("\t")[2])
        except (OSError, StopIteration, ValueError):
            continue
        if target.endswith(".jet") and size >= MIN_TRACKED_ARCHIVE_BYTES:
            candidates.append((position, size))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0] / item[1])


def wait_for_import_commit_window(importer: str) -> bool:
    missing_progress = 0
    last_report = 0.0
    while unit_properties(importer).get("SubState") == "running":
        progress = importer_archive_progress(importer)
        if progress is None:
            missing_progress += 1
            if missing_progress >= 3:
                print(
                    f"importer {importer} has no measurable archive progress; pausing early",
                    flush=True,
                )
                return True
        else:
            missing_progress = 0
            position, size = progress
            fraction = min(position / size, 1.0)
            now = time.monotonic()
            if now - last_report >= 300:
                print(
                    f"importer {importer} validation is {fraction:.1%} complete",
                    flush=True,
                )
                last_report = now
            if fraction >= PAUSE_AT_ARCHIVE_FRACTION:
                return True
        time.sleep(POLL_SECONDS)
    return False


def stop_controllers(
    importer: str,
    invocation: str,
    controllers: tuple[str, ...],
    known_commands: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    """Authenticate and stop all controllers, including bounded late arrivals."""

    commands = dict(known_commands or {})
    pending = controllers
    discovery_retries = 0
    while pending:
        if len(set(commands).union(pending)) > MAX_CONTROLLERS:
            raise RuntimeError("too many controller units appeared while pausing")
        # Authenticate a stable snapshot before mutating any unit.  A controller
        # can finish naturally after list-units reports it but before its
        # command is captured.  That race is safe only when a fresh discovery
        # confirms the unit is no longer running; every other authentication
        # error remains fatal.
        batch = {}
        for unit in pending:
            try:
                batch[unit] = command_from_systemd(unit)
            except ControllerNotLiveError:
                pass
        live_now = controller_units(running_units())
        unmanaged_now = tuple(unit for unit in live_now if unit not in commands)
        if set(unmanaged_now) != set(batch):
            discovery_retries += 1
            if discovery_retries > MAX_DISCOVERY_RETRIES:
                raise RuntimeError(
                    "controller set did not stabilize while pausing importer"
                )
            pending = unmanaged_now
            continue
        discovery_retries = 0
        if not batch:
            raise RuntimeError("no live controller remained while pausing importer")
        changed = [
            unit
            for unit, command in batch.items()
            if unit in commands and commands[unit] != command
        ]
        if changed:
            raise RuntimeError(
                f"controller command changed while pausing: {tuple(sorted(changed))}"
            )
        commands.update(batch)
        persist_pause_state(importer, invocation, dict(commands))
        run(["/usr/bin/systemctl", "stop", *sorted(batch)])
        remaining = controller_units(running_units())
        failed = tuple(unit for unit in remaining if unit in commands)
        if failed:
            raise RuntimeError(f"controller units failed to stop: {failed}")
        pending = tuple(unit for unit in remaining if unit not in commands)
    return commands


def recover_paused_controllers() -> bool:
    state = load_pause_state()
    if state is None:
        return False
    importer = state["importer"]
    invocation = state["importer_invocation_id"]
    controllers = state["controllers"]
    properties = unit_properties(importer)
    if not paused_importer_is_terminal(importer, invocation, properties):
        # The guard may have crashed after persisting intent but before stopping
        # one or more controllers.  Re-authenticate every live controller,
        # preserve already-stopped commands, and complete the pause before
        # waiting for the importer.
        live_controllers = controller_units(running_units())
        controllers = stop_controllers(
            importer,
            invocation,
            live_controllers,
            known_commands=controllers,
        )
        properties = wait_for_importer(importer)
        if not paused_importer_is_terminal(importer, invocation, properties):
            raise RuntimeError("paused importer did not reach a terminal state")
    if controllers_deliberately_paused():
        print(
            f"importer {importer} ended with {properties}; "
            "controllers remain deliberately paused",
            flush=True,
        )
        return True

    failures = []
    for unit, command in controllers.items():
        try:
            current = unit_properties(unit)
            if (
                current.get("LoadState") == "loaded"
                and current.get("ActiveState") == "active"
                and current.get("SubState") == "running"
            ):
                if command_from_systemd(unit) != command:
                    raise RuntimeError("live controller command differs from pause state")
                continue
            start_controller(unit, command)
        except Exception as error:
            failures.append(f"{unit}: {error}")
    if failures:
        raise RuntimeError("; ".join(failures))
    remove_pause_state()
    print(f"importer {importer} ended with {properties}; controllers resumed", flush=True)
    return True


def protect_import(importer: str, controllers: tuple[str, ...]) -> None:
    properties = unit_properties(importer)
    invocation = properties.get("InvocationID", "")
    if (
        properties.get("LoadState") != "loaded"
        or properties.get("ActiveState") != "active"
        or properties.get("SubState") != "running"
        or properties.get("NRestarts") != "0"
        or INVOCATION_ID_RE.fullmatch(invocation) is None
    ):
        raise RuntimeError(f"importer is not a stable live invocation: {importer}")
    commands = stop_controllers(importer, invocation, controllers)
    print(
        f"paused {len(commands)} controller(s) for importer {importer}",
        flush=True,
    )
    recover_paused_controllers()


def main() -> None:
    while True:
        if recover_paused_controllers():
            time.sleep(POLL_SECONDS)
            continue
        active = running_units()
        importers = sorted(
            unit for unit in active if unit.startswith("jetstreamer-root-import-")
        )
        if len(importers) > 1:
            raise RuntimeError(f"multiple public importers are active: {importers}")
        if importers:
            if not wait_for_import_commit_window(importers[0]):
                continue
            active = running_units()
            controllers = controller_units(active)
            if not controllers:
                if controllers_deliberately_paused():
                    result = wait_for_importer(importers[0])
                    print(
                        f"importer {importers[0]} ended with {result}; "
                        "controllers remain deliberately paused",
                        flush=True,
                    )
                    continue
                raise RuntimeError(f"importer {importers[0]} has no active controller owner")
            protect_import(importers[0], controllers)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
