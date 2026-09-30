#!/usr/bin/python3
"""Pause historical schedulers around the final public-import commit window.

Producer and importer units are independent transient services.  This guard
keeps schedulers from admitting another public importer while the active one
finishes its last archive validation and commits its batch.  Controller
commands are recovered from journald and authenticated against their sealed
script digest before any controller is stopped or later restarted.

If the operator-created pause sentinel exists, controllers stay fail-closed
after the importer exits and must be resumed deliberately.
"""

import hashlib
import json
import os
import shlex
import stat
import subprocess
import time


POLL_SECONDS = 10
PAUSE_AT_ARCHIVE_FRACTION = 0.85
MIN_TRACKED_ARCHIVE_BYTES = 1 << 30
MAX_CONTROLLERS = 64
PAUSE_SENTINEL = "/home/sol/.jetstreamer-private/runtime/controllers-paused-for-credentials"


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


def controller_units(units: set[str]) -> tuple[str, ...]:
    """Return every running Horizon scheduler controller.

    Controller unit names are operational identifiers and change whenever a
    scheduler is replaced.  Do not maintain an allowlist here.  The command
    for each discovered unit is independently authenticated by
    ``command_from_journal`` before the unit can be stopped or restarted.
    """

    controllers = tuple(
        sorted(
            unit
            for unit in units
            if unit.startswith("jetstreamer-")
            and "controller" in unit
            and unit.endswith(".service")
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


def command_from_journal(unit: str) -> list[str]:
    result = run(
        [
            "/usr/bin/journalctl",
            "--unit",
            unit,
            "--output=json",
            "--no-pager",
        ]
    )
    command_line = None
    for line in result.stdout.splitlines():
        record = json.loads(line)
        candidate = record.get("_CMDLINE")
        if isinstance(candidate, str) and candidate.startswith("/usr/bin/python3 "):
            command_line = candidate
    if command_line is None:
        raise RuntimeError(f"no controller command line in journal for {unit}")
    command = shlex.split(command_line)
    if len(command) < 3 or command[0] != "/usr/bin/python3":
        raise RuntimeError(f"unsafe controller command for {unit}")
    script = command[1]
    if not script.startswith("/usr/local/lib/jetstreamer/adaptive-root-cohort-sweep-"):
        raise RuntimeError(f"unexpected controller script for {unit}: {script}")
    state_arguments = [value for value in command if value.startswith("--state-dir=")]
    digest_arguments = [value for value in command if value.startswith("--controller-sha256=")]
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
    if "--retry-failed" not in command:
        command.append("--retry-failed")
    return command


def start_controller(unit: str, command: list[str]) -> None:
    state_directory = next(
        value.partition("=")[2] for value in command if value.startswith("--state-dir=")
    )
    properties = unit_properties(unit)
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
        if properties.get("SubState") == "running":
            time.sleep(POLL_SECONDS)
            continue
        return properties


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


def stop_controllers(controllers: tuple[str, ...]) -> dict[str, list[str]]:
    """Authenticate and stop all controllers, including bounded late arrivals."""

    commands: dict[str, list[str]] = {}
    pending = controllers
    while pending:
        if len(commands) + len(pending) > MAX_CONTROLLERS:
            raise RuntimeError("too many controller units appeared while pausing")
        # Authenticate the complete batch before mutating any unit in it.
        batch = {unit: command_from_journal(unit) for unit in pending}
        commands.update(batch)
        run(["/usr/bin/systemctl", "stop", *pending])
        remaining = controller_units(running_units())
        failed = tuple(unit for unit in remaining if unit in commands)
        if failed:
            raise RuntimeError(f"controller units failed to stop: {failed}")
        pending = tuple(unit for unit in remaining if unit not in commands)
    return commands


def protect_import(importer: str, controllers: tuple[str, ...]) -> None:
    commands = stop_controllers(controllers)
    print(
        f"paused {len(commands)} controller(s) for importer {importer}",
        flush=True,
    )
    result = wait_for_importer(importer)
    if controllers_deliberately_paused():
        print(
            f"importer {importer} ended with {result}; controllers remain deliberately paused",
            flush=True,
        )
        return
    failures = []
    for unit, command in commands.items():
        try:
            start_controller(unit, command)
        except Exception as error:
            failures.append(f"{unit}: {error}")
    if failures:
        raise RuntimeError("; ".join(failures))
    print(f"importer {importer} ended with {result}; controllers resumed", flush=True)


def main() -> None:
    while True:
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
