use std::{
    collections::HashSet,
    fs,
    path::{Component, Path},
    thread,
    time::Duration,
};

pub(crate) const GIB: u64 = 1024 * 1024 * 1024;

const DEFAULT_PROTECTED_MEMORY_BYTES: u64 = 400 * GIB;
const DEFAULT_MIN_SYSTEM_RESERVE_BYTES: u64 = 64 * GIB;
const DEFAULT_SYSTEM_RESERVE_PERCENT: u64 = 8;
const DEFAULT_OWNED_BUDGET_PERCENT: u64 = 90;
const DEFAULT_MAX_CONCURRENCY: usize = 3;
const DEFAULT_THREADS_PER_EPOCH: usize = 32;
const DEFAULT_UNKNOWN_EPOCH_MEMORY_BYTES: u64 = 128 * GIB;
const DEFAULT_UNKNOWN_EPOCH_BUDGET_PERCENT: u64 = 45;
const MIN_EPOCH_MEMORY_BYTES: u64 = 64 * GIB;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostTelemetry {
    pub(crate) memory_total_bytes: u64,
    pub(crate) memory_available_bytes: u64,
    pub(crate) logical_cpus: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CgroupMemorySnapshot {
    pub(crate) max_bytes: u64,
    pub(crate) current_bytes: u64,
    pub(crate) dedicated: bool,
}

impl HostTelemetry {
    fn is_reliable(self) -> bool {
        self.memory_total_bytes > 0
            && self.memory_available_bytes <= self.memory_total_bytes
            && self.logical_cpus > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EpochAdmissionPolicy {
    pub(crate) protected_memory_bytes: u64,
    pub(crate) min_system_reserve_bytes: u64,
    pub(crate) max_concurrency: usize,
    pub(crate) threads_per_epoch: usize,
    pub(crate) epoch_memory_reservation_bytes: Option<u64>,
}

impl Default for EpochAdmissionPolicy {
    fn default() -> Self {
        Self {
            // Leave room for a large protected co-resident workload. This is
            // configurable by the supervisor for each deployment.
            protected_memory_bytes: DEFAULT_PROTECTED_MEMORY_BYTES,
            min_system_reserve_bytes: DEFAULT_MIN_SYSTEM_RESERVE_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            // The supervisor replaces this with the configured worker cost;
            // this standalone default remains conservative for policy users.
            threads_per_epoch: DEFAULT_THREADS_PER_EPOCH,
            epoch_memory_reservation_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionSnapshot {
    pub(crate) telemetry_reliable: bool,
    pub(crate) system_reserve_bytes: u64,
    pub(crate) owned_budget_bytes: u64,
    pub(crate) normal_owned_limit_bytes: u64,
    pub(crate) epoch_reservation_bytes: u64,
    pub(crate) capacity: usize,
}

impl EpochAdmissionPolicy {
    /// Computes a static upper-bound reservation for each epoch and the number
    /// of epochs that may coexist. RSS is deliberately not an input: a young
    /// worker's current RSS is not a safe prediction of its eventual peak.
    ///
    /// Missing or malformed host telemetry fails closed to serial operation.
    pub(crate) fn evaluate(
        self,
        telemetry: Option<HostTelemetry>,
        running_epochs: usize,
    ) -> AdmissionSnapshot {
        let Some(telemetry) = telemetry.filter(|telemetry| telemetry.is_reliable()) else {
            return AdmissionSnapshot {
                telemetry_reliable: false,
                system_reserve_bytes: 0,
                owned_budget_bytes: 0,
                normal_owned_limit_bytes: 0,
                epoch_reservation_bytes: DEFAULT_UNKNOWN_EPOCH_MEMORY_BYTES,
                capacity: 1,
            };
        };

        let proportional_reserve =
            percent(telemetry.memory_total_bytes, DEFAULT_SYSTEM_RESERVE_PERCENT);
        let system_reserve_bytes = self.min_system_reserve_bytes.max(proportional_reserve);
        let owned_budget_bytes = telemetry
            .memory_total_bytes
            .saturating_sub(self.protected_memory_bytes)
            .saturating_sub(system_reserve_bytes);
        let normal_owned_limit_bytes = percent(owned_budget_bytes, DEFAULT_OWNED_BUDGET_PERCENT);
        let epoch_reservation_bytes = self
            .epoch_memory_reservation_bytes
            .unwrap_or_else(|| {
                percent(owned_budget_bytes, DEFAULT_UNKNOWN_EPOCH_BUDGET_PERCENT)
                    .min(DEFAULT_UNKNOWN_EPOCH_MEMORY_BYTES)
            })
            .max(MIN_EPOCH_MEMORY_BYTES);

        let memory_capacity = normal_owned_limit_bytes / epoch_reservation_bytes;
        let cpu_capacity = (telemetry.logical_cpus / self.threads_per_epoch.max(1)).max(1);
        let static_capacity = usize::try_from(memory_capacity)
            .unwrap_or(usize::MAX)
            .min(cpu_capacity)
            .min(self.max_concurrency.max(1));

        // MemAvailable already reflects co-resident allocations, so subtracting
        // the protected allowance from it would double-count those workloads.
        // It only needs to retain the host reserve plus the next epoch's full
        // static reservation.
        let live_additional_capacity = telemetry
            .memory_available_bytes
            .saturating_sub(system_reserve_bytes)
            / epoch_reservation_bytes;
        let live_capacity = running_epochs
            .saturating_add(usize::try_from(live_additional_capacity).unwrap_or(usize::MAX));
        let capacity = static_capacity.min(live_capacity);

        AdmissionSnapshot {
            telemetry_reliable: true,
            system_reserve_bytes,
            owned_budget_bytes,
            normal_owned_limit_bytes,
            epoch_reservation_bytes,
            capacity,
        }
    }
}

pub(crate) fn ramped_capacity(
    admitted_capacity: usize,
    running_epochs: usize,
    observation_elapsed: Duration,
    settle_interval: Duration,
) -> usize {
    if running_epochs == 0 {
        return admitted_capacity.min(1);
    }
    if observation_elapsed < settle_interval {
        admitted_capacity.min(running_epochs)
    } else {
        admitted_capacity.min(running_epochs.saturating_add(1))
    }
}

pub(crate) fn contained_capacity(
    admission: AdmissionSnapshot,
    cgroup: Option<CgroupMemorySnapshot>,
    baseline_bytes: u64,
    running_epochs: usize,
) -> usize {
    let Some(cgroup) = cgroup.filter(|snapshot| {
        snapshot.dedicated
            && snapshot.max_bytes <= admission.owned_budget_bytes
            && baseline_bytes <= snapshot.max_bytes
    }) else {
        return admission.capacity.min(1);
    };

    // Reserve the supervisor's pre-job footprint before assigning the finite
    // cgroup remainder to epoch workers. This prevents `memory.max / R` from
    // silently charging the parent's own memory to no reservation at all.
    let baseline_capacity =
        cgroup.max_bytes.saturating_sub(baseline_bytes) / admission.epoch_reservation_bytes;

    // `memory.current` is sampled again before every spawn. Existing workers
    // may consume less than their bound, but their unused reservation is not
    // lent to a new worker: the group must retain one complete additional
    // reservation before additive admission.
    let remaining_bytes = cgroup.max_bytes.saturating_sub(cgroup.current_bytes);
    let live_additional_capacity = remaining_bytes / admission.epoch_reservation_bytes;
    let live_capacity = running_epochs
        .saturating_add(usize::try_from(live_additional_capacity).unwrap_or(usize::MAX));
    let capacity = admission
        .capacity
        .min(usize::try_from(baseline_capacity).unwrap_or(usize::MAX))
        .min(live_capacity);

    // A qualified reservation is an asserted upper bound. If the managed
    // cohort has already crossed it, stop ramping immediately; never treat a
    // young worker's current RSS as proof that another one will fit.
    let observed_job_bytes = cgroup.current_bytes.saturating_sub(baseline_bytes);
    let running_reservation = admission
        .epoch_reservation_bytes
        .saturating_mul(u64::try_from(running_epochs).unwrap_or(u64::MAX));
    if running_epochs > 0 && observed_job_bytes > running_reservation {
        capacity.min(running_epochs)
    } else {
        capacity
    }
}

pub(crate) fn qualified_parallelism(
    requested: usize,
    explicit_memory_bound: bool,
    memory_bound_qualified: bool,
    explicit_disk_bound: bool,
    disk_bound_qualified: bool,
) -> usize {
    if explicit_memory_bound
        && memory_bound_qualified
        && explicit_disk_bound
        && disk_bound_qualified
    {
        requested.max(1)
    } else {
        1
    }
}

pub(crate) fn disk_allows_additional_epoch(
    available_bytes: Option<u64>,
    reservation_bytes: u64,
    running_epochs: usize,
) -> bool {
    let Some(available_bytes) = available_bytes else {
        return running_epochs == 0;
    };
    let required = reservation_bytes
        .saturating_mul(u64::try_from(running_epochs.saturating_add(1)).unwrap_or(u64::MAX));
    available_bytes >= required
}

fn percent(value: u64, percentage: u64) -> u64 {
    value.saturating_mul(percentage) / 100
}

pub(crate) fn read_host_telemetry() -> Option<HostTelemetry> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    let (memory_total_bytes, memory_available_bytes) = parse_meminfo(&meminfo)?;
    let logical_cpus = thread::available_parallelism().ok()?.get();
    let telemetry = HostTelemetry {
        memory_total_bytes,
        memory_available_bytes,
        logical_cpus,
    };
    telemetry.is_reliable().then_some(telemetry)
}

pub(crate) fn read_current_cgroup_memory(
    owned_process_groups: &[u32],
) -> Option<CgroupMemorySnapshot> {
    let membership = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = parse_unified_cgroup_path(&membership)?;
    read_cgroup_memory_at(
        Path::new("/sys/fs/cgroup"),
        relative,
        std::process::id(),
        owned_process_groups,
    )
}

fn read_cgroup_memory_at(
    root: &Path,
    relative: &str,
    supervisor_pid: u32,
    owned_process_groups: &[u32],
) -> Option<CgroupMemorySnapshot> {
    let mut cgroup = root.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => cgroup.push(component),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    // Bracket the max read with current reads and retain the larger value. The
    // files are not atomically snapshot-able, so this is the conservative
    // coherent observation from one resolved cgroup.
    let current_before =
        parse_memory_current(&fs::read_to_string(cgroup.join("memory.current")).ok()?)?;
    let max_bytes = parse_finite_memory_max(&fs::read_to_string(cgroup.join("memory.max")).ok()?)?;
    let current_after =
        parse_memory_current(&fs::read_to_string(cgroup.join("memory.current")).ok()?)?;
    let members = read_cgroup_members_recursive(&cgroup)?;
    let dedicated = cgroup_members_are_owned(
        supervisor_pid,
        owned_process_groups,
        &members
            .into_iter()
            .map(|pid| (pid, read_process_group(pid)))
            .collect::<Vec<_>>(),
    );
    Some(CgroupMemorySnapshot {
        max_bytes,
        current_bytes: current_before.max(current_after),
        dedicated,
    })
}

fn read_cgroup_members_recursive(root: &Path) -> Option<Vec<u32>> {
    let mut pending = vec![root.to_path_buf()];
    let mut members = Vec::new();
    while let Some(directory) = pending.pop() {
        let procs = fs::read_to_string(directory.join("cgroup.procs")).ok()?;
        for line in procs.lines() {
            members.push(line.trim().parse::<u32>().ok()?);
        }
        for entry in fs::read_dir(&directory).ok()? {
            let entry = entry.ok()?;
            if entry.file_type().ok()?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Some(members)
}

fn read_process_group(pid: u32) -> Option<u32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_process_group_from_stat(&stat)
}

fn parse_process_group_from_stat(stat: &str) -> Option<u32> {
    let command_end = stat.rfind(") ")?;
    // Fields after the command start at state (3), then ppid (4), pgrp (5).
    stat[command_end + 2..]
        .split_whitespace()
        .nth(2)?
        .parse()
        .ok()
}

fn cgroup_members_are_owned(
    supervisor_pid: u32,
    owned_process_groups: &[u32],
    members: &[(u32, Option<u32>)],
) -> bool {
    let owned = owned_process_groups.iter().copied().collect::<HashSet<_>>();
    members.iter().all(|(pid, process_group)| {
        *pid == supervisor_pid || process_group.is_some_and(|group| owned.contains(&group))
    })
}

fn parse_unified_cgroup_path(contents: &str) -> Option<&str> {
    contents.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        match (fields.next(), fields.next(), fields.next()) {
            (Some("0"), Some(""), Some(path)) if path.starts_with('/') => Some(path),
            _ => None,
        }
    })
}

fn parse_finite_memory_max(contents: &str) -> Option<u64> {
    let value = contents.trim();
    if value == "max" {
        return None;
    }
    value.parse::<u64>().ok().filter(|value| *value > 0)
}

fn parse_memory_current(contents: &str) -> Option<u64> {
    contents.trim().parse::<u64>().ok()
}

#[cfg(unix)]
pub(crate) fn read_filesystem_available_bytes(path: &Path) -> Option<u64> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: successful statvfs initialized the structure.
    let stats = unsafe { stats.assume_init() };
    stats.f_bavail.checked_mul(stats.f_frsize)
}

fn parse_meminfo(contents: &str) -> Option<(u64, u64)> {
    let mut total_kib = None;
    let mut available_kib = None;
    for line in contents.lines() {
        let (key, value) = line.split_once(':')?;
        if key != "MemTotal" && key != "MemAvailable" {
            continue;
        }
        let mut fields = value.split_whitespace();
        let kib = fields.next()?.parse::<u64>().ok()?;
        if fields.next()? != "kB" || fields.next().is_some() {
            return None;
        }
        match key {
            "MemTotal" => total_kib = Some(kib),
            "MemAvailable" => available_kib = Some(kib),
            _ => unreachable!(),
        }
    }
    let total_bytes = total_kib?.checked_mul(1024)?;
    let available_bytes = available_kib?.checked_mul(1024)?;
    (available_bytes <= total_bytes).then_some((total_bytes, available_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_755_gib_host_admits_at_most_two_epochs() {
        let snapshot = EpochAdmissionPolicy::default().evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 500 * GIB,
                logical_cpus: 64,
            }),
            0,
        );

        assert!(snapshot.telemetry_reliable);
        assert_eq!(snapshot.system_reserve_bytes, 64 * GIB);
        assert_eq!(snapshot.owned_budget_bytes, 291 * GIB);
        assert_eq!(snapshot.normal_owned_limit_bytes, 281_212_983_705);
        assert_eq!(snapshot.epoch_reservation_bytes, 128 * GIB);
        assert_eq!(snapshot.capacity, 2);
    }

    #[test]
    fn explicit_bounded_reservation_can_admit_three_epochs() {
        let snapshot = EpochAdmissionPolicy {
            threads_per_epoch: 16,
            epoch_memory_reservation_bytes: Some(64 * GIB),
            ..EpochAdmissionPolicy::default()
        }
        .evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 500 * GIB,
                logical_cpus: 64,
            }),
            0,
        );

        assert_eq!(snapshot.capacity, 3);
        assert_eq!(contained_capacity(snapshot, None, 0, 0), 1);
        assert_eq!(
            contained_capacity(
                snapshot,
                Some(CgroupMemorySnapshot {
                    max_bytes: snapshot.owned_budget_bytes + 1,
                    current_bytes: 0,
                    dedicated: true,
                }),
                0,
                0,
            ),
            1
        );
        assert_eq!(
            contained_capacity(
                snapshot,
                Some(CgroupMemorySnapshot {
                    max_bytes: 138 * GIB,
                    current_bytes: 10 * GIB,
                    dedicated: true,
                }),
                10 * GIB,
                0,
            ),
            2
        );
        assert_eq!(
            contained_capacity(
                snapshot,
                Some(CgroupMemorySnapshot {
                    max_bytes: 202 * GIB,
                    current_bytes: 10 * GIB,
                    dedicated: true,
                }),
                10 * GIB,
                0,
            ),
            3
        );
    }

    #[test]
    fn reliable_low_headroom_blocks_all_new_admission() {
        let snapshot = EpochAdmissionPolicy::default().evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 191 * GIB,
                logical_cpus: 64,
            }),
            0,
        );

        assert!(snapshot.telemetry_reliable);
        assert_eq!(snapshot.capacity, 0);
    }

    #[test]
    fn exact_live_headroom_admits_one_epoch() {
        let snapshot = EpochAdmissionPolicy::default().evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 192 * GIB,
                logical_cpus: 64,
            }),
            0,
        );

        assert_eq!(snapshot.capacity, 1);
    }

    #[test]
    fn cpu_capacity_prevents_oversubscription() {
        let snapshot = EpochAdmissionPolicy::default().evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 500 * GIB,
                logical_cpus: 32,
            }),
            0,
        );

        assert_eq!(snapshot.capacity, 1);
    }

    #[test]
    fn telemetry_failure_falls_back_to_serial() {
        let policy = EpochAdmissionPolicy::default();
        assert_eq!(policy.evaluate(None, 0).capacity, 1);
        assert_eq!(
            policy
                .evaluate(
                    Some(HostTelemetry {
                        memory_total_bytes: 0,
                        memory_available_bytes: 0,
                        logical_cpus: 64,
                    }),
                    0,
                )
                .capacity,
            1
        );
    }

    #[test]
    fn live_headroom_is_charged_only_for_an_additional_epoch() {
        let snapshot = EpochAdmissionPolicy::default().evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 192 * GIB,
                logical_cpus: 64,
            }),
            1,
        );

        assert_eq!(snapshot.capacity, 2);
    }

    #[test]
    fn concurrency_ramps_only_after_the_observation_interval() {
        let settle = Duration::from_secs(60);
        assert_eq!(ramped_capacity(2, 0, Duration::ZERO, settle), 1);
        assert_eq!(ramped_capacity(2, 1, Duration::from_secs(59), settle), 1);
        assert_eq!(ramped_capacity(2, 1, settle, settle), 2);
        assert_eq!(ramped_capacity(3, 1, settle, settle), 2);
        assert_eq!(ramped_capacity(3, 2, settle, settle), 3);
        assert_eq!(ramped_capacity(0, 0, settle, settle), 0);
    }

    #[test]
    fn parses_only_finite_unified_cgroup_limits() {
        let fixture = "11:memory:/legacy\n0::/managed/jetstreamer\n";
        assert_eq!(
            parse_unified_cgroup_path(fixture),
            Some("/managed/jetstreamer")
        );
        assert_eq!(
            parse_finite_memory_max("274877906944\n"),
            Some(274_877_906_944)
        );
        assert_eq!(parse_finite_memory_max("max\n"), None);
        assert_eq!(parse_finite_memory_max("0\n"), None);
        assert_eq!(parse_memory_current("0\n"), Some(0));
        assert_eq!(parse_memory_current("invalid\n"), None);
        assert_eq!(parse_unified_cgroup_path("2:memory:/legacy\n"), None);
    }

    #[test]
    fn cgroup_capacity_reserves_baseline_and_live_headroom() {
        let admission = EpochAdmissionPolicy {
            threads_per_epoch: 16,
            epoch_memory_reservation_bytes: Some(64 * GIB),
            ..EpochAdmissionPolicy::default()
        }
        .evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 500 * GIB,
                logical_cpus: 64,
            }),
            0,
        );
        let baseline = 10 * GIB;

        assert_eq!(
            contained_capacity(
                admission,
                Some(CgroupMemorySnapshot {
                    max_bytes: 137 * GIB,
                    current_bytes: baseline,
                    dedicated: true,
                }),
                baseline,
                0,
            ),
            1,
            "baseline + two reservations does not fit"
        );
        assert_eq!(
            contained_capacity(
                admission,
                Some(CgroupMemorySnapshot {
                    max_bytes: 202 * GIB,
                    current_bytes: 139 * GIB,
                    dedicated: true,
                }),
                baseline,
                1,
            ),
            1,
            "less than one full live reservation refuses another child"
        );
    }

    #[test]
    fn cgroup_over_reservation_freezes_the_ramp() {
        let admission = EpochAdmissionPolicy {
            threads_per_epoch: 16,
            epoch_memory_reservation_bytes: Some(64 * GIB),
            ..EpochAdmissionPolicy::default()
        }
        .evaluate(
            Some(HostTelemetry {
                memory_total_bytes: 755 * GIB,
                memory_available_bytes: 500 * GIB,
                logical_cpus: 64,
            }),
            1,
        );
        let baseline = 10 * GIB;
        assert_eq!(
            contained_capacity(
                admission,
                Some(CgroupMemorySnapshot {
                    max_bytes: 202 * GIB,
                    current_bytes: 75 * GIB,
                    dedicated: true,
                }),
                baseline,
                1,
            ),
            1,
            "65 GiB observed for a 64 GiB bound must stop additive admission"
        );
    }

    #[test]
    fn parallelism_requires_explicit_qualified_memory_and_disk_bounds() {
        assert_eq!(qualified_parallelism(3, false, false, false, false), 1);
        assert_eq!(qualified_parallelism(3, true, false, true, true), 1);
        assert_eq!(qualified_parallelism(3, true, true, true, false), 1);
        assert_eq!(qualified_parallelism(3, true, true, true, true), 3);
    }

    #[test]
    fn disk_gate_reserves_running_jobs_and_the_next_child() {
        assert!(disk_allows_additional_epoch(Some(3 * GIB), GIB, 2));
        assert!(!disk_allows_additional_epoch(Some(3 * GIB - 1), GIB, 2));
        assert!(disk_allows_additional_epoch(None, GIB, 0));
        assert!(!disk_allows_additional_epoch(None, GIB, 1));
    }

    #[test]
    fn reads_cgroup_current_and_max_from_one_resolved_directory() {
        let root = tempfile::TempDir::new().unwrap();
        let group = root.path().join("managed/epochs");
        fs::create_dir_all(&group).unwrap();
        fs::write(group.join("memory.max"), "2048\n").unwrap();
        fs::write(group.join("memory.current"), "1024\n").unwrap();
        fs::write(group.join("cgroup.procs"), "").unwrap();

        assert_eq!(
            read_cgroup_memory_at(root.path(), "/managed/epochs", 99, &[]),
            Some(CgroupMemorySnapshot {
                max_bytes: 2048,
                current_bytes: 1024,
                dedicated: true,
            })
        );
        assert_eq!(
            read_cgroup_memory_at(root.path(), "/../escape", 99, &[]),
            None
        );
    }

    #[test]
    fn shared_cgroup_membership_is_rejected() {
        let supervisor = 100;
        let owned_group = 200;
        assert!(cgroup_members_are_owned(
            supervisor,
            &[owned_group],
            &[(supervisor, Some(100)), (201, Some(owned_group))],
        ));
        assert!(!cgroup_members_are_owned(
            supervisor,
            &[owned_group],
            &[
                (supervisor, Some(100)),
                (201, Some(owned_group)),
                (301, Some(300))
            ],
        ));
        assert!(!cgroup_members_are_owned(
            supervisor,
            &[owned_group],
            &[(supervisor, Some(100)), (201, None)],
        ));
    }

    #[test]
    fn parses_process_group_after_parenthesized_command() {
        assert_eq!(
            parse_process_group_from_stat("123 (worker with spaces) S 7 456 0 0"),
            Some(456)
        );
    }

    #[test]
    fn parses_linux_meminfo_in_kib() {
        let contents =
            "MemTotal:       2048 kB\nMemFree:         256 kB\nMemAvailable:   1024 kB\n";
        assert_eq!(parse_meminfo(contents), Some((2_097_152, 1_048_576)));
        assert_eq!(parse_meminfo("MemTotal: 2048 kB\n"), None);
        assert_eq!(
            parse_meminfo("MemTotal: 2048 MB\nMemAvailable: 1024 kB\n"),
            None
        );
    }
}
