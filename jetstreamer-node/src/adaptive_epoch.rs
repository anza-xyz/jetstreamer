use std::{
    fs,
    path::{Component, Path, PathBuf},
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
    cgroup_memory_max: Option<u64>,
) -> usize {
    let Some(limit) = cgroup_memory_max.filter(|limit| *limit <= admission.owned_budget_bytes)
    else {
        return admission.capacity.min(1);
    };
    let cgroup_capacity = limit / admission.epoch_reservation_bytes;
    admission
        .capacity
        .min(usize::try_from(cgroup_capacity).unwrap_or(usize::MAX))
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

pub(crate) fn read_current_cgroup_memory_max() -> Option<u64> {
    let membership = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = parse_unified_cgroup_path(&membership)?;
    let mut memory_max = PathBuf::from("/sys/fs/cgroup");
    for component in Path::new(relative).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => memory_max.push(component),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    memory_max.push("memory.max");
    parse_finite_memory_max(&fs::read_to_string(memory_max).ok()?)
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
        assert_eq!(contained_capacity(snapshot, None), 1);
        assert_eq!(
            contained_capacity(snapshot, Some(snapshot.owned_budget_bytes + 1)),
            1
        );
        assert_eq!(contained_capacity(snapshot, Some(128 * GIB)), 2);
        assert_eq!(contained_capacity(snapshot, Some(192 * GIB)), 3);
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
        assert_eq!(parse_unified_cgroup_path("2:memory:/legacy\n"), None);
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
