//! System capability helpers for sizing the firehose runtime.
use std::cmp;
/// Environment variable that overrides detected network throughput in megabytes.
const NETWORK_CAPACITY_OVERRIDE_ENV: &str = "JETSTREAMER_NETWORK_CAPACITY_MB";
const DEFAULT_NETWORK_CAPACITY_MB: u64 = 1_000;

/// Calculates an optimal number of firehose threads for the current machine.
///
/// The heuristic picks whichever constraint is tighter between CPU availability
/// and network capacity using:
///
/// `min(num_cpu_cores * 4, network_interface_bandwidth_capacity_megabytes / 285)`
///
/// The returned thread count is always in the inclusive range
/// `[1, num_cpu_cores * 4]`. The network capacity defaults to an assumed
/// 1,000 MB/s link unless overridden via the
/// `JETSTREAMER_NETWORK_CAPACITY_MB` environment variable.
#[inline]
pub fn optimal_firehose_thread_count() -> usize {
    compute_optimal_thread_count(detect_cpu_core_count(), detect_network_capacity_megabytes())
}

#[inline(always)]
fn detect_cpu_core_count() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

#[inline(always)]
fn detect_network_capacity_megabytes() -> Option<u64> {
    network_capacity_override().or(Some(DEFAULT_NETWORK_CAPACITY_MB))
}

fn network_capacity_override() -> Option<u64> {
    std::env::var(NETWORK_CAPACITY_OVERRIDE_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[inline(always)]
fn compute_optimal_thread_count(
    cpu_cores: usize,
    network_capacity_megabytes: Option<u64>,
) -> usize {
    let cpu_limited = cmp::max(1, cpu_cores.saturating_mul(4));

    if let Some(capacity) = network_capacity_megabytes.filter(|value| *value > 0) {
        let network_limited = cmp::max(1u64, capacity / 250);
        cmp::min(cpu_limited as u64, network_limited)
            .max(1u64)
            .min(usize::MAX as u64) as usize
    } else {
        cpu_limited
    }
}

#[cfg(test)]
mod tests {
    use super::{NETWORK_CAPACITY_OVERRIDE_ENV, compute_optimal_thread_count};
    use std::env;

    #[test]
    fn cpu_bound_when_network_unknown() {
        assert_eq!(compute_optimal_thread_count(8, None), 32);
    }

    #[test]
    fn network_bottleneck_limits_threads() {
        let cpu_cores = 32;
        let network_capacity_mb = Some(2_850);
        assert_eq!(
            compute_optimal_thread_count(cpu_cores, network_capacity_mb),
            11
        );
    }

    #[test]
    fn cpu_bottleneck_limits_threads() {
        let cpu_cores = 4;
        let network_capacity_mb = Some(100_000); // network allows way more threads
        assert_eq!(
            compute_optimal_thread_count(cpu_cores, network_capacity_mb),
            16
        );
    }

    #[test]
    fn minimum_thread_floor() {
        assert_eq!(compute_optimal_thread_count(1, Some(10)), 1);
    }

    #[test]
    fn override_env_takes_precedence() {
        let high_guard = EnvGuard::set(NETWORK_CAPACITY_OVERRIDE_ENV, "1000");
        let high_capacity = super::detect_network_capacity_megabytes();
        let high_threads = super::optimal_firehose_thread_count();
        drop(high_guard);

        let low_guard = EnvGuard::set(NETWORK_CAPACITY_OVERRIDE_ENV, "10");
        let low_capacity = super::detect_network_capacity_megabytes();
        let low_threads = super::optimal_firehose_thread_count();
        drop(low_guard);

        assert!(high_capacity.unwrap() >= low_capacity.unwrap());
        assert!(high_threads >= low_threads);
    }

    #[test]
    fn override_env_invalid_values_are_ignored() {
        let guard = EnvGuard::set(NETWORK_CAPACITY_OVERRIDE_ENV, "not-a-number");
        assert_eq!(super::network_capacity_override(), None);
        drop(guard);
    }

    #[test]
    fn default_capacity_matches_expected() {
        let guard = EnvGuard::unset(NETWORK_CAPACITY_OVERRIDE_ENV);
        assert_eq!(
            super::detect_network_capacity_megabytes(),
            Some(super::DEFAULT_NETWORK_CAPACITY_MB)
        );
        drop(guard);
    }

    struct EnvGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = env::var(key).ok();
            unsafe {
                env::set_var(key, value);
            }
            Self { key, original }
        }

        fn unset(key: &'static str) -> Self {
            let original = env::var(key).ok();
            unsafe {
                env::remove_var(key);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                unsafe {
                    env::set_var(self.key, value);
                }
            } else {
                unsafe {
                    env::remove_var(self.key);
                }
            }
        }
    }
}
