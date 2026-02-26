//! System capability helpers for sizing the firehose runtime.
use std::cmp;
/// Environment variable that overrides detected network throughput in megabytes.
const NETWORK_CAPACITY_OVERRIDE_ENV: &str = "JETSTREAMER_NETWORK_CAPACITY_MB";
const DEFAULT_NETWORK_CAPACITY_MB: u64 = 1_000;
const DEFAULT_BUFFER_WINDOW_FALLBACK_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_BUFFER_WINDOW_PERCENT_NUMERATOR: u64 = 15;
const DEFAULT_BUFFER_WINDOW_PERCENT_DENOMINATOR: u64 = 100;
const DEFAULT_BUFFER_WINDOW_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

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

/// Returns the default ripget sequential download window size in bytes.
///
/// The default is the lower of 15% of detected available RAM and 4 GiB, falling back to 512 MiB
/// when RAM cannot be detected.
#[inline]
pub fn default_firehose_buffer_window_bytes() -> u64 {
    default_buffer_window_bytes()
}

/// Parses a human-readable byte size (for example `4GiB`, `512mb`, or `1073741824`).
///
/// Returns `None` when the input is invalid or smaller than two bytes.
#[inline]
pub fn parse_buffer_window_bytes(value: &str) -> Option<u64> {
    parse_byte_size(value).filter(|parsed| *parsed >= 2)
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

fn default_buffer_window_bytes() -> u64 {
    let computed = detect_available_memory_bytes()
        .map(compute_default_buffer_window_bytes)
        .unwrap_or(DEFAULT_BUFFER_WINDOW_FALLBACK_BYTES);
    computed.max(2)
}

#[inline(always)]
fn compute_default_buffer_window_bytes(available_memory_bytes: u64) -> u64 {
    let window = (available_memory_bytes as u128)
        .saturating_mul(DEFAULT_BUFFER_WINDOW_PERCENT_NUMERATOR as u128)
        / (DEFAULT_BUFFER_WINDOW_PERCENT_DENOMINATOR as u128);
    window.min(DEFAULT_BUFFER_WINDOW_MAX_BYTES as u128) as u64
}

fn parse_byte_size(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let split_idx = trimmed
        .char_indices()
        .find_map(|(idx, ch)| {
            if ch.is_ascii_digit() || ch == '_' {
                None
            } else {
                Some(idx)
            }
        })
        .unwrap_or(trimmed.len());
    let (number_part, suffix_part) = trimmed.split_at(split_idx);
    let number: u64 = number_part.replace('_', "").parse().ok()?;
    let suffix = suffix_part.trim().to_ascii_lowercase();

    let multiplier = match suffix.as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1_000u64,
        "m" | "mb" => 1_000_000u64,
        "g" | "gb" => 1_000_000_000u64,
        "ki" | "kib" => 1_024u64,
        "mi" | "mib" => 1_048_576u64,
        "gi" | "gib" => 1_073_741_824u64,
        _ => return None,
    };
    number.checked_mul(multiplier)
}

#[cfg(target_os = "linux")]
fn detect_available_memory_bytes() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::sysinfo>::uninit();
    let rc = unsafe { libc::sysinfo(info.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let info = unsafe { info.assume_init() };
    let freeram = u64::try_from(info.freeram).ok()?;
    let mem_unit = u64::from(info.mem_unit.max(1));
    freeram.checked_mul(mem_unit)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn detect_available_memory_bytes() -> Option<u64> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    // `_SC_AVPHYS_PAGES` is not available on all Unix targets (including macOS), so use
    // physical pages as a portable fallback for default sizing.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    if pages <= 0 {
        return None;
    }
    let bytes = (pages as u128).saturating_mul(page_size as u128);
    Some(bytes.min(u64::MAX as u128) as u64)
}

#[cfg(not(unix))]
fn detect_available_memory_bytes() -> Option<u64> {
    None
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
    use super::{
        NETWORK_CAPACITY_OVERRIDE_ENV, compute_default_buffer_window_bytes,
        compute_optimal_thread_count, parse_buffer_window_bytes, parse_byte_size,
    };
    use serial_test::serial;
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
    #[serial]
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
    #[serial]
    fn override_env_invalid_values_are_ignored() {
        let guard = EnvGuard::set(NETWORK_CAPACITY_OVERRIDE_ENV, "not-a-number");
        assert_eq!(super::network_capacity_override(), None);
        drop(guard);
    }

    #[test]
    #[serial]
    fn default_capacity_matches_expected() {
        let guard = EnvGuard::unset(NETWORK_CAPACITY_OVERRIDE_ENV);
        assert_eq!(
            super::detect_network_capacity_megabytes(),
            Some(super::DEFAULT_NETWORK_CAPACITY_MB)
        );
        drop(guard);
    }

    #[test]
    fn computes_default_window_from_available_ram() {
        // 16 GiB -> 2.4 GiB (15%)
        let available = 16u64 * 1024 * 1024 * 1024;
        let expected = 2_576_980_377u64;
        assert_eq!(compute_default_buffer_window_bytes(available), expected);
    }

    #[test]
    fn caps_default_window_at_four_gib() {
        // 64 GiB -> 9.6 GiB (15%), capped to 4 GiB.
        let available = 64u64 * 1024 * 1024 * 1024;
        let expected = 4u64 * 1024 * 1024 * 1024;
        assert_eq!(compute_default_buffer_window_bytes(available), expected);
    }

    #[test]
    fn parses_human_readable_buffer_window_values() {
        assert_eq!(parse_byte_size("123"), Some(123));
        assert_eq!(parse_byte_size("256mb"), Some(256_000_000));
        assert_eq!(parse_byte_size("256MiB"), Some(268_435_456));
        assert_eq!(parse_byte_size("1_024"), Some(1024));
    }

    #[test]
    fn parse_buffer_window_rejects_invalid_values() {
        assert_eq!(parse_buffer_window_bytes("nope"), None);
        assert_eq!(parse_buffer_window_bytes("1"), None);
    }

    #[test]
    fn default_buffer_window_is_nonzero() {
        assert!(super::default_firehose_buffer_window_bytes() >= 2);
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
