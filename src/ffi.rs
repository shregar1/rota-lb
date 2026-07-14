//! FFI bindings for `rota`.
//!
//! Exposes a C ABI for use from Bun, Python, or any language with FFI
//! support. Thread-safe and panic-safe.
//!
//! **Note on tracing**: This module emits `tracing` events (`warn!` / `error!`)
//! on null pointers, mutex poisoning, and invalid inputs. The host process
//! must initialise a [`tracing_subscriber`] (e.g. `fmt().init()`) for these
//! events to appear in logs. If no subscriber is registered the events are
//! silently discarded — this is safe but makes debugging harder.

// The FFI surface intentionally uses unsafe, no_mangle, and pub functions
// that return pointers to private types — allow the relevant lints.
#![allow(
    clippy::missing_safety_doc,
    clippy::not_unsafe_ptr_arg_deref,
    missing_docs,
    unsafe_code,
    private_interfaces,
    clippy::cargo_common_metadata
)]

use std::ffi::{CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;
use std::time::Duration;

use crate::constants::STRATEGY_NAMES;
use crate::strategies::{
    Failover, HashByAddr, HealthWeighted, LeastConnections, LowestRtt, Random, RoundRobin, Sticky,
    WeightedRoundRobin,
};
use crate::traits::strategy::TunnelMetrics;
use crate::traits::strategy::{BalanceStrategy, PoolView};

// ============================================================================
//  Canonical strategy enum — single source of truth for variant mapping
// ============================================================================

/// Every load-balancing strategy exposed through the C ABI.
///
/// Add a new variant here, then add the corresponding `return` in `build()`
/// and `name()`. The C-side `strategy_kind` integer (`i32`) must match the
/// discriminant value exactly.
#[repr(i32)]
#[derive(Clone, Copy, Debug)]
enum FfiStrategy {
    RoundRobin = 0,
    Random = 1,
    LowestRtt = 2,
    LeastConnections = 3,
    HashByAddr = 4,
    WeightedRoundRobin = 5,
    Failover = 6,
    HealthWeighted = 7,
    Sticky = 8,
}

impl FfiStrategy {
    const fn from_i32(n: i32) -> Option<Self> {
        match n {
            0 => Some(Self::RoundRobin),
            1 => Some(Self::Random),
            2 => Some(Self::LowestRtt),
            3 => Some(Self::LeastConnections),
            4 => Some(Self::HashByAddr),
            5 => Some(Self::WeightedRoundRobin),
            6 => Some(Self::Failover),
            7 => Some(Self::HealthWeighted),
            8 => Some(Self::Sticky),
            _ => None,
        }
    }

    fn build(self, backend_count: usize) -> Box<dyn BalanceStrategy + Send> {
        // The cast below depends on the highest discriminant matching the
        // length of `STRATEGY_NAMES`. If a new variant is added without
        // updating `STRATEGY_NAMES` (or vice versa), the FFI name cache
        // and `name()` indexing would silently mismatch or panic. Catch
        // that at compile time.
        const _: () = assert!(
            STRATEGY_NAMES.len() == (FfiStrategy::Sticky as usize + 1),
            "STRATEGY_NAMES length must match FfiStrategy variant count"
        );
        match self {
            Self::RoundRobin => Box::new(RoundRobin::new()),
            Self::Random => Box::new(Random::new()),
            Self::LowestRtt => Box::new(LowestRtt::new()),
            Self::LeastConnections => Box::new(LeastConnections::new()),
            Self::HashByAddr => Box::new(HashByAddr::new()),
            Self::WeightedRoundRobin => Box::new(WeightedRoundRobin::new()),
            Self::Failover => Box::new(Failover::with_backend_count(backend_count)),
            Self::HealthWeighted => Box::new(HealthWeighted::new()),
            Self::Sticky => Box::new(Sticky::new()),
        }
    }

    fn name(self) -> &'static str {
        STRATEGY_NAMES[self as usize]
    }
}

// ============================================================================
//  Version
// ============================================================================

/// Library version and ABI metadata. Returned by `rota_lb_version`.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct RotaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    /// Expected size of the `rota_metric` struct. TS validates this on load.
    pub metric_struct_size: u32,
}

/// Flat C-compatible metric struct (32 bytes). TS sends these; we convert
/// to `TunnelMetrics` internally.
#[derive(Clone, Copy)]
#[repr(C)]
struct FfiMetric {
    rtt_us: u64, // 0 = unknown
    active_connections: u32,
    recent_errors: u32,
    total_dials: u64,
    total_errors: u64,
}

const _: () = assert!(size_of::<FfiMetric>() == 32);

const fn ffi_to_tunnel(m: &FfiMetric) -> TunnelMetrics {
    TunnelMetrics {
        rtt: if m.rtt_us == 0 {
            None
        } else {
            Some(Duration::from_micros(m.rtt_us))
        },
        active_connections: m.active_connections,
        recent_errors: m.recent_errors,
        total_dials: m.total_dials,
        total_errors: m.total_errors,
    }
}

const VERSION: RotaVersion = RotaVersion {
    major: 0,
    minor: 1,
    patch: 0,
    metric_struct_size: 32,
};

// ============================================================================
//  Opaque handle
// ============================================================================

struct FfiLoadBalancer {
    strategy: Mutex<Box<dyn BalanceStrategy + Send>>,
    backend_count: usize,
    strategy_kind: FfiStrategy,
    /// Pre-allocated buffer to avoid per-call Vec allocation in `select()`.
    scratch: Mutex<Vec<TunnelMetrics>>,
}

/// Compile-time assertion: `FfiLoadBalancer` must be Send + Sync.
#[cfg(test)]
#[test]
fn ffi_lb_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<FfiLoadBalancer>();
    assert_sync::<FfiLoadBalancer>();
}

// ============================================================================
//  Panic-safe wrappers
// ============================================================================

/// Catches panics from the inner closure and returns `$default`.
///
/// # Safety
/// - The closure must not contain values that implement `Drop` whose
///   invariants could be violated by being dropped after a panic.
///   In particular, no lock guards (which may hold mutex state) may be
///   stored across the unwind boundary.
/// - `$default` must be `Send + Sync` (required by `AssertUnwindSafe`).
macro_rules! catch_panic {
    ($body:expr, $default:expr) => {
        match catch_unwind(AssertUnwindSafe(|| $body)) {
            Ok(v) => v,
            Err(_) => $default,
        }
    };
}

// ============================================================================
//  Public C ABI
// ============================================================================

/// Write version metadata into a caller-provided buffer.
///
/// The buffer must be 16 bytes (4 × u32): major, minor, patch,
/// `metric_struct_size`. Safe to call before any other FFI function.
///
/// # Returns
/// 0 on success, -1 if `out` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_version(out: *mut RotaVersion) -> i32 {
    catch_panic!(
        {
            if out.is_null() {
                tracing::warn!("rota_lb_version: null output buffer");
                return -1;
            }
            unsafe { *out = VERSION };
            0
        },
        -1
    )
}

/// Create a load balancer.
///
/// Returns an opaque pointer, or null on failure. Must be freed with
/// `rota_lb_destroy`.
///
/// `strategy_kind` — see [`FfiStrategy`] for valid discriminants.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_create(n: u32, strategy_kind: i32) -> *mut FfiLoadBalancer {
    catch_panic!(
        {
            if n == 0 {
                tracing::warn!("rota_lb_create: n=0, returning null");
                return std::ptr::null_mut();
            }
            let Some(kind) = FfiStrategy::from_i32(strategy_kind) else {
                tracing::warn!(strategy_kind, "rota_lb_create: unknown strategy");
                return std::ptr::null_mut();
            };
            let strategy = kind.build(n as usize);
            Box::into_raw(Box::new(FfiLoadBalancer {
                strategy: Mutex::new(strategy),
                backend_count: n as usize,
                strategy_kind: kind,
                scratch: Mutex::new(Vec::with_capacity(n as usize)),
            }))
        },
        std::ptr::null_mut()
    )
}

/// Free a load balancer. Safe to call with null pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_destroy(lb: *mut FfiLoadBalancer) {
    catch_panic!(
        {
            if !lb.is_null() {
                drop(Box::from_raw(lb));
            }
        },
        ()
    );
}

/// Select a backend index.
///
/// # Parameters
/// - `lb`: opaque handle from `rota_lb_create`
/// - `dial_addr`: null-terminated UTF-8 string
/// - `metrics`: array of `n` `rota_metric` structs (32 bytes each)
/// - `n`: number of backends
///
/// # Returns
/// Index in `[0, n)` on success, `u32::MAX` on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_select(
    lb: *mut FfiLoadBalancer,
    dial_addr: *const std::ffi::c_char,
    metrics: *const FfiMetric,
    n: u32,
) -> u32 {
    if lb.is_null() || dial_addr.is_null() || metrics.is_null() {
        tracing::warn!("rota_lb_select: null argument(s)");
        return u32::MAX;
    }
    catch_panic!({ select_impl(&*lb, dial_addr, metrics, n) }, u32::MAX)
}

#[allow(clippy::significant_drop_tightening)]
unsafe fn select_impl(
    lb: &FfiLoadBalancer,
    dial_addr: *const std::ffi::c_char,
    metrics: *const FfiMetric,
    n: u32,
) -> u32 {
    let Ok(addr) = (unsafe { CStr::from_ptr(dial_addr) }).to_str() else {
        tracing::warn!("rota_lb_select: invalid dial_addr");
        return u32::MAX;
    };
    let count = n as usize;
    if count == 0 || count != lb.backend_count {
        tracing::warn!(
            count,
            expected = lb.backend_count,
            "rota_lb_select: metric count mismatch"
        );
        return u32::MAX;
    }
    let ffi_slice = unsafe { std::slice::from_raw_parts(metrics, count) };

    let Ok(mut scratch) = lb.scratch.lock() else {
        tracing::error!("rota_lb_select: scratch mutex poisoned");
        return u32::MAX;
    };
    scratch.clear();
    scratch.extend(ffi_slice.iter().map(ffi_to_tunnel));

    let view = PoolView {
        dial_addr: addr,
        metrics: &scratch,
    };

    lb.strategy.lock().map_or_else(
        |_| {
            tracing::error!("rota_lb_select: strategy mutex poisoned");
            u32::MAX
        },
        |mut s| u32::try_from(s.pick(&view)).unwrap_or(u32::MAX),
    )
}

/// Report a dial error. Rotates failover primary, updates health scores, etc.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_report_error(lb: *mut FfiLoadBalancer, idx: u32) {
    catch_panic!(
        {
            if lb.is_null() {
                tracing::warn!("rota_lb_report_error: null lb");
                return;
            }
            let lb = &*lb;
            if let Ok(mut s) = lb.strategy.lock() {
                s.report_error(idx as usize);
            } else {
                tracing::error!("rota_lb_report_error: strategy mutex poisoned");
            }
        },
        ()
    );
}

/// Report a successful dial. No-op for strategies that don't track success
/// (the default impl). Lets `Sticky` and custom strategies get positive
/// confirmation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_report_success(lb: *mut FfiLoadBalancer, idx: u32) {
    catch_panic!(
        {
            if lb.is_null() {
                tracing::warn!("rota_lb_report_success: null lb");
                return;
            }
            let lb = &*lb;
            if let Ok(mut s) = lb.strategy.lock() {
                s.report_success(idx as usize);
            } else {
                tracing::error!("rota_lb_report_success: strategy mutex poisoned");
            }
        },
        ()
    );
}

/// Get the human-readable strategy name.
///
/// The returned pointer is valid for the process lifetime (backed by a
/// static cache). Do NOT free it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_strategy_name(
    lb: *mut FfiLoadBalancer,
) -> *const std::ffi::c_char {
    // Static cache: one CString per variant, sized to match the enum.
    // Indexed by the strategy's discriminant so we never need a lock.
    use std::sync::OnceLock;
    static CACHE: OnceLock<Box<[CString]>> = OnceLock::new();
    let names = CACHE.get_or_init(|| {
        (0..=FfiStrategy::Sticky as isize)
            .filter_map(|i| i32::try_from(i).ok().and_then(FfiStrategy::from_i32))
            .map(|s| CString::new(s.name()).unwrap())
            .collect()
    });

    catch_panic!(
        {
            if lb.is_null() {
                tracing::warn!("rota_lb_strategy_name: null lb");
                return names[0].as_ptr();
            }
            let lb = &*lb;
            names[lb.strategy_kind as usize].as_ptr()
        },
        names[0].as_ptr()
    )
}

/// Read the current per-backend metrics back into a caller buffer.
///
/// The `out` buffer must be `backend_count * 32` bytes, holding the same
/// `rota_metric` layout used in `rota_lb_select`. The metrics reflect what
/// the Rust side has observed since the load balancer was created (some
/// strategies like `HealthWeighted` mutate them in response to error
/// reports; others like `RoundRobin` keep them as the caller passed in).
///
/// If no `select` has been called yet (or `reset` was just called), the
/// buffer is filled with zeros.
///
/// # Returns
/// 0 on success, -1 on invalid arguments.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_metrics(lb: *mut FfiLoadBalancer, out: *mut FfiMetric) -> i32 {
    catch_panic!(
        {
            if lb.is_null() || out.is_null() {
                tracing::warn!("rota_lb_metrics: null lb or out");
                return -1;
            }
            let lb = &*lb;

            let out_slice = std::slice::from_raw_parts_mut(out, lb.backend_count);

            // Snapshot scratch under the lock, then copy out after dropping.
            // On poison, fall back to an empty snapshot so the caller still
            // gets well-formed (zeroed) metrics rather than an error.
            let snapshot: Vec<TunnelMetrics> = lb.scratch.lock().map_or_else(
                |e| {
                    tracing::error!("rota_lb_metrics: scratch mutex poisoned: {e}");
                    Vec::new()
                },
                |g| g.clone(),
            );

            for (i, out_entry) in out_slice.iter_mut().enumerate() {
                *out_entry = snapshot.get(i).map_or(
                    FfiMetric {
                        rtt_us: 0,
                        active_connections: 0,
                        recent_errors: 0,
                        total_dials: 0,
                        total_errors: 0,
                    },
                    |m| FfiMetric {
                        // Duration::as_micros returns u128. Cap at u64::MAX
                        // rather than truncating — only happens after ~580k
                        // years of duration, but we want the cast to be
                        // explicit so a future refactor doesn't accidentally
                        // wrap on a real-world RTT.
                        rtt_us: m
                            .rtt
                            .map_or(0, |d| d.as_micros().try_into().unwrap_or(u64::MAX)),
                        active_connections: m.active_connections,
                        recent_errors: m.recent_errors,
                        total_dials: m.total_dials,
                        total_errors: m.total_errors,
                    },
                );
            }
            0
        },
        -1
    )
}

/// Hot-swap the strategy on an existing load balancer.
///
/// The `strategy_kind` integer must be a valid `FfiStrategy` discriminant
/// (0..=8). On success, the next `rota_lb_select` uses the new strategy.
/// The previous strategy's state is discarded; new strategies start
/// fresh.
///
/// This is useful for runtime tuning (e.g., switching from `RoundRobin`
/// to `LeastConnections` when load profiles change) without recreating
/// the load balancer and losing any cached references.
///
/// # Returns
/// 0 on success, -1 if lb is null or `strategy_kind` is unknown.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_set_strategy(lb: *mut FfiLoadBalancer, strategy_kind: i32) -> i32 {
    catch_panic!(
        {
            if lb.is_null() {
                tracing::warn!("rota_lb_set_strategy: null lb");
                return -1;
            }
            let Some(kind) = FfiStrategy::from_i32(strategy_kind) else {
                tracing::warn!(strategy_kind, "rota_lb_set_strategy: unknown strategy",);
                return -1;
            };
            let lb = &mut *lb;
            lb.strategy.lock().map_or_else(
                |_| {
                    tracing::error!("rota_lb_set_strategy: strategy mutex poisoned");
                    -1
                },
                |mut s| {
                    let from = format!("{:?}", lb.strategy_kind);
                    *s = kind.build(lb.backend_count);
                    lb.strategy_kind = kind;
                    tracing::info!("rota strategy swapped: {from} -> {kind:?}");
                    0
                },
            )
        },
        -1
    )
}

/// Reset the strategy's internal state.
///
/// `RoundRobin.next`, `Failover.primary`, `Sticky.pinned` etc. are reset to
/// their initial values. The strategy kind itself is unchanged.
///
/// Useful for:
/// - Test fixtures that need a clean slate
/// - Operational recovery after a transient error storm
/// - Forcing re-evaluation when topology changes
///
/// # Returns
/// 0 on success, -1 if lb is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_reset(lb: *mut FfiLoadBalancer) -> i32 {
    catch_panic!(
        {
            if lb.is_null() {
                tracing::warn!("rota_lb_reset: null lb");
                return -1;
            }
            let lb = &*lb;
            let kind = lb.strategy_kind;
            lb.strategy.lock().map_or_else(
                |_| {
                    tracing::error!("rota_lb_reset: strategy mutex poisoned");
                    -1
                },
                |mut s| {
                    *s = kind.build(lb.backend_count);
                    tracing::info!("rota strategy reset: {kind:?}");
                    0
                },
            )
        },
        -1
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    fn make_metrics(n: u32) -> Vec<FfiMetric> {
        vec![
            FfiMetric {
                rtt_us: 0,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0
            };
            n as usize
        ]
    }

    #[test]
    fn create_destroy_roundtrip() {
        let lb = unsafe { rota_lb_create(3, 0) };
        assert!(!lb.is_null());
        unsafe { rota_lb_destroy(lb) };
    }

    #[test]
    fn create_zero_returns_null() {
        let lb = unsafe { rota_lb_create(0, 0) };
        assert!(lb.is_null());
    }

    #[test]
    fn create_invalid_strategy_returns_null() {
        let lb = unsafe { rota_lb_create(3, 99) };
        assert!(lb.is_null());
    }

    #[test]
    fn select_returns_valid_index() {
        let lb = unsafe { rota_lb_create(3, 0) };
        assert!(!lb.is_null());
        let metrics = make_metrics(3);
        let addr = CString::new("api.example.com:443").unwrap();
        let idx = unsafe { rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3) };
        assert!(idx < 3);
        unsafe { rota_lb_destroy(lb) };
    }

    #[test]
    fn select_null_returns_max() {
        let addr = CString::new("x:1").unwrap();
        let m = FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        };
        unsafe {
            assert_eq!(
                rota_lb_select(std::ptr::null_mut(), addr.as_ptr(), &m, 1),
                u32::MAX,
            );
        }
    }

    #[test]
    fn destroy_null_does_not_crash() {
        unsafe { rota_lb_destroy(std::ptr::null_mut()) };
    }

    #[test]
    fn failover_rotates_on_error() {
        let lb = unsafe { rota_lb_create(3, 6) };
        assert!(!lb.is_null());
        let metrics = make_metrics(3);
        let addr = CString::new("x:1").unwrap();
        unsafe {
            let first = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
            assert_eq!(first, 0);
            rota_lb_report_error(lb, 0);
            let second = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
            assert_eq!(second, 1);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn version_returns_expected_sizes() {
        let mut v = RotaVersion {
            major: 0,
            minor: 0,
            patch: 0,
            metric_struct_size: 0,
        };
        let rc = unsafe { rota_lb_version(&mut v) };
        assert_eq!(rc, 0);
        assert_eq!(v.metric_struct_size, 32);
        assert_eq!(size_of::<FfiMetric>(), 32);
    }

    #[test]
    fn version_writes_to_null_buffer_returns_minus_one() {
        let rc = unsafe { rota_lb_version(std::ptr::null_mut()) };
        assert_eq!(rc, -1);
    }

    #[test]
    fn ffi_to_tunnel_roundtrip() {
        let ffi = FfiMetric {
            rtt_us: 50_000,
            active_connections: 3,
            recent_errors: 1,
            total_dials: 10,
            total_errors: 2,
        };
        let tunnel = ffi_to_tunnel(&ffi);
        assert_eq!(tunnel.rtt, Some(Duration::from_millis(50)));
        assert_eq!(tunnel.active_connections, 3);
        assert_eq!(tunnel.recent_errors, 1);
        assert_eq!(tunnel.total_dials, 10);
        assert_eq!(tunnel.total_errors, 2);
    }

    #[test]
    fn ffi_to_tunnel_zero_rtt_is_none() {
        let ffi = FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        };
        let tunnel = ffi_to_tunnel(&ffi);
        assert!(tunnel.rtt.is_none());
    }

    #[test]
    fn round_robin_walks() {
        let lb = unsafe { rota_lb_create(4, 0) };
        assert!(!lb.is_null());
        let metrics = make_metrics(4);
        let addr = CString::new("x:1").unwrap();
        unsafe {
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4), 0);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4), 1);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4), 2);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4), 3);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4), 0);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn strategy_name_is_retrievable() {
        let lb = unsafe { rota_lb_create(2, 7) };
        assert!(!lb.is_null());
        unsafe {
            let name_ptr = rota_lb_strategy_name(lb);
            let name = CStr::from_ptr(name_ptr).to_str().unwrap();
            assert_eq!(name, "health_weighted");
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn health_weighted_prefers_low_rtt() {
        let lb = unsafe { rota_lb_create(2, 7) };
        assert!(!lb.is_null());
        let metrics = [
            FfiMetric {
                rtt_us: 100_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 10_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
        ];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 1);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn random_picks_in_range_via_ffi() {
        let lb = unsafe { rota_lb_create(5, 1) };
        assert!(!lb.is_null());
        let metrics = [FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 5];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            for _ in 0..20 {
                let i = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 5);
                assert!(i < 5, "Random returned out-of-range {i}");
            }
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn sticky_pins_first_choice_via_ffi() {
        let lb = unsafe { rota_lb_create(4, 8) };
        assert!(!lb.is_null());
        let metrics = [
            FfiMetric {
                rtt_us: 50_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 10_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 200_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 100_000,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            },
        ];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            let first = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4);
            for _ in 0..10 {
                let next = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4);
                assert_eq!(next, first, "Sticky should pin");
            }
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn report_success_does_not_panic() {
        let lb = unsafe { rota_lb_create(3, 0) };
        assert!(!lb.is_null());
        let metrics = [FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 3];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            let idx = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
            rota_lb_report_success(lb, idx);
            // Should still pick round-robin after success report.
            let next = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
            assert!(next < 3);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn report_success_null_does_not_crash() {
        unsafe { rota_lb_report_success(std::ptr::null_mut(), 0) };
    }

    #[test]
    fn scratch_buffer_stress_test() {
        // 1000 sequential selects on the same lb; verifies the scratch
        // Vec doesn't drift or accumulate state between calls.
        let lb = unsafe { rota_lb_create(4, 0) };
        assert!(!lb.is_null());
        let metrics = [FfiMetric {
            rtt_us: 1000,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 4];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            let mut prev = u32::MAX;
            for _ in 0..1000 {
                let i = rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 4);
                assert!(i < 4);
                if prev != u32::MAX {
                    assert_eq!(i, (prev + 1) % 4, "round_robin should advance by 1");
                }
                prev = i;
            }
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn metrics_readback_returns_caller_values() {
        // Caller's input metrics are echoed back via rota_lb_metrics
        // because RoundRobin doesn't mutate them.
        let lb = unsafe { rota_lb_create(3, 0) };
        assert!(!lb.is_null());
        let input = [
            FfiMetric {
                rtt_us: 1000,
                active_connections: 5,
                recent_errors: 0,
                total_dials: 10,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 2000,
                active_connections: 6,
                recent_errors: 0,
                total_dials: 20,
                total_errors: 0,
            },
            FfiMetric {
                rtt_us: 3000,
                active_connections: 7,
                recent_errors: 0,
                total_dials: 30,
                total_errors: 0,
            },
        ];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            rota_lb_select(lb, addr.as_ptr(), input.as_ptr(), 3);
            let mut output = [FfiMetric {
                rtt_us: 0,
                active_connections: 0,
                recent_errors: 0,
                total_dials: 0,
                total_errors: 0,
            }; 3];
            let rc = rota_lb_metrics(lb, output.as_mut_ptr());
            assert_eq!(rc, 0);
            for i in 0..3 {
                assert_eq!(output[i].rtt_us, input[i].rtt_us);
                assert_eq!(output[i].active_connections, input[i].active_connections);
                assert_eq!(output[i].total_dials, input[i].total_dials);
            }
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn metrics_readback_zero_rtt_is_zero() {
        let lb = unsafe { rota_lb_create(2, 0) };
        // lb starts with empty scratch buffer; reading before any select
        // should yield zeroed metrics.
        let mut output = [FfiMetric {
            rtt_us: 99,
            active_connections: 99,
            recent_errors: 99,
            total_dials: 99,
            total_errors: 99,
        }; 2];
        unsafe {
            let rc = rota_lb_metrics(lb, output.as_mut_ptr());
            assert_eq!(rc, 0);
            assert_eq!(output[0].rtt_us, 0);
            assert_eq!(output[1].rtt_us, 0);
        }
        unsafe { rota_lb_destroy(lb) };
    }

    #[test]
    fn metrics_readback_null_args_return_minus_one() {
        let m = FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        };
        let m_ref: *const FfiMetric = &m;
        let m_ptr: *mut FfiMetric = m_ref as usize as *mut FfiMetric;
        unsafe {
            assert_eq!(rota_lb_metrics(std::ptr::null_mut(), m_ptr), -1);
            let lb = rota_lb_create(1, 0);
            assert_eq!(rota_lb_metrics(lb, std::ptr::null_mut()), -1);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn set_strategy_hot_swaps_round_robin_to_failover() {
        // RoundRobin starts; switch to Failover; the next select
        // behaviour must reflect the new strategy.
        let lb = unsafe { rota_lb_create(2, 0) }; // 0 = RoundRobin
        assert!(!lb.is_null());
        let metrics = [FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 2];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            // Pre-swap: round-robin walks 0, 1, 0, 1.
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 0);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 1);

            // Swap to Failover (6).
            let rc = rota_lb_set_strategy(lb, 6);
            assert_eq!(rc, 0);

            // After swap, Failover is freshly initialized — primary = 0.
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 0);
            // report_error rotates failover primary to 1.
            rota_lb_report_error(lb, 0);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 1);

            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn set_strategy_invalid_returns_minus_one() {
        let lb = unsafe { rota_lb_create(2, 0) };
        unsafe {
            assert_eq!(rota_lb_set_strategy(lb, 99), -1);
            // Strategy unchanged after failed swap — still RoundRobin.
            let name_ptr = rota_lb_strategy_name(lb);
            let name = CStr::from_ptr(name_ptr).to_str().unwrap();
            assert_eq!(name, "round_robin");
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn set_strategy_null_returns_minus_one() {
        unsafe {
            assert_eq!(rota_lb_set_strategy(std::ptr::null_mut(), 0), -1);
        }
    }

    #[test]
    fn reset_clears_failover_primary() {
        let lb = unsafe { rota_lb_create(3, 6) }; // Failover
        assert!(!lb.is_null());
        let metrics = [FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 3];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            // Initial select primes Failover.len and returns primary 0.
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 0);
            // Report error on primary → primary advances to 1.
            rota_lb_report_error(lb, 0);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 1);

            // Reset — primary should be back at 0.
            let rc = rota_lb_reset(lb);
            assert_eq!(rc, 0);
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 0);

            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn reset_resets_round_robin_cursor() {
        let lb = unsafe { rota_lb_create(3, 0) }; // RoundRobin
        let metrics = [FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        }; 3];
        let addr = CString::new("x:1").unwrap();
        unsafe {
            // Walk to position 2.
            rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
            rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);

            let rc = rota_lb_reset(lb);
            assert_eq!(rc, 0);
            // After reset, cursor is 0 again.
            assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 0);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn reset_null_returns_minus_one() {
        unsafe {
            assert_eq!(rota_lb_reset(std::ptr::null_mut()), -1);
        }
    }
}
