use std::ffi::{CStr, CString};
use std::sync::OnceLock;

use crate::enums::ffi::FfiStrategy;

use super::types::{ffi_to_tunnel, FfiLoadBalancer, FfiMetric, RotaVersion, VERSION};

/// Catches panics from the inner closure and returns `$default`.
macro_rules! catch_panic {
    ($body:expr, $default:expr) => {
        match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| $body)) {
            Ok(v) => v,
            Err(_) => $default,
        }
    };
}

/// Write version metadata into a caller-provided buffer.
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

/// Create a load balancer. Returns an opaque pointer, or null on failure.
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
                strategy: ::std::sync::Mutex::new(strategy),
                backend_count: n as usize,
                strategy_kind: kind,
                scratch: ::std::sync::Mutex::new(Vec::with_capacity(n as usize)),
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

/// Select a backend index. Returns index in `[0, n)` on success, `u32::MAX` on error.
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

    let view = crate::traits::strategy::PoolView {
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

/// Report a dial error.
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

/// Report a successful dial.
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_strategy_name(
    lb: *mut FfiLoadBalancer,
) -> *const std::ffi::c_char {
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

            let snapshot: Vec<crate::traits::strategy::TunnelMetrics> =
                lb.scratch.lock().map_or_else(
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rota_lb_set_strategy(
    lb: *mut FfiLoadBalancer,
    strategy_kind: i32,
) -> i32 {
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
