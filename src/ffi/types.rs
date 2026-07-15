use std::sync::Mutex;
use std::time::Duration;

use crate::enums::ffi::FfiStrategy;
use crate::traits::strategy::{BalanceStrategy, TunnelMetrics};

/// Library version and ABI metadata.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct RotaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub metric_struct_size: u32,
}

/// Flat C-compatible metric struct (32 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct FfiMetric {
    pub rtt_us: u64,
    pub active_connections: u32,
    pub recent_errors: u32,
    pub total_dials: u64,
    pub total_errors: u64,
}

const _: () = assert!(size_of::<FfiMetric>() == 32);

pub(super) const fn ffi_to_tunnel(m: &FfiMetric) -> TunnelMetrics {
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

pub(super) const VERSION: RotaVersion = RotaVersion {
    major: 0,
    minor: 1,
    patch: 0,
    metric_struct_size: 32,
};

pub(super) struct FfiLoadBalancer {
    pub strategy: Mutex<Box<dyn BalanceStrategy + Send>>,
    pub backend_count: usize,
    pub strategy_kind: FfiStrategy,
    pub scratch: Mutex<Vec<TunnelMetrics>>,
}

#[cfg(test)]
#[test]
fn ffi_lb_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<FfiLoadBalancer>();
    assert_sync::<FfiLoadBalancer>();
}
