use std::ffi::CString;
use std::ffi::CStr;
use std::mem::size_of;
use std::time::Duration;

use super::api::*;
use super::types::FfiMetric;

fn make_metrics(n: u32) -> Vec<FfiMetric> {
    vec![
        FfiMetric {
            rtt_us: 0,
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
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
    use super::types::RotaVersion;
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
    let tunnel = super::types::ffi_to_tunnel(&ffi);
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
    let tunnel = super::types::ffi_to_tunnel(&ffi);
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
    let lb = unsafe { rota_lb_create(2, 0) };
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
        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 0);
        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 1);

        let rc = rota_lb_set_strategy(lb, 6);
        assert_eq!(rc, 0);

        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 2), 0);
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
    let lb = unsafe { rota_lb_create(3, 6) };
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
        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 0);
        rota_lb_report_error(lb, 0);
        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 1);

        let rc = rota_lb_reset(lb);
        assert_eq!(rc, 0);
        assert_eq!(rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3), 0);

        rota_lb_destroy(lb);
    }
}

#[test]
fn reset_resets_round_robin_cursor() {
    let lb = unsafe { rota_lb_create(3, 0) };
    let metrics = [FfiMetric {
        rtt_us: 0,
        active_connections: 0,
        recent_errors: 0,
        total_dials: 0,
        total_errors: 0,
    }; 3];
    let addr = CString::new("x:1").unwrap();
    unsafe {
        rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);
        rota_lb_select(lb, addr.as_ptr(), metrics.as_ptr(), 3);

        let rc = rota_lb_reset(lb);
        assert_eq!(rc, 0);
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
