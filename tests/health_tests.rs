//! Tests for the health checking module.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;
use tokio::sync::Mutex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::health::{
    is_healthy, record_dial_result, HealthCheckConfig, HealthChecker, HealthState,
};
use rota_lb::strategy::TunnelMetrics;

struct HealthMockBackend {
    fail_count: Arc<AtomicU32>,
    #[allow(dead_code)]
    name: String,
}

impl HealthMockBackend {
    fn new(name: &str) -> Self {
        Self {
            fail_count: Arc::new(AtomicU32::new(0)),
            name: name.to_string(),
        }
    }

    fn with_failures(name: &str, count: u32) -> Self {
        Self {
            fail_count: Arc::new(AtomicU32::new(count)),
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl Backend for HealthMockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend("simulated health failure"));
        }
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {}
}

#[test]
fn is_healthy_with_no_errors() {
    let metrics = TunnelMetrics::default();
    assert!(is_healthy(&metrics, 3));
}

#[test]
fn is_healthy_below_threshold() {
    let metrics = TunnelMetrics {
        recent_errors: 2,
        ..Default::default()
    };
    assert!(is_healthy(&metrics, 3));
}

#[test]
fn is_healthy_at_threshold() {
    let metrics = TunnelMetrics {
        recent_errors: 3,
        ..Default::default()
    };
    assert!(!is_healthy(&metrics, 3));
}

#[test]
fn is_healthy_above_threshold() {
    let metrics = TunnelMetrics {
        recent_errors: 5,
        ..Default::default()
    };
    assert!(!is_healthy(&metrics, 3));
}

#[test]
fn is_healthy_zero_threshold() {
    let metrics = TunnelMetrics::default();
    assert!(!is_healthy(&metrics, 0));
}

#[test]
fn is_healthy_very_high_threshold() {
    let metrics = TunnelMetrics::default();
    assert!(is_healthy(&metrics, u32::MAX));
}

#[test]
fn record_dial_result_success_resets_errors() {
    let mut metrics = vec![TunnelMetrics::default()];
    metrics[0].recent_errors = 5;
    record_dial_result(&mut metrics, 0, true);
    assert_eq!(metrics[0].recent_errors, 0);
}

#[test]
fn record_dial_result_failure_increments_errors() {
    let mut metrics = vec![TunnelMetrics::default()];
    record_dial_result(&mut metrics, 0, false);
    assert_eq!(metrics[0].recent_errors, 1);
    assert_eq!(metrics[0].total_errors, 1);
}

#[test]
fn record_dial_result_failure_increments_both_counters() {
    let mut metrics = vec![TunnelMetrics::default()];
    record_dial_result(&mut metrics, 0, false);
    record_dial_result(&mut metrics, 0, false);
    assert_eq!(metrics[0].recent_errors, 2);
    assert_eq!(metrics[0].total_errors, 2);
}

#[test]
fn record_dial_result_success_after_failures() {
    let mut metrics = vec![TunnelMetrics::default()];
    record_dial_result(&mut metrics, 0, false);
    record_dial_result(&mut metrics, 0, false);
    record_dial_result(&mut metrics, 0, true);
    assert_eq!(metrics[0].recent_errors, 0);
    assert_eq!(metrics[0].total_errors, 2);
}

#[test]
fn record_dial_result_out_of_bounds() {
    let mut metrics = vec![];
    record_dial_result(&mut metrics, 0, true);
}

#[test]
fn record_dial_result_multiple_indices() {
    let mut metrics = vec![TunnelMetrics::default(); 3];
    record_dial_result(&mut metrics, 0, false);
    record_dial_result(&mut metrics, 1, false);
    record_dial_result(&mut metrics, 2, false);
    assert_eq!(metrics[0].recent_errors, 1);
    assert_eq!(metrics[1].recent_errors, 1);
    assert_eq!(metrics[2].recent_errors, 1);
}

#[test]
fn health_check_config_default() {
    let config = HealthCheckConfig::default();
    assert_eq!(config.interval, Duration::from_secs(30));
    assert_eq!(config.timeout, Duration::from_secs(5));
    assert_eq!(config.unhealthy_threshold, 3);
    assert_eq!(config.healthy_threshold, 2);
    assert_eq!(config.check_addr, "");
    assert!(config.check_addr.is_empty());
}

#[test]
fn health_check_config_clone() {
    let config = HealthCheckConfig::default();
    let _cloned = config.clone();
}

#[test]
fn health_check_config_debug() {
    let config = HealthCheckConfig::default();
    let _ = format!("{:?}", config);
}

#[test]
fn health_state_equality() {
    assert_eq!(HealthState::Healthy, HealthState::Healthy);
    assert_eq!(HealthState::Unhealthy, HealthState::Unhealthy);
    assert_eq!(HealthState::Unknown, HealthState::Unknown);
    assert_ne!(HealthState::Healthy, HealthState::Unhealthy);
}

#[test]
fn health_state_debug() {
    let s = format!("{:?}", HealthState::Healthy);
    assert!(s.contains("Healthy"));
}

#[test]
#[allow(clippy::clone_on_copy)]
fn health_state_clone() {
    let s = HealthState::Healthy;
    let _cloned = s.clone();
}

#[test]
fn health_state_copy() {
    let s = HealthState::Healthy;
    let _copy = s;
}

#[tokio::test]
async fn lb_with_failing_backend_updates_metrics() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(HealthMockBackend::with_failures("a", 1)),
        Box::new(HealthMockBackend::new("b")),
    ];
    let lb = rota_lb::LoadBalancer::new(backends, rota_lb::round_robin()).unwrap();

    let r = lb.dial("test:80").await;
    assert!(r.is_err());

    let metrics = lb.metrics().await;
    assert!(metrics[0].recent_errors > 0);
}

#[tokio::test]
async fn health_checker_spawn_and_shutdown() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(HealthMockBackend::new("a"))];
    let metrics = Arc::new(Mutex::new(vec![TunnelMetrics::default()]));
    let config = HealthCheckConfig {
        interval: Duration::from_millis(50),
        timeout: Duration::from_millis(10),
        unhealthy_threshold: 3,
        healthy_threshold: 2,
        check_addr: "test:80".to_string(),
    };
    let checker = HealthChecker::spawn(backends, metrics, config);
    tokio::time::sleep(Duration::from_millis(20)).await;
    checker.shutdown().await;
}

#[tokio::test]
async fn health_checker_with_failing_backend() {
    let backends: Vec<Box<dyn Backend>> =
        vec![Box::new(HealthMockBackend::with_failures("a", 100))];
    let metrics = Arc::new(Mutex::new(vec![TunnelMetrics::default()]));
    let config = HealthCheckConfig {
        interval: Duration::from_millis(50),
        timeout: Duration::from_millis(10),
        unhealthy_threshold: 3,
        healthy_threshold: 2,
        check_addr: "test:80".to_string(),
    };
    let checker = HealthChecker::spawn(backends, metrics.clone(), config);
    tokio::time::sleep(Duration::from_millis(80)).await;
    let m = metrics.lock().await;
    assert!(m[0].recent_errors > 0 || m[0].total_errors > 0);
    drop(m);
    checker.shutdown().await;
}
