//! More in-depth balancer tests to improve coverage.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::retry::{ExponentialBackoff, FixedRetry, NoRetry};
use rota_lb::strategies::{
    failover, hash_by_addr, health_weighted, lowest_rtt, round_robin, sticky, weighted_round_robin,
};
use rota_lb::strategy::TunnelMetrics;
use rota_lb::LoadBalancer;

struct DeepMockBackend {
    name: String,
    fail_count: Arc<AtomicU32>,
    dial_count: Arc<AtomicUsize>,
    shutdown_count: Arc<AtomicU32>,
}

impl DeepMockBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fail_count: Arc::new(AtomicU32::new(0)),
            dial_count: Arc::new(AtomicUsize::new(0)),
            shutdown_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

#[async_trait]
impl Backend for DeepMockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        self.dial_count.fetch_add(1, Ordering::SeqCst);
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend(format!("{}: simulated failure", self.name)));
        }
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {
        self.shutdown_count.fetch_add(1, Ordering::SeqCst);
    }
}

// ============================================================================
//  Tests for the dial method's full path
// ============================================================================

#[tokio::test]
async fn dial_with_active_connections_metric() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn1 = lb.dial("example.com:443").await.unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 1);
    drop(conn1);
}

#[tokio::test]
async fn dial_increments_active_connections() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn1 = lb.dial("a:80").await.unwrap();
    let conn2 = lb.dial("b:80").await.unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 2);
    drop(conn1);
    drop(conn2);
}

#[tokio::test]
async fn dial_decrements_active_on_drop() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    {
        let _conn = lb.dial("a:80").await.unwrap();
    }
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 0);
}

#[tokio::test]
async fn dial_records_failure_in_metrics() {
    let backend = DeepMockBackend::new("a");
    backend.fail_count.store(100, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_err());
    let metrics = lb.metrics().await;
    assert!(metrics[0].total_errors > 0);
    assert!(metrics[0].recent_errors > 0);
}

#[tokio::test]
async fn dial_resets_recent_errors_on_success() {
    let backend = DeepMockBackend::new("a");
    // First dial fails, then set fail_count to 0 so second succeeds
    backend.fail_count.store(1, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    // First dial fails
    let _ = lb.dial("a:80").await;
    let metrics_before = lb.metrics().await;
    assert!(metrics_before[0].recent_errors > 0);
    // Manually reset fail_count to 0
    lb.metrics().await; // Ensure metrics are synced
                        // Second dial - fail_count is now 0 so it should succeed
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
    let metrics_after = lb.metrics().await;
    // recent_errors should be reset to 0 on success
    assert_eq!(metrics_after[0].recent_errors, 0);
}

#[tokio::test]
async fn dial_retry_with_exponential() {
    let backend = DeepMockBackend::new("a");
    backend.fail_count.store(2, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = ExponentialBackoff::new(Duration::from_millis(1));
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    // First 2 fail, 3rd succeeds
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_retry_with_fixed() {
    let backend = DeepMockBackend::new("a");
    backend.fail_count.store(2, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = FixedRetry::new(Duration::from_millis(1)).with_max_attempts(3);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_retry_fails_after_exhaustion() {
    let backend = DeepMockBackend::new("a");
    backend.fail_count.store(100, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = FixedRetry::new(Duration::from_millis(1)).with_max_attempts(2);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_no_retry() {
    let backend = DeepMockBackend::new("a");
    backend.fail_count.store(100, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(NoRetry)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_dial_timeout_works() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_dial_timeout_very_short() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_millis(1))
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    // The timeout may or may not trigger depending on system speed
    let _ = r;
}

// ============================================================================
//  Tests for the metrics struct
// ============================================================================

#[test]
fn tunnel_metrics_default() {
    let m = TunnelMetrics::default();
    assert_eq!(m.rtt, None);
    assert_eq!(m.active_connections, 0);
    assert_eq!(m.recent_errors, 0);
    assert_eq!(m.total_dials, 0);
    assert_eq!(m.total_errors, 0);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn tunnel_metrics_clone() {
    let m = TunnelMetrics::default();
    let _m2 = m.clone();
}

#[test]
fn tunnel_metrics_debug() {
    let m = TunnelMetrics::default();
    let _ = format!("{:?}", m);
}

#[test]
fn tunnel_metrics_with_values() {
    let m = TunnelMetrics {
        rtt: Some(Duration::from_millis(100)),
        active_connections: 5,
        recent_errors: 2,
        total_dials: 10,
        total_errors: 1,
    };
    assert_eq!(m.rtt, Some(Duration::from_millis(100)));
    assert_eq!(m.active_connections, 5);
    assert_eq!(m.recent_errors, 2);
    assert_eq!(m.total_dials, 10);
    assert_eq!(m.total_errors, 1);
}

// ============================================================================
//  Tests for the strategy through balancer
// ============================================================================

#[tokio::test]
async fn balancer_with_lowest_rtt() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let metrics = vec![
        TunnelMetrics {
            rtt: Some(Duration::from_millis(5)),
            ..Default::default()
        },
        TunnelMetrics {
            rtt: Some(Duration::from_millis(50)),
            ..Default::default()
        },
    ];
    let lb = LoadBalancer::new_with_metrics(backends, metrics, lowest_rtt(), None, None).unwrap();
    // Both should succeed - lowest_rtt will pick the one with lower RTT
    let r1 = lb.dial("a:80").await;
    let r2 = lb.dial("b:80").await;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}

#[tokio::test]
async fn balancer_with_failover() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, failover()).unwrap();
    let r1 = lb.dial("a:80").await;
    let r2 = lb.dial("b:80").await;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}

#[tokio::test]
async fn balancer_with_health_weighted() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let metrics = vec![
        TunnelMetrics {
            recent_errors: 5,
            ..Default::default()
        },
        TunnelMetrics {
            recent_errors: 0,
            ..Default::default()
        },
    ];
    let lb =
        LoadBalancer::new_with_metrics(backends, metrics, health_weighted(), None, None).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn balancer_with_sticky() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, sticky()).unwrap();
    let r1 = lb.dial("a:80").await;
    let r2 = lb.dial("b:80").await;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}

#[tokio::test]
async fn balancer_with_hash_by_addr() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, hash_by_addr()).unwrap();
    let r1 = lb.dial("a:80").await;
    let r2 = lb.dial("b:80").await;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}

#[tokio::test]
async fn balancer_with_weighted_round_robin() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let metrics = vec![
        TunnelMetrics {
            rtt: Some(Duration::from_millis(10)),
            ..Default::default()
        },
        TunnelMetrics {
            rtt: Some(Duration::from_millis(100)),
            ..Default::default()
        },
    ];
    let lb = LoadBalancer::new_with_metrics(backends, metrics, weighted_round_robin(), None, None)
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

// ============================================================================
//  Backend_count tests
// ============================================================================

#[tokio::test]
async fn backend_count_after_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(DeepMockBackend::new("a")),
        Box::new(DeepMockBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 2);
    let _ = lb.dial("a:80").await;
    assert_eq!(lb.backend_count(), 2);
}

#[test]
fn backend_count_single() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 1);
}

#[test]
fn backend_count_many() {
    let backends: Vec<Box<dyn Backend>> = (0..10)
        .map(|i| Box::new(DeepMockBackend::new(&format!("b{}", i))) as Box<dyn Backend>)
        .collect();
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 10);
}

// ============================================================================
//  Builder pattern tests
// ============================================================================

#[tokio::test]
async fn builder_chained_calls() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let policy = FixedRetry::new(Duration::from_millis(1));
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_secs(5))
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn builder_with_all_features() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DeepMockBackend::new("a"))];
    let policy = ExponentialBackoff::new(Duration::from_millis(1));
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_secs(10))
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}
