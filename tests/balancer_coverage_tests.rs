//! Additional tests to improve balancer.rs coverage.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::retry::{ExponentialBackoff, FixedRetry, NoRetry, RetryOnError};
use rota_lb::strategies::{
    failover, health_weighted, lowest_rtt, round_robin, sticky, HashByAddr, WeightedRoundRobin,
};
use rota_lb::strategy::TunnelMetrics;
use rota_lb::LoadBalancer;

struct FullMockBackend {
    name: String,
    fail_count: Arc<AtomicU32>,
    dial_count: Arc<AtomicUsize>,
    shutdown_called: Arc<AtomicU32>,
    response_time: Duration,
}

impl FullMockBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fail_count: Arc::new(AtomicU32::new(0)),
            dial_count: Arc::new(AtomicUsize::new(0)),
            shutdown_called: Arc::new(AtomicU32::new(0)),
            response_time: Duration::from_millis(1),
        }
    }
}

#[async_trait]
impl Backend for FullMockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        self.dial_count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.response_time).await;
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend(format!("{}: simulated failure", self.name)));
        }
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {
        self.shutdown_called.fetch_add(1, Ordering::SeqCst);
    }
}

// ============================================================================
//  Constructor coverage
// ============================================================================

#[test]
fn new_with_metrics_empty_backends() {
    let result = LoadBalancer::new_with_metrics(vec![], vec![], round_robin(), None, None);
    assert!(result.is_err());
}

#[test]
fn new_with_metrics_mismatched_lengths() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let metrics = vec![TunnelMetrics::default(), TunnelMetrics::default()];
    let result = LoadBalancer::new_with_metrics(backends, metrics, round_robin(), None, None);
    assert!(result.is_err());
    if let Err(Error::Factory(ref e)) = result {
        assert!(e.0.contains("initial_metrics.len"));
    } else {
        panic!("Expected Factory error");
    }
}

#[test]
fn new_with_metrics_succeeds() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullMockBackend::new("a")),
        Box::new(FullMockBackend::new("b")),
    ];
    let metrics = vec![TunnelMetrics::default(), TunnelMetrics::default()];
    let result = LoadBalancer::new_with_metrics(backends, metrics, round_robin(), None, None);
    assert!(result.is_ok());
}

#[test]
fn new_with_metrics_with_timeout_and_retry() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let metrics = vec![TunnelMetrics::default()];
    let result = LoadBalancer::new_with_metrics(
        backends,
        metrics,
        round_robin(),
        Some(Duration::from_secs(5)),
        None,
    );
    assert!(result.is_ok());
}

#[test]
fn builder_default() {
    let _b = LoadBalancer::builder();
}

#[test]
fn builder_debug() {
    let b = LoadBalancer::builder();
    let _ = format!("{:?}", b);
}

#[test]
fn builder_into_inner() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let b = LoadBalancer::builder().backends(backends);
    let _b = b;
}

// ============================================================================
//  dial method coverage
// ============================================================================

#[tokio::test]
async fn dial_no_backends_errors() {
    // This is tested in integration_tests, but let's make sure it's covered
    let backends: Vec<Box<dyn Backend>> = vec![];
    let result = LoadBalancer::new(backends, round_robin());
    assert!(result.is_err());
}

#[tokio::test]
async fn dial_with_all_strategies() {
    fn make_backends() -> Vec<Box<dyn Backend>> {
        vec![
            Box::new(FullMockBackend::new("a")),
            Box::new(FullMockBackend::new("b")),
            Box::new(FullMockBackend::new("c")),
        ]
    }

    // Test round_robin
    let lb = LoadBalancer::new(make_backends(), round_robin()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test lowest_rtt
    let lb = LoadBalancer::new(make_backends(), lowest_rtt()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test health_weighted
    let lb = LoadBalancer::new(make_backends(), health_weighted()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test failover
    let lb = LoadBalancer::new(make_backends(), failover()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test sticky
    let lb = LoadBalancer::new(make_backends(), sticky()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test hash_by_addr
    let lb = LoadBalancer::new(make_backends(), HashByAddr::new()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Test weighted_round_robin
    let lb = LoadBalancer::new(make_backends(), WeightedRoundRobin::new()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_dial_timeout() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_exponential_backoff() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(ExponentialBackoff::new(Duration::from_millis(1)))
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_fixed_retry() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(FixedRetry::new(Duration::from_millis(1)))
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_no_retry() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(NoRetry)
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_retry_on_error() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let policy = RetryOnError::new(FixedRetry::new(Duration::from_millis(1)), |_| true);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

// ============================================================================
//  metrics method coverage
// ============================================================================

#[tokio::test]
async fn metrics_after_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
    let metrics = lb.metrics().await;
    assert_eq!(metrics.len(), 1);
    assert!(metrics[0].total_dials > 0);
}

#[tokio::test]
async fn metrics_with_seeded_values() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let initial_metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(50)),
        active_connections: 0,
        recent_errors: 0,
        total_dials: 0,
        total_errors: 0,
    }];
    let lb = LoadBalancer::new_with_metrics(backends, initial_metrics, round_robin(), None, None)
        .unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].rtt, Some(Duration::from_millis(50)));
}

// ============================================================================
//  shutdown coverage
// ============================================================================

#[tokio::test]
async fn shutdown_calls_each_backend() {
    let a = FullMockBackend::new("a");
    let b = FullMockBackend::new("b");
    let a_shutdown = a.shutdown_called.clone();
    let b_shutdown = b.shutdown_called.clone();

    let backends: Vec<Box<dyn Backend>> = vec![Box::new(a), Box::new(b)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    lb.shutdown().await;

    assert_eq!(a_shutdown.load(Ordering::SeqCst), 1);
    assert_eq!(b_shutdown.load(Ordering::SeqCst), 1);
}

// ============================================================================
//  backend_count
// ============================================================================

#[test]
fn backend_count_returns_correct() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullMockBackend::new("a")),
        Box::new(FullMockBackend::new("b")),
        Box::new(FullMockBackend::new("c")),
    ];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 3);
}

// ============================================================================
//  Multi-dial scenarios
// ============================================================================

#[tokio::test]
async fn multiple_dials_increment_total() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    for _ in 0..3 {
        let conn = lb.dial("example.com:443").await.unwrap();
        drop(conn);
    }
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].total_dials, 3);
}

#[tokio::test]
async fn dial_then_shutdown() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullMockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
    lb.shutdown().await;
}
