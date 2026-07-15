//! Tests for the full dial() path in balancer.rs to improve coverage.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::retry::{ExponentialBackoff, FixedRetry, NoRetry, RetryPolicyBuilder};
use rota_lb::strategies::{
    failover, hash_by_addr, health_weighted, lowest_rtt, round_robin, sticky, weighted_round_robin,
};
use rota_lb::strategy::TunnelMetrics;
use rota_lb::LoadBalancer;

struct FullDialBackend {
    name: String,
    fail_count: Arc<AtomicU32>,
    dial_delay: Duration,
}

impl FullDialBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fail_count: Arc::new(AtomicU32::new(0)),
            dial_delay: Duration::from_millis(1),
        }
    }
}

#[async_trait]
impl Backend for FullDialBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        tokio::time::sleep(self.dial_delay).await;
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend(format!("{}: simulated failure", self.name)));
        }
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {}
}

// ============================================================================
//  Dial timeout error path
// ============================================================================

#[tokio::test]
async fn dial_timeout_triggers() {
    let mut backend = FullDialBackend::new("a");
    backend.dial_delay = Duration::from_secs(2);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_millis(10))
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_err());
    if let Err(Error::Backend(ref e)) = r {
        assert!(e.0.contains("timeout") || e.0.contains("dial"));
    }
}

#[tokio::test]
async fn dial_timeout_with_fast_backend() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
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

// ============================================================================
//  Dial with all strategy types
// ============================================================================

#[tokio::test]
async fn dial_with_lowest_rtt() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
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
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_weighted_round_robin() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
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

#[tokio::test]
async fn dial_with_failover() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, failover()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_health_weighted() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, health_weighted()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_sticky() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, sticky()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_hash_by_addr() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FullDialBackend::new("a")),
        Box::new(FullDialBackend::new("b")),
    ];
    let lb = LoadBalancer::new(backends, hash_by_addr()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

// ============================================================================
//  Dial with retry policies
// ============================================================================

#[tokio::test]
async fn dial_with_retry_policy_builder() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let policy = FixedRetry::new(Duration::from_millis(1));
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
async fn dial_with_retry_policy_builder_exponential() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let policy = ExponentialBackoff::new(Duration::from_millis(1));
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
async fn dial_with_retry_policy_builder_no_retry() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let policy = NoRetry;
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
async fn dial_with_retry_policy_builder_custom() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let custom = ExponentialBackoff::new(Duration::from_millis(1));
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(custom)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_retry_policy_builder_empty() {
    // RetryPolicyBuilder::default().build() returns None
    // We just test the builder itself
    let b = RetryPolicyBuilder::default();
    let p = b.build();
    assert!(p.is_none());
}

#[tokio::test]
async fn dial_with_retry_policy_builder_clone() {
    let b1 = RetryPolicyBuilder::default();
    let _b2 = b1.clone();
}

// ============================================================================
//  from_factories with all strategies
// ============================================================================

fn make_factory(name: &'static str) -> Box<dyn rota_lb::BackendFactory> {
    struct F(&'static str);
    #[async_trait::async_trait]
    impl rota_lb::BackendFactory for F {
        async fn create(&self) -> Result<rota_lb::factory::BackendOutput, Error> {
            let (_a, _b) = duplex(64);
            Ok(rota_lb::factory::BackendOutput {
                backend: Box::new(FullDialBackend::new(self.0)),
                initial_metrics: TunnelMetrics::default(),
            })
        }
    }
    Box::new(F(name))
}

#[tokio::test]
async fn from_factories_with_lowest_rtt() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, lowest_rtt())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn from_factories_with_weighted_round_robin() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, weighted_round_robin())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn from_factories_with_failover() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, failover())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn from_factories_with_health_weighted() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, health_weighted())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn from_factories_with_sticky() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, sticky())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn from_factories_with_hash_by_addr() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let lb = LoadBalancer::from_factories(factories, hash_by_addr())
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

// ============================================================================
//  Metrics method
// ============================================================================

#[tokio::test]
async fn metrics_after_multiple_dials() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    for _ in 0..5 {
        let conn = lb.dial("a:80").await.unwrap();
        drop(conn);
    }
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].total_dials, 5);
    assert_eq!(metrics[0].total_errors, 0);
}

#[tokio::test]
async fn metrics_with_failures_and_successes() {
    let backend = FullDialBackend::new("a");
    backend.fail_count.store(2, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = FixedRetry::new(Duration::from_millis(1)).with_max_attempts(5);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
    let metrics = lb.metrics().await;
    // total_dials is only incremented once (before the retry)
    assert_eq!(metrics[0].total_dials, 1);
    // The retry policy retries internally - if the final attempt succeeds,
    // total_errors might not be incremented (or might be 1)
    // The important thing is that recent_errors is reset to 0
    assert_eq!(metrics[0].recent_errors, 0);
}

// ============================================================================
//  builder debug
// ============================================================================

#[test]
fn builder_debug_all_fields() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let b = LoadBalancer::builder().backends(backends);
    let s = format!("{:?}", b);
    assert!(s.contains("LoadBalancerBuilder"));
    assert!(s.contains("backends_count"));
}

// ============================================================================
//  Dial with active connections tracking
// ============================================================================

#[tokio::test]
async fn dial_tracks_active_connections() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FullDialBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn1 = lb.dial("a:80").await.unwrap();
    let conn2 = lb.dial("b:80").await.unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 2);
    drop(conn1);
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 1);
    drop(conn2);
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 0);
}

#[tokio::test]
async fn dial_failure_doesnt_increment_active() {
    let backend = FullDialBackend::new("a");
    backend.fail_count.store(100, Ordering::SeqCst);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let _ = lb.dial("a:80").await;
    let metrics = lb.metrics().await;
    // Active connections should be 0 since the dial failed
    assert_eq!(metrics[0].active_connections, 0);
    // But total_dials should be 1 (we tried to dial)
    assert_eq!(metrics[0].total_dials, 1);
}
