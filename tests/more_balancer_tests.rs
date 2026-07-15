//! Additional tests for the LoadBalancer to improve coverage.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::retry::{ExponentialBackoff, FixedRetry, NoRetry};
use rota_lb::strategies::{
    failover, hash_by_addr, health_weighted, least_connections, lowest_rtt, random, round_robin,
    sticky, weighted_round_robin,
};
use rota_lb::strategy::TunnelMetrics;
use rota_lb::LoadBalancer;

struct MockBackend {
    name: String,
    fail_count: Arc<AtomicU32>,
    dial_count: Arc<AtomicUsize>,
}

impl MockBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fail_count: Arc::new(AtomicU32::new(0)),
            dial_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Backend for MockBackend {
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

    async fn shutdown(&mut self) {}
}

// ============================================================================
//  LoadBalancer Debug impl
// ============================================================================

#[test]
fn lb_debug_impl() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let s = format!("{:?}", lb);
    assert!(s.contains("LoadBalancer"));
    assert!(s.contains("backend_count"));
}

// ============================================================================
//  new_with_metrics with mismatched length
// ============================================================================

#[test]
fn new_with_metrics_mismatched_length() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let metrics: Vec<TunnelMetrics> = vec![TunnelMetrics::default(), TunnelMetrics::default()];
    let result = LoadBalancer::new_with_metrics(backends, metrics, round_robin(), None, None);
    assert!(result.is_err());
}

// ============================================================================
//  dial with retry
// ============================================================================

#[tokio::test]
async fn dial_with_retry_policy_succeeds() {
    let backend = MockBackend::new("a");
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

    // First 2 attempts fail, 3rd succeeds
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn dial_with_retry_policy_exhausts() {
    let backend = MockBackend::new("a");
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

    let r = lb.dial("example.com:443").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_retry_and_dial_timeout() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let policy = NoRetry;
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_millis(10))
        .retry_policy(policy)
        .build()
        .await
        .unwrap();

    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

// ============================================================================
//  All strategies via builder
// ============================================================================

#[tokio::test]
async fn builder_with_random() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(random())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_lowest_rtt() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(lowest_rtt())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_least_connections() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(least_connections())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_hash_by_addr() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(hash_by_addr())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_weighted_round_robin() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(weighted_round_robin())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_failover() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(failover())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_health_weighted() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(health_weighted())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_sticky() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(sticky())
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

// ============================================================================
//  from_factories with various strategies
// ============================================================================

fn make_factory(name: &'static str) -> Box<dyn rota_lb::BackendFactory> {
    struct F(&'static str);
    #[async_trait::async_trait]
    impl rota_lb::BackendFactory for F {
        async fn create(&self) -> Result<rota_lb::factory::BackendOutput, Error> {
            let _ = duplex(64);
            Ok(rota_lb::factory::BackendOutput {
                backend: Box::new(MockBackend::new(self.0)),
                initial_metrics: TunnelMetrics::default(),
            })
        }
    }
    Box::new(F(name))
}

#[tokio::test]
async fn from_factories_with_random() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, random())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_lowest_rtt() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, lowest_rtt())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_least_connections() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, least_connections())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_hash_by_addr() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, hash_by_addr())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_weighted_round_robin() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, weighted_round_robin())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_failover() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, failover())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_health_weighted() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, health_weighted())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_sticky() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let _lb = LoadBalancer::from_factories(factories, sticky())
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_with_initial_metrics() {
    use rota_lb::strategy::TunnelMetrics;
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(5)),
        ..Default::default()
    }];
    let _lb = LoadBalancer::builder()
        .factories(factories)
        .strategy(round_robin())
        .initial_metrics(metrics)
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn from_factories_mismatched_metrics() {
    use rota_lb::strategy::TunnelMetrics;
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![make_factory("a")];
    let metrics = vec![TunnelMetrics::default(), TunnelMetrics::default()];
    let result = LoadBalancer::builder()
        .factories(factories)
        .strategy(round_robin())
        .initial_metrics(metrics)
        .build()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn from_factories_empty() {
    let factories: Vec<Box<dyn rota_lb::BackendFactory>> = vec![];
    let result = LoadBalancer::from_factories(factories, round_robin()).await;
    assert!(result.is_err());
}
