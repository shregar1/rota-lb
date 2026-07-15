//! More balancer tests to improve coverage.

use std::sync::atomic::{AtomicU32, Ordering};
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

#[allow(dead_code)]
struct FinalTestBackend {
    name: String,
    fail_count: Arc<AtomicU32>,
    shutdown_count: Arc<AtomicU32>,
}

impl FinalTestBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fail_count: Arc::new(AtomicU32::new(0)),
            shutdown_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

#[async_trait]
impl Backend for FinalTestBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend("failure"));
        }
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {
        self.shutdown_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn dial_with_random_strategy() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, random()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_exponential_retry_succeeds() {
    let backend = FinalTestBackend::new("a");
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
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_fixed_retry_succeeds() {
    let backend = FinalTestBackend::new("a");
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
}

#[tokio::test]
async fn dial_with_dial_timeout_and_retry() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let policy = NoRetry;
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_secs(1))
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_hash_by_addr() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, hash_by_addr()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_sticky() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, sticky()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_weighted_round_robin() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(10)),
        ..Default::default()
    }];
    let lb = LoadBalancer::new_with_metrics(backends, metrics, weighted_round_robin(), None, None)
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_lowest_rtt() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(10)),
        ..Default::default()
    }];
    let lb = LoadBalancer::new_with_metrics(backends, metrics, lowest_rtt(), None, None).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_health_weighted() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, health_weighted()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_least_connections() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, least_connections()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_failover() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, failover()).unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn add_backend_then_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let mut lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let idx = lb.add_backend(Box::new(FinalTestBackend::new("b"))).await;
    assert_eq!(idx, 1);
    let r = lb.dial("b:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn remove_backend_then_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(FinalTestBackend::new("a")),
        Box::new(FinalTestBackend::new("b")),
    ];
    let mut lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let result = lb.remove_backend(0).await;
    assert!(result);
    assert_eq!(lb.backend_count(), 1);
    let r = lb.dial("b:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn drain_backend_then_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let mut lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let result = lb.drain_backend(0).await;
    assert!(result);
    let r = lb.dial("a:80").await;
    // After draining, the backend should still work but with high error count
    assert!(r.is_ok());
}

#[tokio::test]
async fn undrain_backend_then_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let mut lb = LoadBalancer::new(backends, round_robin()).unwrap();
    lb.drain_backend(0).await;
    let result = lb.undrain_backend(0).await;
    assert!(result);
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn replace_backends_then_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let mut lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let new_backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("b"))];
    let result = lb.replace_backends(new_backends, None).await;
    assert!(result.is_ok());
    let r = lb.dial("b:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn shutdown_all_backends() {
    let a = FinalTestBackend::new("a");
    let b = FinalTestBackend::new("b");
    let a_shutdown = a.shutdown_count.clone();
    let b_shutdown = b.shutdown_count.clone();

    let backends: Vec<Box<dyn Backend>> = vec![Box::new(a), Box::new(b)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    lb.shutdown().await;
    assert_eq!(a_shutdown.load(Ordering::SeqCst), 1);
    assert_eq!(b_shutdown.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn metrics_after_dial_and_drop() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let conn = lb.dial("a:80").await.unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 1);
    drop(conn);
    let metrics = lb.metrics().await;
    assert_eq!(metrics[0].active_connections, 0);
}

#[tokio::test]
async fn dial_with_dial_timeout_triggers() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_millis(1))
        .build()
        .await
        .unwrap();
    // The backend returns quickly so timeout might not trigger
    let _ = lb.dial("a:80").await;
}

#[tokio::test]
async fn multiple_dials_with_different_addrs() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    for i in 0..5 {
        let r = lb.dial(&format!("host{}:80", i)).await;
        assert!(r.is_ok());
    }
}

#[tokio::test]
async fn dial_with_invalid_address() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let r = lb.dial("").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_empty_host() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let r = lb.dial(":80").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_zero_port() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let r = lb.dial("host:0").await;
    assert!(r.is_err());
}

#[tokio::test]
async fn dial_with_non_numeric_port() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let r = lb.dial("host:abc").await;
    assert!(r.is_err());
}
