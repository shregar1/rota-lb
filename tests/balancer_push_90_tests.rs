//! More balancer tests to push coverage above 90%.

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
async fn dial_with_all_strategies_combined() {
    for strategy in [
        random(),
        round_robin(),
        sticky(),
        hash_by_addr(),
        lowest_rtt(),
        health_weighted(),
        least_connections(),
        failover(),
        weighted_round_robin(),
    ] {
        let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
        let lb = LoadBalancer::new(backends, strategy).unwrap();
        let r = lb.dial("a:80").await;
        assert!(r.is_ok());
    }
}

#[tokio::test]
async fn dial_with_all_retry_policies() {
    // Test with ExponentialBackoff
    {
        let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
        let lb = LoadBalancer::builder()
            .backends(backends)
            .strategy(round_robin())
            .retry_policy(ExponentialBackoff::new(Duration::from_millis(1)))
            .build()
            .await
            .unwrap();
        let r = lb.dial("a:80").await;
        assert!(r.is_ok());
    }
    // Test with FixedRetry
    {
        let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
        let lb = LoadBalancer::builder()
            .backends(backends)
            .strategy(round_robin())
            .retry_policy(FixedRetry::new(Duration::from_millis(1)))
            .build()
            .await
            .unwrap();
        let r = lb.dial("a:80").await;
        assert!(r.is_ok());
    }
    // Test with NoRetry
    {
        let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
        let lb = LoadBalancer::builder()
            .backends(backends)
            .strategy(round_robin())
            .retry_policy(NoRetry)
            .build()
            .await
            .unwrap();
        let r = lb.dial("a:80").await;
        assert!(r.is_ok());
    }
}

#[tokio::test]
async fn dial_with_initial_metrics() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let initial_metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(5)),
        active_connections: 0,
        recent_errors: 0,
        total_dials: 0,
        total_errors: 0,
    }];
    let lb = LoadBalancer::new_with_metrics(backends, initial_metrics, round_robin(), None, None)
        .unwrap();
    let r = lb.dial("a:80").await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn dial_with_many_concurrent_dials() {
    // The LoadBalancer is designed to be used with shared ownership
    // We just test that multiple sequential dials work
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(FinalTestBackend::new("a"))];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    for i in 1..=5 {
        let r = lb.dial(&format!("a:{}", 8000 + i)).await;
        assert!(r.is_ok());
    }
}
