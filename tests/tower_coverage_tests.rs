//! Additional tests for the tower module to improve coverage.

#![cfg(feature = "tower")]

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::retry::ExponentialBackoff;
use rota_lb::strategies::round_robin;
use rota_lb::tower::LbRequest;
use rota_lb::LoadBalancer;
use tower::Service;

struct TowerTestBackend;

#[async_trait]
impl Backend for TowerTestBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }
    async fn shutdown(&mut self) {}
}

#[test]
fn lb_request_debug() {
    let req = LbRequest::new("example.com:443");
    let s = format!("{:?}", req);
    assert!(s.contains("LbRequest"));
}

#[test]
fn lb_request_clone() {
    let req = LbRequest::new("example.com:443");
    let _req2 = req.clone();
}

#[test]
fn lb_request_full_builder() {
    let policy = ExponentialBackoff::new(Duration::from_millis(10));
    let req = LbRequest::new("example.com:443")
        .with_dial_timeout(Duration::from_secs(5))
        .with_retry_policy(policy);
    assert_eq!(req.addr, "example.com:443");
    assert_eq!(req.dial_timeout, Some(Duration::from_secs(5)));
    assert!(req.retry_policy.is_some());
}

#[test]
fn lb_request_empty_addr() {
    let req = LbRequest::new("");
    assert_eq!(req.addr, "");
}

#[test]
fn lb_request_long_addr() {
    let long_addr = "a".repeat(1000) + ":443";
    let req = LbRequest::new(long_addr.clone());
    assert_eq!(req.addr, long_addr);
}

#[tokio::test]
async fn tower_service_poll_ready() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(TowerTestBackend)];
    let lb: Arc<LoadBalancer> = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());
    let _svc = lb.clone();
    // Just verify we can create a request
    let req = LbRequest::new("example.com:443");
    assert_eq!(req.addr, "example.com:443");
}

#[tokio::test]
async fn tower_service_multiple_dials() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(TowerTestBackend)];
    let lb: Arc<LoadBalancer> = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());
    let mut svc = lb.clone();
    for i in 0..3 {
        let req = LbRequest::new(format!("example.com:{}", 443 + i));
        let conn = svc.call(req).await.unwrap();
        drop(conn);
    }
}

#[tokio::test]
async fn tower_service_poll_ready_with_empty() {
    // This should fail since there are no backends
    let backends: Vec<Box<dyn Backend>> = vec![];
    let result = LoadBalancer::new(backends, round_robin());
    assert!(result.is_err());
}

#[tokio::test]
async fn lb_request_poll_ready_check() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(TowerTestBackend)];
    let lb: Arc<LoadBalancer> = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());
    let mut svc = lb.clone();
    let result = std::future::poll_fn(|cx| {
        <Arc<LoadBalancer> as Service<LbRequest>>::poll_ready(&mut svc, cx)
    })
    .await;
    assert!(result.is_ok());
}
