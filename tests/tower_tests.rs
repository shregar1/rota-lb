//! Tests for the tower integration.

#![cfg(feature = "tower")]

use std::time::Duration;
use async_trait::async_trait;
use tokio::io::duplex;
use rota::backend::{Backend, Connection};
use rota::error::Error;
use rota::{LoadBalancer, round_robin};

struct TowerMockBackend {
    #[allow(dead_code)]
    name: String,
}

#[async_trait]
impl Backend for TowerMockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }
    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn tower_lb_request_new() {
    use rota::tower::LbRequest;
    let req = LbRequest::new("example.com:443");
    assert_eq!(req.addr, "example.com:443");
    assert!(req.dial_timeout.is_none());
    assert!(req.retry_policy.is_none());
}

#[tokio::test]
async fn tower_lb_request_with_timeout() {
    use rota::tower::LbRequest;
    let req = LbRequest::new("example.com:443")
        .with_dial_timeout(Duration::from_secs(5));
    assert_eq!(req.dial_timeout, Some(Duration::from_secs(5)));
}

#[tokio::test]
async fn tower_lb_request_with_retry_policy() {
    use rota::retry::ExponentialBackoff;
    use rota::tower::LbRequest;
    let policy = ExponentialBackoff::new(Duration::from_millis(10));
    let req = LbRequest::new("example.com:443")
        .with_retry_policy(policy);
    assert!(req.retry_policy.is_some());
}

#[tokio::test]
async fn tower_service_basic() {
    use tower::Service;
    use std::sync::Arc;
    use rota::tower::LbRequest;

    let backends: Vec<Box<dyn Backend>> = vec![
        Box::new(TowerMockBackend { name: "a".into() }),
        Box::new(TowerMockBackend { name: "b".into() }),
    ];
    let lb: Arc<LoadBalancer> = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());

    let mut svc = lb.clone();
    let req = LbRequest::new("example.com:443");
    let conn = svc.call(req).await.unwrap();
    drop(conn);
}