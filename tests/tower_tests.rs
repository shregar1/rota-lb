//! Tests for the tower integration.

#![cfg(feature = "tower")]

use async_trait::async_trait;
use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::{round_robin, LoadBalancer};
use std::time::Duration;
use tokio::io::duplex;

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
    use rota_lb::tower::LbRequest;
    let req = LbRequest::new("example.com:443");
    assert_eq!(req.addr, "example.com:443");
    assert!(req.dial_timeout.is_none());
    assert!(req.retry_policy.is_none());
}

#[tokio::test]
async fn tower_lb_request_with_timeout() {
    use rota_lb::tower::LbRequest;
    let req = LbRequest::new("example.com:443").with_dial_timeout(Duration::from_secs(5));
    assert_eq!(req.dial_timeout, Some(Duration::from_secs(5)));
}

#[tokio::test]
async fn tower_lb_request_with_retry_policy() {
    use rota_lb::retry::ExponentialBackoff;
    use rota_lb::tower::LbRequest;
    let policy = ExponentialBackoff::new(Duration::from_millis(10));
    let req = LbRequest::new("example.com:443").with_retry_policy(policy);
    assert!(req.retry_policy.is_some());
}

#[tokio::test]
async fn tower_service_basic() {
    use rota_lb::tower::LbRequest;
    use std::sync::Arc;
    use tower::Service;

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
