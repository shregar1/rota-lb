//! Tests for the factory module.

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::strategy::TunnelMetrics;
use rota_lb::BackendFactory;
use rota_lb::BackendOutput;

struct MockFactory {
    name: String,
    fail: bool,
}

#[async_trait]
impl BackendFactory for MockFactory {
    async fn create(&self) -> Result<BackendOutput, Error> {
        if self.fail {
            return Err(Error::factory("factory failed"));
        }
        let _ = duplex(64);
        Ok(BackendOutput {
            backend: Box::new(MockBackend::new(&self.name)),
            initial_metrics: TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                ..Default::default()
            },
        })
    }
}

struct MockBackend {
    #[allow(dead_code)]
    name: String,
}

impl MockBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn from_factories_creates_backends() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![
        Box::new(MockFactory {
            name: "a".into(),
            fail: false,
        }),
        Box::new(MockFactory {
            name: "b".into(),
            fail: false,
        }),
    ];
    let lb = rota_lb::LoadBalancer::from_factories(factories, rota_lb::round_robin())
        .await
        .unwrap();
    let conn = lb.dial("test:80").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn from_factories_empty_errors() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![];
    let result = rota_lb::LoadBalancer::from_factories(factories, rota_lb::round_robin()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn from_factories_propagates_factory_error() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(MockFactory {
        name: "a".into(),
        fail: true,
    })];
    let result = rota_lb::LoadBalancer::from_factories(factories, rota_lb::round_robin()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn from_factories_preserves_order() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![
        Box::new(MockFactory {
            name: "a".into(),
            fail: false,
        }),
        Box::new(MockFactory {
            name: "b".into(),
            fail: false,
        }),
        Box::new(MockFactory {
            name: "c".into(),
            fail: false,
        }),
    ];
    let lb = rota_lb::LoadBalancer::from_factories(factories, rota_lb::round_robin())
        .await
        .unwrap();
    assert_eq!(lb.backend_count(), 3);
}
