//! Tests for the factory module to improve coverage.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::strategies::round_robin;
use rota_lb::strategy::TunnelMetrics;
use rota_lb::{BackendFactory, BackendOutput, LoadBalancer};

struct TestFactory {
    name: String,
    fail: bool,
    count: Arc<AtomicU32>,
}

#[async_trait]
impl BackendFactory for TestFactory {
    async fn create(&self) -> Result<BackendOutput, Error> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(Error::factory(format!("{}: factory failed", self.name)));
        }
        let _ = duplex(64);
        Ok(BackendOutput {
            backend: Box::new(TestBackend::new(&self.name)),
            initial_metrics: TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                ..Default::default()
            },
        })
    }
}

struct TestBackend {
    #[allow(dead_code)]
    name: String,
}

impl TestBackend {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl Backend for TestBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }
    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn factory_struct_creation() {
    let count = Arc::new(AtomicU32::new(0));
    let factory = TestFactory {
        name: "test".to_string(),
        fail: false,
        count: count.clone(),
    };
    let result = factory.create().await;
    assert!(result.is_ok());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn factory_struct_failure() {
    let factory = TestFactory {
        name: "test".to_string(),
        fail: true,
        count: Arc::new(AtomicU32::new(0)),
    };
    let result = factory.create().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn factory_struct_multiple_calls() {
    let count = Arc::new(AtomicU32::new(0));
    let factory = TestFactory {
        name: "test".to_string(),
        fail: false,
        count: count.clone(),
    };
    for _ in 0..3 {
        let _ = factory.create().await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn from_factories_creates_correct_number() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![
        Box::new(TestFactory {
            name: "a".to_string(),
            fail: false,
            count: Arc::new(AtomicU32::new(0)),
        }),
        Box::new(TestFactory {
            name: "b".to_string(),
            fail: false,
            count: Arc::new(AtomicU32::new(0)),
        }),
    ];
    let lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    assert_eq!(lb.backend_count(), 2);
}

#[tokio::test]
async fn from_factories_with_initial_metrics() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(TestFactory {
        name: "a".to_string(),
        fail: false,
        count: Arc::new(AtomicU32::new(0)),
    })];
    let metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(5)),
        ..Default::default()
    }];
    let lb = LoadBalancer::builder()
        .factories(factories)
        .strategy(round_robin())
        .initial_metrics(metrics)
        .build()
        .await
        .unwrap();
    let m = lb.metrics().await;
    assert_eq!(m[0].rtt, Some(Duration::from_millis(5)));
}

#[tokio::test]
async fn factory_output_struct_debug() {
    let output = BackendOutput {
        backend: Box::new(TestBackend::new("test")),
        initial_metrics: TunnelMetrics::default(),
    };
    let _ = format!("{:?}", output);
}

#[tokio::test]
async fn from_factories_uses_provided_metrics_over_factory() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(TestFactory {
        name: "a".to_string(),
        fail: false,
        count: Arc::new(AtomicU32::new(0)),
    })];
    let metrics = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(100)),
        ..Default::default()
    }];
    let lb = LoadBalancer::builder()
        .factories(factories)
        .strategy(round_robin())
        .initial_metrics(metrics)
        .build()
        .await
        .unwrap();
    let m = lb.metrics().await;
    // The provided metrics override the factory's
    assert_eq!(m[0].rtt, Some(Duration::from_millis(100)));
}
