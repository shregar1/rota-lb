//! More tests for the factory module.

use async_trait::async_trait;
use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::strategies::round_robin;
use rota_lb::strategy::TunnelMetrics;
use rota_lb::BackendFactory;
use rota_lb::BackendOutput;
use rota_lb::LoadBalancer;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::duplex;

struct CountingFactory {
    name: String,
    counter: Arc<AtomicU32>,
    fail: bool,
}

#[async_trait]
impl BackendFactory for CountingFactory {
    async fn create(&self) -> Result<BackendOutput, Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(Error::factory("counting factory failed"));
        }
        let _ = duplex(64);
        Ok(BackendOutput {
            backend: Box::new(MockBackend {
                name: self.name.clone(),
            }),
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
async fn factory_called_once_per_backend() {
    let counter = Arc::new(AtomicU32::new(0));
    let factories: Vec<Box<dyn BackendFactory>> = vec![
        Box::new(CountingFactory {
            name: "a".into(),
            counter: counter.clone(),
            fail: false,
        }),
        Box::new(CountingFactory {
            name: "b".into(),
            counter: counter.clone(),
            fail: false,
        }),
    ];
    let _lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn factory_initial_metrics_used() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(CountingFactory {
        name: "a".into(),
        counter: Arc::new(AtomicU32::new(0)),
        fail: false,
    })];
    let lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    let metrics = lb.metrics().await;
    assert_eq!(metrics.len(), 1);
    // Initial RTT should be 10ms from our factory
    assert_eq!(metrics[0].rtt, Some(Duration::from_millis(10)));
}

#[tokio::test]
async fn factory_failure_aborts_creation() {
    let counter = Arc::new(AtomicU32::new(0));
    let factories: Vec<Box<dyn BackendFactory>> = vec![
        Box::new(CountingFactory {
            name: "a".into(),
            counter: counter.clone(),
            fail: false,
        }),
        Box::new(CountingFactory {
            name: "b".into(),
            counter: counter.clone(),
            fail: true,
        }),
    ];
    let result = LoadBalancer::from_factories(factories, round_robin()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn factory_creation_with_zero_backends() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![];
    let result = LoadBalancer::from_factories(factories, round_robin()).await;
    assert!(result.is_err());
    if let Err(Error::NoBackends(_)) = result {
        // Expected
    } else {
        panic!("Expected NoBackends error");
    }
}

#[tokio::test]
async fn factory_creation_with_one_backend() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(CountingFactory {
        name: "only".into(),
        counter: Arc::new(AtomicU32::new(0)),
        fail: false,
    })];
    let lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    assert_eq!(lb.backend_count(), 1);
}

#[tokio::test]
async fn factory_creation_with_many_backends() {
    let factories: Vec<Box<dyn BackendFactory>> = (0..10)
        .map(|i| {
            Box::new(CountingFactory {
                name: format!("b{}", i),
                counter: Arc::new(AtomicU32::new(0)),
                fail: false,
            }) as Box<dyn BackendFactory>
        })
        .collect();
    let lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    assert_eq!(lb.backend_count(), 10);
}

#[tokio::test]
async fn factory_output_struct() {
    let (a, _b) = tokio::io::duplex(64);
    let _ = a;
    let output = BackendOutput {
        backend: Box::new(MockBackend::new("test")),
        initial_metrics: TunnelMetrics::default(),
    };
    // Just verify it can be created
    let _ = output;
}
