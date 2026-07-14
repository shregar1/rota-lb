//! More discovery tests to improve coverage.

#![cfg(feature = "discovery")]

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::duplex;

use rota_lb::backend::{Backend, Connection};
use rota_lb::discovery::{
    BackendDescriptor, BackendFactoryFromDescriptor, Discover, ServiceDiscovery, StaticDiscovery,
};
use rota_lb::error::Error;
use rota_lb::strategies::round_robin;
use rota_lb::LoadBalancer;

struct DiscBackend;

#[async_trait]
impl Backend for DiscBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(64);
        Ok(Box::pin(a))
    }
    async fn shutdown(&mut self) {}
}

struct DiscFactory;

#[async_trait]
impl BackendFactoryFromDescriptor for DiscFactory {
    type Backend = DiscBackend;
    type Error = Error;
    async fn create(&self, _descriptor: &BackendDescriptor) -> Result<Self::Backend, Self::Error> {
        Ok(DiscBackend)
    }
}

#[tokio::test]
async fn discover_start_stop_with_real_dial() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DiscBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let descriptors = vec![BackendDescriptor::new("a", "localhost:8001")];
    let discovery = StaticDiscovery::new(descriptors);
    let factory = DiscFactory;
    let mut discover = Discover::new(lb, discovery, factory, Some(Duration::from_millis(50)));
    assert!(discover.start().await.is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(discover.stop().await.is_ok());
}

#[tokio::test]
async fn discover_dial_after_start() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DiscBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let descriptors = vec![BackendDescriptor::new("a", "localhost:8001")];
    let discovery = StaticDiscovery::new(descriptors);
    let factory = DiscFactory;
    let mut discover = Discover::new(lb, discovery, factory, Some(Duration::from_secs(60)));
    assert!(discover.start().await.is_ok());
    let conn = discover.dial("a:80").await;
    assert!(conn.is_ok());
    assert!(discover.stop().await.is_ok());
}

#[tokio::test]
async fn discover_debug() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DiscBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let descriptors = vec![BackendDescriptor::new("a", "localhost:8001")];
    let discovery = StaticDiscovery::new(descriptors);
    let factory = DiscFactory;
    let discover = Discover::new(lb, discovery, factory, Some(Duration::from_millis(100)));
    let _ = format!("{:?}", discover);
}

#[tokio::test]
async fn discover_with_multiple_descriptors() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DiscBackend), Box::new(DiscBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let descriptors = vec![
        BackendDescriptor::new("a", "localhost:8001"),
        BackendDescriptor::new("b", "localhost:8002"),
    ];
    let discovery = StaticDiscovery::new(descriptors);
    let factory = DiscFactory;
    let mut discover = Discover::new(lb, discovery, factory, Some(Duration::from_millis(50)));
    assert!(discover.start().await.is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(discover.stop().await.is_ok());
}

#[tokio::test]
async fn discover_static_discovery() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(DiscBackend)];
    let _lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let descriptors = vec![BackendDescriptor::new("a", "localhost:8001")];
    let discovery = StaticDiscovery::new(descriptors);
    let result = discovery.discover().await;
    assert!(result.is_ok());
    let descs = result.unwrap();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].id, "a");
}

#[tokio::test]
async fn discover_factory_from_descriptor() {
    let desc = BackendDescriptor::new("a", "localhost:8001");
    let factory = DiscFactory;
    let result = factory.create(&desc).await;
    assert!(result.is_ok());
}
