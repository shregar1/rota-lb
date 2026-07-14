//! Tests for the service discovery module.

#![cfg(feature = "discovery")]

use async_trait::async_trait;
use rota_lb::backend::{Backend, Connection};
use rota_lb::discovery::{
    BackendDescriptor, BackendFactoryFromDescriptor, Discover, ServiceDiscovery, StaticDiscovery,
};
use rota_lb::error::Error;
use rota_lb::{round_robin, LoadBalancer};
use std::collections::HashMap;
use std::time::Duration;

#[allow(dead_code)]
struct MockDiscovery {
    descriptors: Vec<BackendDescriptor>,
}

#[async_trait]
impl ServiceDiscovery for MockDiscovery {
    async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
        Ok(self.descriptors.clone())
    }
}

struct MockBackend;

#[async_trait]
impl Backend for MockBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = tokio::io::duplex(64);
        Ok(Box::pin(a))
    }
    async fn shutdown(&mut self) {}
}

#[derive(Clone)]
struct MockFactory;

#[async_trait]
impl BackendFactoryFromDescriptor for MockFactory {
    type Backend = MockBackend;
    type Error = Error;
    async fn create(&self, _descriptor: &BackendDescriptor) -> Result<Self::Backend, Self::Error> {
        Ok(MockBackend)
    }
}

fn make_desc(id: &str, addr: &str) -> BackendDescriptor {
    BackendDescriptor::new(id, addr)
}

#[tokio::test]
async fn static_discovery_basic() {
    let backends = vec![
        make_desc("a", "localhost:8001"),
        make_desc("b", "localhost:8002"),
    ];
    let discovery = StaticDiscovery::new(backends);
    let result = discovery.discover().await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, "a");
    assert_eq!(result[1].id, "b");
}

#[tokio::test]
async fn static_discovery_poll_interval() {
    let discovery = StaticDiscovery::new(vec![]);
    assert_eq!(discovery.poll_interval(), Duration::from_secs(30));
}

#[tokio::test]
async fn static_discovery_on_start_stop() {
    let discovery = StaticDiscovery::new(vec![]);
    assert!(discovery.on_start().await.is_ok());
    assert!(discovery.on_stop().await.is_ok());
}

#[tokio::test]
async fn discover_new() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let discovery = StaticDiscovery::new(vec![make_desc("a", "localhost:8001")]);
    let factory = MockFactory;
    let _discover = Discover::new(lb, discovery, factory, Some(Duration::from_millis(100)));
}

#[test]
fn backend_descriptor_new() {
    let desc = BackendDescriptor::new("id1", "addr1");
    assert_eq!(desc.id, "id1");
    assert_eq!(desc.addr, "addr1");
    assert!(desc.metadata.is_empty());
    assert!(desc.weight.is_none());
    assert!(desc.health_check.is_none());
}

#[test]
fn backend_descriptor_with_tag() {
    let desc = BackendDescriptor::new("id1", "addr1").with_tag("env", "prod");
    assert_eq!(desc.metadata.get("env"), Some(&"prod".to_string()));
}

#[test]
fn backend_descriptor_with_weight() {
    let desc = BackendDescriptor::new("id1", "addr1").with_weight(10);
    assert_eq!(desc.weight, Some(10));
}

#[test]
fn backend_descriptor_with_health_check() {
    let desc = BackendDescriptor::new("id1", "addr1").with_health_check("http://localhost/health");
    assert_eq!(
        desc.health_check,
        Some("http://localhost/health".to_string())
    );
}

#[test]
fn backend_descriptor_equality() {
    let d1 = make_desc("a", "x:80");
    let d2 = make_desc("a", "x:80");
    let d3 = make_desc("b", "x:80");
    assert_eq!(d1, d2);
    assert_ne!(d1, d3);
}

#[test]
fn backend_descriptor_clone() {
    let d1 = make_desc("a", "x:80");
    let d2 = d1.clone();
    assert_eq!(d1, d2);
}

#[test]
fn backend_descriptor_debug() {
    let d = make_desc("a", "x:80");
    let _ = format!("{:?}", d);
}

#[tokio::test]
async fn discover_start_stop() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let discovery = StaticDiscovery::new(vec![make_desc("a", "localhost:8001")]);
    let factory = MockFactory;
    let mut discover = Discover::new(lb, discovery, factory, Some(Duration::from_millis(50)));
    assert!(discover.start().await.is_ok());
    // Let the loop run a bit
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(discover.stop().await.is_ok());
}

#[tokio::test]
async fn discover_load_balancer_access() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(MockBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let discovery = StaticDiscovery::new(vec![make_desc("a", "localhost:8001")]);
    let factory = MockFactory;
    let discover = Discover::new(lb, discovery, factory, Some(Duration::from_secs(1)));
    let _lb_arc = discover.load_balancer_arc();
}

#[test]
fn metadata_helper() {
    let mut metadata = HashMap::new();
    metadata.insert("region".to_string(), "us-east".to_string());
    let desc = BackendDescriptor::new("a", "x:80").with_tag("region", "us-east");
    assert_eq!(desc.metadata.get("region"), Some(&"us-east".to_string()));
}
