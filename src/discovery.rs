//! Service discovery plugin trait and built-in implementations.
//!
//! This module provides a trait for discovering backends from various sources
//! (Consul, etcd, DNS, static config, etc.) and a `Discover` wrapper that
//! automatically updates a `LoadBalancer` when the backend set changes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::balancer::LoadBalancer;
use crate::backend::Backend;
use crate::error::Error;

#[cfg(feature = "discovery")]
use serde::{Deserialize, Serialize};

/// A backend descriptor returned by service discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "discovery", derive(Serialize, Deserialize))]
pub struct BackendDescriptor {
    /// Unique identifier for this backend instance.
    pub id: String,
    /// The address to dial (host:port, or a URL).
    pub addr: String,
    /// Optional metadata/tags for filtering/routing.
    pub metadata: HashMap<String, String>,
    /// Optional weight for weighted strategies.
    pub weight: Option<u32>,
    /// Optional health check endpoint (if different from addr).
    pub health_check: Option<String>,
}

impl BackendDescriptor {
    /// Create a new backend descriptor.
    pub fn new(id: impl Into<String>, addr: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            addr: addr.into(),
            metadata: HashMap::new(),
            weight: None,
            health_check: None,
        }
    }

    /// Add a metadata tag.
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set weight for weighted strategies.
    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = Some(weight);
        self
    }

    /// Set health check endpoint.
    pub fn with_health_check(mut self, health_check: impl Into<String>) -> Self {
        self.health_check = Some(health_check.into());
        self
    }
}

/// Trait for discovering backends from a service registry.
///
/// Implement this trait to add new service discovery backends.
/// The load balancer will periodically call `discover()` and reconcile
/// its backend pool with the returned descriptors.
#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// Discover currently available backends.
    ///
    /// Called periodically by the `Discover` loop. Should return all
    /// currently healthy backends, or an error if discovery failed.
    async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error>;

    /// Optional: get the suggested poll interval.
    /// Default is 30 seconds.
    fn poll_interval(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Optional: called when discovery starts.
    async fn on_start(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Optional: called when discovery stops.
    async fn on_stop(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// A factory that creates backends from descriptors.
///
/// This trait bridges service discovery (which returns descriptors)
/// and the load balancer (which needs `Backend` instances).
#[async_trait]
pub trait BackendFactoryFromDescriptor: Send + Sync {
    /// The type of backend created by this factory.
    type Backend: Backend;
    /// The error type returned when creation fails.
    type Error: Into<Error>;

    /// Create a backend from a descriptor.
    async fn create(&self, descriptor: &BackendDescriptor) -> Result<Self::Backend, Self::Error>;
}

/// A simple static service discovery source.
///
/// Useful for testing or when backends are known at compile time.
#[derive(Debug, Clone)]
pub struct StaticDiscovery {
    descriptors: Vec<BackendDescriptor>,
}

impl StaticDiscovery {
    /// Create a new static discovery source.
    pub fn new(descriptors: Vec<BackendDescriptor>) -> Self {
        Self { descriptors }
    }

    /// Create from a list of (id, name, address) tuples.
    pub fn from_tuples(backends: Vec<(String, String, String)>) -> Self {
        Self {
            descriptors: backends
                .into_iter()
                .map(|(id, _name, addr)| BackendDescriptor::new(id, addr))
                .collect(),
        }
    }
}

#[async_trait]
impl ServiceDiscovery for StaticDiscovery {
    async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
        Ok(self.descriptors.clone())
    }
}

/// A `LoadBalancer` that automatically reconciles its backends with
/// a `ServiceDiscovery` source.
pub struct Discover<D: ServiceDiscovery + 'static, F: BackendFactoryFromDescriptor + 'static> {
    lb: Arc<LoadBalancer>,
    discovery: D,
    factory: F,
    poll_interval: Duration,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<D, F> std::fmt::Debug for Discover<D, F>
where
    D: ServiceDiscovery + 'static,
    F: BackendFactoryFromDescriptor + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discover")
            .field("backend_count", &self.lb.backend_count())
            .finish_non_exhaustive()
    }
}

impl<D: ServiceDiscovery + Clone + 'static, F: BackendFactoryFromDescriptor + Clone + 'static> Discover<D, F> {
    /// Create a new `Discover` wrapper.
    ///
    /// - `lb`: The load balancer to manage.
    /// - `discovery`: The service discovery source.
    /// - `factory`: Factory for creating backends from descriptors.
    /// - `poll_interval`: How often to poll discovery (default: discovery's interval).
    pub fn new(
        lb: LoadBalancer,
        discovery: D,
        factory: F,
        poll_interval: Option<Duration>,
    ) -> Self {
        let pi = discovery.poll_interval();
        Self {
            lb: Arc::new(lb),
            discovery,
            factory,
            poll_interval: poll_interval.unwrap_or(pi),
            shutdown_tx: None,
        }
    }

    /// Start the background discovery loop.
    pub async fn start(&mut self) -> Result<(), Error> {
        self.discovery.on_start().await?;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let lb = self.lb.clone();
        let discovery = self.discovery.clone();
        let factory = self.factory.clone();
        let interval = self.poll_interval;

        tokio::spawn(async move {
            Self::run_loop(lb, discovery, factory, interval, shutdown_rx).await;
        });

        Ok(())
    }

    /// Stop the background discovery loop.
    pub async fn stop(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    /// Get a reference to the inner load balancer.
    pub fn load_balancer(&self) -> &LoadBalancer {
        &self.lb
    }

    /// Dial through the load balancer (convenience method).
    pub async fn dial(&self, addr: &str) -> Result<crate::balancer::GuardedConnection, Error> {
        self.lb.dial(addr).await
    }

    async fn run_loop(
        lb: Arc<LoadBalancer>,
        discovery: D,
        factory: F,
        interval: Duration,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    match discovery.discover().await {
                        Ok(descriptors) => {
                            if let Err(e) = Self::reconcile(&lb, descriptors, &factory).await {
                                tracing::warn!("Failed to reconcile backends: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Service discovery failed: {}", e);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    }

    async fn reconcile(
        _lb: &LoadBalancer,
        descriptors: Vec<BackendDescriptor>,
        factory: &F,
    ) -> Result<(), Error> {
        // For a full implementation, LoadBalancer would need methods to:
        // 1. Get current backend descriptors/IDs
        // 2. Add/remove/replace backends
        // 3. Update metrics for existing backends
        //
        // For now, this is a placeholder - the real implementation
        // would require adding methods to LoadBalancer to replace backends.
        let _ = (descriptors, factory);
        Ok(())
    }
}

/// DNS-based service discovery (SRV records).
#[cfg(feature = "dns")]
pub mod dns {
    use super::*;
    use trust_dns_resolver::Resolver;

    pub struct DnsDiscovery {
        resolver: Resolver,
        service_name: String,
        port: u16,
    }

    impl DnsDiscovery {
        pub fn new(service_name: String, port: u16) -> Result<Self, Error> {
            let resolver = Resolver::new(
                trust_dns_resolver::config::ResolverConfig::default(),
                trust_dns_resolver::config::ResolverOpts::default(),
            )?;
            Ok(Self {
                resolver,
                service_name,
                port,
            })
        }
    }

    #[async_trait]
    impl ServiceDiscovery for DnsDiscovery {
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let srv_name = format!("_http._tcp.{}", self.service_name);
            let response = self.resolver.srv_lookup(&srv_name).await?;

            let mut descriptors = Vec::new();
            for srv in response.iter() {
                let target = srv.target().to_string();
                let port = srv.port();
                let addr = format!("{}:{}", target.trim_end_matches('.'), port);

                descriptors.push(BackendDescriptor {
                    id: format!("{}:{}", target, port),
                    addr,
                    metadata: HashMap::new(),
                    weight: Some(srv.weight()),
                    health_check: None,
                });
            }
            Ok(descriptors)
        }
    }
}

/// Consul service discovery.
#[cfg(feature = "consul")]
pub mod consul {
    use super::*;
    use consulrs::Client;
    use std::collections::HashMap;

    pub struct ConsulDiscovery {
        client: Client,
        service_name: String,
        tag_filter: Option<String>,
    }

    impl ConsulDiscovery {
        pub fn new(consul_addr: &str, service_name: String) -> Result<Self, Error> {
            let client = Client::new(consul_addr)?;
            Ok(Self {
                client,
                service_name,
                tag_filter: None,
            })
        }

        pub fn with_tag(mut self, tag: String) -> Self {
            self.tag_filter = Some(tag);
            self
        }
    }

    #[async_trait]
    impl ServiceDiscovery for ConsulDiscovery {
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let mut query = consulrs::query::QueryOptions::default();
            if let Some(tag) = &self.tag_filter {
                query.tag = Some(tag.clone());
            }

            let services = self.client
                .health()
                .service(&self.service_name, true, &query, None)
                .await?;

            let mut descriptors = Vec::new();
            for entry in services {
                let service = &entry.service;
                let checks = &entry.checks;

                // Only include if all checks are passing
                if checks.iter().all(|c| c.status == consulrs::api::CheckStatus::Passing) {
                    let addr = format!("{}:{}", service.address, service.port);
                    let mut metadata = HashMap::new();
                    for (k, v) in &service.meta {
                        metadata.insert(k.clone(), v.clone());
                    }
                    if let Some(tags) = &service.tags {
                        metadata.insert("tags".into(), tags.join(","));
                    }

                    descriptors.push(BackendDescriptor {
                        id: service.id.clone(),
                        addr,
                        metadata,
                        weight: Some(1),
                        health_check: Some(format!("{}:{}/health", service.address, service.port)),
                    });
                }
            }
            Ok(descriptors)
        }
    }
}

/// etcd service discovery.
#[cfg(feature = "etcd")]
pub mod etcd {
    use super::*;
    use etcd_client::Client;

    pub struct EtcdDiscovery {
        client: Client,
        prefix: String,
    }

    impl EtcdDiscovery {
        pub async fn new(etcd_endpoints: Vec<String>, prefix: String) -> Result<Self, Error> {
            let client = Client::connect(etcd_endpoints, None).await?;
            Ok(Self { client, prefix })
        }
    }

    #[async_trait]
    impl ServiceDiscovery for EtcdDiscovery {
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let mut descriptors = Vec::new();
            let resp = self.client.get(self.prefix.clone(), Some(etcd_client::GetOptions::new().with_prefix())).await?;

            for kv in resp.kvs() {
                let key = std::str::from_utf8(kv.key())?;
                let value = std::str::from_utf8(kv.value())?;

                if let Ok(addr) = value.parse::<std::net::SocketAddr>() {
                    descriptors.push(BackendDescriptor {
                        id: key.to_string(),
                        addr: addr.to_string(),
                        metadata: HashMap::new(),
                        weight: Some(1),
                        health_check: None,
                    });
                }
            }
            Ok(descriptors)
        }
    }
}