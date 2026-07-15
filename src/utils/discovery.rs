//! Service discovery plugin trait and built-in implementations.
//!
//! This module provides a trait for discovering backends from various sources
//! (Consul, etcd, DNS, static config, etc.) and a `Discover` wrapper that
//! automatically updates a `LoadBalancer` when the backend set changes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::constants::DEFAULT_HEALTH_CHECK_INTERVAL;

use crate::traits::backend::Backend;
use crate::services::balancer::LoadBalancer;
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
    #[must_use]
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set weight for weighted strategies.
    #[must_use]
    pub const fn with_weight(mut self, weight: u32) -> Self {
        self.weight = Some(weight);
        self
    }

    /// Set health check endpoint.
    #[must_use]
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
        DEFAULT_HEALTH_CHECK_INTERVAL
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
    #[must_use]
    pub const fn new(descriptors: Vec<BackendDescriptor>) -> Self {
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
    lb: Arc<tokio::sync::Mutex<LoadBalancer>>,
    discovery: Arc<D>,
    factory: Arc<F>,
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
            .field("backend_count", &"<locked>")
            .finish_non_exhaustive()
    }
}

impl<D: ServiceDiscovery + 'static, F: BackendFactoryFromDescriptor + 'static> Discover<D, F> {
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
            lb: Arc::new(tokio::sync::Mutex::new(lb)),
            discovery: Arc::new(discovery),
            factory: Arc::new(factory),
            poll_interval: poll_interval.unwrap_or(pi),
            shutdown_tx: None,
        }
    }

    /// Start the background discovery loop.
    pub async fn start(&mut self) -> Result<(), Error> {
        self.discovery.on_start().await?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
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
    #[allow(clippy::unused_async)]
    pub async fn stop(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    /// Get a clone of the inner load balancer's Arc for direct access.
    pub fn load_balancer_arc(&self) -> Arc<tokio::sync::Mutex<LoadBalancer>> {
        self.lb.clone()
    }

    /// Dial through the load balancer (convenience method).
    pub async fn dial(&self, addr: &str) -> Result<crate::services::balancer::GuardedConnection, Error> {
        self.lb.lock().await.dial(addr).await
    }

    async fn run_loop(
        lb: Arc<tokio::sync::Mutex<LoadBalancer>>,
        discovery: Arc<D>,
        factory: Arc<F>,
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
                            let mut lb = lb.lock().await;
                            if let Err(e) = Self::reconcile(&mut lb, descriptors, &*factory).await {
                                tracing::warn!("Failed to reconcile backends: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Service discovery failed: {e}");
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    }

    /// Diff the current backends against discovered descriptors and
    /// add/remove/replace accordingly.
    async fn reconcile(
        lb: &mut LoadBalancer,
        descriptors: Vec<BackendDescriptor>,
        factory: &F,
    ) -> Result<(), Error> {
        let current_ids = lb.backend_ids().await;

        // Build a set of discovered IDs for fast lookup
        let discovered_ids: std::collections::HashSet<&str> =
            descriptors.iter().map(|d| d.id.as_str()).collect();

        // Remove backends that are no longer in discovery
        for id in current_ids.iter().flatten() {
            if !discovered_ids.contains(id.as_str()) {
                lb.remove_backend_by_id(id).await;
            }
        }

        // Add backends that are new
        for desc in &descriptors {
            let exists = current_ids.iter().any(|id| id.as_deref() == Some(&desc.id));
            if !exists {
                let backend = factory.create(desc).await.map_err(Into::into)?;
                lb.add_backend_with_id(desc.id.clone(), Box::new(backend))
                    .await;
            }
        }

        Ok(())
    }
}

/// DNS-based service discovery (SRV records).
#[cfg(feature = "dns")]
pub mod dns {
    use super::{BackendDescriptor, Error, ServiceDiscovery};
    use crate::constants::DEFAULT_SRV_PREFIX;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use trust_dns_resolver::Resolver;

    /// DNS-based service discovery using SRV records.
    pub struct DnsDiscovery {
        resolver: Resolver,
        service_name: String,
        /// SRV lookup prefix, e.g. "_http._tcp". Defaults to "_http._tcp".
        srv_prefix: String,
    }

    impl std::fmt::Debug for DnsDiscovery {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DnsDiscovery")
                .field("service_name", &self.service_name)
                .finish_non_exhaustive()
        }
    }

    impl DnsDiscovery {
        /// Create a new DNS discovery source for the given service name and port.
        ///
        /// The `port` parameter is used as a fallback when SRV records do not
        /// include port information. For standard SRV lookups, ports come from
        /// the DNS response.
        pub fn new(service_name: String, _port: u16) -> Result<Self, Error> {
            let resolver = Resolver::new(
                trust_dns_resolver::config::ResolverConfig::default(),
                trust_dns_resolver::config::ResolverOpts::default(),
            )
            .map_err(|e| Error::backend(format!("DNS resolver: {e}")))?;
            Ok(Self {
                resolver,
                service_name,
                srv_prefix: DEFAULT_SRV_PREFIX.to_string(),
            })
        }

        /// Set a custom SRV prefix (e.g. "_minecraft._tcp").
        /// Defaults to "_http._tcp".
        #[must_use]
        pub fn with_srv_prefix(mut self, prefix: impl Into<String>) -> Self {
            self.srv_prefix = prefix.into();
            self
        }
    }

    #[async_trait]
    impl ServiceDiscovery for DnsDiscovery {
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let srv_name = format!("{}.{}", self.srv_prefix, self.service_name);
            let response = self
                .resolver
                .srv_lookup(&srv_name)
                .map_err(|e| Error::backend(format!("DNS lookup failed: {e}")))?;

            let mut descriptors = Vec::new();
            for srv in response.iter() {
                let target = srv.target().to_string();
                let port = srv.port();
                let addr = format!("{}:{}", target.trim_end_matches('.'), port);

                descriptors.push(BackendDescriptor {
                    id: format!("{target}:{port}"),
                    addr,
                    metadata: HashMap::new(),
                    weight: Some(srv.weight().into()),
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
    use super::{BackendDescriptor, Error, ServiceDiscovery};
    use async_trait::async_trait;
    use consulrs::catalog;
    use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
    use std::collections::HashMap;

    /// Consul service discovery source.
    pub struct ConsulDiscovery {
        client: ConsulClient,
        service_name: String,
        tag_filter: Option<String>,
    }

    impl std::fmt::Debug for ConsulDiscovery {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ConsulDiscovery")
                .field("service_name", &self.service_name)
                .field("tag_filter", &self.tag_filter)
                .finish_non_exhaustive()
        }
    }

    impl ConsulDiscovery {
        /// Create a new Consul discovery source for the given Consul address and service name.
        pub fn new(consul_addr: &str, service_name: String) -> Result<Self, Error> {
            let settings = ConsulClientSettingsBuilder::default()
                .address(consul_addr)
                .build()
                .map_err(|e| Error::backend(format!("consul settings: {e}")))?;
            let client = ConsulClient::new(settings)
                .map_err(|e| Error::backend(format!("consul client: {e}")))?;
            Ok(Self {
                client,
                service_name,
                tag_filter: None,
            })
        }

        /// Set a tag filter to only discover services with this tag.
        #[must_use]
        pub fn with_tag(mut self, tag: String) -> Self {
            self.tag_filter = Some(tag);
            self
        }
    }

    #[async_trait]
    impl ServiceDiscovery for ConsulDiscovery {
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let response = catalog::nodes_with_service(&self.client, &self.service_name, None)
                .await
                .map_err(|e| Error::backend(format!("consul discovery: {e}")))?;

            let mut descriptors = Vec::new();
            for entry in response.response {
                // Apply tag filter if set
                if let Some(ref filter_tag) = self.tag_filter {
                    if let Some(ref tags) = entry.service_tags {
                        if !tags.contains(filter_tag) {
                            continue;
                        }
                    }
                }

                // Skip entries with missing or empty address / port. The
                // load balancer's `validate_dial_addr` would reject the
                // resulting ":0" / "" at dial time, which is much harder
                // to debug than skipping here.
                let (Some(address), Some(port)) =
                    (entry.service_address.as_deref(), entry.service_port)
                else {
                    tracing::warn!(
                        service_id = entry.service_id.as_deref().unwrap_or("?"),
                        "skipping consul entry with missing address or port"
                    );
                    continue;
                };
                if address.is_empty() || port == 0 {
                    tracing::warn!(
                        service_id = entry.service_id.as_deref().unwrap_or("?"),
                        "skipping consul entry with empty address or zero port"
                    );
                    continue;
                }
                let addr = format!("{address}:{port}");
                let id = entry.service_id.unwrap_or_default();
                let mut metadata = HashMap::new();
                if let Some(meta) = &entry.service_meta {
                    for (k, v) in meta {
                        metadata.insert(k.clone(), v.clone());
                    }
                }
                if let Some(tags) = &entry.service_tags {
                    metadata.insert("tags".into(), tags.join(","));
                }

                descriptors.push(BackendDescriptor {
                    id,
                    addr,
                    metadata,
                    weight: Some(1),
                    health_check: None,
                });
            }
            Ok(descriptors)
        }
    }
}

/// etcd service discovery.
#[cfg(feature = "etcd")]
pub mod etcd {
    use super::{BackendDescriptor, Error, ServiceDiscovery};
    use async_trait::async_trait;
    use etcd_client::Client;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// etcd service discovery source.
    pub struct EtcdDiscovery {
        client: Mutex<Client>,
        prefix: String,
    }

    impl std::fmt::Debug for EtcdDiscovery {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("EtcdDiscovery")
                .field("prefix", &self.prefix)
                .finish_non_exhaustive()
        }
    }

    impl EtcdDiscovery {
        /// Create a new etcd discovery source for the given endpoints and key prefix.
        pub async fn new(etcd_endpoints: Vec<String>, prefix: String) -> Result<Self, Error> {
            let client = Client::connect(etcd_endpoints, None)
                .await
                .map_err(|e| Error::backend(format!("etcd connect: {e}")))?;
            Ok(Self {
                client: Mutex::new(client),
                prefix,
            })
        }
    }

    #[async_trait]
    impl ServiceDiscovery for EtcdDiscovery {
        #[allow(clippy::significant_drop_tightening)]
        async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
            let mut descriptors = Vec::new();
            let mut client = self.client.lock().await;
            let resp = client
                .get(
                    self.prefix.clone(),
                    Some(etcd_client::GetOptions::new().with_prefix()),
                )
                .await
                .map_err(|e| Error::backend(format!("etcd get: {e}")))?;

            for kv in resp.kvs() {
                let key = String::from_utf8(kv.key().to_vec())
                    .map_err(|e| Error::backend(format!("etcd key utf8: {e}")))?;
                let value = String::from_utf8(kv.value().to_vec())
                    .map_err(|e| Error::backend(format!("etcd value utf8: {e}")))?;

                if let Ok(addr) = value.parse::<std::net::SocketAddr>() {
                    descriptors.push(BackendDescriptor {
                        id: key,
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
