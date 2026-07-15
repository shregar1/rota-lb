//! The `BackendFactory` trait.
//!
//! Some backends can't be constructed up-front: a `WireGuard` tunnel needs a
//! registration handshake; an SSH tunnel needs authentication; an HTTP
//! CONNECT proxy needs a handshake. `BackendFactory` lets the load balancer
//! bring backends up lazily, one per call to `create`.
//!
//! Use [`LoadBalancer::new`](crate::LoadBalancer::new) if you already have
//! the backends. Use [`LoadBalancer::from_factories`](crate::LoadBalancer::from_factories)
//! if you need creation.

use async_trait::async_trait;

use crate::traits::backend::Backend;
use crate::error::Error;
use crate::traits::strategy::TunnelMetrics;

/// What a `BackendFactory::create` call returns: the live backend plus the
/// metrics the load balancer should seed it with.
///
/// Typically an already-measured RTT, since most factories probe before
/// constructing.
pub struct BackendOutput {
    /// The constructed backend, ready to accept `dial` calls.
    pub backend: Box<dyn Backend>,
    /// Initial metrics for this backend (e.g., pre-measured RTT).
    pub initial_metrics: TunnelMetrics,
}

impl std::fmt::Debug for BackendOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendOutput")
            .field("backend", &"<dyn Backend>")
            .field("initial_metrics", &self.initial_metrics)
            .finish()
    }
}

/// Constructs a backend on demand.
///
/// One `BackendFactory` per parallel backend. The load balancer calls
/// `create` once at startup, then holds the resulting backend for the
/// lifetime of the `LoadBalancer`. The factory itself is `&self` so it can
/// share credentials, network state, or configuration across calls.
#[async_trait]
pub trait BackendFactory: Send + Sync {
    /// Create a new backend and its initial metrics.
    ///
    /// Called once per factory at `LoadBalancer` startup. The returned
    /// `Backend` is held for the balancer's lifetime; `initial_metrics`
    /// seeds the per-backend metrics (typically a pre-measured RTT).
    async fn create(&self) -> Result<BackendOutput, Error>;
}
