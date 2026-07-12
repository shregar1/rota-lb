//! The `TunnelFactory` trait.
//!
//! Some backends can't be constructed up-front: a WireGuard tunnel needs a
//! registration handshake; an SSH tunnel needs authentication; an HTTP
//! CONNECT proxy needs a handshake. `TunnelFactory` lets the load balancer
//! bring tunnels up lazily, one per call to `create`.
//!
//! Use [`LoadBalancer::new`](crate::LoadBalancer::new) if you already have
//! the tunnels. Use [`LoadBalancer::from_factories`](crate::LoadBalancer::from_factories)
//! if you need creation.

use async_trait::async_trait;

use crate::error::Error;
use crate::strategy::TunnelMetrics;
use crate::tunnel::Tunnel;

/// What a `TunnelFactory::create` call returns: the live tunnel plus the
/// metrics the load balancer should seed it with (typically an
/// already-measured RTT, since most factories probe before constructing).
pub struct FactoryOutput {
    pub tunnel: Box<dyn Tunnel>,
    pub initial_metrics: TunnelMetrics,
}

/// Constructs a tunnel on demand.
///
/// One `TunnelFactory` per parallel tunnel. The load balancer calls
/// `create` once at startup, then holds the resulting tunnel for the
/// lifetime of the `LoadBalancer`. The factory itself is `&self` so it can
/// share credentials, network state, or configuration across calls.
#[async_trait]
pub trait TunnelFactory: Send + Sync {
    async fn create(&self) -> Result<FactoryOutput, Error>;
}
