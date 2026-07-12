//! Error type for the load balancer.

use std::io;

/// Errors produced by the load balancer and the tunnels it manages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A dial target that is not a valid `"host:port"`.
    #[error("invalid dial address {addr:?}: {reason}")]
    InvalidAddress { addr: String, reason: &'static str },

    /// The load balancer was constructed with zero tunnels.
    #[error("no tunnels available — pool is empty")]
    NoTunnels,

    /// A `TunnelFactory::create` call failed.
    #[error("tunnel factory failed: {0}")]
    Factory(String),

    /// A `Tunnel::dial` call failed.
    #[error("tunnel operation failed: {0}")]
    Tunnel(String),

    /// Underlying I/O error from a stream returned by `Tunnel::dial`.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

/// Convenience: a stringly-typed error with a context message.
impl Error {
    pub fn tunnel(msg: impl Into<String>) -> Self {
        Self::Tunnel(msg.into())
    }

    pub fn factory(msg: impl Into<String>) -> Self {
        Self::Factory(msg.into())
    }
}
