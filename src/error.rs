//! Error type for the load balancer.

use std::io;

/// Errors produced by the load balancer and the backends it manages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A dial target that is not a valid `"host:port"`.
    #[error("invalid dial address {addr:?}: {reason}")]
    InvalidAddress {
        /// The address string that failed validation.
        addr: String,
        /// Why the address was rejected.
        reason: &'static str,
    },

    /// The load balancer was constructed with zero backends.
    #[error("no backends available — pool is empty")]
    NoBackends,

    /// A `BackendFactory::create` call failed.
    #[error("backend factory failed: {0}")]
    Factory(String),

    /// A `Backend::dial` call failed.
    #[error("backend operation failed: {0}")]
    Backend(String),

    /// Underlying I/O error from a stream returned by `Backend::dial`.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

/// Convenience: a stringly-typed error with a context message.
impl Error {
    /// Create a [`Error::Backend`] from a message.
    pub fn backend(msg: impl Into<String>) -> Self {
        Self::Backend(msg.into())
    }

    /// Create a [`Error::Factory`] from a message.
    pub fn factory(msg: impl Into<String>) -> Self {
        Self::Factory(msg.into())
    }
}
