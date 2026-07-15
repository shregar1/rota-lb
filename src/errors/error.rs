use crate::errors::{
    backend_error::BackendError, factory_error::FactoryError, invalid_address::InvalidAddress,
    io_error::IoError, no_backends::NoBackends,
};

/// Errors produced by the load balancer and the backends it manages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A dial target that is not a valid `"host:port"`.
    #[error(transparent)]
    InvalidAddress(#[from] InvalidAddress),

    /// The load balancer was constructed with zero backends.
    #[error(transparent)]
    NoBackends(#[from] NoBackends),

    /// A `BackendFactory::create` call failed.
    #[error(transparent)]
    Factory(#[from] FactoryError),

    /// A `Backend::dial` call failed.
    #[error(transparent)]
    Backend(#[from] BackendError),

    /// Underlying I/O error from a stream returned by `Backend::dial`.
    #[error(transparent)]
    Io(#[from] IoError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(IoError(e))
    }
}

/// Convenience constructors.
impl Error {
    /// Create a [`Error::Backend`] from a message.
    pub fn backend(msg: impl Into<String>) -> Self {
        Self::Backend(BackendError(msg.into()))
    }

    /// Create a [`Error::Factory`] from a message.
    pub fn factory(msg: impl Into<String>) -> Self {
        Self::Factory(FactoryError(msg.into()))
    }

    /// The load balancer was constructed with zero backends.
    pub const fn no_backends() -> Self {
        Self::NoBackends(NoBackends)
    }

    /// A dial target that is not a valid `"host:port"`.
    pub const fn invalid_address(addr: String, reason: &'static str) -> Self {
        Self::InvalidAddress(InvalidAddress { addr, reason })
    }
}
