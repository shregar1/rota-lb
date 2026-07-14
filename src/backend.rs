//! The `Backend` trait and the `Connection` type returned by `dial`.
//!
//! `Backend` is the minimum interface the load balancer needs to work with any
//! "backend" or "pool member".

use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;

/// A bidirectional byte stream — anything that implements tokio's
/// `AsyncRead + AsyncWrite` and is `Send`.
///
/// The exact type is hidden behind a `Pin<Box<dyn ...>>` so any concrete
/// stream type can be returned by any backend impl.
pub type Connection = Pin<Box<dyn AsyncConnection + Send>>;

/// A bidirectional, `Send` async stream. This is a sealed super-trait of
/// `AsyncRead + AsyncWrite` so the public [`Connection`] type can be a single
/// `dyn` trait object.
pub trait AsyncConnection: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncConnection for T {}

/// A "backend" or "pool member" that can open outbound connections.
///
/// Implement this trait to plug any backend into the load balancer. The
/// trait is object-safe (`async_trait` macro + `Box<dyn Backend>`) so
/// different backends can be mixed in the same `LoadBalancer`.
///
/// `Send + Sync` is required for use with the multi-threaded tokio runtime
/// (the default). Single-threaded use cases can wrap the `LoadBalancer` in a
/// `LocalSet`.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Open a connection to `addr`. The address format is implementation-
    /// defined — IP:port, hostname:port, URL, nym address, etc. The stream
    /// returned is bidirectional and `Send`.
    ///
    /// Failures should be reported as `Err(Error::Backend(_))`. I/O errors
    /// during the connect can be wrapped via `Error::Io`.
    async fn dial(&self, addr: &str) -> Result<Connection, Error>;

    /// Tear down the backend and release any held resources.
    ///
    /// Implementations should be idempotent (calling `shutdown` twice is
    /// allowed) and should not panic if the backend is already broken.
    async fn shutdown(&mut self);
}
