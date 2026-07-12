//! The `Tunnel` trait and the `Stream` type returned by `dial`.
//!
//! `Tunnel` is the minimum interface the load balancer needs to work with any
//! "tunnel" or "connection pool member" backend. The only requirement is
//! `dial(addr) -> Stream` (a tokio I/O stream) and `shutdown` (cleanup).
//!
//! This is what makes the load balancer generic: implement `Tunnel` for
//! SSH tunnels, WireGuard netstack instances, SOCKS5 proxies, HTTP CONNECT
//! proxies, database connection pools, API key pools, etc., and the load
//! balancer works with all of them identically.

use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;

/// A bidirectional byte stream — anything that implements tokio's
/// `AsyncRead + AsyncWrite` and is `Send`. The exact type is hidden behind
/// a `Pin<Box<dyn ...>>` so any concrete stream type can be returned by any
/// tunnel impl.
pub type Stream = Pin<Box<dyn AsyncStream + Send>>;

/// A bidirectional, `Send` async stream. This is a sealed super-trait of
/// `AsyncRead + AsyncWrite` so the public [`Stream`] type can be a single
/// `dyn` trait object.
pub trait AsyncStream: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncStream for T {}

/// A "tunnel" or "pool member" that can open outbound connections.
///
/// Implement this trait to plug any backend into the load balancer. The
/// trait is object-safe (`async_trait` macro + `Box<dyn Tunnel>`) so
/// different backends can be mixed in the same `LoadBalancer`.
///
/// `Send + Sync` is required for use with the multi-threaded tokio runtime
/// (the default). Single-threaded use cases can wrap the `LoadBalancer` in a
/// `LocalSet`.
#[async_trait]
pub trait Tunnel: Send + Sync {
    /// Open a connection to `addr`. The address format is implementation-
    /// defined — IP:port, hostname:port, URL, nym address, etc. The stream
    /// returned is bidirectional and `Send`.
    ///
    /// Failures should be reported as `Err(Error::Tunnel(_))`. I/O errors
    /// during the connect can be wrapped via `Error::Io`.
    async fn dial(&self, addr: &str) -> Result<Stream, Error>;

    /// Tear down the tunnel and release any held resources. Consumes the
    /// boxed tunnel; called from `LoadBalancer::shutdown`.
    ///
    /// Implementations should be idempotent (calling `shutdown` twice is
    /// allowed) and should not panic if the tunnel is already broken.
    async fn shutdown(self: Box<Self>);
}
