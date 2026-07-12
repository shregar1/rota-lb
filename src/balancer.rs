//! The `LoadBalancer` — N tunnels, distributed by a strategy.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::factory::{FactoryOutput, TunnelFactory};
use crate::strategy::{BalanceStrategy, PoolView, TunnelMetrics};
use crate::tunnel::{Stream, Tunnel};

/// The load balancer: N tunnels, dial distributed across them by the
/// configured strategy.
pub struct LoadBalancer {
    tunnels: Vec<Box<dyn Tunnel>>,
    metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
    strategy: Arc<Mutex<Box<dyn BalanceStrategy>>>,
    _cancel_token: CancellationToken,
}

impl LoadBalancer {
    /// Build a load balancer from a pre-constructed set of tunnels. Use this
    /// when you have tunnels ready to go (e.g. for tests, or for backends
    /// that don't need a per-instance setup handshake). For backends that
    /// need to register/connect, use [`from_factories`](Self::from_factories).
    ///
    /// Initial metrics for each tunnel default to zero. To seed an RTT, use
    /// [`new_with_metrics`](Self::new_with_metrics).
    pub fn new(
        tunnels: Vec<Box<dyn Tunnel>>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        Self::new_with_metrics(tunnels, Vec::new(), strategy)
    }

    /// Like [`new`](Self::new) but lets the caller seed each tunnel's
    /// initial metrics. `initial_metrics.len()` must equal
    /// `tunnels.len()`.
    pub fn new_with_metrics(
        tunnels: Vec<Box<dyn Tunnel>>,
        initial_metrics: Vec<TunnelMetrics>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        if tunnels.is_empty() {
            return Err(Error::NoTunnels);
        }
        if initial_metrics.len() != tunnels.len() {
            return Err(Error::Factory(format!(
                "initial_metrics.len() ({}) must equal tunnels.len() ({})",
                initial_metrics.len(),
                tunnels.len()
            )));
        }
        Ok(Self {
            tunnels,
            metrics: Arc::new(Mutex::new(initial_metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            _cancel_token: CancellationToken::new(),
        })
    }

    /// Build a load balancer by running each factory's `create` once. Use
    /// this when tunnel construction requires network I/O, registration, or
    /// credentials.
    pub async fn from_factories(
        factories: Vec<Box<dyn TunnelFactory>>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        if factories.is_empty() {
            return Err(Error::NoTunnels);
        }
        let mut tunnels = Vec::with_capacity(factories.len());
        let mut metrics = Vec::with_capacity(factories.len());
        for f in &factories {
            let FactoryOutput { tunnel, initial_metrics } = f.create().await?;
            tunnels.push(tunnel);
            metrics.push(initial_metrics);
        }
        Ok(Self {
            tunnels,
            metrics: Arc::new(Mutex::new(metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            _cancel_token: CancellationToken::new(),
        })
    }

    /// Open a TCP connection through one of the active tunnels, chosen by
    /// the configured strategy. Returns a [`GuardedStream`] which
    /// implements `AsyncRead + AsyncWrite` and decrements the tunnel's
    /// `active_connections` count on drop.
    pub async fn dial(&self, addr: &str) -> Result<GuardedStream, Error> {
        validate_dial_addr(addr)?;

        // Pick + increment active count atomically (so strategies that
        // look at load see a consistent view of the pool).
        let idx = {
            let mut metrics = self.metrics.lock().await;
            let view = PoolView {
                dial_addr: addr,
                metrics: &metrics,
            };
            let mut strategy = self.strategy.lock().await;
            let idx = strategy.pick(&view);
            metrics[idx].active_connections += 1;
            metrics[idx].total_dials += 1;
            idx
        };

        // Open the connection. On failure, roll back the counter and
        // notify the strategy so it can adapt (e.g. Failover rotates).
        let stream_result = self.tunnels[idx].dial(addr).await;
        let stream = match stream_result {
            Ok(s) => s,
            Err(e) => {
                let mut metrics = self.metrics.lock().await;
                metrics[idx].active_connections =
                    metrics[idx].active_connections.saturating_sub(1);
                metrics[idx].total_errors += 1;
                metrics[idx].recent_errors += 1;
                drop(metrics);
                let mut strategy = self.strategy.lock().await;
                strategy.report_error(idx);
                return Err(e);
            }
        };

        let guard = ActiveConnectionGuard {
            metrics: self.metrics.clone(),
            index: idx,
        };

        Ok(GuardedStream {
            inner: stream,
            _guard: guard,
        })
    }

    /// Read-only access to the live per-tunnel metrics. Useful for logging
    /// or external monitoring.
    pub async fn metrics(&self) -> Vec<TunnelMetrics> {
        self.metrics.lock().await.clone()
    }

    /// Number of active tunnels in the pool.
    pub fn tunnel_count(&self) -> usize {
        self.tunnels.len()
    }

    /// Tear every active tunnel down and release resources.
    pub async fn shutdown(self) {
        self._cancel_token.cancel();
        for tunnel in self.tunnels {
            tunnel.shutdown().await;
        }
    }
}

// ============================================================================
//  Stream wrapper
// ============================================================================

/// A stream returned by [`LoadBalancer::dial`]. Wraps the inner stream
/// returned by `Tunnel::dial` plus a drop guard that decrements the
/// tunnel's `active_connections` count. Implements `AsyncRead +
/// AsyncWrite` so it's a drop-in replacement for the inner stream.
pub struct GuardedStream {
    inner: Stream,
    _guard: ActiveConnectionGuard,
}

struct ActiveConnectionGuard {
    metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
    index: usize,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        // `try_lock`: if the load balancer is mid-dial and holding the
        // metrics lock, we don't want to block forever. The active count
        // will be slightly inflated until the next operation — best-effort
        // accounting is fine for strategy input.
        if let Ok(mut metrics) = self.metrics.try_lock() {
            if let Some(m) = metrics.get_mut(self.index) {
                m.active_connections = m.active_connections.saturating_sub(1);
            }
        }
    }
}

impl AsyncRead for GuardedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GuardedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ============================================================================
//  Validation
// ============================================================================

/// Validate a `"host:port"` without resolving it. Mirrors the validation
/// nym-net does for direct `NymNet::dial` calls — same input shape, same
/// error reason, regardless of which entry point the host uses.
fn validate_dial_addr(addr: &str) -> Result<(), Error> {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return Err(Error::InvalidAddress {
            addr: addr.to_owned(),
            reason: "expected \"host:port\"",
        });
    };
    if host.is_empty() {
        return Err(Error::InvalidAddress {
            addr: addr.to_owned(),
            reason: "empty host",
        });
    }
    if port.parse::<u16>().map(|p| p == 0).unwrap_or(true) {
        return Err(Error::InvalidAddress {
            addr: addr.to_owned(),
            reason: "port must be 1-65535",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_dial_addr_accepts_host_port() {
        assert!(validate_dial_addr("example.com:443").is_ok());
        assert!(validate_dial_addr("10.0.0.1:80").is_ok());
    }

    #[test]
    fn validate_dial_addr_rejects_malformed() {
        assert!(validate_dial_addr("example.com").is_err());
        assert!(validate_dial_addr(":443").is_err());
        assert!(validate_dial_addr("example.com:0").is_err());
        assert!(validate_dial_addr("example.com:notaport").is_err());
    }
}
