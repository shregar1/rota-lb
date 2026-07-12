//! The `LoadBalancer` — N backends, distributed by a strategy.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, Connection};
use crate::error::Error;
use crate::factory::{BackendFactory, BackendOutput};
use crate::strategy::{BalanceStrategy, PoolView, TunnelMetrics};

/// The load balancer: N backends, dial distributed across them by the
/// configured strategy.
pub struct LoadBalancer {
    backends: Vec<Box<dyn Backend>>,
    metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
    strategy: Arc<Mutex<Box<dyn BalanceStrategy>>>,
    _cancel_token: CancellationToken,
}

impl std::fmt::Debug for LoadBalancer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadBalancer")
            .field("backend_count", &self.backends.len())
            .finish_non_exhaustive()
    }
}

impl LoadBalancer {
    /// Build a load balancer from a pre-constructed set of backends. Use this
    /// when you have backends ready to go (e.g. for tests, or for backends
    /// that don't need a per-instance setup handshake). For backends that
    /// need to register/connect, use [`from_factories`](Self::from_factories).
    ///
    /// Initial metrics for each backend default to zero. To seed an RTT, use
    /// [`new_with_metrics`](Self::new_with_metrics).
    pub fn new(
        backends: Vec<Box<dyn Backend>>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        Self::new_with_metrics(backends, Vec::new(), strategy)
    }

    /// Like [`new`](Self::new) but lets the caller seed each backend's
    /// initial metrics. `initial_metrics.len()` must equal `backends.len()`.
    pub fn new_with_metrics(
        backends: Vec<Box<dyn Backend>>,
        initial_metrics: Vec<TunnelMetrics>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        if backends.is_empty() {
            return Err(Error::NoBackends);
        }
        if initial_metrics.len() != backends.len() {
            return Err(Error::Factory(format!(
                "initial_metrics.len() ({}) must equal backends.len() ({})",
                initial_metrics.len(),
                backends.len()
            )));
        }
        Ok(Self {
            backends,
            metrics: Arc::new(Mutex::new(initial_metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            _cancel_token: CancellationToken::new(),
        })
    }

    /// Build a load balancer by running each factory's `create` once. Use
    /// this when backend construction requires network I/O, registration, or
    /// credentials.
    pub async fn from_factories(
        factories: Vec<Box<dyn BackendFactory>>,
        strategy: impl BalanceStrategy + 'static,
    ) -> Result<Self, Error> {
        if factories.is_empty() {
            return Err(Error::NoBackends);
        }
        let mut backends = Vec::with_capacity(factories.len());
        let mut metrics = Vec::with_capacity(factories.len());
        for f in &factories {
            let BackendOutput { backend, initial_metrics } = f.create().await?;
            backends.push(backend);
            metrics.push(initial_metrics);
        }
        Ok(Self {
            backends,
            metrics: Arc::new(Mutex::new(metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            _cancel_token: CancellationToken::new(),
        })
    }

    /// Create a new [`LoadBalancerBuilder`] for ergonomic configuration.
    ///
    /// # Example
    /// ```
    /// # use rota::{Backend, Connection, LoadBalancer, round_robin, Error};
    /// # async fn example() -> Result<(), Error> {
    /// let backends: Vec<Box<dyn Backend>> = vec![]; // your backends here
    /// let lb = LoadBalancer::builder()
    ///     .backends(backends)
    ///     .strategy(round_robin())
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> LoadBalancerBuilder {
        LoadBalancerBuilder::default()
    }

    /// Open a TCP connection through one of the active backends, chosen by
    /// the configured strategy. Returns a [`GuardedConnection`] which
    /// implements `AsyncRead + AsyncWrite` and decrements the backend's
    /// `active_connections` count on drop.
    pub async fn dial(&self, addr: &str) -> Result<GuardedConnection, Error> {
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
        let conn_result = self.backends[idx].dial(addr).await;
        let conn = match conn_result {
            Ok(c) => c,
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

        Ok(GuardedConnection {
            inner: conn,
            _guard: guard,
        })
    }

    /// Read-only access to the live per-backend metrics. Useful for logging
    /// or external monitoring.
    pub async fn metrics(&self) -> Vec<TunnelMetrics> {
        self.metrics.lock().await.clone()
    }

    /// Number of active backends in the pool.
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Tear every active backend down and release resources.
    pub async fn shutdown(self) {
        self._cancel_token.cancel();
        for backend in self.backends {
            backend.shutdown().await;
        }
    }
}

/// Builder for [`LoadBalancer`]. Provides a fluent API for configuring
/// the balancer before construction.
///
/// # Example
/// ```
/// # use rota::{Backend, Connection, LoadBalancer, round_robin, Error};
/// # async fn example() -> Result<(), Error> {
/// let backends: Vec<Box<dyn Backend>> = vec![]; // your backends here
/// let lb = LoadBalancer::builder()
///     .backends(backends)
///     .strategy(round_robin())
///     .build().await?;
/// # Ok(()) }
/// ```
pub struct LoadBalancerBuilder {
    backends: Option<Vec<Box<dyn Backend>>>,
    factories: Option<Vec<Box<dyn BackendFactory>>>,
    initial_metrics: Option<Vec<TunnelMetrics>>,
    strategy: Option<Box<dyn BalanceStrategy + 'static>>,
}

impl Default for LoadBalancerBuilder {
    fn default() -> Self {
        Self {
            backends: None,
            factories: None,
            initial_metrics: None,
            strategy: None,
        }
    }
}

impl std::fmt::Debug for LoadBalancerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadBalancerBuilder")
            .field("backends_count", &self.backends.as_ref().map(|b| b.len()))
            .field("factories_count", &self.factories.as_ref().map(|b| b.len()))
            .field("has_initial_metrics", &self.initial_metrics.is_some())
            .field("has_strategy", &self.strategy.is_some())
            .finish_non_exhaustive()
    }
}

impl LoadBalancerBuilder {
    /// Set the pre-constructed backends. Mutually exclusive with [`factories`](Self::factories).
    pub fn backends(mut self, backends: Vec<Box<dyn Backend>>) -> Self {
        self.backends = Some(backends);
        self
    }

    /// Set backend factories for lazy construction. Mutually exclusive with [`backends`](Self::backends).
    pub fn factories(mut self, factories: Vec<Box<dyn BackendFactory>>) -> Self {
        self.factories = Some(factories);
        self
    }

    /// Seed initial metrics for each backend. Must match the number of backends/factories.
    pub fn initial_metrics(mut self, metrics: Vec<TunnelMetrics>) -> Self {
        self.initial_metrics = Some(metrics);
        self
    }

    /// Set the load balancing strategy.
    pub fn strategy(mut self, strategy: impl BalanceStrategy + 'static) -> Self {
        self.strategy = Some(Box::new(strategy));
        self
    }

    /// Build the load balancer. Exactly one of `backends` or `factories` must be set.
    pub async fn build(self) -> Result<LoadBalancer, Error> {
        let strategy = self.strategy.ok_or_else(|| Error::Factory("strategy required".into()))?;

        match (self.backends, self.factories) {
            (Some(backends), None) => {
                let metrics = self.initial_metrics.unwrap_or_default();
                if !metrics.is_empty() && metrics.len() != backends.len() {
                    return Err(Error::Factory(format!(
                        "initial_metrics.len() ({}) must equal backends.len() ({})",
                        metrics.len(),
                        backends.len()
                    )));
                }
                LoadBalancer::new_with_metrics(backends, metrics, strategy)
            }
            (None, Some(factories)) => {
                let metrics = self.initial_metrics.unwrap_or_default();
                if !metrics.is_empty() && metrics.len() != factories.len() {
                    return Err(Error::Factory(format!(
                        "initial_metrics.len() ({}) must equal factories.len() ({})",
                        metrics.len(),
                        factories.len()
                    )));
                }
                // For factories, we need to create them first then build
                if factories.is_empty() {
                    return Err(Error::NoBackends);
                }
                let mut created_backends = Vec::with_capacity(factories.len());
                let mut created_metrics = Vec::with_capacity(factories.len());
                for f in &factories {
                    let BackendOutput { backend, initial_metrics } = f.create().await?;
                    created_backends.push(backend);
                    created_metrics.push(initial_metrics);
                }
                // Use caller-provided metrics if any, otherwise use factory-provided
                let final_metrics = if !metrics.is_empty() { metrics } else { created_metrics };
                LoadBalancer::new_with_metrics(created_backends, final_metrics, strategy)
            }
            (Some(_), Some(_)) => Err(Error::Factory("cannot set both backends and factories".into())),
            (None, None) => Err(Error::Factory("backends or factories required".into())),
        }
    }
}

// ============================================================================
//  Connection wrapper
// ============================================================================

/// A connection returned by [`LoadBalancer::dial`]. Wraps the inner connection
/// returned by `Backend::dial` plus a drop guard that decrements the
/// backend's `active_connections` count. Implements `AsyncRead + AsyncWrite`
/// so it's a drop-in replacement for the inner connection.
pub struct GuardedConnection {
    inner: Connection,
    _guard: ActiveConnectionGuard,
}

impl std::fmt::Debug for GuardedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedConnection")
            .finish_non_exhaustive()
    }
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

impl AsyncRead for GuardedConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GuardedConnection {
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