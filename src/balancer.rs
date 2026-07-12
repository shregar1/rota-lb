//! The `LoadBalancer` — N backends, distributed by a strategy.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::backend::{Backend, Connection};
use crate::error::Error;
use crate::factory::{BackendFactory, BackendOutput};
use crate::retry::RetryPolicy;
use crate::strategy::{BalanceStrategy, PoolView, TunnelMetrics};

/// The load balancer: N backends, dial distributed across them by the
/// configured strategy.
pub struct LoadBalancer {
    backends: Vec<Box<dyn Backend>>,
    metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
    strategy: Arc<Mutex<Box<dyn BalanceStrategy>>>,
    _cancel_token: CancellationToken,
    dial_timeout: Option<Duration>,
    retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
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
        let metrics = (0..backends.len()).map(|_| TunnelMetrics::default()).collect();
        Self::new_with_metrics(backends, metrics, strategy, None, None)
    }

    /// Like [`new`](Self::new) but lets the caller seed each backend's
    /// initial metrics. `initial_metrics.len()` must equal `backends.len()`.
    pub fn new_with_metrics(
        backends: Vec<Box<dyn Backend>>,
        initial_metrics: Vec<TunnelMetrics>,
        strategy: impl BalanceStrategy + 'static,
        dial_timeout: Option<Duration>,
        retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
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
            dial_timeout,
            retry_policy,
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
            dial_timeout: None,
            retry_policy: None,
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
            debug!(
                backend_idx = idx,
                addr = %addr,
                strategy = %strategy.name(),
                "selected backend for dial"
            );
            idx
        };

        // Try to connect with retry logic
        let mut attempt = 0u32;
        let start = std::time::Instant::now();
        
        loop {
            // Open the connection with optional timeout
            let dial_fut = self.backends[idx].dial(addr);
            let conn_result = if let Some(timeout) = self.dial_timeout {
                match tokio::time::timeout(timeout, dial_fut).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::Backend("dial timeout".into())),
                }
            } else {
                dial_fut.await
            };

            match conn_result {
                Ok(conn) => {
                    debug!(
                        backend_idx = idx,
                        attempt = attempt + 1,
                        elapsed_ms = start.elapsed().as_millis(),
                        "dial succeeded"
                    );
                    let guard = ActiveConnectionGuard {
                        metrics: self.metrics.clone(),
                        index: idx,
                    };

                    return Ok(GuardedConnection {
                        inner: conn,
                        _guard: guard,
                    });
                }
                Err(e) => {
                    attempt += 1;
                    warn!(
                        backend_idx = idx,
                        attempt,
                        error = %e,
                        "dial failed"
                    );

                    // Check retry policy
                    let should_retry = if let Some(policy) = &self.retry_policy {
                        // Check total timeout budget
                        if let Some(total_timeout) = policy.total_timeout() {
                            if start.elapsed() >= total_timeout {
                                false
                            } else {
                                policy.should_retry(attempt, &e).is_some()
                            }
                        } else {
                            policy.should_retry(attempt, &e).is_some()
                        }
                    } else {
                        false
                    };

                    if !should_retry {
                        // No more retries - roll back and return error
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

                    // Wait for the retry delay
                    if let Some(policy) = &self.retry_policy {
                        if let Some(delay) = policy.should_retry(attempt, &e) {
                            debug!(
                                backend_idx = idx,
                                attempt,
                                delay_ms = delay.as_millis(),
                                "retrying dial after delay"
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }

                    // No delay returned from policy - give up
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
            }
        }
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
        for mut backend in self.backends {
            backend.shutdown().await;
        }
    }

    // ============================================================================
    //  Dynamic reconfiguration
    // ============================================================================

    /// Add a new backend to the pool at runtime.
    ///
    /// The new backend will be included in the load balancing pool immediately.
    /// Returns the index of the added backend.
    pub async fn add_backend(&mut self, backend: Box<dyn Backend>) -> usize {
        let mut metrics = self.metrics.lock().await;
        let idx = self.backends.len();
        self.backends.push(backend);
        metrics.push(TunnelMetrics::default());
        idx
    }

    /// Remove a backend from the pool by index.
    ///
    /// The backend will be shut down and removed from the pool. Active connections
    /// on that backend will continue until they complete. Returns `true` if the
    /// backend was removed, `false` if the index was out of bounds.
    pub async fn remove_backend(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }
        let mut backend = self.backends.remove(index);
        self.metrics.lock().await.remove(index);
        backend.shutdown().await;
        true
    }

    /// Remove a backend by reference (e.g., by pointer equality).
    pub async fn remove_backend_by_ptr(&mut self, backend: &dyn Backend) -> bool {
        let index = self.backends.iter().position(|b| std::ptr::eq(b.as_ref(), backend));
        if let Some(idx) = index {
            self.remove_backend(idx).await
        } else {
            false
        }
    }

    /// Replace all backends atomically.
    ///
    /// The new backends replace the old pool entirely. Old backends are shut down.
    /// The strategy is reset to its initial state. Returns an error if the new
    /// pool is empty.
    pub async fn replace_backends(
        &mut self,
        new_backends: Vec<Box<dyn Backend>>,
        strategy: Option<Box<dyn BalanceStrategy + 'static>>,
    ) -> Result<(), Error> {
        if new_backends.is_empty() {
            return Err(Error::NoBackends);
        }
        // Shutdown old backends
        for mut backend in self.backends.drain(..) {
            backend.shutdown().await;
        }
        // Reset metrics
        *self.metrics.lock().await = vec![TunnelMetrics::default(); new_backends.len()];
        self.backends = new_backends;
        if let Some(s) = strategy {
            *self.strategy.lock().await = s;
        }
        Ok(())
    }

    /// Drain a backend gracefully - stop sending new connections but wait for
    /// existing connections to complete.
    pub async fn drain_backend(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }
        // Mark as draining by setting a very high error count so strategies avoid it
        let mut metrics = self.metrics.lock().await;
        if let Some(m) = metrics.get_mut(index) {
            m.recent_errors = u32::MAX;
        }
        true
    }

    /// Check if a backend is draining.
    pub async fn is_draining(&self, index: usize) -> bool {
        let metrics = self.metrics.lock().await;
        metrics.get(index).map(|m| m.recent_errors == u32::MAX).unwrap_or(false)
    }

    /// Undrain a backend - allow it to receive new connections again.
    pub async fn undrain_backend(&mut self, index: usize) -> bool {
        let mut metrics = self.metrics.lock().await;
        if let Some(m) = metrics.get_mut(index) {
            if m.recent_errors == u32::MAX {
                m.recent_errors = 0;
                return true;
            }
        }
        false
    }

    /// Replace the strategy at runtime.
    pub fn set_strategy(&mut self, strategy: impl BalanceStrategy + 'static) {
        if let Ok(mut guard) = self.strategy.try_lock() {
            *guard = Box::new(strategy);
        }
    }

    /// Get the current strategy name.
    pub async fn strategy_name(&self) -> String {
        self.strategy.lock().await.name().to_string()
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
    dial_timeout: Option<Duration>,
    retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
}

impl Default for LoadBalancerBuilder {
    fn default() -> Self {
        Self {
            backends: None,
            factories: None,
            initial_metrics: None,
            strategy: None,
            dial_timeout: None,
            retry_policy: None,
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

    /// Set a timeout for each dial attempt. If not set, no timeout is applied.
    pub fn dial_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = Some(timeout);
        self
    }

    /// Set a retry policy for failed dial attempts.
    pub fn retry_policy(mut self, policy: impl RetryPolicy + Send + Sync + 'static) -> Self {
        self.retry_policy = Some(Arc::new(policy));
        self
    }

    /// Build the load balancer. Exactly one of `backends` or `factories` must be set.
    pub async fn build(self) -> Result<LoadBalancer, Error> {
        let strategy = self.strategy.ok_or_else(|| Error::Factory("strategy required".into()))?;
        let dial_timeout = self.dial_timeout;
        let retry_policy = self.retry_policy;

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
                LoadBalancer::new_with_metrics(backends, metrics, strategy, dial_timeout, retry_policy)
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
                LoadBalancer::new_with_metrics(created_backends, final_metrics, strategy, dial_timeout, retry_policy)
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

impl GuardedConnection {
    /// Extract the inner connection, consuming the guard.
    pub fn into_inner(self) -> Connection {
        self.inner
    }
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