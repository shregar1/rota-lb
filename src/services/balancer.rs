//! The `LoadBalancer` — N backends, distributed by a strategy.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::traits::backend::{Backend, Connection};
use crate::error::Error;
use crate::utils::factory::{BackendFactory, BackendOutput};
use crate::utils::retry::RetryPolicy;
use crate::traits::strategy::{BalanceStrategy, PoolView, TunnelMetrics};

/// The load balancer: N backends, dial distributed across them by the
/// configured strategy.
pub struct LoadBalancer {
    backends: Vec<Box<dyn Backend>>,
    ids: Vec<Option<String>>,
    metrics: Arc<RwLock<Vec<TunnelMetrics>>>,
    strategy: Arc<Mutex<Box<dyn BalanceStrategy>>>,
    generation: Arc<AtomicUsize>,
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
        let metrics = (0..backends.len())
            .map(|_| TunnelMetrics::default())
            .collect();
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
        let n = backends.len();
        Ok(Self {
            backends,
            ids: vec![None; n],
            metrics: Arc::new(RwLock::new(initial_metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            generation: Arc::new(AtomicUsize::new(0)),
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
            let BackendOutput {
                backend,
                initial_metrics,
            } = f.create().await?;
            backends.push(backend);
            metrics.push(initial_metrics);
        }
        let n = backends.len();
        Ok(Self {
            backends,
            ids: vec![None; n],
            metrics: Arc::new(RwLock::new(metrics)),
            strategy: Arc::new(Mutex::new(Box::new(strategy))),
            generation: Arc::new(AtomicUsize::new(0)),
            dial_timeout: None,
            retry_policy: None,
        })
    }

    /// Create a new [`LoadBalancerBuilder`] for ergonomic configuration.
    ///
    /// # Example
    /// ```
    /// # use rota_lb::{Backend, Connection, LoadBalancer, round_robin, Error};
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
        self.dial_with_options(addr, None, None).await
    }

    /// Like [`dial`](Self::dial) but lets the caller override the
    /// `dial_timeout` and `retry_policy` for this single call. `None`
    /// means "use the balancer's configured default".
    #[allow(clippy::significant_drop_tightening)]
    pub async fn dial_with_options(
        &self,
        addr: &str,
        dial_timeout: Option<Duration>,
        retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
    ) -> Result<GuardedConnection, Error> {
        validate_dial_addr(addr)?;

        // The pool can become empty after `remove_backend`. Refuse
        // rather than calling into a strategy that may panic on
        // `0 % len`, `gen_range(0..0)`, etc.
        if self.backends.is_empty() {
            return Err(Error::NoBackends);
        }

        let idx = {
            let mut metrics = self.metrics.write().await;
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
        }; // both guards dropped here

        let gen = self.generation.load(Ordering::Relaxed);
        let start = std::time::Instant::now();

        match self
            .dial_with_retry(idx, addr, start, dial_timeout, retry_policy)
            .await
        {
            Ok(conn) => {
                self.handle_dial_success(idx).await;
                debug!(
                    backend_idx = idx,
                    elapsed_ms = start.elapsed().as_millis(),
                    "dial succeeded"
                );
                let guard = ActiveConnectionGuard {
                    metrics: self.metrics.clone(),
                    generation: self.generation.clone(),
                    index: idx,
                    gen_at_creation: gen,
                };
                Ok(GuardedConnection {
                    inner: conn,
                    _guard: guard,
                })
            }
            Err(e) => Err(self.handle_dial_error(idx, e).await),
        }
    }

    async fn handle_dial_success(&self, idx: usize) {
        let mut metrics = self.metrics.write().await;
        if let Some(m) = metrics.get_mut(idx) {
            m.recent_errors = 0;
        }
        drop(metrics);
        self.strategy.lock().await.report_success(idx);
    }

    async fn handle_dial_error(&self, idx: usize, err: Error) -> Error {
        let mut metrics = self.metrics.write().await;
        if let Some(m) = metrics.get_mut(idx) {
            m.active_connections = m.active_connections.saturating_sub(1);
            m.total_errors = m.total_errors.saturating_add(1);
            m.recent_errors = m.recent_errors.saturating_add(1);
        }
        drop(metrics);
        self.strategy.lock().await.report_error(idx);
        err
    }

    async fn dial_with_retry(
        &self,
        idx: usize,
        addr: &str,
        start: std::time::Instant,
        dial_timeout: Option<Duration>,
        retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
    ) -> Result<Connection, Error> {
        let mut attempt = 0u32;
        let effective_timeout = dial_timeout.or(self.dial_timeout);
        let effective_policy = retry_policy.or_else(|| self.retry_policy.clone());
        loop {
            attempt += 1;
            let dial_fut = self.backends[idx].dial(addr);
            let conn_result = if let Some(timeout) = effective_timeout {
                tokio::time::timeout(timeout, dial_fut)
                    .await
                    .unwrap_or_else(|_| Err(Error::Backend("dial timeout".into())))
            } else {
                dial_fut.await
            };

            match conn_result {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    warn!(backend_idx = idx, attempt, error = %e, "dial failed");
                    let delay = effective_policy.as_ref().and_then(|policy| {
                        let within_budget =
                            policy.total_timeout().map_or(true, |t| start.elapsed() < t);
                        within_budget
                            .then(|| policy.should_retry(attempt, &e))
                            .flatten()
                    });
                    let Some(delay) = delay else {
                        return Err(e);
                    };
                    debug!(attempt, delay_ms = delay.as_millis(), "retrying dial");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Read-only access to the live per-backend metrics. Useful for logging
    /// or external monitoring.
    pub async fn metrics(&self) -> Vec<TunnelMetrics> {
        self.metrics.read().await.clone()
    }

    /// Number of active backends in the pool.
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Tear every active backend down and release resources.
    pub async fn shutdown(self) {
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
        let mut metrics = self.metrics.write().await;
        let idx = self.backends.len();
        self.backends.push(backend);
        self.ids.push(None);
        metrics.push(TunnelMetrics::default());
        idx
    }

    /// Add a backend with an ID for service discovery tracking.
    pub async fn add_backend_with_id(&mut self, id: String, backend: Box<dyn Backend>) -> usize {
        let mut metrics = self.metrics.write().await;
        let idx = self.backends.len();
        self.backends.push(backend);
        self.ids.push(Some(id));
        metrics.push(TunnelMetrics::default());
        idx
    }

    /// Remove a backend from the pool by index.
    ///
    /// The backend will be shut down and removed from the pool. Existing
    /// [`GuardedConnection`] instances on that backend will continue until
    /// dropped — their drop guard will detect the generation change and
    /// skip the metrics decrement.
    ///
    /// Returns `true` if the backend was removed, `false` if out of bounds.
    pub async fn remove_backend(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        let mut backend = self.backends.remove(index);
        self.ids.remove(index);
        self.metrics.write().await.remove(index);
        backend.shutdown().await;
        true
    }

    /// Remove a backend by reference (e.g., by pointer equality).
    pub async fn remove_backend_by_ptr(&mut self, backend: &dyn Backend) -> bool {
        let index = self
            .backends
            .iter()
            .position(|b| std::ptr::eq(b.as_ref(), backend));
        if let Some(idx) = index {
            self.remove_backend(idx).await
        } else {
            false
        }
    }

    /// Remove a backend by its service-discovery ID. Returns `true` if found
    /// and removed.
    pub async fn remove_backend_by_id(&mut self, id: &str) -> bool {
        if let Some(idx) = self.ids.iter().position(|i| i.as_deref() == Some(id)) {
            self.remove_backend(idx).await
        } else {
            false
        }
    }

    /// Return the list of tracked backend IDs.
    #[allow(clippy::unused_async)]
    pub async fn backend_ids(&self) -> Vec<Option<String>> {
        self.ids.clone()
    }

    /// Replace all backends atomically.
    ///
    /// The new backends replace the old pool entirely. Old backends are shut down.
    /// If `strategy` is `Some`, the strategy instance is replaced wholesale
    /// (the old strategy's state — e.g. `Failover`'s primary pin, `Sticky`'s
    /// pinned index, `WeightedRoundRobin`'s RTT cache — is discarded).
    /// If `strategy` is `None`, the existing strategy instance is preserved
    /// along with its state. Returns an error if the new pool is empty.
    pub async fn replace_backends(
        &mut self,
        new_backends: Vec<Box<dyn Backend>>,
        strategy: Option<Box<dyn BalanceStrategy + 'static>>,
    ) -> Result<(), Error> {
        if new_backends.is_empty() {
            return Err(Error::NoBackends);
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        for mut backend in self.backends.drain(..) {
            backend.shutdown().await;
        }
        self.ids.clear();
        self.ids.resize(new_backends.len(), None);
        *self.metrics.write().await = vec![TunnelMetrics::default(); new_backends.len()];
        self.backends = new_backends;
        if let Some(s) = strategy {
            *self.strategy.lock().await = s;
        }
        Ok(())
    }

    /// Drain a backend gracefully - stop sending new connections but wait for
    /// existing connections to complete.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn drain_backend(&mut self, index: usize) -> bool {
        if index >= self.backends.len() {
            return false;
        }
        let mut metrics = self.metrics.write().await;
        if let Some(m) = metrics.get_mut(index) {
            m.recent_errors = u32::MAX;
        }
        true
    }

    /// Check if a backend is draining.
    pub async fn is_draining(&self, index: usize) -> bool {
        let metrics = self.metrics.read().await;
        metrics
            .get(index)
            .is_some_and(|m| m.recent_errors == u32::MAX)
    }

    /// Undrain a backend - allow it to receive new connections again.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn undrain_backend(&mut self, index: usize) -> bool {
        let mut metrics = self.metrics.write().await;
        if let Some(m) = metrics.get_mut(index) {
            if m.recent_errors == u32::MAX {
                m.recent_errors = 0;
                return true;
            }
        }
        false
    }

    /// Replace the strategy at runtime. This is not a hot path (admin ops),
    /// so we use a blocking lock rather than silently dropping the request.
    pub async fn set_strategy(&mut self, strategy: impl BalanceStrategy + 'static) {
        *self.strategy.lock().await = Box::new(strategy);
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
/// # use rota_lb::{Backend, Connection, LoadBalancer, round_robin, Error};
/// # async fn example() -> Result<(), Error> {
/// let backends: Vec<Box<dyn Backend>> = vec![]; // your backends here
/// let lb = LoadBalancer::builder()
///     .backends(backends)
///     .strategy(round_robin())
///     .build().await?;
/// # Ok(()) }
/// ```
#[derive(Default)]
pub struct LoadBalancerBuilder {
    backends: Option<Vec<Box<dyn Backend>>>,
    factories: Option<Vec<Box<dyn BackendFactory>>>,
    initial_metrics: Option<Vec<TunnelMetrics>>,
    strategy: Option<Box<dyn BalanceStrategy + 'static>>,
    dial_timeout: Option<Duration>,
    retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
}

impl std::fmt::Debug for LoadBalancerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadBalancerBuilder")
            .field("backends_count", &self.backends.as_ref().map(Vec::len))
            .field("factories_count", &self.factories.as_ref().map(Vec::len))
            .field("has_initial_metrics", &self.initial_metrics.is_some())
            .field("has_strategy", &self.strategy.is_some())
            .finish_non_exhaustive()
    }
}

impl LoadBalancerBuilder {
    /// Set the pre-constructed backends. Mutually exclusive with [`factories`](Self::factories).
    #[must_use]
    pub fn backends(mut self, backends: Vec<Box<dyn Backend>>) -> Self {
        self.backends = Some(backends);
        self
    }

    /// Set backend factories for lazy construction. Mutually exclusive with [`backends`](Self::backends).
    #[must_use]
    pub fn factories(mut self, factories: Vec<Box<dyn BackendFactory>>) -> Self {
        self.factories = Some(factories);
        self
    }

    /// Seed initial metrics for each backend. Must match the number of backends/factories.
    #[must_use]
    pub fn initial_metrics(mut self, metrics: Vec<TunnelMetrics>) -> Self {
        self.initial_metrics = Some(metrics);
        self
    }

    /// Set the load balancing strategy.
    #[must_use]
    pub fn strategy(mut self, strategy: impl BalanceStrategy + 'static) -> Self {
        self.strategy = Some(Box::new(strategy));
        self
    }

    /// Set a timeout for each dial attempt. If not set, no timeout is applied.
    #[must_use]
    pub const fn dial_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = Some(timeout);
        self
    }

    /// Set a retry policy for failed dial attempts.
    #[must_use]
    pub fn retry_policy(mut self, policy: impl RetryPolicy + 'static) -> Self {
        self.retry_policy = Some(Arc::new(policy));
        self
    }

    /// Build the load balancer. Exactly one of `backends` or `factories` must be set.
    pub async fn build(self) -> Result<LoadBalancer, Error> {
        let strategy = self
            .strategy
            .ok_or_else(|| Error::Factory("strategy required".into()))?;
        let dial_timeout = self.dial_timeout;
        let retry_policy = self.retry_policy;

        match (self.backends, self.factories) {
            (Some(backends), None) => {
                let metrics = match self.initial_metrics {
                    Some(m) if m.len() == backends.len() => m,
                    Some(m) => {
                        return Err(Error::Factory(format!(
                            "initial_metrics.len() ({}) must equal backends.len() ({})",
                            m.len(),
                            backends.len()
                        )))
                    }
                    None => vec![TunnelMetrics::default(); backends.len()],
                };
                LoadBalancer::new_with_metrics(
                    backends,
                    metrics,
                    strategy,
                    dial_timeout,
                    retry_policy,
                )
            }
            (None, Some(factories)) => {
                // For factories, we need to create them first then build
                if factories.is_empty() {
                    return Err(Error::NoBackends);
                }
                let mut created_backends = Vec::with_capacity(factories.len());
                let mut created_metrics = Vec::with_capacity(factories.len());
                for f in &factories {
                    let BackendOutput {
                        backend,
                        initial_metrics,
                    } = f.create().await?;
                    created_backends.push(backend);
                    created_metrics.push(initial_metrics);
                }
                let final_metrics = match self.initial_metrics {
                    Some(m) if m.len() == factories.len() => m,
                    Some(m) => {
                        return Err(Error::Factory(format!(
                            "initial_metrics.len() ({}) must equal factories.len() ({})",
                            m.len(),
                            factories.len()
                        )))
                    }
                    None => created_metrics,
                };
                LoadBalancer::new_with_metrics(
                    created_backends,
                    final_metrics,
                    strategy,
                    dial_timeout,
                    retry_policy,
                )
            }
            (Some(_), Some(_)) => Err(Error::Factory(
                "cannot set both backends and factories".into(),
            )),
            (None, None) => Err(Error::Factory("backends or factories required".into())),
        }
    }
}

// ============================================================================
//  Connection wrapper
// ============================================================================

/// A connection returned by [`LoadBalancer::dial`].
///
/// Wraps the inner connection returned by `Backend::dial` plus a drop guard
/// that decrements the backend's `active_connections` count. Implements
/// `AsyncRead + AsyncWrite` so it's a drop-in replacement for the inner
/// connection.
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
        f.debug_struct("GuardedConnection").finish_non_exhaustive()
    }
}

struct ActiveConnectionGuard {
    metrics: Arc<RwLock<Vec<TunnelMetrics>>>,
    generation: Arc<AtomicUsize>,
    index: usize,
    gen_at_creation: usize,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        // If the generation changed since creation, the metrics vector may
        // have been resized (via remove_backend / replace_backends), making
        // self.index potentially out-of-bounds or pointing to a different
        // backend. For safety we skip the decrement in that case — the
        // active count may be slightly inflated but converges on the next
        // operation.
        if self.generation.load(Ordering::SeqCst) != self.gen_at_creation {
            return;
        }
        if let Ok(mut metrics) = self.metrics.try_write() {
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
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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
    if port.parse::<u16>().map_or(true, |p| p == 0) {
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
