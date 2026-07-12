//! The `BalanceStrategy` trait and the per-tunnel metrics the load balancer
//! tracks.

use std::time::Duration;

/// A snapshot of the active tunnel pool, passed to
/// [`BalanceStrategy::pick`](crate::BalanceStrategy::pick) on every dial.
///
/// The view borrows from the load balancer so strategies see the metrics
/// at the moment of the dial — no atomics or channels.
pub struct PoolView<'a> {
    /// The address being dialed. Available to address-aware strategies
    /// (e.g. `HashByAddr`).
    pub dial_addr: &'a str,
    /// Per-tunnel metrics. `metrics.len() == active tunnel count`.
    pub metrics: &'a [TunnelMetrics],
}

impl<'a> PoolView<'a> {
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

/// Live metrics for one active tunnel. Snapshot at dial time; updated by the
/// load balancer as connections come and go.
///
/// The fields are public so strategies can read them directly and factories
/// can seed the initial RTT.
#[derive(Clone, Copy, Default, Debug)]
pub struct TunnelMetrics {
    /// Last-measured RTT to the tunnel's remote endpoint. `None` if not yet
    /// probed or if the probe failed.
    pub rtt: Option<Duration>,
    /// Number of in-flight (open) connections on this tunnel right now.
    /// Strategies like `LeastConnections` read this to avoid hot spots.
    pub active_connections: u32,
    /// Dial errors since the tunnel came up. Strategies like
    /// `HealthWeighted` penalise tunnels that have been failing.
    pub recent_errors: u32,
    /// Total successful dials on this tunnel since startup.
    pub total_dials: u64,
    /// Total failed dials on this tunnel since startup.
    pub total_errors: u64,
}

/// A traffic distribution strategy for the load balancer.
///
/// Implementations choose which tunnel each new `dial` should go through.
/// Stateless strategies just look at the view and return an index; stateful
/// ones (e.g. `Failover`, `Sticky`, `RoundRobin`) maintain their own state
/// across calls.
pub trait BalanceStrategy: Send {
    /// Pick a tunnel index in `[0, view.len())`. Called once per `dial()`,
    /// after the load balancer has incremented `active_connections` for the
    /// picked tunnel so strategies that look at load see a consistent view.
    fn pick(&mut self, view: &PoolView<'_>) -> usize;

    /// Human-readable name for logging / diagnostics.
    fn name(&self) -> &str;

    /// Called by the load balancer when a `dial()` returns an error.
    /// Default is a no-op; `Failover` uses it to rotate the primary.
    fn report_error(&mut self, _idx: usize) {}
}

/// Blanket impl so `Box<dyn BalanceStrategy>` is itself a `BalanceStrategy`.
/// Lets `LoadBalancer::from_factories(_, strategy: impl BalanceStrategy +
/// 'static)` accept both concrete types and the boxed ones returned by
/// `random()`, `lowest_rtt()`, etc.
impl BalanceStrategy for Box<dyn BalanceStrategy> {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        (**self).pick(view)
    }
    fn name(&self) -> &str {
        (**self).name()
    }
    fn report_error(&mut self, idx: usize) {
        (**self).report_error(idx)
    }
}
