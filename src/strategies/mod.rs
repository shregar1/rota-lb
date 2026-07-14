//! Built-in balance strategies.
//!
//! Each strategy is a small, testable type that implements
//! [`BalanceStrategy`](crate::BalanceStrategy). Use the free constructors at
//! the crate root ([`round_robin`](crate::round_robin), [`random`](crate::random), etc.)
//! for the boxed-dyn convenience, or instantiate the concrete type when you
//! need to keep it.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::constants::{
    DEFAULT_RTT_US, ERROR_PENALTY_US, LOAD_PENALTY_US, MIN_WEIGHT, MS_PER_SECOND, STRATEGY_NAMES,
};
use crate::traits::strategy::{BalanceStrategy, PoolView, TunnelMetrics};

/// Find the index of the tunnel with the lowest RTT. Returns 0 if no RTTs available.
fn find_lowest_rtt(metrics: &[TunnelMetrics]) -> usize {
    let mut best = 0;
    let mut best_rtt = Duration::MAX;
    for (i, m) in metrics.iter().enumerate() {
        if let Some(rtt) = m.rtt {
            if rtt < best_rtt {
                best_rtt = rtt;
                best = i;
            }
        }
    }
    best
}

// ============================================================================
//  1. RoundRobin
// ============================================================================

/// Walk tunnels in order, wrapping at the end. Even distribution, no metrics
/// needed. Best cheap default.
#[derive(Debug, Clone, Copy)]
pub struct RoundRobin {
    next: usize,
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobin {
    /// Create a new round-robin strategy starting at index 0.
    pub const fn new() -> Self {
        Self { next: 0 }
    }
}

impl BalanceStrategy for RoundRobin {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        let len = view.len();
        debug_assert!(len > 0, "RoundRobin::pick called with no tunnels");
        let idx = self.next % len;
        self.next = idx + 1;
        idx
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[0]
    }
}

// ============================================================================
//  2. Random
// ============================================================================

/// Random pick. Even distribution in expectation, no state. Good fallback
/// when no metrics are available.
#[derive(Debug, Clone, Copy)]
pub struct Random;

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

impl Random {
    /// Create a new random strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl BalanceStrategy for Random {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        use rand::Rng;
        rand::thread_rng().gen_range(0..view.len())
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[1]
    }
}

// ============================================================================
//  3. LowestRtt
// ============================================================================

/// Always pick the tunnel with the lowest measured RTT. Ties broken by
/// index. Best for latency-sensitive workloads (gaming, voice). All traffic
/// goes to one tunnel; the others are standby.
#[derive(Debug, Clone, Copy)]
pub struct LowestRtt;

impl Default for LowestRtt {
    fn default() -> Self {
        Self::new()
    }
}

impl LowestRtt {
    /// Create a new lowest-RTT strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl BalanceStrategy for LowestRtt {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        find_lowest_rtt(view.metrics)
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[2]
    }
}

// ============================================================================
//  4. LeastConnections
// ============================================================================

/// Pick the tunnel with the fewest active connections. Ties broken by
/// lowest RTT. Adaptive: spreads load evenly across tunnels as connection
/// lifetimes differ.
#[derive(Debug, Clone, Copy)]
pub struct LeastConnections;

impl Default for LeastConnections {
    fn default() -> Self {
        Self::new()
    }
}

impl LeastConnections {
    /// Create a new least-connections strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl BalanceStrategy for LeastConnections {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        let mut best = 0;
        let mut best_count = u32::MAX;
        for (i, m) in view.metrics.iter().enumerate() {
            if m.active_connections < best_count {
                best_count = m.active_connections;
                best = i;
            } else if m.active_connections == best_count {
                let best_rtt = view.metrics[best].rtt.unwrap_or(Duration::MAX);
                let my_rtt = m.rtt.unwrap_or(Duration::MAX);
                if my_rtt < best_rtt {
                    best = i;
                }
            }
        }
        best
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[3]
    }
}

// ============================================================================
//  5. HashByAddr
// ============================================================================

/// Hash the dial address modulo the tunnel count. The same hostname always
/// goes to the same tunnel — best for HTTP/HTTPS keep-alive.
///
/// Server-side connection caching and any protocol that benefits from sticky
/// connections.
#[derive(Debug, Clone, Copy)]
pub struct HashByAddr;

impl Default for HashByAddr {
    fn default() -> Self {
        Self::new()
    }
}

impl HashByAddr {
    /// Create a new hash-by-address strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl BalanceStrategy for HashByAddr {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        let mut hasher = DefaultHasher::new();
        view.dial_addr.hash(&mut hasher);
        let hash = usize::try_from(hasher.finish()).unwrap_or(0);
        hash % view.len()
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[4]
    }
}

// ============================================================================
//  6. WeightedRoundRobin
// ============================================================================

/// Round-robin but weighted by inverse RTT. The fastest tunnel gets the
/// most slots, but the sequence walks through all of them in order — no
/// tunnel goes idle as long as there's any weight on it.
///
/// Weight = `max(1, 1000 / rtt_ms)`. Tunnels with no RTT get weight 1.
///
/// Uses a virtual scheduler instead of materializing the full weight
/// sequence, avoiding unbounded memory for large pools.
#[derive(Debug, Clone)]
pub struct WeightedRoundRobin {
    rtts: Vec<Option<Duration>>,
    weights: Vec<u32>,
    picks: Vec<u64>,
}

impl Default for WeightedRoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl WeightedRoundRobin {
    /// Create a new weighted round-robin strategy.
    pub const fn new() -> Self {
        Self {
            rtts: Vec::new(),
            weights: Vec::new(),
            picks: Vec::new(),
        }
    }

    fn rebuild(&mut self) {
        self.weights.clear();
        for rtt in &self.rtts {
            let weight = rtt.as_ref().map_or(MIN_WEIGHT, |r| {
                let ms = u64::try_from(r.as_millis()).unwrap_or(1).max(1);
                u32::try_from((u64::from(MS_PER_SECOND) / ms).max(u64::from(MIN_WEIGHT)))
                    .unwrap_or(MIN_WEIGHT)
            });
            self.weights.push(weight);
        }
        self.picks = vec![0; self.weights.len()];
    }
}

impl BalanceStrategy for WeightedRoundRobin {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        let new_rtts: Vec<Option<Duration>> = view.metrics.iter().map(|m| m.rtt).collect();
        if new_rtts != self.rtts {
            self.rtts = new_rtts;
            self.rebuild();
        }
        if self.weights.is_empty() {
            return 0;
        }

        let n = self.weights.len();
        let mut best = 0;
        for i in 1..n {
            if self.picks[i] * u64::from(self.weights[best])
                < self.picks[best] * u64::from(self.weights[i])
            {
                best = i;
            }
        }
        self.picks[best] += 1;
        best
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[5]
    }
}

// ============================================================================
//  7. Failover
// ============================================================================

/// Always pick the "primary" tunnel. On dial error, advance to the next
/// tunnel and use that as the new primary. One tunnel handles all traffic
/// until it fails; the rest are pure standby.
///
/// Combine with `tunnel_count = N` for "use the best, but have N-1
/// fallbacks ready".
#[derive(Debug, Clone, Copy)]
pub struct Failover {
    primary: usize,
    /// Cached pool size. Updated on first `pick()`; the FFI path also
    /// pre-initialises this via [`Failover::with_backend_count`] so that
    /// `report_error` works correctly when called before any `pick`.
    len: usize,
}

impl Default for Failover {
    fn default() -> Self {
        Self::new()
    }
}

impl Failover {
    /// Create a new failover strategy. `len` starts at 0 and is set on the
    /// first `pick()` — use [`Failover::with_backend_count`] when the pool
    /// size is known up-front (e.g. when wired through the C ABI).
    pub const fn new() -> Self {
        Self { primary: 0, len: 0 }
    }

    /// Create a failover strategy with a known pool size. Use this when the
    /// pool is constructed from the FFI layer (where the backend count is
    /// known at create-time). Avoids the silent-no-op in `report_error`
    /// when `idx == 0` is reported before any `pick` — without a pre-set
    /// `len`, `(idx + 1) % (idx + 1) == 0` and the primary wouldn't advance.
    pub const fn with_backend_count(backend_count: usize) -> Self {
        Self {
            primary: 0,
            len: backend_count,
        }
    }
}

impl BalanceStrategy for Failover {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        self.len = view.len();
        if self.len == 0 {
            return 0;
        }
        self.primary % self.len
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[6]
    }
    fn report_error(&mut self, idx: usize) {
        let len = if self.len > 0 { self.len } else { idx + 1 };
        if idx == self.primary {
            self.primary = (idx + 1) % len;
        }
    }
}

// ============================================================================
//  8. HealthWeighted
// ============================================================================

/// Score = `rtt_us + error_penalty * recent_errors + load_penalty *
/// active_connections`. Pick the lowest score.
///
/// Tunnels that have been erroring recently get pushed down; tunnels with
/// more active connections pay a small load penalty.
///
/// Weights (in microseconds):
/// - `error_penalty = 500_000` (each error adds 500ms equivalent)
/// - `load_penalty = 10_000` (each conn adds 10ms)
#[derive(Debug, Clone, Copy)]
pub struct HealthWeighted;

impl Default for HealthWeighted {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthWeighted {
    /// Create a new health-weighted strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl BalanceStrategy for HealthWeighted {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        let mut best = 0;
        let mut best_score = u64::MAX;
        for (i, m) in view.metrics.iter().enumerate() {
            let rtt_us = m.rtt.map_or(DEFAULT_RTT_US, |r| {
                u64::try_from(r.as_micros()).unwrap_or(DEFAULT_RTT_US)
            });
            let error_penalty = u64::from(m.recent_errors) * ERROR_PENALTY_US;
            let load_penalty = u64::from(m.active_connections) * LOAD_PENALTY_US;
            let score = rtt_us + error_penalty + load_penalty;
            if score < best_score {
                best_score = score;
                best = i;
            }
        }
        best
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[7]
    }
}

// ============================================================================
//  9. Sticky
// ============================================================================

/// Pin to a single tunnel for the lifetime of the load balancer.
///
/// On the **first** call to `pick`, selects the lowest-RTT tunnel and
/// remembers it. On every subsequent call, returns that same index
/// regardless of changes in metrics. Dial errors are ignored — the pin
/// is not released.
#[derive(Debug, Clone, Copy)]
pub struct Sticky {
    pinned: Option<usize>,
}

impl Default for Sticky {
    fn default() -> Self {
        Self::new()
    }
}

impl Sticky {
    /// Create a new sticky strategy.
    pub const fn new() -> Self {
        Self { pinned: None }
    }
}

impl BalanceStrategy for Sticky {
    fn pick(&mut self, view: &PoolView<'_>) -> usize {
        if let Some(idx) = self.pinned {
            return idx.min(view.len().saturating_sub(1));
        }
        // First pick: choose the best (lowest RTT). Tiebreak by index.
        let best = find_lowest_rtt(view.metrics);
        self.pinned = Some(best);
        best
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[8]
    }
    fn report_error(&mut self, _idx: usize) {}
}

// ============================================================================
//  Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metrics(rtts: &[Option<u64>], counts: &[u32]) -> Vec<TunnelMetrics> {
        rtts.iter()
            .zip(counts.iter().chain(std::iter::repeat(&0)))
            .map(|(rtt, &active)| TunnelMetrics {
                rtt: rtt.map(Duration::from_millis),
                active_connections: active,
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn round_robin_walks_and_wraps() {
        let mut s = RoundRobin::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0, 0, 0]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
        assert_eq!(s.pick(&v), 1);
        assert_eq!(s.pick(&v), 2);
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn random_picks_in_range() {
        let mut s = Random::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0, 0, 0]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        for _ in 0..100 {
            assert!(s.pick(&v) < 3);
        }
    }

    #[test]
    fn lowest_rtt_picks_fastest() {
        let mut s = LowestRtt::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0, 0, 0]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn lowest_rtt_with_no_rtts_picks_first() {
        let mut s = LowestRtt::new();
        let metrics = make_metrics(&[None, None, None], &[0, 0, 0]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn least_connections_picks_min() {
        let mut s = LeastConnections::new();
        let metrics = make_metrics(&[Some(10), Some(10), Some(10)], &[5, 2, 8]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn least_connections_tiebreaks_by_rtt() {
        let mut s = LeastConnections::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[2, 2, 2]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn hash_by_addr_is_sticky() {
        let mut s = HashByAddr::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView {
            dial_addr: "api.example.com:443",
            metrics: &metrics,
        };
        let a = s.pick(&v);
        let b = s.pick(&v);
        assert_eq!(a, b, "same address should pick the same tunnel");
    }

    #[test]
    fn hash_by_addr_distributes() {
        let mut s = HashByAddr::new();
        let mut hits = [0usize; 3];
        for i in 0..30 {
            let addr = format!("host{i}.example.com:80");
            let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
            let v = PoolView {
                dial_addr: &addr,
                metrics: &metrics,
            };
            hits[s.pick(&v)] += 1;
        }
        assert!(hits.iter().all(|&h| h > 0));
    }

    #[test]
    fn weighted_round_robin_favors_fast_tunnel() {
        let mut s = WeightedRoundRobin::new();
        let metrics = make_metrics(&[Some(10), Some(100), Some(1000)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        let mut hits = [0usize; 3];
        for _ in 0..111 {
            hits[s.pick(&v)] += 1;
        }
        assert!(hits[0] > hits[1], "fastest should get most hits");
        assert!(hits[1] > hits[2], "medium should get more than slowest");
    }

    #[test]
    fn weighted_round_robin_walks_through_sequence() {
        // With RTT weights 333ms, 1000ms, 1000ms (weights 3, 1, 1),
        // the virtual scheduler picks: 0, 1, 2, 0, 0 (interleaved by
        // picks[i] / weights[i] ratio).
        let mut s = WeightedRoundRobin::new();
        let metrics = make_metrics(&[Some(333), Some(1000), Some(1000)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        let mut seq = Vec::new();
        for _ in 0..5 {
            seq.push(s.pick(&v));
        }
        assert_eq!(seq, vec![0, 1, 2, 0, 0]);
    }

    #[test]
    fn failover_returns_primary() {
        let mut s = Failover::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn failover_rotates_on_error() {
        let mut s = Failover::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        s.pick(&v); // initialise len
        s.report_error(0);
        assert_eq!(s.pick(&v), 1);
        s.report_error(1);
        assert_eq!(s.pick(&v), 2);
        s.report_error(2);
        assert_eq!(s.pick(&v), 0, "should wrap back to tunnel 0");
    }

    #[test]
    fn failover_ignores_unrelated_errors() {
        let mut s = Failover::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        s.pick(&v);
        s.report_error(1);
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn health_weighted_picks_fastest_with_no_errors() {
        let mut s = HealthWeighted::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn health_weighted_demotes_erroring_tunnel() {
        let mut s = HealthWeighted::new();
        let metrics = vec![
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                ..Default::default()
            },
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                recent_errors: 1,
                ..Default::default()
            },
        ];
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn health_weighted_adds_load_penalty() {
        let mut s = HealthWeighted::new();
        let metrics = vec![
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                active_connections: 0,
                ..Default::default()
            },
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                active_connections: 5,
                ..Default::default()
            },
        ];
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn sticky_picks_lowest_rtt_on_first_call() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn sticky_pins_to_first_choice() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        let first = s.pick(&v);
        for _ in 0..100 {
            assert_eq!(s.pick(&v), first);
        }
    }

    #[test]
    fn sticky_does_not_repick_when_metrics_change() {
        let mut s = Sticky::new();
        let m1 = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v1 = PoolView {
            dial_addr: "h",
            metrics: &m1,
        };
        let first = s.pick(&v1);
        assert_eq!(first, 1);
        let m2 = make_metrics(&[Some(5), Some(100), Some(200)], &[0; 3]);
        let v2 = PoolView {
            dial_addr: "h",
            metrics: &m2,
        };
        for _ in 0..10 {
            assert_eq!(s.pick(&v2), 1);
        }
    }

    #[test]
    fn sticky_report_error_does_not_release_pin() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        let first = s.pick(&v);
        s.report_error(first);
        s.report_error(first);
        assert_eq!(s.pick(&v), first);
    }

    #[test]
    fn sticky_picks_first_when_no_rtts() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[None, None, None], &[0; 3]);
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };
        assert_eq!(s.pick(&v), 0);
        for _ in 0..5 {
            assert_eq!(s.pick(&v), 0);
        }
    }
}

// ============================================================================
//  Free constructors — return Box<dyn BalanceStrategy> for convenience.
// ============================================================================

/// Round-robin strategy. Even distribution, no metrics required.
pub fn round_robin() -> Box<dyn BalanceStrategy> {
    Box::new(RoundRobin::new())
}

/// Random strategy. Stateless fallback when no metrics available.
pub fn random() -> Box<dyn BalanceStrategy> {
    Box::new(Random::new())
}

/// Lowest-RTT strategy. Picks the tunnel with the lowest measured RTT.
pub fn lowest_rtt() -> Box<dyn BalanceStrategy> {
    Box::new(LowestRtt::new())
}

/// Least-connections strategy. Picks the tunnel with the fewest active connections.
pub fn least_connections() -> Box<dyn BalanceStrategy> {
    Box::new(LeastConnections::new())
}

/// Hash-by-address strategy. Same hostname always routes to the same tunnel.
pub fn hash_by_addr() -> Box<dyn BalanceStrategy> {
    Box::new(HashByAddr::new())
}

/// Weighted round-robin. Weights by inverse RTT.
pub fn weighted_round_robin() -> Box<dyn BalanceStrategy> {
    Box::new(WeightedRoundRobin::new())
}

/// Failover strategy. Uses primary tunnel until it fails, then rotates.
pub fn failover() -> Box<dyn BalanceStrategy> {
    Box::new(Failover::new())
}

/// Health-weighted strategy. Scores by RTT + error penalty + load penalty.
pub fn health_weighted() -> Box<dyn BalanceStrategy> {
    Box::new(HealthWeighted::new())
}

/// Sticky strategy. Pins to the first-chosen tunnel for the balancer's lifetime.
pub fn sticky() -> Box<dyn BalanceStrategy> {
    Box::new(Sticky::new())
}

#[cfg(test)]
mod constructor_tests {
    use super::*;

    #[test]
    fn free_constructors_return_boxed_strategies() {
        let metrics = vec![
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                ..Default::default()
            },
            TunnelMetrics {
                rtt: Some(Duration::from_millis(20)),
                ..Default::default()
            },
        ];
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };

        let mut strategies: Vec<Box<dyn BalanceStrategy>> = vec![
            round_robin(),
            random(),
            lowest_rtt(),
            least_connections(),
            hash_by_addr(),
            weighted_round_robin(),
            failover(),
            health_weighted(),
            sticky(),
        ];
        for s in &mut strategies {
            let _ = s.name();
            assert!(s.pick(&v) < 2);
        }
        assert_eq!(strategies.len(), 9);
    }
}
