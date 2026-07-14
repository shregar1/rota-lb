use std::time::Duration;

use crate::constants::STRATEGY_NAMES;
use crate::traits::strategy::{BalanceStrategy, PoolView};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::strategy::TunnelMetrics;
    use std::time::Duration;

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
    fn least_connections_picks_min() {
        let mut s = LeastConnections::new();
        let metrics = make_metrics(&[Some(10), Some(10), Some(10)], &[5, 2, 8]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn least_connections_tiebreaks_by_rtt() {
        let mut s = LeastConnections::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[2, 2, 2]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
    }
}
