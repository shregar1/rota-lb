use crate::constants::STRATEGY_NAMES;
use crate::traits::strategy::{BalanceStrategy, PoolView};

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
    fn round_robin_walks_and_wraps() {
        let mut s = RoundRobin::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0, 0, 0]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
        assert_eq!(s.pick(&v), 1);
        assert_eq!(s.pick(&v), 2);
        assert_eq!(s.pick(&v), 0);
    }
}
