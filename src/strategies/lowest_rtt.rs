use std::time::Duration;

use crate::constants::STRATEGY_NAMES;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::strategy::TunnelMetrics;

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
    fn lowest_rtt_picks_fastest() {
        let mut s = LowestRtt::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0, 0, 0]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn lowest_rtt_with_no_rtts_picks_first() {
        let mut s = LowestRtt::new();
        let metrics = make_metrics(&[None, None, None], &[0, 0, 0]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
    }
}
