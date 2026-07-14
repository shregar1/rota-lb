use std::time::Duration;

use crate::constants::{MIN_WEIGHT, MS_PER_SECOND, STRATEGY_NAMES};
use crate::traits::strategy::{BalanceStrategy, PoolView};

/// Round-robin but weighted by inverse RTT. The fastest tunnel gets the
/// most slots, but the sequence walks through all of them in order — no
/// tunnel goes idle as long as there's any weight on it.
///
/// Weight = `max(1, 1000 / rtt_ms)`. Tunnels with no RTT get weight 1.
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
    fn weighted_round_robin_favors_fast_tunnel() {
        let mut s = WeightedRoundRobin::new();
        let metrics = make_metrics(&[Some(10), Some(100), Some(1000)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        let mut hits = [0usize; 3];
        for _ in 0..111 {
            hits[s.pick(&v)] += 1;
        }
        assert!(hits[0] > hits[1], "fastest should get most hits");
        assert!(hits[1] > hits[2], "medium should get more than slowest");
    }

    #[test]
    fn weighted_round_robin_walks_through_sequence() {
        let mut s = WeightedRoundRobin::new();
        let metrics = make_metrics(&[Some(333), Some(1000), Some(1000)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        let mut seq = Vec::new();
        for _ in 0..5 {
            seq.push(s.pick(&v));
        }
        assert_eq!(seq, vec![0, 1, 2, 0, 0]);
    }
}
