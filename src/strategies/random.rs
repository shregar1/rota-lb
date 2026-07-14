use crate::constants::STRATEGY_NAMES;
use crate::traits::strategy::{BalanceStrategy, PoolView};

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
    fn random_picks_in_range() {
        let mut s = Random::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0, 0, 0]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        for _ in 0..100 {
            assert!(s.pick(&v) < 3);
        }
    }
}
