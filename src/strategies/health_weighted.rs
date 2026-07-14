use crate::constants::{DEFAULT_RTT_US, ERROR_PENALTY_US, LOAD_PENALTY_US, STRATEGY_NAMES};
use crate::traits::strategy::{BalanceStrategy, PoolView};

/// Score = `rtt_us + error_penalty * recent_errors + load_penalty *
/// active_connections`. Pick the lowest score.
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
    fn health_weighted_picks_fastest_with_no_errors() {
        let mut s = HealthWeighted::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn health_weighted_demotes_erroring_tunnel() {
        let mut s = HealthWeighted::new();
        let metrics = vec![
            TunnelMetrics { rtt: Some(Duration::from_millis(10)), ..Default::default() },
            TunnelMetrics { rtt: Some(Duration::from_millis(10)), recent_errors: 1, ..Default::default() },
        ];
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn health_weighted_adds_load_penalty() {
        let mut s = HealthWeighted::new();
        let metrics = vec![
            TunnelMetrics { rtt: Some(Duration::from_millis(10)), active_connections: 0, ..Default::default() },
            TunnelMetrics { rtt: Some(Duration::from_millis(10)), active_connections: 5, ..Default::default() },
        ];
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
    }
}
