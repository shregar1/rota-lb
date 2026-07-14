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

/// Pin to a single tunnel for the lifetime of the load balancer.
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
        let best = find_lowest_rtt(view.metrics);
        self.pinned = Some(best);
        best
    }
    fn name(&self) -> &str {
        STRATEGY_NAMES[8]
    }
    fn report_error(&mut self, _idx: usize) {}
}

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
    fn sticky_picks_lowest_rtt_on_first_call() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 1);
    }

    #[test]
    fn sticky_pins_to_first_choice() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        let first = s.pick(&v);
        for _ in 0..100 {
            assert_eq!(s.pick(&v), first);
        }
    }

    #[test]
    fn sticky_does_not_repick_when_metrics_change() {
        let mut s = Sticky::new();
        let m1 = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v1 = PoolView { dial_addr: "h", metrics: &m1 };
        let first = s.pick(&v1);
        assert_eq!(first, 1);
        let m2 = make_metrics(&[Some(5), Some(100), Some(200)], &[0; 3]);
        let v2 = PoolView { dial_addr: "h", metrics: &m2 };
        for _ in 0..10 {
            assert_eq!(s.pick(&v2), 1);
        }
    }

    #[test]
    fn sticky_report_error_does_not_release_pin() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        let first = s.pick(&v);
        s.report_error(first);
        s.report_error(first);
        assert_eq!(s.pick(&v), first);
    }

    #[test]
    fn sticky_picks_first_when_no_rtts() {
        let mut s = Sticky::new();
        let metrics = make_metrics(&[None, None, None], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
        for _ in 0..5 {
            assert_eq!(s.pick(&v), 0);
        }
    }
}
