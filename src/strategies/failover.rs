use crate::constants::STRATEGY_NAMES;
use crate::traits::strategy::{BalanceStrategy, PoolView};

/// Always pick the "primary" tunnel. On dial error, advance to the next
/// tunnel and use that as the new primary. One tunnel handles all traffic
/// until it fails; the rest are pure standby.
#[derive(Debug, Clone, Copy)]
pub struct Failover {
    primary: usize,
    len: usize,
}

impl Default for Failover {
    fn default() -> Self {
        Self::new()
    }
}

impl Failover {
    /// Create a new failover strategy. `len` starts at 0 and is set on the
    /// first `pick()`.
    pub const fn new() -> Self {
        Self { primary: 0, len: 0 }
    }

    /// Create a failover strategy with a known pool size.
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
    fn failover_returns_primary() {
        let mut s = Failover::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        assert_eq!(s.pick(&v), 0);
        assert_eq!(s.pick(&v), 0);
    }

    #[test]
    fn failover_rotates_on_error() {
        let mut s = Failover::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        s.pick(&v);
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
        let v = PoolView { dial_addr: "h", metrics: &metrics };
        s.pick(&v);
        s.report_error(1);
        assert_eq!(s.pick(&v), 0);
    }
}
