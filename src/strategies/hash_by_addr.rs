use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::constants::STRATEGY_NAMES;
use crate::traits::strategy::{BalanceStrategy, PoolView};

/// Hash the dial address modulo the tunnel count. The same hostname always
/// goes to the same tunnel — best for HTTP/HTTPS keep-alive.
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
    fn hash_by_addr_is_sticky() {
        let mut s = HashByAddr::new();
        let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3]);
        let v = PoolView { dial_addr: "api.example.com:443", metrics: &metrics };
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
            let v = PoolView { dial_addr: &addr, metrics: &metrics };
            hits[s.pick(&v)] += 1;
        }
        assert!(hits.iter().all(|&h| h > 0));
    }
}
