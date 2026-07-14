use proptest::prelude::*;
use rota_lb::{
    BalanceStrategy, Failover, HashByAddr, HealthWeighted, LeastConnections, LowestRtt, PoolView,
    Random, RoundRobin, Sticky, TunnelMetrics, WeightedRoundRobin,
};
use std::time::Duration;

fn make_pool_view(n: usize) -> PoolView<'static> {
    let metrics = (0..n)
        .map(|i| TunnelMetrics {
            rtt: Some(Duration::from_millis((i + 1) as u64 * 10)),
            active_connections: 0,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        })
        .collect::<Vec<_>>();
    let addr = format!("host{}.example.com:443", n);
    PoolView {
        dial_addr: Box::leak(addr.into_boxed_str()),
        metrics: Box::leak(metrics.into_boxed_slice()),
    }
}

#[allow(dead_code)]
fn make_pool_view_with_metrics(
    n: usize,
    rtts: &[Option<u64>],
    active: &[u32],
) -> PoolView<'static> {
    let metrics = rtts
        .iter()
        .zip(active.iter().chain(std::iter::repeat(&0)))
        .map(|(rtt, &active)| TunnelMetrics {
            rtt: rtt.map(Duration::from_millis),
            active_connections: active,
            recent_errors: 0,
            total_dials: 0,
            total_errors: 0,
        })
        .collect::<Vec<_>>();
    let addr = format!("host{}.example.com:443", n);
    PoolView {
        dial_addr: Box::leak(addr.into_boxed_str()),
        metrics: Box::leak(metrics.into_boxed_slice()),
    }
}

proptest! {
    #[test]
    fn round_robin_returns_valid_index(n in 1usize..64) {
        let mut s = RoundRobin::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn random_returns_valid_index(n in 1usize..64) {
        let mut s = Random::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn lowest_rtt_returns_valid_index(n in 1usize..64) {
        let mut s = LowestRtt::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn least_connections_returns_valid_index(n in 1usize..64) {
        let mut s = LeastConnections::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn hash_by_addr_returns_valid_index(n in 1usize..64) {
        let mut s = HashByAddr::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn hash_by_addr_is_deterministic(n in 1usize..64) {
        let mut s = HashByAddr::new();
        let view = make_pool_view(n);
        let idx1 = s.pick(&view);
        let idx2 = s.pick(&view);
        prop_assert_eq!(idx1, idx2);
    }

    #[test]
    fn weighted_round_robin_returns_valid_index(n in 1usize..64) {
        let mut s = WeightedRoundRobin::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn failover_returns_valid_index(n in 1usize..64) {
        let mut s = Failover::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn health_weighted_returns_valid_index(n in 1usize..64) {
        let mut s = HealthWeighted::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn sticky_returns_valid_index(n in 1usize..64) {
        let mut s = Sticky::new();
        let view = make_pool_view(n);
        let idx = s.pick(&view);
        prop_assert!(idx < n);
    }

    #[test]
    fn round_robin_cycles_through_all(n in 1usize..20) {
        let mut s = RoundRobin::new();
        let view = make_pool_view(n);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..n * 3 {
            let idx = s.pick(&view);
            seen.insert(idx);
        }
        prop_assert_eq!(seen.len(), n);
    }

    #[test]
    fn lowest_rtt_picks_fastest(n in 2usize..10) {
        let mut s = LowestRtt::new();
        let metrics = (0..n).map(|i| TunnelMetrics {
            rtt: Some(Duration::from_millis((i + 1) as u64 * 10)),
            ..Default::default()
        }).collect::<Vec<_>>();
        let view = PoolView {
            dial_addr: "example.com:443",
            metrics: &metrics,
        };
        let idx = s.pick(&view);
        prop_assert_eq!(idx, 0);
    }

    #[test]
    fn least_connections_picks_min_active(n in 2usize..10) {
        let mut s = LeastConnections::new();
        let mut metrics = vec![TunnelMetrics::default(); n];
        for (i, m) in metrics.iter_mut().enumerate() {
            m.active_connections = i as u32;
        }
        let view = PoolView {
            dial_addr: "example.com:443",
            metrics: &metrics,
        };
        let idx = s.pick(&view);
        prop_assert_eq!(idx, 0);
    }
}
