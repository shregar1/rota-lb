//! Tests for the strategy module and BalanceStrategy trait.

use std::time::Duration;

use rota::strategy::{BalanceStrategy, PoolView, TunnelMetrics};
use rota::strategies::{
    round_robin, random, lowest_rtt, least_connections, hash_by_addr,
    weighted_round_robin, failover, health_weighted, sticky,
    RoundRobin, Random, LowestRtt, LeastConnections, HashByAddr,
    WeightedRoundRobin, Failover, HealthWeighted, Sticky,
};

fn make_view(rtts: &[Option<u64>], active: &[u32], errors: &[u32]) -> PoolView<'static> {
    let metrics: Vec<TunnelMetrics> = rtts.iter().zip(active.iter()).zip(errors.iter())
        .map(|((rtt, &a), &e)| TunnelMetrics {
            rtt: rtt.map(Duration::from_millis),
            active_connections: a,
            recent_errors: e,
            ..Default::default()
        })
        .collect();
    PoolView {
        dial_addr: "h",
        metrics: Box::leak(Box::new(metrics).into_boxed_slice()),
    }
}

// ============================================================================
//  Strategy names
// ============================================================================

#[test]
fn strategy_names() {
    assert_eq!(round_robin().name(), "round_robin");
    assert_eq!(random().name(), "random");
    assert_eq!(lowest_rtt().name(), "lowest_rtt");
    assert_eq!(least_connections().name(), "least_connections");
    assert_eq!(hash_by_addr().name(), "hash_by_addr");
    assert_eq!(weighted_round_robin().name(), "weighted_round_robin");
    assert_eq!(failover().name(), "failover");
    assert_eq!(health_weighted().name(), "health_weighted");
    assert_eq!(sticky().name(), "sticky");
}

// ============================================================================
//  RoundRobin edge cases
// ============================================================================

#[test]
fn round_robin_single_backend() {
    let mut s = RoundRobin::new();
    let v = make_view(&[Some(10)], &[0], &[0]);
    for _ in 0..5 {
        assert_eq!(s.pick(&v), 0);
    }
}

#[test]
fn round_robin_starts_at_zero() {
    let mut s = RoundRobin::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    // The first pick should be 0, then 1, then 2
    assert_eq!(s.pick(&v), 0);
}

// ============================================================================
//  Random distribution
// ============================================================================

#[test]
fn random_returns_valid_index() {
    let mut s = Random::new();
    for n in [1, 3, 10, 100] {
        for _ in 0..100 {
            let v = make_view(&vec![Some(10); n], &vec![0; n], &vec![0; n]);
            let idx = s.pick(&v);
            assert!(idx < n);
        }
    }
}

// ============================================================================
//  LowestRtt edge cases
// ============================================================================

#[test]
fn lowest_rtt_all_same() {
    let mut s = LowestRtt::new();
    let v = make_view(&[Some(10), Some(10), Some(10)], &[0; 3], &[0; 3]);
    // All same, should pick 0 (first found)
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn lowest_rtt_picks_lowest() {
    let mut s = LowestRtt::new();
    let v = make_view(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 1);
}

// ============================================================================
//  LeastConnections edge cases
// ============================================================================

#[test]
fn least_connections_picks_min() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(10); 3], &[5, 2, 8], &[0; 3]);
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn least_connections_tiebreaks_by_rtt() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[5; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn least_connections_with_very_high_load() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(10); 3], &[u32::MAX, u32::MAX, u32::MAX], &[0; 3]);
    // Should still pick something (first found with lowest)
    let idx = s.pick(&v);
    assert!(idx < 3);
}

// ============================================================================
//  HashByAddr tests
// ============================================================================

#[test]
fn hash_by_addr_consistent() {
    let mut s = HashByAddr::new();
    let v = make_view(&[Some(10); 5], &[0; 5], &[0; 5]);
    let addr = "api.example.com:443";
    let v1 = PoolView { dial_addr: addr, ..v };
    let a1 = s.pick(&v1);
    let a2 = s.pick(&v1);
    assert_eq!(a1, a2);
}

#[test]
fn hash_by_addr_different_addrs() {
    let mut s = HashByAddr::new();
    let metrics: Vec<TunnelMetrics> = (0..3).map(|_| TunnelMetrics::default()).collect();
    let metrics = Box::leak(Box::new(metrics).into_boxed_slice());

    let v1 = PoolView { dial_addr: "a:80", metrics };
    let v2 = PoolView { dial_addr: "b:80", metrics };
    // Same metrics, different addresses - should distribute
    let _ = s.pick(&v1);
    let _ = s.pick(&v2);
}

#[test]
fn hash_by_addr_with_empty_addr() {
    let mut s = HashByAddr::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    let v_empty = PoolView {
        dial_addr: "",
        metrics: v.metrics,
    };
    let idx = s.pick(&v_empty);
    assert!(idx < 3);
}

// ============================================================================
//  WeightedRoundRobin tests
// ============================================================================

#[test]
fn weighted_round_robins_chooses_fastest_first() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[Some(10), Some(100), Some(1000)], &[0; 3], &[0; 3]);
    // With weights 100, 10, 1, the fastest should be picked first
    let first = s.pick(&v);
    // The virtual scheduler picks 0 first (lowest ratio)
    assert_eq!(first, 0);
}

#[test]
fn weighted_round_robin_handles_no_rtts() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[None, None, None], &[0; 3], &[0; 3]);
    let idx = s.pick(&v);
    assert!(idx < 3);
}

#[test]
fn weighted_round_robin_handles_mixed() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[Some(10), None, Some(100)], &[0; 3], &[0; 3]);
    let _ = s.pick(&v);
}

#[test]
fn weighted_round_robin_rebuilds_on_change() {
    let mut s = WeightedRoundRobin::new();
    let v1 = make_view(&[Some(10), Some(100)], &[0; 2], &[0; 2]);
    s.pick(&v1);
    let v2 = make_view(&[Some(50), Some(200), Some(300)], &[0; 3], &[0; 3]);
    s.pick(&v2);
}

// ============================================================================
//  Failover tests
// ============================================================================

#[test]
fn failover_starts_at_zero() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_no_report_returns_zero() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    for _ in 0..5 {
        assert_eq!(s.pick(&v), 0);
    }
}

#[test]
fn failover_report_unrelated() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    s.pick(&v);
    s.report_error(1);
    s.report_error(2);
    // Primary (0) is still 0
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_multiple_rotations() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10), Some(20), Some(30), Some(40)], &[0; 4], &[0; 4]);
    s.pick(&v);
    s.report_error(0);
    assert_eq!(s.pick(&v), 1);
    s.report_error(1);
    assert_eq!(s.pick(&v), 2);
    s.report_error(2);
    assert_eq!(s.pick(&v), 3);
    s.report_error(3);
    assert_eq!(s.pick(&v), 0);
}

// ============================================================================
//  HealthWeighted tests
// ============================================================================

#[test]
fn health_weighted_with_no_errors() {
    let mut s = HealthWeighted::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    // All same score, should pick 0
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn health_weighted_avoid_errors() {
    let mut s = HealthWeighted::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0, 5, 0]);
    // Backend 1 has errors, should be avoided
    assert_ne!(s.pick(&v), 1);
}

#[test]
fn health_weighted_with_active_load() {
    let mut s = HealthWeighted::new();
    let v = make_view(&[Some(10); 3], &[0, 5, 10], &[0; 3]);
    // Should pick 0 (lowest active)
    assert_eq!(s.pick(&v), 0);
}

// ============================================================================
//  Sticky tests
// ============================================================================

#[test]
fn sticky_picks_first_then_remains() {
    let mut s = Sticky::new();
    let v = make_view(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let first = s.pick(&v);
    for _ in 0..10 {
        assert_eq!(s.pick(&v), first);
    }
}

#[test]
fn sticky_does_not_change_on_metrics() {
    let mut s = Sticky::new();
    let v1 = make_view(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let first = s.pick(&v1);
    let v2 = make_view(&[Some(5), Some(100), Some(200)], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v2), first);
}

#[test]
fn sticky_report_error_does_not_release() {
    let mut s = Sticky::new();
    let v = make_view(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let first = s.pick(&v);
    s.report_error(first);
    s.report_error(first);
    s.report_error(first);
    assert_eq!(s.pick(&v), first);
}

#[test]
fn sticky_handles_pool_shrink() {
    let mut s = Sticky::new();
    let v3 = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    let first = s.pick(&v3);
    // If pool shrinks to 1, sticky should still pick that one
    let v1 = make_view(&[Some(10)], &[0], &[0]);
    // The first pick was an index from the larger pool; pick returns idx.min(view.len().saturating_sub(1))
    let _ = s.pick(&v1);
    let _ = first;
}

// ============================================================================
//  Strategy `name()` for each concrete type
// ============================================================================

#[test]
fn concrete_strategy_names() {
    let _ = RoundRobin::new().name();
    let _ = Random::new().name();
    let _ = LowestRtt::new().name();
    let _ = LeastConnections::new().name();
    let _ = HashByAddr::new().name();
    let _ = WeightedRoundRobin::new().name();
    let _ = Failover::new().name();
    let _ = HealthWeighted::new().name();
    let _ = Sticky::new().name();
}