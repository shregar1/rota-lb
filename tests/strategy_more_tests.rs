//! More strategy tests to improve coverage.

use std::time::Duration;
use rota::{BalanceStrategy, PoolView, TunnelMetrics};
use rota::strategies::{
    round_robin, random, lowest_rtt, least_connections, hash_by_addr,
    weighted_round_robin, failover, health_weighted, sticky,
    RoundRobin, Random, LowestRtt, LeastConnections, HashByAddr,
    WeightedRoundRobin, Failover, HealthWeighted, Sticky,
};

fn make_view<'a>(metrics: &'a [TunnelMetrics], addr: &'a str) -> PoolView<'a> {
    PoolView {
        dial_addr: addr,
        metrics,
    }
}

fn make_metrics(rtts: &[Option<u64>], active: &[u32], errors: &[u32]) -> Vec<TunnelMetrics> {
    rtts.iter().zip(active.iter()).zip(errors.iter())
        .map(|((rtt, &a), &e)| TunnelMetrics {
            rtt: rtt.map(Duration::from_millis),
            active_connections: a,
            recent_errors: e,
            ..Default::default()
        })
        .collect()
}

#[test]
fn round_robin_with_single_backend() {
    let mut s = RoundRobin::new();
    let metrics = make_metrics(&[Some(10)], &[0], &[0]);
    let v = make_view(&metrics, "a:80");
    assert_eq!(s.pick(&v), 0);
    assert_eq!(s.pick(&v), 0);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn round_robin_name() {
    let s = RoundRobin::new();
    assert_eq!(s.name(), "round_robin");
}

#[test]
fn random_name() {
    let s = Random::new();
    assert_eq!(s.name(), "random");
}

#[test]
fn lowest_rtt_name() {
    let s = LowestRtt::new();
    assert_eq!(s.name(), "lowest_rtt");
}

#[test]
fn least_connections_name() {
    let s = LeastConnections::new();
    assert_eq!(s.name(), "least_connections");
}

#[test]
fn hash_by_addr_name() {
    let s = HashByAddr::new();
    assert_eq!(s.name(), "hash_by_addr");
}

#[test]
fn weighted_round_robin_name() {
    let s = WeightedRoundRobin::new();
    assert_eq!(s.name(), "weighted_round_robin");
}

#[test]
fn failover_name() {
    let s = Failover::new();
    assert_eq!(s.name(), "failover");
}

#[test]
fn health_weighted_name() {
    let s = HealthWeighted::new();
    assert_eq!(s.name(), "health_weighted");
}

#[test]
fn sticky_name() {
    let s = Sticky::new();
    assert_eq!(s.name(), "sticky");
}

#[test]
fn round_robin_debug() {
    let s = RoundRobin::new();
    let _ = format!("{:?}", s);
}

#[test]
fn random_debug() {
    let s = Random::new();
    let _ = format!("{:?}", s);
}

#[test]
fn lowest_rtt_debug() {
    let s = LowestRtt::new();
    let _ = format!("{:?}", s);
}

#[test]
fn least_connections_debug() {
    let s = LeastConnections::new();
    let _ = format!("{:?}", s);
}

#[test]
fn hash_by_addr_debug() {
    let s = HashByAddr::new();
    let _ = format!("{:?}", s);
}

#[test]
fn weighted_round_robin_debug() {
    let s = WeightedRoundRobin::new();
    let _ = format!("{:?}", s);
}

#[test]
fn failover_debug() {
    let s = Failover::new();
    let _ = format!("{:?}", s);
}

#[test]
fn health_weighted_debug() {
    let s = HealthWeighted::new();
    let _ = format!("{:?}", s);
}

#[test]
fn sticky_debug() {
    let s = Sticky::new();
    let _ = format!("{:?}", s);
}

#[test]
fn round_robin_clone_independence() {
    let mut s1 = RoundRobin::new();
    let mut s2 = s1.clone();
    let metrics = make_metrics(&[Some(10), Some(20)], &[0, 0], &[0, 0]);
    let v = make_view(&metrics, "a:80");
    assert_eq!(s1.pick(&v), 0);
    assert_eq!(s2.pick(&v), 0);
    assert_eq!(s1.pick(&v), 1);
    assert_eq!(s2.pick(&v), 1);
}

#[test]
fn random_with_many_picks() {
    let mut s = Random::new();
    let metrics = make_metrics(&[Some(10); 10], &[0; 10], &[0; 10]);
    let v = make_view(&metrics, "a:80");
    let mut counts = [0; 10];
    for _ in 0..1000 {
        let idx = s.pick(&v);
        counts[idx] += 1;
    }
    for &c in &counts {
        assert!(c > 0);
    }
}

#[test]
fn lowest_rtt_with_all_same() {
    let mut s = LowestRtt::new();
    let metrics = make_metrics(&[Some(50); 5], &[0; 5], &[0; 5]);
    let v = make_view(&metrics, "a:80");
    // All same RTT, should pick first found (index 0)
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn lowest_rtt_with_zero_rtt() {
    let mut s = LowestRtt::new();
    let metrics = make_metrics(&[Some(0); 3], &[0; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    // Zero RTT should still work
    let _ = s.pick(&v);
}

#[test]
fn least_connections_with_zero_active() {
    let mut s = LeastConnections::new();
    let metrics = make_metrics(&[Some(10); 3], &[0, 0, 0], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn least_connections_with_different_active() {
    let mut s = LeastConnections::new();
    let metrics = make_metrics(&[Some(10); 3], &[5, 2, 8], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn least_connections_with_ties_and_rtt() {
    let mut s = LeastConnections::new();
    let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[5; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    // All have same active, tiebreak by RTT
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn hash_by_addr_consistent() {
    let mut s = HashByAddr::new();
    let metrics = make_metrics(&[Some(10); 5], &[0; 5], &[0; 5]);
    let v1 = make_view(&metrics, "example.com:80");
    let a1 = s.pick(&v1);
    let a2 = s.pick(&v1);
    assert_eq!(a1, a2);
}

#[test]
fn hash_by_addr_different_metrics() {
    let mut s = HashByAddr::new();
    let metrics = make_metrics(&[Some(10), Some(20)], &[0; 2], &[0; 2]);
    let v1 = make_view(&metrics, "a:80");
    let v2 = make_view(&metrics, "b:80");
    let _ = s.pick(&v1);
    let _ = s.pick(&v2);
}

#[test]
fn weighted_round_robin_single() {
    let mut s = WeightedRoundRobin::new();
    let metrics = make_metrics(&[Some(10)], &[0], &[0]);
    let v = make_view(&metrics, "a:80");
    assert_eq!(s.pick(&v), 0);
    assert_eq!(s.pick(&v), 0);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_advances_on_error() {
    let mut s = Failover::new();
    let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    let _ = s.pick(&v); // Initialize
    s.report_error(0);
    // After error on backend 0, should rotate to 1
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn failover_advances_multiple_times() {
    let mut s = Failover::new();
    let metrics = make_metrics(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    let _ = s.pick(&v);
    s.report_error(0);
    assert_eq!(s.pick(&v), 1);
    s.report_error(1);
    assert_eq!(s.pick(&v), 2);
    s.report_error(2);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn health_weighted_prefers_lower_errors() {
    let mut s = HealthWeighted::new();
    let metrics = make_metrics(
        &[Some(10), Some(10)],
        &[0, 0],
        &[0, 5],
    );
    let v = make_view(&metrics, "a:80");
    // Backend 0 has no errors, backend 1 has 5 errors
    // HealthWeighted should prefer backend 0
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn health_weighted_considers_load() {
    let mut s = HealthWeighted::new();
    let metrics = make_metrics(
        &[Some(10), Some(10)],
        &[0, 10],
        &[0, 0],
    );
    let v = make_view(&metrics, "a:80");
    // Backend 0 has no load, backend 1 has load
    // HealthWeighted should prefer backend 0
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn sticky_first_pick_then_remains() {
    let mut s = Sticky::new();
    let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    let first = s.pick(&v);
    for _ in 0..20 {
        assert_eq!(s.pick(&v), first);
    }
}

#[test]
fn sticky_picks_lowest_rtt_first() {
    let mut s = Sticky::new();
    let metrics = make_metrics(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let v = make_view(&metrics, "a:80");
    let first = s.pick(&v);
    // Lowest RTT is 10ms (index 1)
    assert_eq!(first, 1);
}

#[test]
fn random_with_empty_view() {
    // Random with empty view panics - this is by design
    // (uniform distribution over 0..0 is undefined)
    // So we don't test this case
}

#[test]
fn round_robin_with_zero_backends() {
    // RoundRobin panics with no backends - this is by design (debug_assert)
    // So we don't test this case
}

#[test]
fn free_function_names() {
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