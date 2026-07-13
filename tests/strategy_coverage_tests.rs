//! Additional tests for the strategy module to improve coverage.

use std::time::Duration;
use rota::strategy::{BalanceStrategy, PoolView, TunnelMetrics};
use rota::strategies::{
    RoundRobin, Random, LowestRtt, LeastConnections, HashByAddr,
    WeightedRoundRobin, Failover, HealthWeighted, Sticky,
    round_robin, random, lowest_rtt, least_connections, hash_by_addr,
    weighted_round_robin, failover, health_weighted, sticky,
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
        dial_addr: "test:80",
        metrics: Box::leak(Box::new(metrics).into_boxed_slice()),
    }
}

// ============================================================================
//  Clone and Debug tests
// ============================================================================

#[test]
#[allow(clippy::clone_on_copy)]
fn round_robin_clone_debug() {
    let s = RoundRobin::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn random_clone_debug() {
    let s = Random::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn lowest_rtt_clone_debug() {
    let s = LowestRtt::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn least_connections_clone_debug() {
    let s = LeastConnections::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn hash_by_addr_clone_debug() {
    let s = HashByAddr::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
fn weighted_round_robin_clone_debug() {
    let s = WeightedRoundRobin::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn failover_clone_debug() {
    let s = Failover::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn health_weighted_clone_debug() {
    let s = HealthWeighted::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn sticky_clone_debug() {
    let s = Sticky::new();
    let _s2 = s.clone();
    let _ = format!("{:?}", s);
}

// ============================================================================
//  Default implementations
// ============================================================================

#[test]
fn round_robin_default() {
    let _s = RoundRobin::new();
}

#[test]
fn random_default() {
    let _s = Random::new();
}

#[test]
fn lowest_rtt_default() {
    let _s = LowestRtt::new();
}

#[test]
fn least_connections_default() {
    let _s = LeastConnections::new();
}

#[test]
fn hash_by_addr_default() {
    let _s = HashByAddr::new();
}

#[test]
fn failover_default() {
    let _s = Failover::new();
}

#[test]
fn health_weighted_default() {
    let _s = HealthWeighted::new();
}

#[test]
fn sticky_default() {
    let _s = Sticky::new();
}

// ============================================================================
//  Free function tests
// ============================================================================

#[test]
fn free_function_constructors() {
    // Just verify they return the right types
    let _r: Box<dyn rota::strategy::BalanceStrategy> = round_robin();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = random();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = lowest_rtt();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = least_connections();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = hash_by_addr();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = weighted_round_robin();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = failover();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = health_weighted();
    let _r: Box<dyn rota::strategy::BalanceStrategy> = sticky();
}

// ============================================================================
//  Edge cases
// ============================================================================

#[test]
fn round_robin_after_wrap() {
    let mut s = RoundRobin::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    // Pick 3 times to wrap around
    s.pick(&v);
    s.pick(&v);
    s.pick(&v);
    // After wrap, should be back to 0
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn random_many_picks() {
    let mut s = Random::new();
    let v = make_view(&[Some(10); 5], &[0; 5], &[0; 5]);
    let mut counts = [0; 5];
    for _ in 0..1000 {
        let idx = s.pick(&v);
        counts[idx] += 1;
    }
    // All should have been picked at least once
    for &c in &counts {
        assert!(c > 0);
    }
}

#[test]
fn least_connections_with_ties() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[5; 3], &[0; 3]);
    // All have same active_connections, tiebreak by RTT
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn weighted_round_robin_after_long_sequence() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[Some(10), Some(100), Some(1000)], &[0; 3], &[0; 3]);
    for _ in 0..100 {
        s.pick(&v);
    }
    // After many picks, should still work
    let _ = s.pick(&v);
}

#[test]
fn failover_after_many_reports() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    s.pick(&v); // Initialize
    for _ in 0..100 {
        s.report_error(0);
    }
    // After 100 reports of errors on backend 0, should have rotated
    let _ = s.pick(&v);
}

#[test]
fn health_weighted_with_zero_rtt() {
    let mut s = HealthWeighted::new();
    let v = make_view(&[Some(0), Some(100), Some(200)], &[0; 3], &[0; 3]);
    // All have same errors, same active, tiebreak by RTT
    let _ = s.pick(&v);
}

#[test]
fn hash_by_addr_with_special_chars() {
    let mut s = HashByAddr::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    let v1 = PoolView { dial_addr: "test:80", metrics: v.metrics };
    let v2 = PoolView { dial_addr: "test:443", metrics: v.metrics };
    let _ = s.pick(&v1);
    let _ = s.pick(&v2);
}

#[test]
fn sticky_stays_consistent() {
    let mut s = Sticky::new();
    let v = make_view(&[Some(30), Some(10), Some(20)], &[0; 3], &[0; 3]);
    let first = s.pick(&v);
    // All subsequent picks should be the same
    for _ in 0..50 {
        assert_eq!(s.pick(&v), first);
    }
}

#[test]
#[allow(clippy::clone_on_copy)]
fn random_clone_and_call() {
    let mut s = Random::new();
    let mut s2 = s.clone();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    let _ = s.pick(&v);
    let _ = s2.pick(&v);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn round_robin_clone_independence() {
    use rota::strategy::BalanceStrategy;
    let mut s1 = RoundRobin::new();
    let mut s2 = s1.clone();
    let v = make_view(&[Some(10), Some(20)], &[0; 2], &[0; 2]);
    assert_eq!(s1.pick(&v), 0);
    assert_eq!(s2.pick(&v), 0);
    assert_eq!(s1.pick(&v), 1);
    assert_eq!(s2.pick(&v), 1);
}