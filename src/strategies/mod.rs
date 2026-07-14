//! Built-in balance strategies.
//!
//! Each strategy is a small, testable type that implements
//! [`BalanceStrategy`](crate::BalanceStrategy). Use the free constructors at
//! the crate root ([`round_robin`](crate::round_robin), [`random`](crate::random), etc.)
//! for the boxed-dyn convenience, or instantiate the concrete type when you
//! need to keep it.

#![allow(missing_docs)]

pub mod failover;
pub mod hash_by_addr;
pub mod health_weighted;
pub mod least_connections;
pub mod lowest_rtt;
pub mod random;
pub mod round_robin;
pub mod sticky;
pub mod weighted_round_robin;

pub use failover::Failover;
pub use hash_by_addr::HashByAddr;
pub use health_weighted::HealthWeighted;
pub use least_connections::LeastConnections;
pub use lowest_rtt::LowestRtt;
pub use random::Random;
pub use round_robin::RoundRobin;
pub use sticky::Sticky;
pub use weighted_round_robin::WeightedRoundRobin;

use crate::traits::strategy::BalanceStrategy;

// ============================================================================
//  Free constructors — return Box<dyn BalanceStrategy> for convenience.
// ============================================================================

/// Round-robin strategy. Even distribution, no metrics required.
pub fn round_robin() -> Box<dyn BalanceStrategy> {
    Box::new(RoundRobin::new())
}

/// Random strategy. Stateless fallback when no metrics available.
pub fn random() -> Box<dyn BalanceStrategy> {
    Box::new(Random::new())
}

/// Lowest-RTT strategy. Picks the tunnel with the lowest measured RTT.
pub fn lowest_rtt() -> Box<dyn BalanceStrategy> {
    Box::new(LowestRtt::new())
}

/// Least-connections strategy. Picks the tunnel with the fewest active connections.
pub fn least_connections() -> Box<dyn BalanceStrategy> {
    Box::new(LeastConnections::new())
}

/// Hash-by-address strategy. Same hostname always routes to the same tunnel.
pub fn hash_by_addr() -> Box<dyn BalanceStrategy> {
    Box::new(HashByAddr::new())
}

/// Weighted round-robin. Weights by inverse RTT.
pub fn weighted_round_robin() -> Box<dyn BalanceStrategy> {
    Box::new(WeightedRoundRobin::new())
}

/// Failover strategy. Uses primary tunnel until it fails, then rotates.
pub fn failover() -> Box<dyn BalanceStrategy> {
    Box::new(Failover::new())
}

/// Health-weighted strategy. Scores by RTT + error penalty + load penalty.
pub fn health_weighted() -> Box<dyn BalanceStrategy> {
    Box::new(HealthWeighted::new())
}

/// Sticky strategy. Pins to the first-chosen tunnel for the balancer's lifetime.
pub fn sticky() -> Box<dyn BalanceStrategy> {
    Box::new(Sticky::new())
}

#[cfg(test)]
mod constructor_tests {
    use super::*;
    use crate::traits::strategy::{PoolView, TunnelMetrics};
    use std::time::Duration;

    fn make_metrics() -> Vec<TunnelMetrics> {
        vec![
            TunnelMetrics {
                rtt: Some(Duration::from_millis(10)),
                ..Default::default()
            },
            TunnelMetrics {
                rtt: Some(Duration::from_millis(20)),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn free_constructors_return_boxed_strategies() {
        let metrics = make_metrics();
        let v = PoolView {
            dial_addr: "h",
            metrics: &metrics,
        };

        let mut strategies: Vec<Box<dyn BalanceStrategy>> = vec![
            round_robin(),
            random(),
            lowest_rtt(),
            least_connections(),
            hash_by_addr(),
            weighted_round_robin(),
            failover(),
            health_weighted(),
            sticky(),
        ];
        for s in &mut strategies {
            let _ = s.name();
            assert!(s.pick(&v) < 2);
        }
        assert_eq!(strategies.len(), 9);
    }
}
