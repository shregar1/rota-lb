use std::time::Duration;

/// Default weight for RTT-based strategies (1000 = 1 second in ms).
pub const MS_PER_SECOND: u32 = 1000;

/// Minimum weight for weighted strategies.
pub const MIN_WEIGHT: u32 = 1;

/// `HealthWeighted` error penalty in microseconds (500ms equivalent).
pub const ERROR_PENALTY_US: u64 = 500_000;

/// `HealthWeighted` load penalty per active connection in microseconds (10ms).
pub const LOAD_PENALTY_US: u64 = 10_000;

/// Default score when no RTT is available (1 second in microseconds).
pub const DEFAULT_RTT_US: u64 = 1_000_000;

/// Strategy names, indexed by `FfiStrategy` discriminant order.
pub const STRATEGY_NAMES: &[&str] = &[
    "round_robin",
    "random",
    "lowest_rtt",
    "least_connections",
    "hash_by_addr",
    "weighted_round_robin",
    "failover",
    "health_weighted",
    "sticky",
];

/// `WeightedRoundRobin` weight calculation: `max(1, 1000 / rtt_ms)`
pub fn calculate_weight(rtt: Duration) -> u32 {
    let ms = u64::try_from(rtt.as_millis()).unwrap_or(0).max(1);
    u32::try_from((u64::from(MS_PER_SECOND) / ms).max(u64::from(MIN_WEIGHT))).unwrap_or(MIN_WEIGHT)
}
