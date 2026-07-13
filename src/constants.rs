//! Constants for the load balancer strategies.

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

/// Default dial timeout if not configured.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Default health check interval.
pub const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default health check timeout.
pub const DEFAULT_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default unhealthy threshold (consecutive failures before marking unhealthy).
pub const DEFAULT_UNHEALTHY_THRESHOLD: u32 = 3;

/// Default healthy threshold (consecutive successes before marking healthy).
pub const DEFAULT_HEALTHY_THRESHOLD: u32 = 2;

/// Maximum number of backends supported (arbitrary sanity limit).
pub const MAX_BACKENDS: usize = 10_000;

/// Minimum number of backends required.
pub const MIN_BACKENDS: usize = 1;

/// Default maximum retry delay.
pub const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Default retry multiplier (exponential backoff base).
pub const DEFAULT_RETRY_MULTIPLIER: f64 = 2.0;

/// Jitter factor: delay is multiplied by (0.5 + jitter * 0.5).
pub const JITTER_FACTOR: f64 = 0.5;

/// Default ALPN protocols (h2, http/1.1).
pub const DEFAULT_ALPN_PROTOCOLS: &[&[u8]] = &[b"h2", b"http/1.1"];

/// Default DNS SRV record prefix.
pub const DEFAULT_SRV_PREFIX: &str = "_http._tcp";

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
    u32::try_from((u64::from(MS_PER_SECOND) / ms).max(u64::from(MIN_WEIGHT)))
        .unwrap_or(MIN_WEIGHT)
}