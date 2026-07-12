//! Constants for the load balancer strategies.

use std::time::Duration;

/// Default weight for RTT-based strategies (1000 = 1 second in ms).
pub const MS_PER_SECOND: u32 = 1000;

/// Minimum weight for weighted strategies.
pub const MIN_WEIGHT: u32 = 1;

/// HealthWeighted error penalty in microseconds (500ms equivalent).
pub const ERROR_PENALTY_US: u64 = 500_000;

/// HealthWeighted load penalty per active connection in microseconds (10ms).
pub const LOAD_PENALTY_US: u64 = 10_000;

/// WeightedRoundRobin weight calculation: max(1, 1000 / rtt_ms)
pub fn calculate_weight(rtt: Duration) -> u32 {
    let ms = rtt.as_millis().max(1) as u32;
    (MS_PER_SECOND / ms).max(MIN_WEIGHT)
}

/// Default score when no RTT is available (1 second in microseconds).
pub const DEFAULT_RTT_US: u64 = 1_000_000;

/// Default dial timeout if not configured.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Default health check interval.
pub const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default unhealthy threshold (consecutive failures before marking unhealthy).
pub const DEFAULT_UNHEALTHY_THRESHOLD: u32 = 3;

/// Default healthy threshold (consecutive successes before marking healthy).
pub const DEFAULT_HEALTHY_THRESHOLD: u32 = 2;

/// Maximum number of backends supported (arbitrary sanity limit).
pub const MAX_BACKENDS: usize = 10_000;

/// Minimum number of backends required.
pub const MIN_BACKENDS: usize = 1;