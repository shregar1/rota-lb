use std::time::Duration;

/// Default health check interval.
pub const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Default health check timeout.
pub const DEFAULT_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default unhealthy threshold (consecutive failures before marking unhealthy).
pub const DEFAULT_UNHEALTHY_THRESHOLD: u32 = 3;

/// Default healthy threshold (consecutive successes before marking healthy).
pub const DEFAULT_HEALTHY_THRESHOLD: u32 = 2;
