use std::time::Duration;

/// Default maximum retry delay.
pub const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Default retry multiplier (exponential backoff base).
pub const DEFAULT_RETRY_MULTIPLIER: f64 = 2.0;

/// Jitter factor: delay is multiplied by (0.5 + jitter * 0.5).
pub const JITTER_FACTOR: f64 = 0.5;
