use std::time::Duration;

/// Default dial timeout if not configured.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of backends supported (arbitrary sanity limit).
pub const MAX_BACKENDS: usize = 10_000;

/// Minimum number of backends required.
pub const MIN_BACKENDS: usize = 1;

/// Default ALPN protocols (h2, http/1.1).
pub const DEFAULT_ALPN_PROTOCOLS: &[&[u8]] = &[b"h2", b"http/1.1"];

/// Default DNS SRV record prefix.
pub const DEFAULT_SRV_PREFIX: &str = "_http._tcp";
