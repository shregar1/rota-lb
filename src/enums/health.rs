/// Health state of a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    /// Backend is healthy and receiving traffic.
    Healthy,
    /// Backend is unhealthy and should not receive traffic.
    Unhealthy,
    /// Initial state before first health check.
    Unknown,
}
