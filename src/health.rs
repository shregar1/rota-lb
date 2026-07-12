//! Health checking for backends.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::backend::Backend;
use crate::constants::*;
use crate::strategy::TunnelMetrics;

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

/// Configuration for active health checks.
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Interval between health checks.
    pub interval: Duration,
    /// Timeout for each health check.
    pub timeout: Duration,
    /// Number of consecutive failures before marking unhealthy.
    pub unhealthy_threshold: u32,
    /// Number of consecutive successes before marking healthy.
    pub healthy_threshold: u32,
    /// Address to dial for health checks (can be different from traffic address).
    pub check_addr: String,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
            timeout: Duration::from_secs(5),
            unhealthy_threshold: DEFAULT_UNHEALTHY_THRESHOLD,
            healthy_threshold: DEFAULT_HEALTHY_THRESHOLD,
            check_addr: "localhost:80".to_string(),
        }
    }
}

/// Handle for controlling health checks.
#[derive(Debug)]
pub struct HealthChecker {
    /// Shutdown signal for the health check task.
    shutdown_tx: mpsc::Sender<()>,
    /// Task handle for joining.
    task_handle: tokio::task::JoinHandle<()>,
}

impl HealthChecker {
    /// Spawn a background health checker for the given backends.
    ///
    /// Returns a `HealthChecker` handle that can be used to shut down the checker.
    pub fn spawn(
        backends: Vec<Box<dyn Backend>>,
        metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
        config: HealthCheckConfig,
    ) -> Self {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        
        let task_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.interval);
            let mut consecutive_failures = vec![0u32; backends.len()];
            let mut consecutive_successes = vec![0u32; backends.len()];
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        for (i, backend) in backends.iter().enumerate() {
                            let result = tokio::time::timeout(
                                config.timeout,
                                backend.dial(&config.check_addr)
                            ).await;
                            
                            let mut metrics_guard = metrics.lock().unwrap();
                            let is_healthy = result.is_ok();
                            
                            if is_healthy {
                                consecutive_successes[i] += 1;
                                consecutive_failures[i] = 0;
                                if consecutive_successes[i] >= config.healthy_threshold {
                                    metrics_guard[i].recent_errors = 0;
                                }
                            } else {
                                consecutive_failures[i] += 1;
                                consecutive_successes[i] = 0;
                                metrics_guard[i].recent_errors = consecutive_failures[i];
                                metrics_guard[i].total_errors += 1;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        Self { shutdown_tx, task_handle }
    }

    /// Shut down the health checker.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(()).await;
        let _ = self.task_handle.await;
    }
}

/// Passive health monitoring - updates metrics based on dial results.
///
/// This is automatically done by the LoadBalancer's `dial` method.
/// Strategies like `HealthWeighted` and `Failover` use these metrics.
pub fn record_dial_result(
    metrics: &mut [TunnelMetrics],
    index: usize,
    success: bool,
) {
    if success {
        metrics[index].recent_errors = 0;
    } else {
        metrics[index].recent_errors = metrics[index].recent_errors.saturating_add(1);
        metrics[index].total_errors = metrics[index].total_errors.saturating_add(1);
    }
}

/// Determine if a backend should be considered healthy for routing.
pub fn is_healthy(metrics: &TunnelMetrics, unhealthy_threshold: u32) -> bool {
    metrics.recent_errors < unhealthy_threshold
}