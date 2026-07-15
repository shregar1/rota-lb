//! Health checking for backends.

use crate::traits::backend::Backend;
use crate::constants::{
    DEFAULT_HEALTHY_THRESHOLD, DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_TIMEOUT,
    DEFAULT_UNHEALTHY_THRESHOLD,
};
use crate::traits::strategy::TunnelMetrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

pub use crate::enums::health::HealthState;

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
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            unhealthy_threshold: DEFAULT_UNHEALTHY_THRESHOLD,
            healthy_threshold: DEFAULT_HEALTHY_THRESHOLD,
            check_addr: String::new(),
        }
    }
}

/// Handle for controlling health checks.
#[derive(Debug)]
pub struct HealthChecker {
    shutdown_tx: mpsc::Sender<()>,
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
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let check_addr = config.check_addr.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_loop(backends, metrics, config, check_addr, shutdown_rx).await;
        });

        Self {
            shutdown_tx,
            task_handle,
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn run_loop(
        backends: Vec<Box<dyn Backend>>,
        metrics: Arc<Mutex<Vec<TunnelMetrics>>>,
        config: HealthCheckConfig,
        check_addr: String,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut consecutive_failures = vec![0u32; backends.len()];
        let mut consecutive_successes = vec![0u32; backends.len()];

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let futs: Vec<_> = backends.iter().enumerate().map(|(i, backend)| {
                        let addr = check_addr.clone();
                        async move {
                            let result = tokio::time::timeout(config.timeout, backend.dial(&addr)).await;
                            (i, result)
                        }
                    }).collect();
                    let results: Vec<_> = futures::future::join_all(futs).await;

                    let mut guard = metrics.lock().await;
                    for (i, result) in results {
                        let Some(m) = guard.get_mut(i) else { continue };
                        let dial_ok = result.as_ref().is_ok_and(Result::is_ok);
                        if dial_ok {
                            consecutive_successes[i] += 1;
                            consecutive_failures[i] = 0;
                            if consecutive_successes[i] >= config.healthy_threshold {
                                m.recent_errors = 0;
                            }
                        } else {
                            consecutive_failures[i] += 1;
                            consecutive_successes[i] = 0;
                            m.recent_errors = consecutive_failures[i];
                            m.total_errors = m.total_errors.saturating_add(1);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    }

    /// Shut down the health checker.
    ///
    /// If `shutdown` is not called explicitly, dropping the
    /// `HealthChecker` will:
    /// - drop the mpsc sender, causing the run loop's `shutdown_rx.recv()`
    ///   to return `None` and break the loop, and
    /// - drop the `JoinHandle`, cancelling the spawned task if still running.
    ///
    /// Explicit `shutdown` is preferred when you want to await clean
    /// termination (e.g. flushing logs).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(()).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task_handle).await;
    }
}

/// Passive health monitoring - updates metrics based on dial results.
///
/// This is automatically done by the `LoadBalancer`'s `dial` method.
/// Strategies like `HealthWeighted` and `Failover` use these metrics.
pub fn record_dial_result(metrics: &mut [TunnelMetrics], index: usize, success: bool) {
    let Some(m) = metrics.get_mut(index) else {
        return;
    };
    if success {
        m.recent_errors = 0;
    } else {
        m.recent_errors = m.recent_errors.saturating_add(1);
        m.total_errors = m.total_errors.saturating_add(1);
    }
}

/// Determine if a backend should be considered healthy for routing.
pub const fn is_healthy(metrics: &TunnelMetrics, unhealthy_threshold: u32) -> bool {
    metrics.recent_errors < unhealthy_threshold
}
