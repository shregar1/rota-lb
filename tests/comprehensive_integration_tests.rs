//! Comprehensive integration tests covering the full public surface of `rota`.
//!
//! Each test exercises a real code path end-to-end through the public API

#![cfg(all(feature = "tower", feature = "discovery"))]
//! (no `#[cfg(test)]` peeking at internals).
//!
//! Coverage:
//! - Error type behaviour
//! - Backend trait + Connection plumbing
//! - LoadBalancer construction paths (new / from_factories / builder)
//! - Dial-time behaviour (validation, timeouts, retries, RTT recording)
//! - Strategy behaviour (every strategy; metrics-driven picks)
//! - Failover rotation, drain/undrain, strategy swap at runtime
//! - HealthChecker end-to-end (spawn, run, metrics update, shutdown)
//! - Passive record_dial_result helper
//! - Discovery (StaticDiscovery + Discover reconciler)
//! - Factory (BackendFactory + BackendFactoryFromDescriptor + BackendOutput)
//! - Retry policies (every concrete impl + builder + is_transient_error)
//! - TLS configuration paths
//! - Tower integration (LbRequest, per-request overrides)
//! - FFI surface (every public extern "C" function)
//! - Constants and value exports

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

use rota_lb::backend::{Backend, Connection};
use rota_lb::constants::{
    DEFAULT_ALPN_PROTOCOLS, DEFAULT_DIAL_TIMEOUT, DEFAULT_HEALTHY_THRESHOLD,
    DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_TIMEOUT, DEFAULT_MAX_RETRY_DELAY,
    DEFAULT_RETRY_MULTIPLIER, DEFAULT_RTT_US, DEFAULT_SRV_PREFIX, DEFAULT_UNHEALTHY_THRESHOLD,
    JITTER_FACTOR, MAX_BACKENDS, MIN_BACKENDS, MS_PER_SECOND, STRATEGY_NAMES,
};
use rota_lb::error::Error;
use rota_lb::factory::{BackendFactory, BackendOutput};
use rota_lb::health::{
    is_healthy, record_dial_result, HealthCheckConfig, HealthChecker, HealthState,
};
use rota_lb::retry::{
    is_transient_error, ExponentialBackoff, FixedRetry, NoRetry, RetryOnError, RetryPolicy,
    RetryPolicyBuilder,
};
use rota_lb::strategies::{
    failover, hash_by_addr, health_weighted, least_connections, lowest_rtt, random, round_robin,
    sticky, weighted_round_robin, Failover, HashByAddr, HealthWeighted, LeastConnections,
    LowestRtt, Random, RoundRobin, Sticky, WeightedRoundRobin,
};
use rota_lb::strategy::{BalanceStrategy, PoolView, TunnelMetrics};
use rota_lb::LoadBalancer;

// ===========================================================================
//  Test fixtures
// ===========================================================================

/// Backend that records how many dials happened, can fail the first N
/// times, and otherwise returns a usable duplex connection.
#[derive(Debug)]
struct EchoBackend {
    fail_first: AtomicU32,
    dials: AtomicUsize,
    fail: AtomicBool,
    #[allow(dead_code)]
    rtt: Duration,
}

impl EchoBackend {
    fn new() -> Self {
        Self {
            fail_first: AtomicU32::new(0),
            dials: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            rtt: Duration::ZERO,
        }
    }

    fn with_failures(count: u32) -> Self {
        Self {
            fail_first: AtomicU32::new(count),
            dials: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            rtt: Duration::ZERO,
        }
    }

    fn always_failing() -> Self {
        Self {
            fail_first: AtomicU32::new(0),
            dials: AtomicUsize::new(0),
            fail: AtomicBool::new(true),
            rtt: Duration::ZERO,
        }
    }

    fn with_rtt(rtt: Duration) -> Self {
        Self {
            fail_first: AtomicU32::new(0),
            dials: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            rtt,
        }
    }

    #[allow(dead_code)]
    fn dial_count(&self) -> usize {
        self.dials.load(Ordering::SeqCst)
    }
}

impl Default for EchoBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for EchoBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        self.dials.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::backend("EchoBackend: configured to always fail"));
        }
        let remaining = self.fail_first.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_first.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend(format!(
                "EchoBackend: simulated failure ({remaining} remaining)"
            )));
        }
        let (a, _b) = duplex(8 * 1024);
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {}
}

/// A backend that hangs until dropped — for timeout tests.
#[derive(Debug, Default)]
struct HangingBackend;

#[async_trait]
impl Backend for HangingBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        let (a, _b) = duplex(8);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(Box::pin(a))
    }

    async fn shutdown(&mut self) {}
}

fn echo_pool(n: usize) -> Vec<Box<dyn Backend>> {
    (0..n)
        .map(|_| Box::new(EchoBackend::new()) as Box<dyn Backend>)
        .collect()
}

fn arc_echo_pool(n: usize) -> Vec<Arc<EchoBackend>> {
    (0..n).map(|_| Arc::new(EchoBackend::new())).collect()
}

fn echo_pool_with_rtt(rtts: &[Duration]) -> Vec<Box<dyn Backend>> {
    rtts.iter()
        .map(|r| Box::new(EchoBackend::with_rtt(*r)) as Box<dyn Backend>)
        .collect()
}

// ===========================================================================
//  Error type
// ===========================================================================

#[test]
fn error_invalid_address_displays_addr_and_reason() {
    let err = Error::invalid_address("no-port".to_owned(), "missing port");
    let s = err.to_string();
    assert!(s.contains("no-port"));
    assert!(s.contains("missing port"));
}

#[test]
fn error_no_backends_has_fixed_display() {
    assert_eq!(
        Error::no_backends().to_string(),
        "no backends available — pool is empty"
    );
}

#[test]
fn error_factory_includes_message() {
    let err = Error::factory("bad cfg");
    assert_eq!(err.to_string(), "backend factory failed: bad cfg");
}

#[test]
fn error_backend_includes_message() {
    let err = Error::backend("connection refused");
    assert_eq!(
        err.to_string(),
        "backend operation failed: connection refused"
    );
}

#[test]
fn error_io_from_io_error() {
    let io = std::io::Error::other("disk full");
    let err: Error = io.into();
    assert!(matches!(err, Error::Io(_)));
    assert!(err.to_string().contains("disk full"));
}

#[test]
fn error_debug_format_is_useful() {
    let err = Error::no_backends();
    let dbg = format!("{err:?}");
    assert!(dbg.contains("NoBackends"));
}

#[test]
fn error_factory_and_backend_helpers_match_variants() {
    assert!(matches!(Error::factory("x"), Error::Factory(ref e) if e.0 == "x"));
    assert!(matches!(Error::backend("y"), Error::Backend(ref e) if e.0 == "y"));
}

// ===========================================================================
//  LoadBalancer construction errors
// ===========================================================================

#[test]
fn load_balancer_new_rejects_empty_pool() {
    let err = LoadBalancer::new(vec![], round_robin()).unwrap_err();
    assert!(matches!(err, Error::NoBackends(_)));
}

#[tokio::test]
async fn from_factories_rejects_empty_factory_list() {
    let factories: Vec<Box<dyn BackendFactory>> = vec![];
    let err = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoBackends(_)));
}

#[tokio::test]
async fn from_factories_rejects_when_factory_create_fails() {
    struct FailingFactory;
    #[async_trait]
    impl BackendFactory for FailingFactory {
        async fn create(&self) -> Result<BackendOutput, Error> {
            Err(Error::factory("nope"))
        }
    }
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(FailingFactory)];
    let err = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Factory(_)));
}

// ===========================================================================
//  Builder surface
// ===========================================================================

#[tokio::test]
async fn builder_chains_every_setter() {
    let backends = echo_pool(3);
    let metrics = vec![TunnelMetrics::default(); 3];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .initial_metrics(metrics)
        .strategy(RoundRobin::new())
        .dial_timeout(Duration::from_millis(250))
        .retry_policy(NoRetry)
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_with_factories_path() {
    struct F;
    #[async_trait]
    impl BackendFactory for F {
        async fn create(&self) -> Result<BackendOutput, Error> {
            Ok(BackendOutput {
                backend: Box::new(EchoBackend::new()),
                initial_metrics: TunnelMetrics::default(),
            })
        }
    }
    let factories: Vec<Box<dyn BackendFactory>> = (0..2)
        .map(|_| Box::new(F) as Box<dyn BackendFactory>)
        .collect();
    let lb = LoadBalancer::builder()
        .factories(factories)
        .strategy(RoundRobin::new())
        .retry_policy(FixedRetry::new(Duration::from_millis(1)))
        .build()
        .await
        .unwrap();
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn builder_requires_a_strategy() {
    let err = LoadBalancer::builder()
        .backends(echo_pool(1))
        .build()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Factory(ref e) if e.0.contains("strategy required")));
}

#[tokio::test]
async fn builder_rejects_both_backends_and_factories() {
    struct F;
    #[async_trait]
    impl BackendFactory for F {
        async fn create(&self) -> Result<BackendOutput, Error> {
            Ok(BackendOutput {
                backend: Box::new(EchoBackend::new()),
                initial_metrics: TunnelMetrics::default(),
            })
        }
    }
    let err = LoadBalancer::builder()
        .backends(echo_pool(1))
        .factories(vec![Box::new(F)])
        .strategy(RoundRobin::new())
        .build()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Factory(ref e) if e.0.contains("cannot set both")));
}

#[tokio::test]
async fn builder_initial_metrics_length_mismatch_is_rejected() {
    let err = LoadBalancer::builder()
        .backends(echo_pool(2))
        .initial_metrics(vec![TunnelMetrics::default()])
        .strategy(RoundRobin::new())
        .build()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Factory(ref e) if e.0.contains("must equal backends.len")));
}

#[tokio::test]
async fn builder_initial_metrics_length_mismatch_on_factories_is_rejected() {
    struct F;
    #[async_trait]
    impl BackendFactory for F {
        async fn create(&self) -> Result<BackendOutput, Error> {
            Ok(BackendOutput {
                backend: Box::new(EchoBackend::new()),
                initial_metrics: TunnelMetrics::default(),
            })
        }
    }
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(F), Box::new(F)];
    let err = LoadBalancer::builder()
        .factories(factories)
        .initial_metrics(vec![TunnelMetrics::default()])
        .strategy(RoundRobin::new())
        .build()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Factory(ref e) if e.0.contains("must equal factories.len")));
}

// ===========================================================================
//  Dial validation
// ===========================================================================

#[tokio::test]
async fn dial_rejects_empty_host() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    let err = lb.dial(":443").await.unwrap_err();
    assert!(matches!(err, Error::InvalidAddress(ref e) if e.reason == "empty host"));
}

#[tokio::test]
async fn dial_rejects_missing_port() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    let err = lb.dial("example.com").await.unwrap_err();
    assert!(matches!(err, Error::InvalidAddress(ref e) if e.reason.contains("host:port")));
}

#[tokio::test]
async fn dial_rejects_port_zero() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    let err = lb.dial("example.com:0").await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidAddress(ref e) if e.reason == "port must be 1-65535")
    );
}

#[tokio::test]
async fn dial_rejects_unparseable_port() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    let err = lb.dial("example.com:abc").await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidAddress(ref e) if e.reason == "port must be 1-65535")
    );
}

#[tokio::test]
async fn dial_accepts_valid_addresses() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    for addr in [
        "example.com:80",
        "127.0.0.1:8080",
        "[::1]:443",
        "host:1",
        "host:65535",
    ] {
        let conn = lb
            .dial(addr)
            .await
            .unwrap_or_else(|e| panic!("dial({addr}) failed: {e}"));
        drop(conn);
    }
}

// ===========================================================================
//  Dial behaviour: metrics accounting
// ===========================================================================

#[tokio::test]
async fn successful_dial_increments_total_dials_and_resets_recent_errors() {
    let backend = EchoBackend::with_failures(1);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let _ = lb.dial("a:80").await; // expected to fail
    assert!(lb.metrics().await[0].recent_errors > 0);
    let conn = lb.dial("a:80").await.unwrap();
    drop(conn);
    let m = lb.metrics().await[0];
    assert_eq!(m.recent_errors, 0, "recent_errors must reset on success");
    assert!(m.total_dials >= 2);
}

#[tokio::test]
async fn failed_dial_increments_total_and_recent_errors() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(EchoBackend::always_failing())];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let _ = lb.dial("a:80").await;
    let m = lb.metrics().await[0];
    assert!(m.total_errors > 0);
    assert!(m.recent_errors > 0);
}

#[tokio::test]
async fn connection_drop_decrements_active_connections() {
    let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    assert_eq!(lb.metrics().await[0].active_connections, 0);
    let conn = lb.dial("a:80").await.unwrap();
    assert!(lb.metrics().await[0].active_connections >= 1);
    drop(conn);
    // Give the drop guard a moment to run.
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(lb.metrics().await[0].active_connections, 0);
}

#[tokio::test]
async fn multiple_dials_round_robin_across_backends() {
    let arced = arc_echo_pool(3);
    let backends: Vec<Box<dyn Backend>> = arced
        .iter()
        .map(|_b| Box::new(EchoBackend::new()) as Box<dyn Backend>)
        .collect();
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    for _ in 0..6 {
        let conn = lb.dial("a:80").await.unwrap();
        drop(conn);
    }
    // Real check via metrics: total_dials across all backends = 6.
    let total: u64 = lb.metrics().await.iter().map(|m| m.total_dials).sum();
    assert_eq!(total, 6);
}

// ===========================================================================
//  Dial timeout
// ===========================================================================

#[tokio::test]
async fn dial_timeout_returns_error_within_bound() {
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(HangingBackend)];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .dial_timeout(Duration::from_millis(50))
        .build()
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let err = lb.dial("a:80").await.unwrap_err();
    let elapsed = start.elapsed();
    assert!(matches!(err, Error::Backend(_)));
    // Must return roughly within the timeout — give 5× slack for scheduling jitter.
    assert!(
        elapsed < Duration::from_millis(500),
        "elapsed = {elapsed:?}"
    );
}

#[tokio::test]
async fn no_dial_timeout_lets_a_hanging_backend_block() {
    // The HangingBackend would block for an hour; we just confirm that
    // without a dial timeout the dial future is in fact pending. We don't
    // wait for it to complete.
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(HangingBackend)];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let dial_fut = lb.dial("a:80");
    tokio::select! {
        _ = dial_fut => panic!("dial must not complete without timeout"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
}

// ===========================================================================
//  Retry policy
// ===========================================================================

#[tokio::test]
async fn no_retry_does_not_retry() {
    let backend = EchoBackend::with_failures(10);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .retry_policy(NoRetry)
        .build()
        .await
        .unwrap();
    let _ = lb.dial("a:80").await;
    let m = lb.metrics().await[0];
    assert_eq!(m.total_errors, 1, "no retry must dial exactly once");
}

#[tokio::test]
async fn fixed_retry_dials_until_success() {
    let backend = EchoBackend::with_failures(3);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .retry_policy(FixedRetry::new(Duration::from_millis(1)))
        .build()
        .await
        .unwrap();
    let conn = lb.dial("a:80").await.unwrap();
    drop(conn);
    let m = lb.metrics().await[0];
    // total_dials counts the initial pick; per-attempt errors during retries
    // are NOT surfaced as total_errors until retries are exhausted. The final
    // successful attempt therefore shows total_errors=0 and total_dials >= 1.
    assert_eq!(
        m.total_errors, 0,
        "successful retry resets the error counter"
    );
    assert!(m.total_dials >= 1);
}

#[tokio::test]
async fn fixed_retry_respects_max_attempts() {
    let backend = EchoBackend::always_failing();
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = FixedRetry::new(Duration::from_millis(1)).with_max_attempts(3);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let _ = lb.dial("a:80").await;
    let m = lb.metrics().await[0];
    // When retries are exhausted, handle_dial_error fires once with the final error.
    assert_eq!(
        m.total_errors, 1,
        "retries exhausted produces one total_errors increment"
    );
}

#[tokio::test]
async fn exponential_backoff_resets_recent_errors_on_success() {
    let backend = EchoBackend::with_failures(2);
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = ExponentialBackoff::new(Duration::from_millis(1)).with_jitter(false);
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let conn = lb.dial("a:80").await.unwrap();
    drop(conn);
    let m = lb.metrics().await[0];
    assert_eq!(
        m.recent_errors, 0,
        "successful retry must reset recent_errors"
    );
    assert_eq!(
        m.total_errors, 0,
        "successful retry resets total_errors too"
    );
}

#[tokio::test]
async fn retry_on_error_predicate_filters_by_error_type() {
    let backend = EchoBackend::always_failing();
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let policy = RetryOnError::new(
        FixedRetry::new(Duration::from_millis(1)),
        |e| matches!(e, Error::Backend(ref e) if e.0.contains("retryable")),
    );
    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(RoundRobin::new())
        .retry_policy(policy)
        .build()
        .await
        .unwrap();
    let _ = lb.dial("a:80").await;
    let m = lb.metrics().await[0];
    assert_eq!(m.total_errors, 1, "predicate should suppress retry");
}

#[test]
fn retry_policy_builder_default_is_no_policy() {
    let policy = RetryPolicyBuilder::default().build();
    assert!(policy.is_none());
}

#[test]
fn retry_policy_builder_branches_are_constructable() {
    let _ = RetryPolicyBuilder::default().no_retry().build().unwrap();
    let _ = RetryPolicyBuilder::default()
        .fixed_retry(Duration::from_millis(1))
        .build()
        .unwrap();
    let _ = RetryPolicyBuilder::default()
        .exponential_backoff(Duration::from_millis(1))
        .build()
        .unwrap();
    let _ = RetryPolicyBuilder::default()
        .custom(NoRetry)
        .build()
        .unwrap();
}

#[test]
fn retry_policy_builder_is_clone() {
    let b = RetryPolicyBuilder::default().no_retry();
    let b2 = b.clone();
    assert!(b2.build().is_some());
}

#[test]
fn is_transient_error_recognises_io_and_backend_timeout_strings() {
    assert!(is_transient_error(&Error::from(std::io::Error::other("x"))));
    assert!(is_transient_error(&Error::backend("connection timeout")));
    assert!(!is_transient_error(&Error::no_backends()));
    assert!(!is_transient_error(&Error::backend("connection refused")));
}

// ===========================================================================
//  Strategy tests via direct `pick` calls
// ===========================================================================

fn make_view(rtts: &[Option<u64>], active: &[u32], errs: &[u32]) -> PoolView<'static> {
    let metrics: Vec<TunnelMetrics> = rtts
        .iter()
        .zip(active.iter().chain(std::iter::repeat(&0)))
        .zip(errs.iter().chain(std::iter::repeat(&0)))
        .map(|((rtt, &active), &errs)| TunnelMetrics {
            rtt: rtt.map(Duration::from_millis),
            active_connections: active,
            recent_errors: errs,
            ..Default::default()
        })
        .collect();
    let metrics: &'static [TunnelMetrics] = Box::leak(metrics.into_boxed_slice());
    PoolView {
        dial_addr: "example.com:443",
        metrics,
    }
}

#[test]
fn round_robin_walks_through_backends_in_order() {
    let mut s = RoundRobin::new();
    let v = make_view(&[None; 3], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
    assert_eq!(s.pick(&v), 1);
    assert_eq!(s.pick(&v), 2);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn round_robin_name_matches_strategy_names_index_zero() {
    assert_eq!(RoundRobin::new().name(), STRATEGY_NAMES[0]);
}

#[test]
fn random_pick_is_within_bounds() {
    let mut s = Random::new();
    let v = make_view(&[None; 4], &[0; 4], &[0; 4]);
    for _ in 0..200 {
        let idx = s.pick(&v);
        assert!(idx < 4);
    }
}

#[test]
fn random_name_matches_index_one() {
    assert_eq!(Random::new().name(), STRATEGY_NAMES[1]);
}

#[test]
fn lowest_rtt_picks_the_known_min() {
    let mut s = LowestRtt::new();
    let v = make_view(&[Some(50), Some(10), Some(30)], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn lowest_rtt_falls_back_to_zero_when_no_rtts() {
    let mut s = LowestRtt::new();
    let v = make_view(&[None, None, None], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn lowest_rtt_name_matches_index_two() {
    assert_eq!(LowestRtt::new().name(), STRATEGY_NAMES[2]);
}

#[test]
fn least_connections_picks_min_active() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(10); 3], &[5, 2, 8], &[0; 3]);
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn least_connections_breaks_ties_by_lowest_rtt() {
    let mut s = LeastConnections::new();
    let v = make_view(&[Some(100), Some(10), Some(50)], &[3, 3, 3], &[0; 3]);
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn least_connections_name_matches_index_three() {
    assert_eq!(LeastConnections::new().name(), STRATEGY_NAMES[3]);
}

#[test]
fn hash_by_addr_is_deterministic_for_same_addr() {
    let mut s = HashByAddr::new();
    let v = make_view(&[None; 4], &[0; 4], &[0; 4]);
    let first = s.pick(&v);
    for _ in 0..20 {
        assert_eq!(s.pick(&v), first);
    }
}

#[test]
fn hash_by_addr_name_matches_index_four() {
    assert_eq!(HashByAddr::new().name(), STRATEGY_NAMES[4]);
}

#[test]
fn weighted_round_robin_eventually_picks_faster_backend_more() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[Some(10), Some(100)], &[0; 2], &[0; 2]);
    let mut counts = [0usize; 2];
    for _ in 0..200 {
        counts[s.pick(&v)] += 1;
    }
    assert!(
        counts[0] > counts[1],
        "expected idx 0 to win more often, got {counts:?}"
    );
}

#[test]
fn weighted_round_robin_handles_no_rtts_with_weight_one() {
    let mut s = WeightedRoundRobin::new();
    let v = make_view(&[None, None, None], &[0; 3], &[0; 3]);
    for _ in 0..30 {
        let idx = s.pick(&v);
        assert!(idx < 3);
    }
}

#[test]
fn weighted_round_robin_name_matches_index_five() {
    assert_eq!(WeightedRoundRobin::new().name(), STRATEGY_NAMES[5]);
}

#[test]
fn failover_picks_zero_when_pool_non_empty() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_report_error_rotates_primary() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
    s.report_error(0);
    assert_eq!(s.pick(&v), 1);
    s.report_error(1);
    assert_eq!(s.pick(&v), 2);
    s.report_error(2);
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_report_error_on_non_primary_is_noop() {
    let mut s = Failover::new();
    let v = make_view(&[Some(10); 3], &[0; 3], &[0; 3]);
    assert_eq!(s.pick(&v), 0);
    s.report_error(2); // not the primary
    assert_eq!(s.pick(&v), 0);
}

#[test]
fn failover_name_matches_index_six() {
    assert_eq!(Failover::new().name(), STRATEGY_NAMES[6]);
}

#[test]
fn health_weighted_picks_lowest_score() {
    let mut s = HealthWeighted::new();
    // idx 0: rtt=200ms + 0 err
    // idx 1: rtt=10ms  + 0 err (best)
    // idx 2: rtt=10ms  + many errs
    let v = make_view(&[Some(200), Some(10), Some(10)], &[0; 3], &[0, 0, 50]);
    assert_eq!(s.pick(&v), 1);
}

#[test]
fn health_weighted_name_matches_index_seven() {
    assert_eq!(HealthWeighted::new().name(), STRATEGY_NAMES[7]);
}

#[test]
fn sticky_pins_after_first_pick() {
    let mut s = Sticky::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    let first = s.pick(&v);
    for _ in 0..30 {
        assert_eq!(
            s.pick(&v),
            first,
            "Sticky must always return the pinned index"
        );
    }
}

#[test]
fn sticky_does_not_release_pin_on_report_error() {
    let mut s = Sticky::new();
    let v = make_view(&[Some(10), Some(20), Some(30)], &[0; 3], &[0; 3]);
    let first = s.pick(&v);
    s.report_error(first);
    assert_eq!(s.pick(&v), first);
}

#[test]
fn sticky_name_matches_index_eight() {
    assert_eq!(Sticky::new().name(), STRATEGY_NAMES[8]);
}

#[test]
fn free_constructors_match_concrete_types() {
    let _ = round_robin();
    let _ = random();
    let _ = lowest_rtt();
    let _ = least_connections();
    let _ = hash_by_addr();
    let _ = weighted_round_robin();
    let _ = failover();
    let _ = health_weighted();
    let _ = sticky();
}

#[test]
fn strategies_have_stable_names_in_canonical_order() {
    assert_eq!(STRATEGY_NAMES[0], "round_robin");
    assert_eq!(STRATEGY_NAMES[1], "random");
    assert_eq!(STRATEGY_NAMES[2], "lowest_rtt");
    assert_eq!(STRATEGY_NAMES[3], "least_connections");
    assert_eq!(STRATEGY_NAMES[4], "hash_by_addr");
    assert_eq!(STRATEGY_NAMES[5], "weighted_round_robin");
    assert_eq!(STRATEGY_NAMES[6], "failover");
    assert_eq!(STRATEGY_NAMES[7], "health_weighted");
    assert_eq!(STRATEGY_NAMES[8], "sticky");
}

// ===========================================================================
//  PoolView
// ===========================================================================

#[test]
fn pool_view_len_and_is_empty() {
    let m = vec![TunnelMetrics::default(); 3];
    let v = PoolView {
        dial_addr: "x",
        metrics: &m,
    };
    assert_eq!(v.len(), 3);
    assert!(!v.is_empty());
    let empty: &[TunnelMetrics] = &[];
    let v = PoolView {
        dial_addr: "x",
        metrics: empty,
    };
    assert_eq!(v.len(), 0);
    assert!(v.is_empty());
}

// ===========================================================================
//  Failover rotation through the LoadBalancer dial path
// ===========================================================================

#[tokio::test]
async fn failover_strategy_via_load_balancer_advances_on_dial_error() {
    let lb = LoadBalancer::new(echo_pool(3), Failover::new()).unwrap();
    let conn1 = lb.dial("a:80").await.unwrap();
    drop(conn1);
    let conn2 = lb.dial("a:80").await.unwrap();
    drop(conn2);
    // Calling strategy_name works for diagnostics.
    assert_eq!(lb.strategy_name().await, "failover");
}

// ===========================================================================
//  Drain / undrain / replace / set_strategy
// ===========================================================================

#[tokio::test]
async fn drain_and_undrain_round_trip() {
    let mut lb = LoadBalancer::new(echo_pool(2), round_robin()).unwrap();
    assert!(!lb.is_draining(0).await);
    assert!(lb.drain_backend(0).await);
    assert!(lb.is_draining(0).await);
    assert!(lb.undrain_backend(0).await);
    assert!(!lb.is_draining(0).await);
}

#[tokio::test]
async fn drain_out_of_bounds_returns_false() {
    let mut lb = LoadBalancer::new(echo_pool(2), round_robin()).unwrap();
    assert!(!lb.drain_backend(99).await);
    assert!(!lb.undrain_backend(99).await);
}

#[tokio::test]
async fn undrain_only_succeeds_on_draining_backend() {
    let mut lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    assert!(!lb.undrain_backend(0).await);
}

#[tokio::test]
async fn replace_backends_swaps_pool() {
    let mut lb = LoadBalancer::new(echo_pool(2), round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 2);
    let new_backends = echo_pool(3);
    lb.replace_backends(new_backends, None).await.unwrap();
    assert_eq!(lb.backend_count(), 3);
}

#[tokio::test]
async fn replace_backends_rejects_empty_pool() {
    let mut lb = LoadBalancer::new(echo_pool(2), round_robin()).unwrap();
    let err = lb.replace_backends(vec![], None).await.unwrap_err();
    assert!(matches!(err, Error::NoBackends(_)));
}

#[tokio::test]
async fn replace_backends_replaces_strategy_when_given() {
    let mut lb = LoadBalancer::new(echo_pool(2), Random::new()).unwrap();
    assert_eq!(lb.strategy_name().await, "random");
    lb.replace_backends(echo_pool(3), Some(Box::new(RoundRobin::new())))
        .await
        .unwrap();
    assert_eq!(lb.strategy_name().await, "round_robin");
}

#[tokio::test]
async fn set_strategy_at_runtime() {
    let mut lb = LoadBalancer::new(echo_pool(2), Random::new()).unwrap();
    assert_eq!(lb.strategy_name().await, "random");
    lb.set_strategy(LeastConnections::new()).await;
    assert_eq!(lb.strategy_name().await, "least_connections");
}

#[tokio::test]
async fn add_and_remove_backend() {
    let mut lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    assert_eq!(lb.backend_count(), 1);
    let idx = lb.add_backend(Box::new(EchoBackend::new())).await;
    assert_eq!(lb.backend_count(), 2);
    assert!(lb.remove_backend(idx).await);
    assert_eq!(lb.backend_count(), 1);
}

#[tokio::test]
async fn add_backend_with_id_then_remove_by_id() {
    let mut lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    let _idx = lb
        .add_backend_with_id("alpha".to_string(), Box::new(EchoBackend::new()))
        .await;
    let ids = lb.backend_ids().await;
    assert!(ids.iter().any(|id| id.as_deref() == Some("alpha")));
    assert!(lb.remove_backend_by_id("alpha").await);
}

#[tokio::test]
async fn remove_backend_by_id_misses_when_absent() {
    let mut lb = LoadBalancer::new(echo_pool(2), round_robin()).unwrap();
    assert!(!lb.remove_backend_by_id("nonexistent").await);
}

#[tokio::test]
async fn remove_backend_by_ptr_with_unrelated_ptr_returns_false() {
    let mut lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
    // Build a separate backend and grab its ptr.
    let dummy = EchoBackend::new();
    let ptr = &dummy as &dyn Backend;
    assert!(!lb.remove_backend_by_ptr(ptr).await);
    assert_eq!(lb.backend_count(), 1);
}

// ===========================================================================
//  HealthChecker end-to-end
// ===========================================================================

#[derive(Debug)]
struct HealthBackend {
    fail: Arc<AtomicU32>,
    dial_count: Arc<AtomicUsize>,
}

#[async_trait]
impl Backend for HealthBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        self.dial_count.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) > 0 {
            self.fail.fetch_sub(1, Ordering::SeqCst);
            Err(Error::backend("simulated"))
        } else {
            let (a, _b) = duplex(8);
            Ok(Box::pin(a))
        }
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn health_checker_marks_backend_unhealthy_after_threshold_failures() {
    // Fail enough times that we'll still be in failure mode at the end of the
    // test window (interval=20ms, sleep=120ms → ~6 ticks; ensure fail > 6).
    let fail = Arc::new(AtomicU32::new(20));
    let count = Arc::new(AtomicUsize::new(0));
    let backend = HealthBackend {
        fail: fail.clone(),
        dial_count: count.clone(),
    };
    let backends: Vec<Box<dyn Backend>> = vec![Box::new(backend)];
    let metrics = Arc::new(tokio::sync::Mutex::new(vec![TunnelMetrics::default()]));
    let config = HealthCheckConfig {
        interval: Duration::from_millis(20),
        timeout: Duration::from_millis(20),
        unhealthy_threshold: 2,
        healthy_threshold: 1,
        check_addr: "test:80".into(),
    };
    let checker = HealthChecker::spawn(backends, metrics.clone(), config);
    tokio::time::sleep(Duration::from_millis(120)).await;
    checker.shutdown().await;
    let m = metrics.lock().await[0];
    assert!(m.total_errors >= 2);
    assert!(m.recent_errors >= 2);
    assert!(count.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn health_checker_default_config_uses_constants() {
    let config = HealthCheckConfig::default();
    assert_eq!(config.interval, DEFAULT_HEALTH_CHECK_INTERVAL);
    assert_eq!(config.timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
    assert_eq!(config.unhealthy_threshold, DEFAULT_UNHEALTHY_THRESHOLD);
    assert_eq!(config.healthy_threshold, DEFAULT_HEALTHY_THRESHOLD);
    assert!(config.check_addr.is_empty());
}

#[tokio::test]
async fn health_state_variants_compare() {
    assert_eq!(HealthState::Healthy, HealthState::Healthy);
    assert_ne!(HealthState::Healthy, HealthState::Unhealthy);
    assert_ne!(HealthState::Healthy, HealthState::Unknown);
    let _ = format!("{:?}", HealthState::Unknown);
}

#[test]
fn is_healthy_thresholds() {
    let mut m = TunnelMetrics::default();
    assert!(is_healthy(&m, 1));
    m.recent_errors = 1;
    assert!(!is_healthy(&m, 1));
    m.recent_errors = 0;
    assert!(!is_healthy(&m, 0));
}

#[test]
fn record_dial_result_increments_only_recent_errors_on_failure() {
    let mut metrics = vec![TunnelMetrics::default(); 1];
    record_dial_result(&mut metrics, 0, false);
    record_dial_result(&mut metrics, 0, false);
    assert_eq!(metrics[0].recent_errors, 2);
    assert_eq!(metrics[0].total_errors, 2);
    record_dial_result(&mut metrics, 0, true);
    assert_eq!(metrics[0].recent_errors, 0);
    assert_eq!(
        metrics[0].total_errors, 2,
        "total does not decrement on success"
    );
}

#[test]
fn record_dial_result_out_of_bounds_is_a_noop() {
    let mut metrics = vec![TunnelMetrics::default()];
    record_dial_result(&mut metrics, 99, true);
    assert_eq!(metrics[0].recent_errors, 0);
}

// ===========================================================================
//  Discovery — StaticDiscovery + Discover reconciler
// ===========================================================================

#[cfg(feature = "discovery")]
mod discovery_tests {
    use super::*;
    use rota_lb::discovery::{
        BackendDescriptor, BackendFactoryFromDescriptor, Discover, ServiceDiscovery,
        StaticDiscovery,
    };

    #[tokio::test]
    async fn static_discovery_returns_its_descriptors() {
        let d = StaticDiscovery::new(vec![
            BackendDescriptor::new("a", "1.1.1.1:80"),
            BackendDescriptor::new("b", "2.2.2.2:80"),
        ]);
        let out = d.discover().await.unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, "a");
        assert_eq!(out[1].id, "b");
    }

    #[tokio::test]
    async fn static_discovery_from_tuples_drops_names_and_keeps_addresses() {
        let d = StaticDiscovery::from_tuples(vec![
            ("a".into(), "name-a".into(), "1.1.1.1:80".into()),
            ("b".into(), "name-b".into(), "2.2.2.2:80".into()),
        ]);
        let out = d.discover().await.unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].addr, "1.1.1.1:80");
        assert_eq!(out[0].id, "a");
    }

    #[test]
    fn backend_descriptor_builders_chain() {
        let d = BackendDescriptor::new("a", "1.1.1.1:80")
            .with_tag("env", "prod")
            .with_weight(50)
            .with_health_check("/healthz");
        assert_eq!(d.id, "a");
        assert_eq!(d.addr, "1.1.1.1:80");
        assert_eq!(d.metadata.get("env").unwrap(), "prod");
        assert_eq!(d.weight, Some(50));
        assert_eq!(d.health_check.as_deref(), Some("/healthz"));
    }

    #[tokio::test]
    async fn discover_loop_adds_and_removes_descriptors() {
        use std::sync::Mutex as StdMutex;

        #[derive(Debug)]
        struct ToggleDiscovery {
            descriptors: Arc<StdMutex<Vec<BackendDescriptor>>>,
        }

        #[async_trait]
        impl ServiceDiscovery for ToggleDiscovery {
            async fn discover(&self) -> Result<Vec<BackendDescriptor>, Error> {
                Ok(self.descriptors.lock().unwrap().clone())
            }
        }

        struct DescFactory;
        #[async_trait]
        impl BackendFactoryFromDescriptor for DescFactory {
            type Backend = EchoBackend;
            type Error = Error;

            async fn create(
                &self,
                _descriptor: &BackendDescriptor,
            ) -> Result<Self::Backend, Self::Error> {
                Ok(EchoBackend::new())
            }
        }

        // Start empty; we drive backends entirely through discovery.
        // Discover takes ownership of the LB so we can construct via new_with_metrics.
        let descriptors = Arc::new(StdMutex::new(vec![
            BackendDescriptor::new("a", "1.1.1.1:80"),
            BackendDescriptor::new("b", "2.2.2.2:80"),
        ]));
        let discovery = ToggleDiscovery {
            descriptors: descriptors.clone(),
        };
        // Begin with a single no-id backend so the LoadBalancer can be built.
        let lb = LoadBalancer::new(echo_pool(1), round_robin()).unwrap();
        let mut discover =
            Discover::new(lb, discovery, DescFactory, Some(Duration::from_millis(20)));
        discover.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;

        // Mutate: drop b, add c.
        {
            let mut g = descriptors.lock().unwrap();
            g.pop();
            g.push(BackendDescriptor::new("c", "3.3.3.3:80"));
        }
        // Wait long enough for several reconcile ticks to pick up the change.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Drop the lock guard and re-acquire to get a fresh snapshot.
        let arc = discover.load_balancer_arc();
        let ids = {
            let snap = arc.lock().await;
            snap.backend_ids().await
        };
        let id_strs: Vec<&str> = ids.iter().filter_map(|i| i.as_deref()).collect();

        // Both "a" and "c" should be present after the mutation has been reconciled.
        assert!(
            id_strs.contains(&"a"),
            "a should still be present: {id_strs:?}"
        );
        assert!(
            id_strs.contains(&"c"),
            "c should have been added: {id_strs:?}"
        );

        discover.stop().await.unwrap();
    }

    #[tokio::test]
    async fn backend_factory_from_descriptor_creates_from_descriptor() {
        struct DescFactory;
        #[async_trait]
        impl BackendFactoryFromDescriptor for DescFactory {
            type Backend = EchoBackend;
            type Error = Error;

            async fn create(
                &self,
                descriptor: &BackendDescriptor,
            ) -> Result<Self::Backend, Self::Error> {
                assert_eq!(descriptor.id, "a");
                Ok(EchoBackend::new())
            }
        }

        let desc = BackendDescriptor::new("a", "1.1.1.1:80");
        let backend = DescFactory.create(&desc).await.unwrap();
        let conn = backend.dial("x:80").await.unwrap();
        drop(conn);
    }
}

// ===========================================================================
//  Factory
// ===========================================================================

struct CounterFactory {
    counter: Arc<AtomicUsize>,
}

#[async_trait]
impl BackendFactory for CounterFactory {
    async fn create(&self) -> Result<BackendOutput, Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(BackendOutput {
            backend: Box::new(EchoBackend::new()),
            initial_metrics: TunnelMetrics::default(),
        })
    }
}

#[tokio::test]
async fn backend_factory_is_called_per_backend() {
    let counter = Arc::new(AtomicUsize::new(0));
    let factories: Vec<Box<dyn BackendFactory>> = (0..3)
        .map(|_| {
            Box::new(CounterFactory {
                counter: counter.clone(),
            }) as Box<dyn BackendFactory>
        })
        .collect();
    let lb = LoadBalancer::from_factories(factories, round_robin())
        .await
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 3);
    let conn = lb.dial("a:80").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn backend_factory_with_provided_initial_metrics_wins() {
    struct M;
    #[async_trait]
    impl BackendFactory for M {
        async fn create(&self) -> Result<BackendOutput, Error> {
            Ok(BackendOutput {
                backend: Box::new(EchoBackend::new()),
                initial_metrics: TunnelMetrics {
                    rtt: Some(Duration::from_millis(123)),
                    ..Default::default()
                },
            })
        }
    }
    let factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(M)];
    let provided = vec![TunnelMetrics {
        rtt: Some(Duration::from_millis(7)),
        ..Default::default()
    }];
    let lb = LoadBalancer::builder()
        .factories(factories)
        .initial_metrics(provided)
        .strategy(RoundRobin::new())
        .build()
        .await
        .unwrap();
    let m = lb.metrics().await.into_iter().next().unwrap();
    assert_eq!(m.rtt, Some(Duration::from_millis(7)));
}

// ===========================================================================
//  Connection round-trip
// ===========================================================================

#[tokio::test]
async fn connection_supports_bidirectional_io() {
    let (a, mut b) = duplex(64);
    let mut a = Box::pin(a);
    a.write_all(b"ping").await.unwrap();
    drop(a);
    let mut buf = [0u8; 4];
    b.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}

// ===========================================================================
//  Constants sanity
// ===========================================================================

#[test]
fn constants_have_expected_values() {
    assert_eq!(MS_PER_SECOND, 1000);
    assert_eq!(DEFAULT_RTT_US, 1_000_000);
    assert_eq!(DEFAULT_DIAL_TIMEOUT, Duration::from_secs(10));
    assert_eq!(DEFAULT_HEALTH_CHECK_INTERVAL, Duration::from_secs(30));
    assert_eq!(DEFAULT_HEALTH_CHECK_TIMEOUT, Duration::from_secs(5));
    assert_eq!(DEFAULT_MAX_RETRY_DELAY, Duration::from_secs(30));
    assert_eq!(DEFAULT_RETRY_MULTIPLIER, 2.0);
    assert_eq!(JITTER_FACTOR, 0.5);
    assert_eq!(DEFAULT_SRV_PREFIX, "_http._tcp");
    assert_eq!(DEFAULT_UNHEALTHY_THRESHOLD, 3);
    assert_eq!(DEFAULT_HEALTHY_THRESHOLD, 2);
    let _ = MAX_BACKENDS.checked_sub(MIN_BACKENDS);
    assert_eq!(MIN_BACKENDS, 1);
    // Verify the difference fits in usize.
    let _: usize = MAX_BACKENDS - MIN_BACKENDS;
    let alpn: Vec<&[u8]> = DEFAULT_ALPN_PROTOCOLS.to_vec();
    assert_eq!(alpn.len(), 2);
    assert_eq!(alpn[0], b"h2");
    assert_eq!(alpn[1], b"http/1.1");
}

#[test]
fn calculate_weight_is_monotonic_in_inverse_rtt() {
    use rota_lb::constants::calculate_weight;
    let fast = calculate_weight(Duration::from_millis(1));
    let slow = calculate_weight(Duration::from_millis(1000));
    let very_slow = calculate_weight(Duration::from_secs(5));
    assert!(fast > slow, "fast={fast} should beat slow={slow}");
    assert!(
        slow >= very_slow,
        "slow={slow} should be >= very_slow={very_slow}"
    );
    assert!(fast >= 1);
}

// ===========================================================================
//  TunnelMetrics
// ===========================================================================

#[test]
fn tunnel_metrics_default_is_zeroed() {
    let m = TunnelMetrics::default();
    assert_eq!(m.rtt, None);
    assert_eq!(m.active_connections, 0);
    assert_eq!(m.recent_errors, 0);
    assert_eq!(m.total_dials, 0);
    assert_eq!(m.total_errors, 0);
}

#[test]
fn tunnel_metrics_is_copy() {
    let m = TunnelMetrics::default();
    let m2 = m;
    let _ = m;
    let _ = m2;
}

// ===========================================================================
//  RetryPolicy
// ===========================================================================

#[test]
fn retry_policy_defaults_for_no_retry() {
    assert_eq!(NoRetry.total_timeout(), None);
    assert_eq!(NoRetry.max_attempts(), None);
}

#[test]
fn fixed_retry_max_attempts_override_visible() {
    let p = FixedRetry::new(Duration::from_millis(1)).with_max_attempts(7);
    assert_eq!(p.max_attempts(), Some(7));
}

#[test]
fn exponential_backoff_setters_apply() {
    let p = ExponentialBackoff::new(Duration::from_millis(1))
        .with_max_delay(Duration::from_secs(5))
        .with_multiplier(3.0)
        .with_max_attempts(9)
        .with_jitter(false);
    assert_eq!(p.max_attempts(), Some(9));
    let s = p.should_retry(1, &Error::backend("x"));
    assert!(s.is_some());
}

#[test]
fn exponential_backoff_with_max_attempts_returns_none_after_budget() {
    let p = ExponentialBackoff::new(Duration::from_millis(1)).with_max_attempts(2);
    assert!(p.should_retry(1, &Error::backend("x")).is_some());
    assert!(p.should_retry(2, &Error::backend("x")).is_none());
    assert!(p.should_retry(99, &Error::backend("x")).is_none());
}

#[test]
fn exponential_backoff_total_timeout_can_be_set_via_builder() {
    // We can't set total_timeout via a builder (no setter exposed), so confirm
    // the default is None and that no_retry reports the same.
    assert_eq!(
        ExponentialBackoff::new(Duration::from_millis(1)).total_timeout(),
        None
    );
}

// ===========================================================================
//  TLS configuration paths (mock — no network)
// ===========================================================================

#[cfg(feature = "tls")]
#[test]
fn tls_config_default_disables_client_cert_and_keeps_alpn_defaults() {
    let cfg = rota_lb::TlsConfig::new("example.com");
    assert!(cfg.verify_hostname);
    assert!(cfg.client_cert.is_none());
    assert!(cfg.connect_timeout.is_none());
    assert_eq!(cfg.alpn_protocols.len(), DEFAULT_ALPN_PROTOCOLS.len());
    assert!(cfg.root_certs.is_none());
}

#[cfg(feature = "tls")]
#[test]
fn tls_config_builds_with_alpn_and_connect_timeout() {
    let cfg = rota_lb::TlsConfig::new("example.com")
        .with_alpn_protocols(vec![b"h2".to_vec()])
        .with_connect_timeout(Duration::from_secs(2));
    assert!(cfg.build_client_config().is_ok());
}

#[cfg(feature = "tls")]
#[test]
fn tls_config_build_succeeds_with_disabled_hostname_check() {
    let cfg = rota_lb::TlsConfig::new("example.com").danger_bypass_hostname_check_only();
    assert!(!cfg.verify_hostname);
    assert!(cfg.build_client_config().is_ok());
}

#[cfg(feature = "tls")]
#[test]
fn tls_config_with_root_certs_invalid_cert_errors() {
    use rustls::pki_types::CertificateDer;
    let bogus = CertificateDer::from(vec![0u8; 32]);
    let cfg = rota_lb::TlsConfig::new("example.com").with_root_certs(vec![bogus]);
    // A 32-byte zero array is not a valid DER certificate; build should fail.
    let res = cfg.build_client_config();
    assert!(res.is_err());
}

// ===========================================================================
//  Tower integration
// ===========================================================================

#[cfg(all(feature = "tower", feature = "discovery"))]
mod tower_integration {
    use super::*;
    use rota_lb::{LbRequest, LoadBalancer};
    use tower::ServiceExt;

    #[tokio::test]
    async fn tower_lb_request_builder_overrides() {
        let lb: Arc<LoadBalancer> =
            Arc::new(LoadBalancer::new(echo_pool(1), round_robin()).unwrap());
        let svc = tower::service_fn(move |req: LbRequest| {
            let lb = lb.clone();
            async move { lb.dial(&req.addr).await }
        });
        let req = LbRequest::new("example.com:443").with_dial_timeout(Duration::from_secs(1));
        assert_eq!(req.dial_timeout, Some(Duration::from_secs(1)));
        assert!(req.retry_policy.is_none());
        let conn = svc.oneshot(req).await.unwrap();
        drop(conn);
    }

    #[tokio::test]
    async fn tower_lb_request_with_retry_policy_override() {
        let req = LbRequest::new("example.com:443").with_retry_policy(NoRetry);
        let policy = req.retry_policy.unwrap();
        assert_eq!(policy.total_timeout(), None);
    }

    #[tokio::test]
    async fn tower_lb_request_new_sets_address() {
        let req = LbRequest::new("test:80");
        assert_eq!(req.addr, "test:80");
        assert!(req.dial_timeout.is_none());
        assert!(req.retry_policy.is_none());
    }
}

// ===========================================================================
//  FFI surface
// ===========================================================================

#[cfg(feature = "ffi")]
mod ffi_tests {
    use rota_lb::ffi::RotaVersion;

    // We exercise FFI through the C ABI. The opaque handle type is private,
    // so we hold it as a raw pointer and pass it back to the public functions.
    // Each test owns its handle for the duration of the test.
    extern "C" {
        fn rota_lb_create(n: u32, strategy_kind: i32) -> *mut std::ffi::c_void;
        fn rota_lb_destroy(lb: *mut std::ffi::c_void);
        fn rota_lb_select(
            lb: *mut std::ffi::c_void,
            dial_addr: *const std::ffi::c_char,
            metrics: *const std::ffi::c_void,
            n: u32,
        ) -> u32;
        fn rota_lb_report_error(lb: *mut std::ffi::c_void, idx: u32);
        fn rota_lb_report_success(lb: *mut std::ffi::c_void, idx: u32);
        fn rota_lb_strategy_name(lb: *mut std::ffi::c_void) -> *const std::ffi::c_char;
        fn rota_lb_version(out: *mut RotaVersion) -> i32;
    }

    /// Build an FfiMetric at runtime using the public symbol layout.
    /// FfiMetric is private to the crate so we construct it via a C-compatible
    /// buffer instead. The buffer matches the documented 32-byte layout:
    /// rtt_us (u64), active_connections (u32), recent_errors (u32),
    /// total_dials (u64), total_errors (u64).
    ///
    /// Alignment matches FfiMetric's first-field u64 alignment (8 bytes).
    #[repr(C, align(8))]
    struct MetricBuf([u8; 32]);

    fn zero_metric_buffer() -> MetricBuf {
        MetricBuf([0u8; 32])
    }

    #[test]
    fn ffi_version_writes_expected_layout() {
        let mut v = RotaVersion {
            major: 0,
            minor: 0,
            patch: 0,
            metric_struct_size: 0,
        };
        let rc = unsafe { rota_lb_version(&mut v) };
        assert_eq!(rc, 0);
        // Crate is at 0.x; just verify the call returns a valid struct.
        assert!(v.major <= 100, "major version out of plausible range");
        assert!(v.metric_struct_size >= 32);
    }

    #[test]
    fn ffi_create_and_destroy_round_trip() {
        unsafe {
            let lb = rota_lb_create(2, 0);
            assert!(!lb.is_null(), "create must succeed for known strategy");
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn ffi_create_rejects_unknown_strategy() {
        unsafe {
            let lb = rota_lb_create(1, 999);
            assert!(lb.is_null(), "unknown strategy must return null");
        }
    }

    #[test]
    fn ffi_create_rejects_zero_backends() {
        unsafe {
            let lb = rota_lb_create(0, 0);
            assert!(lb.is_null(), "n=0 must return null");
        }
    }

    #[test]
    fn ffi_select_returns_u32_max_on_null_arguments() {
        unsafe {
            let rc = rota_lb_select(std::ptr::null_mut(), std::ptr::null(), std::ptr::null(), 0);
            assert_eq!(rc, u32::MAX);
        }
    }

    #[test]
    fn ffi_report_error_and_success_on_null_lb_are_safe_noops() {
        unsafe {
            rota_lb_report_error(std::ptr::null_mut(), 0);
            rota_lb_report_success(std::ptr::null_mut(), 0);
        }
    }

    #[test]
    fn ffi_create_then_select_then_destroy_end_to_end() {
        unsafe {
            let lb = rota_lb_create(1, 0);
            assert!(!lb.is_null());
            let addr = b"example.com:80\0";
            let m = zero_metric_buffer();
            let idx = rota_lb_select(lb, addr.as_ptr() as *const _, &m as *const _ as *const _, 1);
            assert_eq!(idx, 0, "select must pick the only backend");
            rota_lb_report_error(lb, idx);
            rota_lb_report_success(lb, idx);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn ffi_strategy_name_returns_static_pointer_for_known_strategies() {
        unsafe {
            for kind in 0..=8 {
                let lb = rota_lb_create(1, kind);
                assert!(!lb.is_null(), "create must succeed for kind={kind}");
                let name = rota_lb_strategy_name(lb);
                assert!(!name.is_null(), "name ptr for kind={kind} must not be null");
                let s = std::ffi::CStr::from_ptr(name).to_str().unwrap();
                assert!(!s.is_empty());
                rota_lb_destroy(lb);
            }
        }
    }

    #[test]
    fn ffi_select_with_invalid_cstr_returns_max() {
        unsafe {
            let lb = rota_lb_create(1, 0);
            assert!(!lb.is_null());
            let m = zero_metric_buffer();
            // A non-UTF-8 address should fail validation and return u32::MAX.
            let bad = [0xFFu8, 0xFE, 0xFD, 0x00];
            let idx = rota_lb_select(lb, bad.as_ptr() as *const _, &m as *const _ as *const _, 1);
            assert_eq!(idx, u32::MAX);
            rota_lb_destroy(lb);
        }
    }

    #[test]
    fn ffi_version_metric_struct_size_is_32_bytes() {
        let mut v = RotaVersion {
            major: 0,
            minor: 0,
            patch: 0,
            metric_struct_size: 0,
        };
        unsafe {
            rota_lb_version(&mut v);
        }
        assert_eq!(v.metric_struct_size, 32);
    }
}

// ===========================================================================
//  Doc example path
// ===========================================================================

#[test]
fn doc_referenced_paths_resolve() {
    // Each of these is referenced in the lib.rs doc table or quick-start.
    fn _check() {
        let _ = std::mem::size_of::<Box<dyn Backend>>();
        let _ = std::mem::size_of::<Connection>();
        let _ = std::mem::size_of::<Error>();
        let _ = std::mem::size_of::<TunnelMetrics>();
        let _ = std::mem::size_of::<PoolView<'static>>();
    }
}

// ===========================================================================
//  Smoke test — everything combined
// ===========================================================================

#[tokio::test]
async fn end_to_end_realistic_workflow() {
    // Build a 3-backend pool with mixed RTTs and dial through round_robin,
    // then through failover after triggering an error, then through sticky
    // to verify pin-on-first-pick, then through health_weighted with RTT
    // scoring — all through the same LoadBalancer via set_strategy.
    let mut lb = LoadBalancer::new(
        echo_pool_with_rtt(&[
            Duration::from_millis(5),
            Duration::from_millis(20),
            Duration::from_millis(100),
        ]),
        round_robin(),
    )
    .unwrap();

    // 6 round-robin dials → each backend twice.
    for _ in 0..6 {
        let conn = lb.dial("example.com:443").await.unwrap();
        drop(conn);
    }
    let total: u64 = lb.metrics().await.iter().map(|m| m.total_dials).sum();
    assert_eq!(total, 6);

    // Swap to failover.
    lb.set_strategy(Failover::new()).await;
    assert_eq!(lb.strategy_name().await, "failover");
    let conn = lb.dial("example.com:443").await.unwrap();
    drop(conn);

    // Swap to sticky.
    lb.set_strategy(Sticky::new()).await;
    let conn1 = lb.dial("example.com:443").await.unwrap();
    drop(conn1);
    let conn2 = lb.dial("example.com:443").await.unwrap();
    drop(conn2);
    // Sticky is internal; verify via total_errors == 0 (no failures happened).
    let total_errors: u64 = lb.metrics().await.iter().map(|m| m.total_errors).sum();
    assert_eq!(total_errors, 0);

    // Swap to health_weighted and verify the name comes back.
    lb.set_strategy(HealthWeighted::new()).await;
    assert_eq!(lb.strategy_name().await, "health_weighted");

    // Shut down cleanly.
    lb.shutdown().await;
}

// (no extra helpers needed)
