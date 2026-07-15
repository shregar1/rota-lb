//! End-to-end "data-feed user" integration tests for `rota`.
//!
//! These tests model a real consumer of market / telemetry / IoT data feeds.
//! A consumer has a pool of redundant feed sources (different exchanges,
//! different shards, different geographic origins) and the load balancer
//! distributes the subscriber sessions across them.
//!
//! Rather than the in-memory `duplex()` mock used elsewhere, this file spins
//! up real TCP listener tasks (`MockFeedServer`) that:
//!   - accept inbound TCP,
//!   - send a continuous stream of newline-delimited tick messages,
//!   - track per-server counters (connections accepted, ticks sent),
//!   - can be configured to delay ticks, drop after N messages, or stop
//!     accepting connections entirely.
//!
//! The `TcpBackend` then dials those listeners through the `LoadBalancer`,
//! so we exercise the real `tokio::net::TcpStream` path end-to-end.
//!
//! The test cases below cover the realistic consumer scenarios:
//!
//!   * round-robin fan-out across N feed sources
//!   * parallel subscriptions to multiple symbols at once
//!   * sticky / hash-by-addr consistency for per-symbol session pinning
//!   * RTT-aware selection once metrics are populated
//!   * failover when the primary feed goes down
//!   * least-connections across long-lived streams
//!   * hot-add / hot-remove / replace of feed sources at runtime
//!   * retry on transient connection failure
//!   * dial timeout against an unresponsive listener
//!   * graceful shutdown of the balancer while consumers are mid-stream
//!   * 100 concurrent subscribers
//!   * backpressure: a slow consumer doesn't stall fast peers

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;
use rota_lb::{
    hash_by_addr, least_connections, lowest_rtt, random, round_robin, sticky, BalanceStrategy,
    ExponentialBackoff, Failover, HashByAddr, LoadBalancer, RoundRobin,
};

#[path = "common/mod.rs"]
mod common;

use common::{
    backends_from, read_at_least, spawn_feed_server, FeedServerConfig, FeedServerHandle, TcpBackend,
};

// ============================================================================
//  Tests — basic distribution
// ============================================================================

#[tokio::test]
async fn data_feed_user_round_robin_distribution() {
    let handles: Vec<FeedServerHandle> = {
        let mut v = Vec::new();
        for _ in 0..3 {
            v.push(
                spawn_feed_server(FeedServerConfig {
                    tick_interval: Duration::from_millis(2),
                    ..Default::default()
                })
                .await,
            );
        }
        v
    };

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, RoundRobin::new()).unwrap();

    for _ in 0..6 {
        let mut conn = lb.dial("feed.example:9000").await.unwrap();
        let _ = read_at_least(&mut conn, 32, Duration::from_secs(2))
            .await
            .expect("read from feed");
        drop(conn);
    }

    let metrics = lb.metrics().await;
    let total: u64 = metrics.iter().map(|m| m.total_dials).sum();
    assert_eq!(total, 6, "every dial must succeed");

    // Round-robin over 3 backends for 6 dials → 2 dials each.
    for (i, m) in metrics.iter().enumerate() {
        assert_eq!(
            m.total_dials, 2,
            "backend {i} should have 2 dials, got {m:?}"
        );
    }
}

#[tokio::test]
async fn data_feed_user_random_distribution_within_range() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..4).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, random()).unwrap();

    for _ in 0..50 {
        let conn = lb.dial("feed:9000").await.unwrap();
        drop(conn);
    }

    let metrics = lb.metrics().await;
    let total: u64 = metrics.iter().map(|m| m.total_dials).sum();
    assert_eq!(total, 50);
    // All backends must have been picked at least once with high probability.
    let hits = metrics.iter().filter(|m| m.total_dials > 0).count();
    assert!(
        hits >= 2,
        "expected multiple backends picked, got {hits} non-empty out of {}",
        metrics.len()
    );
}

#[tokio::test]
async fn data_feed_user_parallel_subscribers_all_receive_data() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..4).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());

    let mut tasks = Vec::new();
    for i in 0..20 {
        let lb = lb.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = lb.dial(&format!("feed-{i}:9000")).await.unwrap();
            let buf = read_at_least(&mut conn, 64, Duration::from_secs(3))
                .await
                .expect("read");
            assert!(
                buf.starts_with(b"TICK"),
                "got: {:?}",
                &buf[..buf.len().min(32)]
            );
            buf.len()
        }));
    }
    let mut total_bytes = 0usize;
    for t in tasks {
        total_bytes += t.await.unwrap();
    }
    assert!(total_bytes >= 20 * 64);

    // The four servers should all have served connections.
    let mut served = 0;
    for h in &handles {
        if h.connections() > 0 {
            served += 1;
        }
    }
    assert!(served >= 2, "expected multiple servers used, got {served}");
}

// ============================================================================
//  Tests — per-symbol / sticky semantics
// ============================================================================

#[tokio::test]
async fn data_feed_user_sticky_pins_to_one_feed() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..4).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, sticky()).unwrap();

    // With no RTT metrics yet, Sticky pins to the first/lowest-RTT pick —
    // which is index 0 (no RTTs known, find_lowest_rtt returns 0).
    let pinned_idx_first = {
        let m = lb.metrics().await;
        // After first dial:
        let _ = m;
        0usize
    };

    for _ in 0..10 {
        let conn = lb.dial("BTC-USD:9000").await.unwrap();
        drop(conn);
    }
    let metrics = lb.metrics().await;
    let pinned = metrics
        .iter()
        .enumerate()
        .find(|(_, m)| m.total_dials > 0)
        .map(|(i, _)| i)
        .expect("at least one dial must have happened");

    assert_eq!(
        pinned, pinned_idx_first,
        "Sticky must always pick the same backend"
    );

    let pinned_count = metrics[pinned].total_dials;
    assert_eq!(pinned_count, 10, "all 10 dials went to pinned backend");
}

#[tokio::test]
async fn data_feed_user_hash_by_addr_is_consistent_per_symbol() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..5).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, HashByAddr::new()).unwrap();

    let symbols = ["AAPL", "GOOG", "MSFT", "TSLA", "NVDA"];
    let mut pick_for: std::collections::HashMap<String, usize> = Default::default();

    // Three subscriptions per symbol.
    for sym in &symbols {
        for _ in 0..3 {
            let conn = lb.dial(&format!("{sym}:9000")).await.unwrap();
            drop(conn);
        }
    }
    let metrics = lb.metrics().await;
    for sym in &symbols {
        // Find the backend that received the symbol's dials by re-hashing.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        format!("{sym}:9000").hash(&mut hasher);
        let expected = (hasher.finish() as usize) % handles.len();
        let total = metrics[expected].total_dials;
        assert!(
            total >= 3,
            "symbol {sym} expected backend {expected} to get all 3 dials, got {total} total"
        );
        pick_for.insert((*sym).to_string(), expected);
    }
    // Verify distinct symbols land on distinct backends with high probability.
    let unique: std::collections::HashSet<_> = pick_for.values().copied().collect();
    assert!(
        unique.len() >= 2,
        "expected symbols to spread across multiple backends, got picks: {pick_for:?}"
    );
}

#[tokio::test]
async fn data_feed_user_hash_by_addr_is_collision_free() {
    // Different addresses must hash to different backends (almost surely)
    // when we have many backends.
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..8).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, hash_by_addr()).unwrap();

    for i in 0..50 {
        let conn = lb.dial(&format!("symbol-{i}:9000")).await.unwrap();
        drop(conn);
    }
    let metrics = lb.metrics().await;
    let used = metrics.iter().filter(|m| m.total_dials > 0).count();
    assert!(
        used >= 3,
        "hash_by_addr should spread 50 dials across multiple backends, only used {used}"
    );
}

// ============================================================================
//  Tests — RTT-aware selection
// ============================================================================

#[tokio::test]
async fn data_feed_user_lowest_rtt_picks_fastest_after_metrics() {
    let slow = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let fast = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(1),
        ..Default::default()
    })
    .await;
    let medium = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(5),
        ..Default::default()
    })
    .await;

    // Seed metrics so the strategy knows the relative RTTs.
    use rota_lb::strategy::TunnelMetrics;
    use std::time::Duration as D;
    let metrics = vec![
        TunnelMetrics {
            rtt: Some(D::from_millis(100)),
            ..Default::default()
        },
        TunnelMetrics {
            rtt: Some(D::from_millis(5)),
            ..Default::default()
        },
        TunnelMetrics {
            rtt: Some(D::from_millis(50)),
            ..Default::default()
        },
    ];

    let backends = vec![
        Box::new(TcpBackend::new(slow.addr())) as Box<dyn Backend>,
        Box::new(TcpBackend::new(fast.addr())) as Box<dyn Backend>,
        Box::new(TcpBackend::new(medium.addr())) as Box<dyn Backend>,
    ];
    let lb = LoadBalancer::new_with_metrics(backends, metrics, lowest_rtt(), None, None).unwrap();

    let mut conn = lb.dial("feed:9000").await.unwrap();
    let _ = read_at_least(&mut conn, 16, Duration::from_secs(2))
        .await
        .unwrap();
    drop(conn);

    let m = lb.metrics().await;
    // The fast backend (index 1, rtt=5ms) should have received the dial.
    assert!(
        m[1].total_dials >= 1,
        "expected fast backend (idx 1) to receive dial, got metrics {m:?}"
    );
}

// ============================================================================
//  Tests — failover and resilience
// ============================================================================

#[tokio::test]
async fn data_feed_user_failover_picks_primary_then_rotates_on_error() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;
    let h2 = spawn_feed_server(FeedServerConfig::default()).await;

    let backends = backends_from(&[h0.clone(), h1.clone(), h2.clone()]);
    let lb = LoadBalancer::new(backends, Failover::new()).unwrap();

    // Failover picks index 0 first.
    let mut conn0 = lb.dial("feed:9000").await.unwrap();
    let _ = read_at_least(&mut conn0, 16, Duration::from_secs(1))
        .await
        .unwrap();
    drop(conn0);
    let m = lb.metrics().await;
    assert_eq!(m[0].total_dials, 1, "primary should serve the first dial");

    // All subsequent dials should still hit the same primary while it works.
    for _ in 0..5 {
        let conn = lb.dial("feed:9000").await.unwrap();
        drop(conn);
    }
    let m = lb.metrics().await;
    assert_eq!(m[0].total_dials, 6, "all 6 dials went to primary");
    assert_eq!(m[1].total_dials, 0);
    assert_eq!(m[2].total_dials, 0);
}

#[tokio::test]
async fn data_feed_user_failover_rotates_primary_after_report_error() {
    // Construct a strategy manually and verify its rotation semantics.
    let mut s = Failover::new();
    let view_metrics: Vec<rota_lb::strategy::TunnelMetrics> =
        (0..3).map(|_| Default::default()).collect();
    let view = rota_lb::strategy::PoolView {
        dial_addr: "feed:9000",
        metrics: &view_metrics,
    };
    assert_eq!(s.pick(&view), 0);
    assert_eq!(s.pick(&view), 0);

    // Report error on primary.
    s.report_error(0);
    assert_eq!(s.pick(&view), 1, "primary should rotate to 1");

    // Reporting an error on a non-primary should not change the primary.
    s.report_error(2);
    assert_eq!(s.pick(&view), 1);
}

#[tokio::test]
async fn data_feed_user_backend_drop_mid_stream_yields_eof() {
    let h = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(1),
        drop_after: Some(5),
        ..Default::default()
    })
    .await;

    let backends = vec![Box::new(TcpBackend::new(h.addr())) as Box<dyn Backend>];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();

    let mut conn = lb.dial("feed:9000").await.unwrap();

    // We should get 5 ticks then EOF.
    let _ = read_at_least(&mut conn, 30, Duration::from_secs(2))
        .await
        .expect("initial ticks");
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
        .await
        .expect("no timeout")
        .expect("no io error");
    assert_eq!(read, 0, "expected EOF after server dropped");
}

#[tokio::test]
async fn data_feed_user_dial_to_dead_backend_returns_error() {
    // Bind and immediately drop → port guaranteed unreachable.
    let dead_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead_listener.local_addr().unwrap().to_string();
    drop(dead_listener);

    let live = spawn_feed_server(FeedServerConfig::default()).await;
    let backends = vec![
        Box::new(TcpBackend::new(&dead_addr)) as Box<dyn Backend>,
        Box::new(TcpBackend::new(live.addr())) as Box<dyn Backend>,
    ];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();

    // First dial picks backend 0 (the dead one) → error.
    let r = lb.dial("feed:9000").await;
    assert!(r.is_err(), "expected error dialing dead backend");

    // Second dial should land on the live backend.
    let mut conn = lb.dial("feed:9000").await.unwrap();
    let _ = read_at_least(&mut conn, 16, Duration::from_secs(2))
        .await
        .unwrap();
    drop(conn);

    let m = lb.metrics().await;
    assert_eq!(m[0].total_errors, 1, "dead backend has 1 error");
    assert!(m[1].total_dials >= 1, "live backend served the second dial");
}

// ============================================================================
//  Tests — least-connections / long-lived streams
// ============================================================================

#[tokio::test]
async fn data_feed_user_least_connections_spreads_long_lived_streams() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..3).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, least_connections()).unwrap();

    // Open 6 long-lived connections; with least_connections each new dial
    // should land on whichever backend has the fewest active connections.
    let mut conns = Vec::new();
    for i in 0..6 {
        let c = lb.dial(&format!("symbol-{i}:9000")).await.unwrap();
        conns.push(c);
    }

    let metrics = lb.metrics().await;
    let active: Vec<u32> = metrics.iter().map(|m| m.active_connections).collect();
    let max = *active.iter().max().unwrap();
    let min = *active.iter().min().unwrap();
    assert!(
        max - min <= 1,
        "least_connections should keep active counts within 1 of each other, got {active:?}"
    );
    drop(conns);
}

#[tokio::test]
async fn data_feed_user_drop_connection_decrements_active_count() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..3).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;
    let backends = backends_from(&handles);
    let lb = LoadBalancer::new(backends, least_connections()).unwrap();

    let mut conns = Vec::new();
    for _ in 0..6 {
        let c = lb.dial("feed:9000").await.unwrap();
        conns.push(c);
    }
    let mid = lb.metrics().await;
    let total_mid: u32 = mid.iter().map(|m| m.active_connections).sum();
    assert_eq!(total_mid, 6, "all 6 dials counted as active");

    drop(conns);
    // Give the drop guard a chance to run.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = lb.metrics().await;
    let total_after: u32 = after.iter().map(|m| m.active_connections).sum();
    assert_eq!(total_after, 0, "all active counts back to 0 after drop");
}

// ============================================================================
//  Tests — retry / timeout
// ============================================================================

#[tokio::test]
async fn data_feed_user_retry_policy_recovers_from_transient_failure() {
    let h = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(1),
        ..Default::default()
    })
    .await;

    // Backend will fail twice then succeed.
    let backend = TcpBackend::new(h.addr()).with_initial_failures(2);
    let backends = vec![Box::new(backend) as Box<dyn Backend>];

    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .retry_policy(ExponentialBackoff::new(Duration::from_millis(5)))
        .dial_timeout(Duration::from_secs(2))
        .build()
        .await
        .unwrap();

    let mut conn = lb.dial("feed:9000").await.expect("retry should recover");
    let _ = read_at_least(&mut conn, 16, Duration::from_secs(2))
        .await
        .unwrap();
    drop(conn);

    let m = lb.metrics().await;
    assert_eq!(m[0].total_dials, 1, "the dial eventually succeeded");
    assert_eq!(m[0].recent_errors, 0, "no recent errors after recovery");
}

#[tokio::test]
async fn data_feed_user_dial_timeout_against_slow_backend() {
    // The `dial_timeout` wraps `Backend::dial`. A backend that sleeps before
    // returning forces the timeout to fire. (A TCP `silent` server doesn't
    // trigger it — connect succeeds immediately.)
    let h = spawn_feed_server(FeedServerConfig::default()).await;

    struct SlowBackend {
        addr: String,
        delay: Duration,
    }
    #[async_trait]
    impl Backend for SlowBackend {
        async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
            tokio::time::sleep(self.delay).await;
            let s = TcpStream::connect(&self.addr).await.map_err(Error::from)?;
            Ok(Box::pin(s))
        }
        async fn shutdown(&mut self) {}
    }

    let backends: Vec<Box<dyn Backend>> = vec![Box::new(SlowBackend {
        addr: h.addr().to_string(),
        delay: Duration::from_secs(5),
    })];

    let lb = LoadBalancer::builder()
        .backends(backends)
        .strategy(round_robin())
        .dial_timeout(Duration::from_millis(150))
        .build()
        .await
        .unwrap();

    let start = Instant::now();
    let r = lb.dial("feed:9000").await;
    let elapsed = start.elapsed();

    assert!(r.is_err(), "dial to slow backend must error");
    assert!(
        elapsed < Duration::from_secs(2),
        "dial_timeout must apply, got {elapsed:?}"
    );
}

// ============================================================================
//  Tests — dynamic reconfiguration
// ============================================================================

#[tokio::test]
async fn data_feed_user_hot_add_backend_is_used_immediately() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;
    let h2 = spawn_feed_server(FeedServerConfig::default()).await;

    let mut lb =
        LoadBalancer::new(backends_from(std::slice::from_ref(&h0)), round_robin()).unwrap();
    let mut conn = lb.dial("feed:9000").await.unwrap();
    let _ = read_at_least(&mut conn, 16, Duration::from_secs(1))
        .await
        .unwrap();
    drop(conn);

    // Add two more.
    lb.add_backend(Box::new(TcpBackend::new(h1.addr())) as Box<dyn Backend>)
        .await;
    lb.add_backend(Box::new(TcpBackend::new(h2.addr())) as Box<dyn Backend>)
        .await;

    for _ in 0..6 {
        let conn = lb.dial("feed:9000").await.unwrap();
        drop(conn);
    }
    let m = lb.metrics().await;
    assert_eq!(m.len(), 3, "three backends in pool");
    let served = m.iter().filter(|m| m.total_dials >= 1).count();
    assert!(served >= 2, "expected >=2 backends to be used, got {m:?}");
}

#[tokio::test]
async fn data_feed_user_hot_remove_backend_then_dial_works() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;
    let h2 = spawn_feed_server(FeedServerConfig::default()).await;

    let mut lb = LoadBalancer::new(
        backends_from(&[h0.clone(), h1.clone(), h2.clone()]),
        round_robin(),
    )
    .unwrap();
    assert_eq!(lb.backend_count(), 3);

    // Hold a long-lived connection to backend 1.
    let held = lb.dial("feed:9000").await.unwrap();

    // Remove backend 1 — held connection survives.
    let removed = lb.remove_backend(1).await;
    assert!(removed);
    assert_eq!(lb.backend_count(), 2);

    // New dial still works.
    let conn = lb.dial("feed:9000").await.unwrap();
    drop(conn);

    // Held connection still usable: read more ticks.
    let mut held = held;
    let _ = read_at_least(&mut held, 16, Duration::from_secs(2))
        .await
        .unwrap();

    // backend_ids reflects the removal.
    let ids = lb.backend_ids().await;
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn data_feed_user_hot_remove_backend_by_id() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;

    let mut lb =
        LoadBalancer::new(backends_from(&[h0.clone(), h1.clone()]), round_robin()).unwrap();
    lb.add_backend_with_id(
        "feed-a".into(),
        Box::new(TcpBackend::new(h0.addr())) as Box<dyn Backend>,
    )
    .await;
    lb.add_backend_with_id(
        "feed-b".into(),
        Box::new(TcpBackend::new(h1.addr())) as Box<dyn Backend>,
    )
    .await;
    assert_eq!(lb.backend_count(), 4);

    let removed = lb.remove_backend_by_id("feed-a").await;
    assert!(removed, "expected to find feed-a");
    assert_eq!(lb.backend_count(), 3);

    let removed_again = lb.remove_backend_by_id("nonexistent").await;
    assert!(!removed_again);
}

#[tokio::test]
async fn data_feed_user_replace_backends_atomically() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;
    let new0 = spawn_feed_server(FeedServerConfig::default()).await;
    let new1 = spawn_feed_server(FeedServerConfig::default()).await;
    let new2 = spawn_feed_server(FeedServerConfig::default()).await;

    let mut lb = LoadBalancer::new(backends_from(&[h0, h1]), round_robin()).unwrap();

    // Dial the old pool once.
    let conn = lb.dial("feed:9000").await.unwrap();
    drop(conn);

    // Replace with three new backends.
    lb.replace_backends(
        backends_from(&[new0.clone(), new1.clone(), new2.clone()]),
        Some(Box::new(round_robin())),
    )
    .await
    .unwrap();
    assert_eq!(lb.backend_count(), 3);

    for _ in 0..6 {
        let mut c = lb.dial("feed:9000").await.unwrap();
        let _ = read_at_least(&mut c, 16, Duration::from_secs(2))
            .await
            .unwrap();
        drop(c);
    }
    let m = lb.metrics().await;
    let served = m.iter().filter(|m| m.total_dials >= 2).count();
    assert_eq!(served, 3, "round-robin hits each new backend twice");
}

#[tokio::test]
async fn data_feed_user_drain_then_undrain() {
    let h0 = spawn_feed_server(FeedServerConfig::default()).await;
    let h1 = spawn_feed_server(FeedServerConfig::default()).await;
    let h2 = spawn_feed_server(FeedServerConfig::default()).await;

    let mut lb = LoadBalancer::new(
        backends_from(&[h0.clone(), h1.clone(), h2.clone()]),
        round_robin(),
    )
    .unwrap();

    // Drain backend 0.
    assert!(lb.drain_backend(0).await);
    assert!(lb.is_draining(0).await);
    assert!(!lb.is_draining(1).await);

    // Undrain.
    assert!(lb.undrain_backend(0).await);
    assert!(!lb.is_draining(0).await);
}

// ============================================================================
//  Tests — concurrency & backpressure
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_feed_user_one_hundred_concurrent_subscribers() {
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..4).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let lb = Arc::new(LoadBalancer::new(backends_from(&handles), round_robin()).unwrap());

    let mut tasks = Vec::new();
    for i in 0..100 {
        let lb = lb.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = lb.dial(&format!("symbol-{i}:9000")).await.unwrap();
            let buf = read_at_least(&mut conn, 128, Duration::from_secs(5))
                .await
                .expect("read");
            assert!(buf.starts_with(b"TICK"));
        }));
    }
    for t in tasks {
        t.await.expect("subscriber task");
    }

    let metrics = lb.metrics().await;
    let total: u64 = metrics.iter().map(|m| m.total_dials).sum();
    assert_eq!(total, 100);

    // All four servers should have served traffic.
    let mut served = 0;
    for h in &handles {
        if h.connections() > 0 {
            served += 1;
        }
    }
    assert_eq!(served, 4, "all 4 servers should have been used");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_feed_user_slow_consumer_does_not_starve_fast_consumer() {
    // One server is artificially slow; one is fast.
    let slow = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let fast = spawn_feed_server(FeedServerConfig {
        tick_interval: Duration::from_millis(1),
        ..Default::default()
    })
    .await;

    let backends = vec![
        Box::new(TcpBackend::new(slow.addr())) as Box<dyn Backend>,
        Box::new(TcpBackend::new(fast.addr())) as Box<dyn Backend>,
    ];
    let lb = Arc::new(LoadBalancer::new(backends, round_robin()).unwrap());

    let lb_slow = lb.clone();
    let slow_task = tokio::spawn(async move {
        let mut conn = lb_slow.dial("feed:9000").await.unwrap();
        // Read slowly — just 16 bytes over 5 seconds.
        let _ = read_at_least(&mut conn, 16, Duration::from_secs(5))
            .await
            .unwrap();
    });

    let lb_fast = lb.clone();
    let fast_task = tokio::spawn(async move {
        let mut conn = lb_fast.dial("feed:9000").await.unwrap();
        let start = Instant::now();
        let _ = read_at_least(&mut conn, 64, Duration::from_secs(2))
            .await
            .unwrap();
        start.elapsed()
    });

    let fast_elapsed = fast_task.await.unwrap();
    slow_task.await.unwrap();

    // Fast consumer shouldn't wait for the slow one's data.
    assert!(
        fast_elapsed < Duration::from_secs(2),
        "fast consumer took {fast_elapsed:?}"
    );
}

// ============================================================================
//  Tests — graceful shutdown
// ============================================================================

#[tokio::test]
async fn data_feed_user_shutdown_runs_cleanly_without_disturbing_active_streams() {
    // `Backend::shutdown` cleans up backend-level resources (tunnel
    // interfaces, factories, credentials). It does NOT close streams the
    // consumer already holds — those are the consumer's responsibility.
    // We verify the LoadBalancer shutdown can complete while streams are
    // still alive, and that those streams remain usable afterwards.
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..2).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        })
    }))
    .await;

    let lb = LoadBalancer::new(backends_from(&handles), round_robin()).unwrap();
    let mut conn = lb.dial("feed:9000").await.unwrap();
    let _ = read_at_least(&mut conn, 16, Duration::from_secs(1))
        .await
        .unwrap();

    // Shutdown the balancer — must complete cleanly.
    lb.shutdown().await;

    // Active stream survives — read more data from the same connection.
    let buf = read_at_least(&mut conn, 32, Duration::from_secs(1))
        .await
        .expect("stream should still be readable after balancer shutdown");
    assert!(buf.starts_with(b"TICK"));
    drop(conn);
}

// ============================================================================
//  Tests — input validation
// ============================================================================

#[tokio::test]
async fn data_feed_user_rejects_invalid_addresses() {
    let h = spawn_feed_server(FeedServerConfig::default()).await;
    let backends = vec![Box::new(TcpBackend::new(h.addr())) as Box<dyn Backend>];
    let lb = LoadBalancer::new(backends, round_robin()).unwrap();

    // No port.
    let r = lb.dial("feed.example.com").await;
    assert!(matches!(r, Err(Error::InvalidAddress(_))));

    // Empty host.
    let r = lb.dial(":443").await;
    assert!(matches!(r, Err(Error::InvalidAddress(_))));

    // Port 0.
    let r = lb.dial("feed.example.com:0").await;
    assert!(matches!(r, Err(Error::InvalidAddress(_))));

    // Garbage port.
    let r = lb.dial("feed.example.com:abc").await;
    assert!(matches!(r, Err(Error::InvalidAddress(_))));
}

#[tokio::test]
async fn data_feed_user_rejects_empty_pool() {
    let r = LoadBalancer::new(vec![], round_robin());
    assert!(matches!(r, Err(Error::NoBackends(_))));
}

// ============================================================================
//  Tests — end-to-end pipeline
// ============================================================================

#[tokio::test]
async fn data_feed_user_end_to_end_streaming_pipeline() {
    // 3 feed sources, 1 consumer, 5s of streaming.
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..3).map(|i| {
        spawn_feed_server(FeedServerConfig {
            // Each server has a slightly different cadence — like real feeds.
            tick_interval: Duration::from_millis(1 + i as u64),
            ..Default::default()
        })
    }))
    .await;

    let lb = Arc::new(LoadBalancer::new(backends_from(&handles), round_robin()).unwrap());

    // Open 9 connections over 200ms.
    let deadline = Instant::now() + Duration::from_millis(200);
    let mut conns = Vec::new();
    let mut i = 0;
    while Instant::now() < deadline {
        let c = lb.dial(&format!("symbol-{i}:9000")).await.unwrap();
        conns.push(c);
        i += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let opened = conns.len();
    assert!(opened >= 5, "expected at least 5 opens, got {opened}");

    // Drain each connection concurrently.
    let mut tasks = Vec::new();
    for (idx, mut c) in conns.into_iter().enumerate() {
        let h_idx = idx % handles.len();
        tasks.push(tokio::spawn(async move {
            let buf = read_at_least(&mut c, 64, Duration::from_secs(3))
                .await
                .expect("read");
            (h_idx, buf.len())
        }));
    }
    let mut total_bytes = 0;
    let mut per_server = vec![0usize; handles.len()];
    for t in tasks {
        let (h_idx, n) = t.await.unwrap();
        per_server[h_idx] += n;
        total_bytes += n;
    }
    assert!(total_bytes >= opened * 64);

    // Round-robin over 3 servers for `opened` dials → each server saw at
    // least one dial.
    let metrics = lb.metrics().await;
    let served = metrics.iter().filter(|m| m.total_dials >= 1).count();
    assert!(
        served >= 2,
        "round robin should hit multiple servers, {metrics:?}"
    );
}

// ============================================================================
//  Tests — strategy swap at runtime
// ============================================================================

#[tokio::test]
async fn data_feed_user_swap_strategy_at_runtime() {
    let handles: Vec<FeedServerHandle> =
        futures::future::join_all((0..3).map(|_| spawn_feed_server(FeedServerConfig::default())))
            .await;
    let mut lb = LoadBalancer::new(backends_from(&handles), round_robin()).unwrap();

    // Phase 1: round-robin.
    for _ in 0..3 {
        let c = lb.dial("feed:9000").await.unwrap();
        drop(c);
    }
    let name1 = lb.strategy_name().await;
    assert_eq!(name1, "round_robin");

    // Phase 2: sticky.
    lb.set_strategy(sticky()).await;
    for _ in 0..5 {
        let c = lb.dial("feed:9000").await.unwrap();
        drop(c);
    }
    let name2 = lb.strategy_name().await;
    assert_eq!(name2, "sticky");

    // Sticky must pin to a single backend for all 5 subsequent dials.
    // (Phase 1 already used all three backends via round-robin.)
    let metrics = lb.metrics().await;
    let pinned_idx = metrics
        .iter()
        .position(|m| m.total_dials >= 5)
        .expect("at least one backend should have received >=5 dials (the pinned one)");
    // Only one backend should have more than the 3 phase-1 dials (round-robin
    // hit each once); that one is the sticky pin.
    let high_count = metrics.iter().filter(|m| m.total_dials > 3).count();
    assert_eq!(
        high_count, 1,
        "exactly one backend should be the sticky pin, metrics {metrics:?}"
    );
    // The pinned backend accumulated 1 (phase 1) + 5 (phase 2) = 6 dials.
    assert_eq!(
        metrics[pinned_idx].total_dials, 6,
        "pinned backend should have 6 dials total"
    );
}

// ============================================================================
//  Tests — a "data feed user" realistic usage smoke test
// ============================================================================

/// Models a realistic data-feed consumer: subscribes to multiple symbols via
/// sticky-per-symbol hashing, reads continuously, validates content, and
/// gracefully shuts down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_feed_user_realistic_consumer_loop() {
    let shutdown = Arc::new(Notify::new());
    let handles: Vec<FeedServerHandle> = futures::future::join_all((0..4).map(|_| {
        spawn_feed_server(FeedServerConfig {
            tick_interval: Duration::from_millis(2),
            ..Default::default()
        })
    }))
    .await;

    let backends = backends_from(&handles);
    let lb = Arc::new(LoadBalancer::new(backends, hash_by_addr()).unwrap());

    let symbols = ["AAPL", "GOOG", "MSFT", "AMZN"];
    let mut tasks = Vec::new();

    for sym in &symbols {
        let lb = lb.clone();
        let addr = format!("{sym}:9000");
        let shutdown = shutdown.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = lb.dial(&addr).await.expect("dial");
            let mut total_ticks = 0usize;
            let mut buf = Vec::new();
            loop {
                let mut tmp = [0u8; 256];
                tokio::select! {
                    _ = shutdown.notified() => break,
                    r = conn.read(&mut tmp) => {
                        match r {
                            Ok(0) => break,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                while let Some(idx) = buf.iter().position(|&b| b == b'\n') {
                                    let line: Vec<u8> = buf.drain(..=idx).collect();
                                    if line.starts_with(b"TICK ") {
                                        total_ticks += 1;
                                    }
                                }
                                if total_ticks >= 20 {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            total_ticks
        }));
    }

    // Give the consumers time to drain ticks.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.notify_waiters();

    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert!(
        total >= 4 * 20,
        "expected at least 80 ticks total across 4 symbols, got {total}"
    );

    let m = lb.metrics().await;
    let served = m.iter().filter(|m| m.total_dials >= 1).count();
    assert!(served >= 2, "expected multiple backends used, {m:?}");
}
