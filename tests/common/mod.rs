//! Shared mock helpers for `data_feed_user*` integration tests.
//!
//! Each integration-test file under `tests/` is its own crate, so to avoid
//! duplicating the mock-server scaffolding, the helpers live here. Each
//! consuming file declares `mod common;` and `use common::*;`.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use rota_lb::backend::{Backend, Connection};
use rota_lb::error::Error;

// ============================================================================
//  Mock feed server
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FeedServerHandle {
    pub addr: String,
    pub connections: Arc<AtomicU32>,
    pub ticks_sent: Arc<AtomicU64>,
    pub bytes_sent: Arc<AtomicU64>,
    pub dial_failures: Arc<AtomicU32>,
    pub accept_new: Arc<AtomicBool>,
}

impl FeedServerHandle {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    #[allow(dead_code)]
    pub fn connections(&self) -> u32 {
        self.connections.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn ticks_sent(&self) -> u64 {
        self.ticks_sent.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn stop_accepting(&self) {
        self.accept_new.store(false, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    pub fn allow_again(&self) {
        self.accept_new.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FeedServerConfig {
    /// Delay between ticks.
    pub tick_interval: Duration,
    /// If `Some(n)`, server drops the connection after sending `n` ticks.
    pub drop_after: Option<u64>,
    /// If true, server holds the connection open without sending anything.
    pub silent: bool,
    /// If `Some(n)`, server refuses connections once `n` are already open.
    pub max_connections: Option<u32>,
    /// Optional closure-style hook called once when a connection is opened.
    /// Stored as `fn()` to avoid lifetime plumbing in tests.
    pub on_connect: Option<fn()>,
}

/// Spawn a mock feed server. Returns a handle with counters.
pub async fn spawn_feed_server(config: FeedServerConfig) -> FeedServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock feed server");
    let addr = listener.local_addr().expect("local_addr").to_string();

    let connections = Arc::new(AtomicU32::new(0));
    let ticks_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let dial_failures = Arc::new(AtomicU32::new(0));
    let accept_new = Arc::new(AtomicBool::new(true));

    let handle = FeedServerHandle {
        addr: addr.clone(),
        connections: connections.clone(),
        ticks_sent: ticks_sent.clone(),
        bytes_sent: bytes_sent.clone(),
        dial_failures: dial_failures.clone(),
        accept_new: accept_new.clone(),
    };

    tokio::spawn(async move {
        loop {
            if !accept_new.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }

            let (sock, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => {
                    dial_failures.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
            };

            if let Some(max) = config.max_connections {
                let prev = connections.fetch_add(1, Ordering::SeqCst);
                if prev + 1 > max {
                    connections.fetch_sub(1, Ordering::SeqCst);
                    drop(sock);
                    continue;
                }
            } else {
                connections.fetch_add(1, Ordering::SeqCst);
            }

            let cfg = config;
            let ticks_sent = ticks_sent.clone();
            let bytes_sent = bytes_sent.clone();
            tokio::spawn(async move {
                let _ = feed_one_subscriber(sock, cfg, ticks_sent, bytes_sent).await;
            });
        }
    });

    handle
}

async fn feed_one_subscriber(
    mut sock: TcpStream,
    cfg: FeedServerConfig,
    ticks_sent: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
) -> std::io::Result<()> {
    if let Some(cb) = cfg.on_connect {
        cb();
    }
    if cfg.silent {
        let mut buf = [0u8; 64];
        let _ = sock.read(&mut buf).await;
        return Ok(());
    }

    let mut seq: u64 = 0;
    loop {
        if let Some(limit) = cfg.drop_after {
            if seq >= limit {
                let _ = sock.shutdown().await;
                return Ok(());
            }
        }
        let line = format!("TICK {seq}\n");
        sock.write_all(line.as_bytes()).await?;
        let n = line.len() as u64;
        ticks_sent.fetch_add(1, Ordering::SeqCst);
        bytes_sent.fetch_add(n, Ordering::SeqCst);
        seq += 1;
        tokio::time::sleep(cfg.tick_interval).await;
    }
}

// ============================================================================
//  TCP backend
// ============================================================================

/// A `Backend` that dials a specific TCP address.
#[derive(Debug)]
pub struct TcpBackend {
    pub addr: String,
    pub dial_count: Arc<AtomicUsize>,
    pub fail_count: Arc<AtomicU32>,
    pub shutdown_count: Arc<AtomicU32>,
}

impl TcpBackend {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            dial_count: Arc::new(AtomicUsize::new(0)),
            fail_count: Arc::new(AtomicU32::new(0)),
            shutdown_count: Arc::new(AtomicU32::new(0)),
        }
    }

    #[allow(dead_code)]
    pub fn with_initial_failures(self, n: u32) -> Self {
        self.fail_count.store(n, Ordering::SeqCst);
        self
    }

    #[allow(dead_code)]
    pub fn shutdown_calls(&self) -> u32 {
        self.shutdown_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Backend for TcpBackend {
    async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
        self.dial_count.fetch_add(1, Ordering::SeqCst);
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::backend(format!("{}: simulated failure", self.addr)));
        }
        let stream = TcpStream::connect(&self.addr).await.map_err(Error::from)?;
        Ok(Box::pin(stream))
    }

    async fn shutdown(&mut self) {
        self.shutdown_count.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn backends_from(handles: &[FeedServerHandle]) -> Vec<Box<dyn Backend>> {
    handles
        .iter()
        .map(|h| Box::new(TcpBackend::new(h.addr())) as Box<dyn Backend>)
        .collect()
}

// ============================================================================
//  Read helpers
// ============================================================================

/// Read at least `min_bytes` from `conn` or time out.
pub async fn read_at_least(
    conn: &mut rota_lb::GuardedConnection,
    min_bytes: usize,
    timeout: Duration,
) -> Result<Vec<u8>, &'static str> {
    let mut buf = Vec::with_capacity(min_bytes * 2);
    let deadline = std::time::Instant::now() + timeout;
    while buf.len() < min_bytes {
        let mut tmp = [0u8; 1024];
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("timeout");
        }
        let n = match tokio::time::timeout(remaining, conn.read(&mut tmp)).await {
            Ok(Ok(0)) => return Err("eof"),
            Ok(Ok(n)) => n,
            Ok(Err(_)) => return Err("io error"),
            Err(_) => return Err("timeout"),
        };
        buf.extend_from_slice(&tmp[..n]);
    }
    Ok(buf)
}
