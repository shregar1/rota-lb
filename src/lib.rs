#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    rust_2018_idioms,
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_import_braces,
    unused_qualifications,
    variant_size_differences
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

//! `rota` — generic load balancer over a pool of backends.
//!
//! Distribute outbound traffic across N parallel backends with pluggable
//! strategies. Works with any backend that implements the [`Backend`] trait
//! — VPN tunnels, SSH tunnels, HTTP CONNECT proxies, SOCKS5 proxies,
//! database connection pools, API endpoint pools, etc.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::pin::Pin;
//! use async_trait::async_trait;
//! use tokio::io::{duplex, AsyncRead, AsyncWrite};
//! use rota::{Backend, Connection, LoadBalancer, round_robin, Error};
//!
//! struct DuplexBackend;
//!
//! #[async_trait]
//! impl Backend for DuplexBackend {
//!     async fn dial(&self, _addr: &str) -> Result<Connection, Error> {
//!         let (a, _b) = duplex(1024);
//!         Ok(Box::pin(a))
//!     }
//!     async fn shutdown(self: Box<Self>) {}
//! }
//!
//! # async fn example() -> Result<(), rota::Error> {
//! let backends: Vec<Box<dyn Backend>> = (0..3)
//!     .map(|_| Box::new(DuplexBackend) as Box<dyn Backend>)
//!     .collect();
//! let lb = LoadBalancer::new(backends, round_robin())?;
//! let mut conn = lb.dial("example.com:443").await?;
//! // ... use conn as a tokio AsyncRead+AsyncWrite ...
//! lb.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Strategies
//!
//! Nine built-in strategies. Each is a concrete type that implements
//! [`BalanceStrategy`]. The free functions ([`round_robin`], [`random`], etc.)
//! return `Box<dyn BalanceStrategy>` for convenience.
//!
//! | Strategy | Use when |
//! |---|---|
//! | [`RoundRobin`] | Default. Even distribution, no metrics. |
//! | [`Random`] | Stateless fallback |
//! | [`LowestRtt`] | Latency-sensitive (gaming, voice) |
//! | [`LeastConnections`] | Long-lived heterogeneous streams |
//! | [`HashByAddr`] | HTTP keep-alive / connection caching |
//! | [`WeightedRoundRobin`] | RTT-aware round-robin |
//! | [`Failover`] | "Use the best, N-1 standbys"; rotates on dial error |
//! | [`HealthWeighted`] | Smart default once you have dial history |
//! | [`Sticky`] | Pin to one backend forever |
//!
//! ## Two ways to wire it up
//!
//! - [`LoadBalancer::new`] — caller provides pre-constructed backends
//! - [`LoadBalancer::from_factories`] — caller provides factories; the
//!   balancer constructs each backend via `BackendFactory::create`
//!
//! ## License
//!
//! Dual-licensed under MIT or Apache-2.0 at your option.

mod balancer;
mod backend;
mod constants;
mod error;
mod factory;
mod health;
mod retry;
mod strategies;
mod strategy;

// Public re-exports.
pub use balancer::{GuardedConnection, LoadBalancer};
pub use backend::{Backend, Connection};
pub use constants::*;
pub use error::Error;
pub use factory::{BackendFactory, BackendOutput};
pub use health::{HealthCheckConfig, HealthChecker, HealthState, is_healthy, record_dial_result};
pub use retry::{ExponentialBackoff, FixedRetry, NoRetry, RetryOnError, RetryPolicy, RetryPolicyBuilder, is_transient_error};
pub use strategy::{BalanceStrategy, PoolView, TunnelMetrics};
pub use strategies::{
    Failover, HashByAddr, HealthWeighted, LeastConnections, LowestRtt, Random, RoundRobin,
    Sticky, WeightedRoundRobin,
};

// Free constructors returning Box<dyn BalanceStrategy>.
pub use strategies::{
    failover, hash_by_addr, health_weighted, least_connections, lowest_rtt, random, round_robin,
    sticky, weighted_round_robin,
};