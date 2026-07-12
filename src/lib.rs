//! `rota` — generic load balancer over a pool of backends.
//!
//! Distribute outbound traffic across N parallel backends with pluggable
//! strategies. Works with any backend that implements the [`Tunnel`] trait
//! — VPN tunnels, SSH tunnels, HTTP CONNECT proxies, SOCKS5 proxies,
//! database connection pools, API endpoint pools, etc.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::pin::Pin;
//! use async_trait::async_trait;
//! use tokio::io::{duplex, AsyncRead, AsyncWrite};
//! use rota::{Tunnel, Stream, LoadBalancer, round_robin, Error};
//!
//! struct DuplexTunnel;
//!
//! #[async_trait]
//! impl Tunnel for DuplexTunnel {
//!     async fn dial(&self, _addr: &str) -> Result<Stream, Error> {
//!         let (a, _b) = duplex(1024);
//!         Ok(Box::pin(a))
//!     }
//!     async fn shutdown(self: Box<Self>) {}
//! }
//!
//! # async fn example() -> Result<(), rota::Error> {
//! let tunnels: Vec<Box<dyn Tunnel>> = (0..3)
//!     .map(|_| Box::new(DuplexTunnel) as Box<dyn Tunnel>)
//!     .collect();
//! let lb = LoadBalancer::new(tunnels, round_robin())?;
//! let mut stream = lb.dial("example.com:443").await?;
//! // ... use stream as a tokio AsyncRead+AsyncWrite ...
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
//! | [`Sticky`] | Pin to one tunnel forever |
//!
//! ## Two ways to wire it up
//!
//! - [`LoadBalancer::new`] — caller provides pre-constructed tunnels
//! - [`LoadBalancer::from_factories`] — caller provides factories; the
//!   balancer constructs each tunnel via `TunnelFactory::create`
//!
//! ## License
//!
//! Dual-licensed under MIT or Apache-2.0 at your option.

mod balancer;
mod error;
mod factory;
mod strategies;
mod strategy;
mod tunnel;

// Public re-exports.
pub use balancer::{GuardedStream, LoadBalancer};
pub use error::Error;
pub use factory::{FactoryOutput, TunnelFactory};
pub use strategies::{
    Failover, HashByAddr, HealthWeighted, LeastConnections, LowestRtt, Random, RoundRobin,
    Sticky, WeightedRoundRobin,
};
pub use strategy::{BalanceStrategy, PoolView, TunnelMetrics};
pub use tunnel::{Stream, Tunnel};

// Free constructors returning Box<dyn BalanceStrategy>.
pub use strategies::{
    failover, hash_by_addr, health_weighted, least_connections, lowest_rtt, random, round_robin,
    sticky, weighted_round_robin,
};
