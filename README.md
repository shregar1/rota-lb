# rota-lb

[![Crates.io](https://img.shields.io/crates/v/rota-lb.svg)](https://crates.io/crates/rota-lb)
[![Docs.rs](https://img.shields.io/docsrs/rota-lb)](https://docs.rs/rota-lb)
[![CI](https://github.com/shregar1/rota-lb/actions/workflows/ci.yml/badge.svg)](https://github.com/shregar1/rota-lb/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://opensource.org/licenses)

**Generic load balancer over a pool of backends.** Distribute outbound TCP connections across N parallel backends with pluggable strategies. No sidecar required.

```rust
use rota_lb::{LoadBalancer, round_robin};

let lb = LoadBalancer::new(backends, round_robin())?;
let mut stream = lb.dial("example.com:443").await?;
```

## Features

- **9 strategies** — round-robin, random, sticky, hash-by-addr, lowest-RTT, least-connections, weighted round-robin, failover, health-weighted
- **Retry & backoff** — pluggable policies (exponential backoff, fixed retry, no retry)
- **`tower::Service`** — use with Tower middleware stacks
- **Consul discovery** — auto-sync backends from Consul (optional `discovery` feature)
- **TLS** — wrap backends in TLS (optional `tls` feature)
- **Dynamic reconfiguration** — add, remove, replace, or drain backends at runtime
- **No sidecar** — embed directly in your Rust binary

## Installation

```toml
[dependencies]
rota-lb = "0.1"
```

## Quick start

```rust
use std::pin::Pin;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, duplex};
use rota_lb::{Backend, Stream, LoadBalancer, round_robin, Error};

struct DuplexBackend;

#[async_trait]
impl Backend for DuplexBackend {
    async fn dial(&self, _addr: &str) -> Result<Stream, Error> {
        let (a, _b) = duplex(1024);
        Ok(Box::pin(a))
    }
    async fn shutdown(self: Box<Self>) {}
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let backends: Vec<Box<dyn Backend>> = (0..3)
        .map(|_| Box::new(DuplexBackend) as Box<dyn Backend>)
        .collect();

    let lb = LoadBalancer::new(backends, round_robin())?;
    let mut stream = lb.dial("example.com:443").await?;

    use tokio::io::AsyncWriteExt;
    stream.write_all(b"hello\n").await?;

    lb.shutdown().await;
    Ok(())
}
```

## Strategies

| Strategy | State | Best for |
|---|---|---|
| `RoundRobin` | `next: usize` | Default. Even distribution, no metrics. |
| `Random` | none | Stateless fallback |
| `LowestRtt` | none | Latency-sensitive workloads |
| `LeastConnections` | none | Long-lived heterogeneous streams |
| `HashByAddr` | none | HTTP keep-alive / sticky connections |
| `WeightedRoundRobin` | precomputed sequence | RTT-aware round-robin |
| `Failover` | `primary: usize` | "Use the best, N-1 standbys" |
| `HealthWeighted` | none | Smart default once you have dial history |
| `Sticky` | `pinned: Option<usize>` | Pin to one backend forever |

Free constructors: [`round_robin`], [`random`], [`lowest_rtt`], [`least_connections`], [`hash_by_addr`], [`weighted_round_robin`], [`failover`], [`health_weighted`], [`sticky`].

## Architecture

```
            ┌──────────────────────┐
            │   BalanceStrategy    │
            │   pick(&PoolView)    │
            └──────────┬───────────┘
                       │ impls (9 strategies)
            ┌──────────▼───────────┐         ┌─────────────────┐
            │     LoadBalancer     │  owns   │ Vec<Box<dyn    │
            │   dial() → stream    │────────▶│   Backend>>    │
            │   shutdown()         │         └────────┬────────┘
            └──────────┬───────────┘                  │ uses
                       │ tracks                       ▼
            ┌──────────▼───────────┐         ┌─────────────────┐
            │   Vec<BackendMetrics>│         │ Backend trait   │
            │   rtt, conns, errs   │         │ dial(addr)      │
            └──────────────────────┘         │ shutdown()      │
                                             └─────────────────┘
```

## Two ways to wire it up

**Direct** — you already have the backends:

```rust
let backends: Vec<Box<dyn Backend>> = vec![/* ... */];
let lb = LoadBalancer::new(backends, strategy)?;
```

**Factory** — backends need to be created (e.g. with credentials, network I/O):

```rust
let factories: Vec<Box<dyn BackendFactory>> = vec![/* ... */];
let lb = LoadBalancer::from_factories(factories, strategy).await?;
```

## Custom strategies

Implement [`BalanceStrategy`] and pass it in. The strategy receives a [`PoolView`] with the dial address and live per-backend metrics.

```rust
use rota_lb::{BalanceStrategy, PoolView, BackendMetrics};

struct AlwaysBackendZero;

impl BalanceStrategy for AlwaysBackendZero {
    fn pick(&mut self, _: &PoolView<'_>) -> usize { 0 }
    fn name(&self) -> &str { "always_zero" }
}

let lb = LoadBalancer::new(backends, AlwaysBackendZero)?;
```

## Use cases

- **Multi-path VPN** — distribute across WiFi + LTE + Ethernet
- **API key rotation** — pool of rate-limited accounts, pick the one with most headroom
- **Database read replicas** — load-balance reads across N replicas
- **SSH bastion rotation** — fail over between bastion hosts
- **Tunnel aggregation** — N parallel WireGuard/SSH/SOCKS5 tunnels

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## Contributing

Patches welcome. Run `cargo test` and `cargo clippy` before opening a PR.
