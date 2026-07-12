# rota

**Generic load balancer over a pool of backends.** Distribute outbound traffic
across N parallel connections with pluggable strategies (round-robin,
lowest-RTT, least-connections, hash-by-addr, weighted, failover, sticky,
health-weighted).

## What it does

`rota` provides a `LoadBalancer` that owns N "backends" (anything that
implements `dial() -> Stream`) and picks one for each connection based on a
configurable strategy. Built-in strategies cover the common cases:
round-robin, random, lowest-RTT, least-connections, hash-by-addr, weighted
round-robin, failover, health-weighted, and sticky.

It's deliberately generic — `Backend` and `Stream` are minimal traits, so
`rota` works with any backend that can hand back a `tokio::io` stream:

- VPN tunnels (WireGuard, OpenVPN, IPsec, Nym mixnet, etc.)
- SSH tunnels
- HTTP CONNECT proxies
- SOCKS5 proxies
- Database connection pools
- API endpoint pools (e.g. multiple accounts / rate-limited keys)
- Any combination of the above

## Quick start

```rust
use std::pin::Pin;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, duplex};
use rota::{Backend, Stream, LoadBalancer, round_robin, Error};

/// A trivial backend: each dial returns a fresh in-memory duplex.
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
async fn main() {
    let backends: Vec<Box<dyn Backend>> = (0..3)
        .map(|_| Box::new(DuplexBackend) as Box<dyn Backend>)
        .collect();

    let lb = LoadBalancer::new(backends, round_robin()).unwrap();
    let mut stream = lb.dial("example.com:443").await.unwrap();

    use tokio::io::AsyncWriteExt;
    stream.write_all(b"hello\n").await.unwrap();
    lb.shutdown().await;
}
```

## Strategies

| Strategy | State | Best for |
|---|---|---|
| [`RoundRobin`](crate::strategies::RoundRobin) | `next: usize` | Default. Even distribution, no metrics. |
| [`Random`](crate::strategies::Random) | none | Stateless fallback |
| [`LowestRtt`](crate::strategies::LowestRtt) | none | Latency-sensitive workloads |
| [`LeastConnections`](crate::strategies::LeastConnections) | none | Long-lived heterogeneous streams |
| [`HashByAddr`](crate::strategies::HashByAddr) | none | HTTP keep-alive / sticky connections |
| [`WeightedRoundRobin`](crate::strategies::WeightedRoundRobin) | precomputed sequence | RTT-aware round-robin |
| [`Failover`](crate::strategies::Failover) | `primary: usize` | "Use the best, N-1 standbys" |
| [`HealthWeighted`](crate::strategies::HealthWeighted) | none | Smart default once you have dial history |
| [`Sticky`](crate::strategies::Sticky) | `pinned: Option<usize>` | Pin to one backend forever |

Free constructors: [`round_robin`](crate::round_robin), [`random`](crate::random),
[`lowest_rtt`](crate::lowest_rtt), [`least_connections`](crate::least_connections),
[`hash_by_addr`](crate::hash_by_addr), [`weighted_round_robin`](crate::weighted_round_robin),
[`failover`](crate::failover), [`health_weighted`](crate::health_weighted),
[`sticky`](crate::sticky).

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

Implement [`BalanceStrategy`] and pass it in. The strategy receives a
[`PoolView`] with the dial address and live per-backend metrics.

```rust
use rota::{BalanceStrategy, PoolView, BackendMetrics};

struct AlwaysBackendZero;

impl BalanceStrategy for AlwaysBackendZero {
    fn pick(&mut self, _: &PoolView<'_>) -> usize { 0 }
    fn name(&self) -> &str { "always_zero" }
}

let lb = LoadBalancer::new(backends, AlwaysBackendZero)?;
```

## Use cases

- **NymVPN** — N parallel WireGuard tunnels through different gateways (see
  the `nym-lb` companion crate)
- **API key rotation** — pool of accounts / rate-limited keys, distribute
  requests to the one with most headroom (similar to
  [`shunt-proxy`](https://crates.io/crates/shunt-proxy) but as a library)
- **Multi-path VPN** — distribute across WiFi + LTE + Ethernet (similar to
  [`triglav`](https://crates.io/crates/triglav) but as a library)
- **Database read replicas** — load-balance reads across N replicas
- **SSH bastion rotation** — fail over between bastion hosts

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at
your option.

## Contributing

Patches welcome. Run `cargo test` and `cargo clippy` before opening a PR.
