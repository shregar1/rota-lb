#![allow(missing_docs)]

/// Every load-balancing strategy exposed through the C ABI.
///
/// Add a new variant here, then add the corresponding `return` in `build()`
/// and `name()`. The C-side `strategy_kind` integer (`i32`) must match the
/// discriminant value exactly.
#[repr(i32)]
#[derive(Clone, Copy, Debug)]
pub enum FfiStrategy {
    RoundRobin = 0,
    Random = 1,
    LowestRtt = 2,
    LeastConnections = 3,
    HashByAddr = 4,
    WeightedRoundRobin = 5,
    Failover = 6,
    HealthWeighted = 7,
    Sticky = 8,
}

impl FfiStrategy {
    pub const fn from_i32(n: i32) -> Option<Self> {
        match n {
            0 => Some(Self::RoundRobin),
            1 => Some(Self::Random),
            2 => Some(Self::LowestRtt),
            3 => Some(Self::LeastConnections),
            4 => Some(Self::HashByAddr),
            5 => Some(Self::WeightedRoundRobin),
            6 => Some(Self::Failover),
            7 => Some(Self::HealthWeighted),
            8 => Some(Self::Sticky),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::Random => "random",
            Self::LowestRtt => "lowest_rtt",
            Self::LeastConnections => "least_connections",
            Self::HashByAddr => "hash_by_addr",
            Self::WeightedRoundRobin => "weighted_round_robin",
            Self::Failover => "failover",
            Self::HealthWeighted => "health_weighted",
            Self::Sticky => "sticky",
        }
    }
}
