use crate::constants::STRATEGY_NAMES;
use crate::enums::ffi::FfiStrategy;
use crate::strategies::{
    Failover, HashByAddr, HealthWeighted, LeastConnections, LowestRtt, Random, RoundRobin, Sticky,
    WeightedRoundRobin,
};
use crate::traits::strategy::BalanceStrategy;

impl FfiStrategy {
    pub(super) fn build(self, backend_count: usize) -> Box<dyn BalanceStrategy + Send> {
        const _: () = assert!(
            STRATEGY_NAMES.len() == (FfiStrategy::Sticky as usize + 1),
            "STRATEGY_NAMES length must match FfiStrategy variant count"
        );
        match self {
            Self::RoundRobin => Box::new(RoundRobin::new()),
            Self::Random => Box::new(Random::new()),
            Self::LowestRtt => Box::new(LowestRtt::new()),
            Self::LeastConnections => Box::new(LeastConnections::new()),
            Self::HashByAddr => Box::new(HashByAddr::new()),
            Self::WeightedRoundRobin => Box::new(WeightedRoundRobin::new()),
            Self::Failover => Box::new(Failover::with_backend_count(backend_count)),
            Self::HealthWeighted => Box::new(HealthWeighted::new()),
            Self::Sticky => Box::new(Sticky::new()),
        }
    }
}
