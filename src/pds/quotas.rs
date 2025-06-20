use core::fmt;
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    vec,
};

use serde::Serialize;

use crate::{
    budget::traits::{Budget, FilterCapacities},
    events::traits::{EpochId, Uri},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum FilterId<E: EpochId = u64, U: Uri = String> {
    /// Non-collusion per-querier filter
    PerQuerier(E, U /* querier URI */),

    /// Collusion filter (tracks overall privacy loss)
    Global(E),

    /// Quota filter regulating Global filter consumption per trigger_uri
    TriggerQuota(E, U /* trigger URI */),

    /// Quota filter regulating Global filter consumption per source_uri
    SourceQuota(E, U /* source URI */),
}

impl<E: EpochId + Display, U: Uri + Display> fmt::Display for FilterId<E, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterId::PerQuerier(epoch_id, querier_uri) => {
                write!(f, "PerQuerier({epoch_id}, {querier_uri})")
            }
            FilterId::Global(epoch_id) => {
                write!(f, "Global({epoch_id})")
            }
            FilterId::TriggerQuota(epoch_id, trigger_uri) => {
                write!(f, "TriggerQuota({epoch_id}, {trigger_uri})")
            }
            FilterId::SourceQuota(epoch_id, source_uri) => {
                write!(f, "SourceQuota({epoch_id}, {source_uri})")
            }
        }
    }
}

/// Struct containing the default capacity for each type of filter.
#[derive(Debug, Clone, Serialize)]
pub struct StaticCapacities<FID, B> {
    pub per_querier: B,
    pub global: B,
    pub trigger_quota: B,
    pub source_quota: B,

    #[serde(skip_serializing)]
    _phantom: std::marker::PhantomData<FID>,
}

impl<FID, B> StaticCapacities<FID, B> {
    pub fn new(
        per_querier: B,
        global: B,
        trigger_quota: B,
        source_quota: B,
    ) -> Self {
        Self {
            per_querier,
            global,
            trigger_quota,
            source_quota,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: Budget, E: EpochId, U: Uri> FilterCapacities
    for StaticCapacities<FilterId<E, U>, B>
{
    type FilterId = FilterId<E, U>;
    type Budget = B;
    type Error = anyhow::Error;

    fn capacity(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        match filter_id {
            FilterId::PerQuerier(..) => Ok(self.per_querier.clone()),
            FilterId::Global(..) => Ok(self.global.clone()),
            FilterId::TriggerQuota(..) => Ok(self.trigger_quota.clone()),
            FilterId::SourceQuota(..) => Ok(self.source_quota.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PdsFilterStatus<FID> {
    /// No filter was out budget, the atomic check passed for this epoch
    Continue,

    /// At least one filter was out of budget, the atomic check failed for this
    /// epoch. The ids of out-of-budget filters are stored in a vector if they
    /// are known. If an unspecified error causes the atomic check to fail,
    /// the vector can be empty.
    OutOfBudget(Vec<FID>),
}

impl<FID> Default for PdsFilterStatus<FID> {
    fn default() -> Self {
        Self::OutOfBudget(vec![])
    }
}
