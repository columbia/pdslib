use std::{fmt::Debug, hash::Hash, vec};

use serde::Serialize;

use crate::budget::traits::{Budget, FilterCapacities};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum FilterId<
    E = usize,  // Epoch ID
    U = String, // URI
> {
    /// Non-collusion per-querier filter
    Nc(E, U /* querier URI */),

    /// Collusion filter (tracks overall privacy loss)
    C(E),

    /// Quota filter regulating c-filter consumption per trigger_uri
    QTrigger(E, U /* trigger URI */),

    /// Quota filter regulating c-filter consumption per source_uri
    QSource(E, U /* source URI */),
}

/// Struct containing the default capacity for each type of filter.
#[derive(Debug, Clone, Serialize)]
pub struct StaticCapacities<FID, B> {
    pub nc: B,
    pub c: B,
    pub qtrigger: B,
    pub qsource: B,

    #[serde(skip_serializing)]
    _phantom: std::marker::PhantomData<FID>,
}

impl<FID, B> StaticCapacities<FID, B> {
    pub fn new(nc: B, c: B, qtrigger: B, qsource: B) -> Self {
        Self {
            nc,
            c,
            qtrigger,
            qsource,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: Budget, E, U> FilterCapacities for StaticCapacities<FilterId<E, U>, B> {
    type FilterId = FilterId<E, U>;
    type Budget = B;
    type Error = anyhow::Error;

    fn capacity(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        match filter_id {
            FilterId::Nc(..) => Ok(self.nc.clone()),
            FilterId::C(..) => Ok(self.c.clone()),
            FilterId::QTrigger(..) => Ok(self.qtrigger.clone()),
            FilterId::QSource(..) => Ok(self.qsource.clone()),
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
