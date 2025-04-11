//! TODO: require Event Storage and Filter Storage to implement `new` with some
//! config param? Or make things a bit more generic, only for those who
//! implement the right traits.

use anyhow::{Context, Result};
use serde::{ser::SerializeStruct, Serialize};

use super::epoch_pds::StaticCapacities;
use crate::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::PureDPBudget,
        release_filter::PureDPBudgetReleaseFilter,
        traits::{Filter, FilterCapacities, FilterStatus, FilterStorage},
    },
    events::{hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent},
    pds::epoch_pds::{EpochPrivateDataService, FilterId},
    queries::ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
};

pub type PpaFilterId = FilterId<usize, String>;

pub type PpaCapacities = StaticCapacities<PpaFilterId, PureDPBudget>;

pub type PpaPds = EpochPrivateDataService<
    PpaFilterStorage,
    HashMapEventStorage<PpaEvent, PpaRelevantEventSelector>,
    PpaHistogramRequest,
    anyhow::Error,
>;

impl PpaPds {
    pub fn new(capacities: PpaCapacities) -> Result<Self, anyhow::Error> {
        let events =
            HashMapEventStorage::<PpaEvent, PpaRelevantEventSelector>::new();

        let filters = PpaFilterStorage::new(capacities)?;

        let pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            _phantom_request: std::marker::PhantomData::<PpaHistogramRequest>,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };
        Ok(pds)
    }
}

// TODO: generic budget and filter?
impl<E, U> Serialize
    for HashMapFilterStorage<
        FilterId<E, U>,
        PureDPBudgetReleaseFilter,
        PureDPBudget,
        StaticCapacities<FilterId<E, U>, PureDPBudget>,
    >
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ncs = vec![];
        let mut cs = vec![];
        let mut qtriggers = vec![];
        let mut qsources = vec![];

        for (filter_id, filter) in &self.filters {
            match filter_id {
                FilterId::Nc(_, _) => ncs.push(filter),
                FilterId::C(_) => cs.push(filter),
                FilterId::QTrigger(_, _) => qtriggers.push(filter),
                FilterId::QSource(_, _) => qsources.push(filter),
            }
        }

        // Serialize the vectors into the desired format
        let mut state =
            serializer.serialize_struct("HashMapFilterStorage", 4)?;
        state.serialize_field("ncs", &ncs)?;
        state.serialize_field("cs", &cs)?;
        state.serialize_field("qtriggers", &qtriggers)?;
        state.serialize_field("qsources", &qsources)?;
        state.end()
    }
}

pub struct PpaFilterStorage {
    pub storage: HashMapFilterStorage<
        PpaFilterId,
        PureDPBudgetReleaseFilter,
        PureDPBudget,
        StaticCapacities<PpaFilterId, PureDPBudget>,
    >,
}

/// A very hardcoded filter storage to experiment with batching.
impl FilterStorage for PpaFilterStorage {
    type Budget = PureDPBudget;
    type Capacities = PpaCapacities;
    type Error = anyhow::Error;
    type FilterId = PpaFilterId;

    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let storage = HashMapFilterStorage::new(capacities)?;
        Ok(Self { storage })
    }

    fn new_filter(
        &mut self,
        filter_id: PpaFilterId,
    ) -> Result<(), anyhow::Error> {
        let capacity = self.storage.capacities.capacity(&filter_id)?;
        let mut filter = PureDPBudgetReleaseFilter::new(capacity)?;

        // TODO: Hacky logic to have some filters locked and others unlocked at
        // initialization time.
        if !matches!(filter_id, FilterId::C(_)) {
            // Set the unlocked budget to capacity.
            filter.release(f64::INFINITY);
        }
        self.storage.filters.insert(filter_id, filter);
        Ok(())
    }

    /// Transparent.
    fn is_initialized(
        &mut self,
        filter_id: &PpaFilterId,
    ) -> Result<bool, Self::Error> {
        self.storage.is_initialized(filter_id)
    }

    fn can_consume(
        &self,
        filter_id: &PpaFilterId,
        budget: &PureDPBudget,
    ) -> Result<bool, Self::Error> {
        self.storage.can_consume(filter_id, budget)
    }

    fn try_consume(
        &mut self,
        filter_id: &PpaFilterId,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        self.storage.try_consume(filter_id, budget)
    }

    fn remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> std::result::Result<Self::Budget, Self::Error> {
        self.storage.remaining_budget(filter_id)
    }
}

impl PpaFilterStorage {
    pub fn release(
        &mut self,
        filter_id: &PpaFilterId,
        budget: f64,
    ) -> Result<()> {
        let filter = self
            .storage
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.release(budget);
        Ok(())
    }

    pub fn remove(
        &mut self,
        filter_id: &PpaFilterId,
    ) -> Result<Option<PureDPBudgetReleaseFilter>> {
        Ok(self.storage.filters.remove(filter_id))
    }

    pub fn set_capacity_to_infinity(
        &mut self,
        filter_id: &PpaFilterId,
    ) -> Result<()> {
        let filter = self
            .storage
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;
        filter.capacity = PureDPBudget::Infinite;
        Ok(())
    }

    pub fn reset(&mut self, filter_id: &PpaFilterId) -> Result<()> {
        let filter = self
            .storage
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.consumed = 0.0;
        Ok(())
    }
}
