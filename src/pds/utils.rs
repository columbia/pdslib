//! TODO: require Event Storage and Filter Storage to implement `new` with some
//! config param? Or make things a bit more generic, only for those who
//! implement the right traits.

use serde::{ser::SerializeStruct, Serialize};

use super::epoch_pds::StaticCapacities;
use crate::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
        traits::EpochEvents,
    },
    pds::epoch_pds::{EpochPrivateDataService, FilterId},
    queries::ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
};

pub type PpaCapacities =
    StaticCapacities<FilterId<usize, String>, PureDPBudget>;

pub type PpaPds = EpochPrivateDataService<
    HashMapFilterStorage<
        FilterId<usize, String>,
        PureDPBudgetFilter,
        PureDPBudget,
        PpaCapacities,
    >,
    HashMapEventStorage<PpaEvent, PpaRelevantEventSelector>,
    PpaHistogramRequest,
    anyhow::Error,
>;

impl PpaPds {
    pub fn new(capacities: PpaCapacities) -> Result<Self, anyhow::Error> {
        let events =
            HashMapEventStorage::<PpaEvent, PpaRelevantEventSelector>::new();

        let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
            HashMapFilterStorage::new(capacities)?;

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
        PureDPBudgetFilter,
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
