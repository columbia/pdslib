//! TODO: require Event Storage and Filter Storage to implement `new` with some config param?
//! Or make things a bit more generic, only for those who implement the right traits.

use crate::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage,
        ppa_event::PpaEvent,
        traits::{
            EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
        },
    },
    pds::epoch_pds::{EpochPrivateDataService, FilterId, PdsReport},
    queries::{
        ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
        traits::EpochReportRequest,
    },
};

use super::epoch_pds::StaticCapacities;

pub type PpaPds = EpochPrivateDataService<
    HashMapFilterStorage<
        FilterId<usize, String>,
        PureDPBudgetFilter,
        PureDPBudget,
        StaticCapacities<FilterId<usize, String>, PureDPBudget>,
    >,
    HashMapEventStorage<PpaEvent, PpaRelevantEventSelector>,
    PpaHistogramRequest,
    anyhow::Error,
>;

impl PpaPds {
    pub fn new(
        capacities: StaticCapacities<FilterId<usize, String>, PureDPBudget>,
    ) -> Result<Self, anyhow::Error> {
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
