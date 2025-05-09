use super::{
    core::PrivateDataServiceCore,
    private_data_service::PrivateDataService,
    quotas::{FilterId, StaticCapacities},
};
use crate::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
        simple_event::SimpleEvent,
    },
    queries::{
        ppa_histogram::PpaHistogramRequest,
        simple_last_touch_histogram::SimpleLastTouchHistogramRequest,
    },
};

// === SimplePds aliases ===

pub type SimpleFilterStorage = HashMapFilterStorage<
    PureDPBudgetFilter,
    StaticCapacities<FilterId<u64, String>, PureDPBudget>,
>;
pub type SimpleEventStorage = HashMapEventStorage<SimpleEvent>;
pub type SimplePdsCore<FS = SimpleFilterStorage> =
    PrivateDataServiceCore<SimpleLastTouchHistogramRequest, FS, anyhow::Error>;
pub type SimplePds<FS = SimpleFilterStorage, ES = SimpleEventStorage> =
    PrivateDataService<SimpleLastTouchHistogramRequest, FS, ES, anyhow::Error>;

// === PPA aliases ===

pub type PpaFilterStorage = HashMapFilterStorage<
    PureDPBudgetFilter,
    StaticCapacities<FilterId<u64, String>, PureDPBudget>,
>;
pub type PpaEventStorage = HashMapEventStorage<PpaEvent>;
pub type PpaPdsCore<FS = PpaFilterStorage, ERR = anyhow::Error> =
    PrivateDataServiceCore<PpaHistogramRequest, FS, ERR>;
pub type PpaPds<
    FS = PpaFilterStorage,
    ES = PpaEventStorage,
    ERR = anyhow::Error,
> = PrivateDataService<PpaHistogramRequest, FS, ES, ERR>;
