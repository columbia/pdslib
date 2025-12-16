use super::{
    core::PrivateDataServiceCore,
    private_data_service::PrivateDataService,
    quotas::{FilterId, StaticCapacities},
};
use crate::{
    actions::hashmap_action_storage::HashMapActionStorage,
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
pub type SimpleActionStorage = HashMapActionStorage<u64, u64, String>;
pub type SimpleEventStorage = HashMapEventStorage<SimpleEvent>;

pub type SimplePdsCore<FS = SimpleFilterStorage, AS = SimpleActionStorage> =
    PrivateDataServiceCore<
        SimpleLastTouchHistogramRequest,
        FS,
        AS,
        anyhow::Error,
    >;
pub type SimplePds<
    FS = SimpleFilterStorage,
    AS = SimpleActionStorage,
    ES = SimpleEventStorage,
> = PrivateDataService<
    SimpleLastTouchHistogramRequest,
    FS,
    AS,
    ES,
    anyhow::Error,
>;

// === PPA aliases ===

pub type PpaFilterStorage<U = String> = HashMapFilterStorage<
    PureDPBudgetFilter,
    StaticCapacities<FilterId<u64, U>, PureDPBudget>,
>;
pub type PpaEventStorage<U = String> = HashMapEventStorage<PpaEvent<U>>;
pub type PpaActionStorage<U = String> = HashMapActionStorage<u64, u64, U>;

pub type PpaPdsCore<
    FS = PpaFilterStorage,
    AS = PpaActionStorage,
    U = String,
    ERR = anyhow::Error,
> = PrivateDataServiceCore<PpaHistogramRequest<U>, FS, AS, ERR>;
pub type PpaPds<
    FS = PpaFilterStorage,
    AS = PpaActionStorage,
    ES = PpaEventStorage,
    U = String,
    ERR = anyhow::Error,
> = PrivateDataService<PpaHistogramRequest<U>, FS, AS, ES, ERR>;
