use std::collections::HashMap;

use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::PureDPBudgetFilter, traits::FilterStorage as _,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage,
        ppa_event::PpaEvent,
        traits::{EventUris, Uri},
    },
    pds::epoch_pds::{EpochPrivateDataService, StaticCapacities},
    queries::{
        ppa_histogram::{
            PpaHistogramConfig, PpaHistogramRequest, PpaRelevantEventSelector,
        },
        traits::ReportRequestUris,
    },
};

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
struct CustomUri;
impl Uri for CustomUri {}

// The recommended way of using generic types your code is to type-alias
// the commonly used types, to not have to repeat the generic bounds everywhere.
type TestEvent = PpaEvent<CustomUri>;
type TestRelevantEventSelector = PpaRelevantEventSelector<CustomUri>;
type TestHistogramRequest = PpaHistogramRequest<CustomUri>;

#[test]
fn main() -> Result<(), anyhow::Error> {
    // This demo is a simple sunny-day scenario, using a custom URI type.
    // This is mainly to prevent a developer from accidentally hardcoding
    // Uri to String, for example.

    let events: HashMapEventStorage<TestEvent, TestRelevantEventSelector> =
        HashMapEventStorage::new();

    let capacities = StaticCapacities::mock();
    let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
        HashMapFilterStorage::new(capacities)?;

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        _phantom_request: std::marker::PhantomData::<TestHistogramRequest>,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

    let event_uris = EventUris {
        source_uri: CustomUri {},
        trigger_uris: vec![CustomUri {}],
        intermediary_uris: vec![CustomUri {}],
        querier_uris: vec![CustomUri {}],
    };
    let report_uris = ReportRequestUris {
        trigger_uri: CustomUri {},
        source_uris: vec![CustomUri {}],
        intermediary_uris: vec![CustomUri {}],
        querier_uris: vec![CustomUri {}],
    };

    let event = TestEvent {
        id: 1,
        timestamp: 1,
        epoch_number: 1,
        histogram_index: 1,
        uris: event_uris.clone(),
        filter_data: 1,
    };

    let always_relevant_event_selector = TestRelevantEventSelector {
        report_request_uris: report_uris.clone(),
        is_matching_event: Box::new(|_| true),
        bucket_intermediary_mapping: HashMap::new(),
    };

    pds.register_event(event.clone())?;

    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 1,
        report_global_sensitivity: 1.0,
        query_global_sensitivity: 1.0,
        requested_epsilon: 1.0,
        histogram_size: 1,
    };
    let report_request =
        TestHistogramRequest::new(config, always_relevant_event_selector)
            .unwrap();
    let _report = pds.compute_report(&report_request)?;

    Ok(())
}
