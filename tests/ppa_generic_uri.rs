use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::PureDPBudgetFilter, traits::FilterStorage as _,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
        traits::EventUris,
    },
    pds::{private_data_service::PrivateDataService, quotas::StaticCapacities},
    queries::{
        ppa_histogram::{
            PpaHistogramConfig, PpaHistogramRequest, PpaRelevantEventSelector,
            RequestedBuckets,
        },
        traits::ReportRequestUris,
    },
};

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
struct CustomUri;

// The recommended way of using generic types in your code is to type-alias
// the commonly used types, to not have to repeat the generic bounds everywhere.
type TestEvent = PpaEvent<CustomUri>;
type TestRelevantEventSelector = PpaRelevantEventSelector<CustomUri>;
type TestHistogramRequest = PpaHistogramRequest<CustomUri>;

#[test]
fn main() -> Result<(), anyhow::Error> {
    // This demo is a simple sunny-day scenario, using a custom URI type.
    // This is mainly to prevent a developer from accidentally hardcoding
    // Uri to String, for example.

    let events: HashMapEventStorage<TestEvent> = HashMapEventStorage::new();

    let capacities = StaticCapacities::mock();
    let filters: HashMapFilterStorage<PureDPBudgetFilter, _> =
        HashMapFilterStorage::new(capacities)?;

    let mut pds =
        PrivateDataService::<_, _, _, anyhow::Error>::new(filters, events);

    let event_uris = EventUris {
        source_uri: CustomUri {},
        trigger_uris: vec![CustomUri {}],
        querier_uris: vec![CustomUri {}],
    };
    let report_uris = ReportRequestUris {
        trigger_uri: CustomUri {},
        source_uris: vec![CustomUri {}],
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
        requested_buckets: RequestedBuckets::AllBuckets,
    };

    pds.register_event(event.clone())?;

    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 1,
        attributable_value: 1.0,
        max_attributable_value: 1.0,
        requested_epsilon: 1.0,
        histogram_size: 1,
    };
    let report_request =
        TestHistogramRequest::new(&config, always_relevant_event_selector)
            .unwrap();
    let _report = pds.compute_report(&report_request)?;

    Ok(())
}
