mod common;

use common::logging;
use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, simple_event::SimpleEvent,
        traits::EventUris,
    },
    pds::epoch_pds::{EpochPrivateDataService, StaticCapacities},
    queries::{
        simple_last_touch_histogram::{
            SimpleLastTouchHistogramRequest, SimpleRelevantEventSelector,
        },
        traits::ReportRequestUris,
    },
};

#[test]
fn main() -> Result<(), anyhow::Error> {
    logging::init_default_logging();
    let events = HashMapEventStorage::new();

    let capacities = StaticCapacities::new(
        PureDPBudget::Epsilon(3.0),
        PureDPBudget::Epsilon(20.0),
        PureDPBudget::Epsilon(3.5),
        PureDPBudget::Epsilon(8.0),
    );
    let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
        HashMapFilterStorage::new(capacities)?;

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        _phantom_request: std::marker::PhantomData::<
            SimpleLastTouchHistogramRequest,
        >,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

    let sample_event_uris = EventUris::mock();
    let sample_report_uris = ReportRequestUris {
        trigger_uri: "shoes.com".to_string(),
        source_uris: vec!["blog.com".to_string()],
        intermediary_uris: vec!["search.engine.com".to_string()],
        querier_uris: vec!["adtech.com".to_string()],
    };

    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        event_key: 3,
        uris: sample_event_uris.clone(),
    };
    let event2 = SimpleEvent {
        id: 1,
        epoch_number: 2,
        event_key: 3,
        uris: sample_event_uris.clone(),
    };
    let event3 = SimpleEvent {
        id: 2,
        epoch_number: 2,
        event_key: 3,
        uris: sample_event_uris.clone(),
    };
    let event4 = SimpleEvent {
        id: 1,
        epoch_number: 3,
        event_key: 3,
        uris: sample_event_uris.clone(),
    };

    let always_relevant_event_selector = SimpleRelevantEventSelector {
        lambda: always_relevant_event,
    };

    pds.register_event(event.clone())?;
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1,
        report_global_sensitivity: 3.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report = pds.compute_report(&report_request)?;
    let bucket = Some((event.event_key, 3.0));
    assert_eq!(
        report
            .get(&report_request.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        bucket
    );

    // Test having multiple events in one epoch
    pds.register_event(event2.clone())?;

    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1, //test restricting the end epoch
        report_global_sensitivity: 0.1, /* Even 0.1 should be enough to go
                       * over the
                       * limit as the current budget left
                       * for
                       * epoch 1 is 0. */
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report2 = pds.compute_report(&report_request2)?;
    // Allocated budget for epoch 1 is 3.0, but 3.0 has already been consumed in
    // the last request, so the budget is depleted. Now, the null report should
    // be returned for this additional query.
    assert_eq!(
        report2
            .get(&report_request2.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        None
    );

    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 2,
        report_global_sensitivity: 3.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report2 = pds.compute_report(&report_request2)?;
    let bucket2 = Some((event2.event_key, 3.0));
    assert_eq!(
        report2
            .get(&report_request2.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        bucket2
    );

    // Test request for epoch empty yet.
    let report_request3_empty = SimpleLastTouchHistogramRequest {
        epoch_start: 3, // Epoch 3 not created yet.
        epoch_end: 3,   // Epoch 3 not created yet.
        report_global_sensitivity: 0.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report3_empty = pds.compute_report(&report_request3_empty)?;
    assert_eq!(
        report3_empty
            .get(&report_request3_empty.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        None
    );

    // Test restricting report_global_sensitivity
    pds.register_event(event4.clone())?;
    let report_request3_over_budget = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        report_global_sensitivity: 4.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report3_over_budget =
        pds.compute_report(&report_request3_over_budget)?;
    assert_eq!(
        report3_over_budget
            .get(&report_request3_over_budget.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        None
    );

    // This tests the case where we meet the first event in epoch 3, below the
    // budget not used.
    let report_request3 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        report_global_sensitivity: 3.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: always_relevant_event_selector,
        report_uris: sample_report_uris.clone(),
    };
    let report3 = pds.compute_report(&report_request3)?;
    let bucket3 = Some((event3.event_key, 3.0));
    assert_eq!(
        report3
            .get(&report_request3.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        bucket3
    );

    // Check that irrelevant events are ignored
    let report_request4 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        report_global_sensitivity: 3.0,
        query_global_sensitivity: 5.0,
        requested_epsilon: 5.0,
        is_relevant_event: SimpleRelevantEventSelector {
            lambda: |e: &SimpleEvent| e.event_key == 1,
        },
        report_uris: sample_report_uris.clone(),
    };
    let report4 = pds.compute_report(&report_request4)?;
    let bucket4: Option<(usize, f64)> = None;
    assert_eq!(
        report4
            .get(&report_request4.report_uris.querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_value,
        bucket4
    );

    Ok(())
}

fn always_relevant_event(_: &SimpleEvent) -> bool {
    true
}
