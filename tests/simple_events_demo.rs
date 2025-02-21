use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, simple_event::SimpleEvent,
    },
    pds::epoch_pds::EpochPrivateDataService,
    queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest,
};

#[test]
fn main() {
    let events = HashMapEventStorage::new();
    let filters: HashMapFilterStorage<usize, PureDPBudgetFilter, PureDPBudget> =
        HashMapFilterStorage::new();

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget::Epsilon(3.0),
        _phantom_request: std::marker::PhantomData::<
            SimpleLastTouchHistogramRequest,
        >,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        event_key: 3,
    };
    let event2 = SimpleEvent {
        id: 1,
        epoch_number: 2,
        event_key: 3,
    };
    let event3 = SimpleEvent {
        id: 2,
        epoch_number: 2,
        event_key: 3,
    };
    let event4 = SimpleEvent {
        id: 1,
        epoch_number: 3,
        event_key: 3,
    };

    pds.register_event(event.clone()).unwrap();
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1,
        attributable_value: 3.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report = pds.compute_report(report_request).unwrap();
    let bucket = Some((event.event_key, 3.0));
    assert_eq!(report.bin_value, bucket);

    // Test having multiple events in one epoch
    pds.register_event(event2.clone()).unwrap();

    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1, //test restricting the end epoch
        attributable_value: 0.1, /* Even 0.1 should be enough to go over the
                       * limit as the current budget left for
                       * epoch 1 is 0. */
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report2 = pds.compute_report(report_request2).unwrap();
    // Allocated budget for epoch 1 is 3.0, but 3.0 has already been consumed in
    // the last request, so the budget is depleted. Now, the null report should
    // be returned for this additional query.
    assert_eq!(report2.bin_value, None);

    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 2,
        attributable_value: 3.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report2 = pds.compute_report(report_request2).unwrap();
    let bucket2 = Some((event2.event_key, 3.0));
    assert_eq!(report2.bin_value, bucket2);

    // Test request for epoch empty yet.
    let report_request3_empty = SimpleLastTouchHistogramRequest {
        epoch_start: 3, // Epoch 3 not created yet.
        epoch_end: 3,   // Epoch 3 not created yet.
        attributable_value: 0.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report3_empty = pds.compute_report(report_request3_empty).unwrap();
    assert_eq!(report3_empty.bin_value, None);

    // Test restricting attributable_value
    pds.register_event(event4.clone()).unwrap();
    let report_request3_over_budget = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        attributable_value: 4.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report3_over_budget =
        pds.compute_report(report_request3_over_budget).unwrap();
    assert_eq!(report3_over_budget.bin_value, None);

    // This tests the case where we meet the first event in epoch 3, below the
    // budget not used.
    let report_request3 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        attributable_value: 3.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: always_relevant_event,
    };
    let report3 = pds.compute_report(report_request3).unwrap();
    let bucket3 = Some((event3.event_key, 3.0));
    assert_eq!(report3.bin_value, bucket3);

    // Check that irrelevant events are ignored
    let report_request4 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        attributable_value: 3.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: |e: &SimpleEvent| e.event_key == 1,
    };
    let report4 = pds.compute_report(report_request4).unwrap();
    let bucket4: Option<(usize, f64)> = None;
    assert_eq!(report4.bin_value, bucket4);
}

fn always_relevant_event(_: &SimpleEvent) -> bool {
    true
}
