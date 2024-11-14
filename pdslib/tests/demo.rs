use pdslib::budget::hashmap_filter_storage::HashMapFilterStorage;
use pdslib::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};
use pdslib::events::simple_events::{SimpleEvent, SimpleEventStorage};
use pdslib::pds::implem::PrivateDataServiceImpl;
use pdslib::pds::traits::PrivateDataService;
use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest;

#[test]
fn main() {
    let events: SimpleEventStorage = SimpleEventStorage::new();
    let filters: HashMapFilterStorage<usize, PureDPBudgetFilter, PureDPBudget> =
        HashMapFilterStorage::new();

    let mut pds = PrivateDataServiceImpl {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget { epsilon: 3.0 },
        _phantom: std::marker::PhantomData::<SimpleLastTouchHistogramRequest>,
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

    let bucket = Some((event.epoch_number, event.event_key, 3.0));
    let bucket2 = Some((event2.epoch_number, event2.event_key, 3.0));
    let bucket3 = Some((event4.epoch_number, event3.event_key, 3.0));

    pds.register_event(event.clone()).unwrap();
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1,
        attributable_value: 3.0,
        noise_scale: 1.0,
    };
    let report = pds.compute_report(report_request);
    assert_eq!(report.attributed_value, bucket);

    //test having multiple events in one epoch
    println!("");
    pds.register_event(event2.clone()).unwrap();
    // pds.register_event(event3.clone()).unwrap();

    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1, //test restricting the end epoch
        attributable_value: 0.1, /* Even 0.1 should be enough to go over the
                       * limit as the current budget left for
                       * epoch 1 is 0. */
        noise_scale: 1.0,
    };
    let report2 = pds.compute_report(report_request2);
    // Allocated budget for epoch 1 is 3.0, but 3.0 has already been consumed in
    // the last request, so the budget is depleted. Now, the null report should
    // be returned for this additional query.
    assert_eq!(report2.attributed_value, None);
    let report_request2 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 2,
        attributable_value: 3.0,
        noise_scale: 1.0,
    };
    let report2 = pds.compute_report(report_request2);
    assert_eq!(report2.attributed_value, bucket2);

    // Test request for epoch empty yet.
    println!("");
    let report_request3_empty = SimpleLastTouchHistogramRequest {
        epoch_start: 3, // Epoch 3 not created yet.
        epoch_end: 3,   // Epoch 3 not created yet.
        attributable_value: 0.0,
        noise_scale: 1.0,
    };
    let report3_empty = pds.compute_report(report_request3_empty);
    assert_eq!(report3_empty.attributed_value, None);

    //test restricting attributable_value
    println!("");
    pds.register_event(event4).unwrap();
    let report_request3_over_budget = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        attributable_value: 4.0,
        noise_scale: 1.0,
    };
    let report3_over_budget = pds.compute_report(report_request3_over_budget);
    assert_eq!(report3_over_budget.attributed_value, None);
    // This tests the case where we meet the first event in epoch 3, below the
    // budget not used.
    let report_request3 = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 3,
        attributable_value: 3.0,
        noise_scale: 1.0,
    };
    let report3 = pds.compute_report(report_request3);
    assert_eq!(report3.attributed_value, bucket3);
}
