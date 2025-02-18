use pdslib::budget::hashmap_filter_storage::HashMapFilterStorage;
use pdslib::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};
use pdslib::events::hashmap_event_storage::HashMapEventStorage;
use pdslib::events::simple_event::SimpleEvent;
use pdslib::pds::epoch_pds::EpochPrivateDataServiceImpl;
use pdslib::pds::traits::PrivateDataService;
use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest;

#[test]
fn main() {
    // This demo represents what happens on a single device.

    // Set up storage and Private Data Service.
    let events = HashMapEventStorage::new();
    let filters: HashMapFilterStorage<usize, PureDPBudgetFilter, PureDPBudget> =
        HashMapFilterStorage::new();

    let mut pds = EpochPrivateDataServiceImpl {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget::Epsilon(3.0),
        _phantom: std::marker::PhantomData::<SimpleLastTouchHistogramRequest>,
    };

    // Create an impression (event, with very basic metadata).
    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        event_key: 3,
    };

    // Save impression.
    pds.register_event(event.clone()).unwrap();

    // Create a request to measure a conversion (report request).
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1,
        attributable_value: 3.0,
        laplace_noise_scale: 1.0,
        is_relevant_event: |e: &SimpleEvent| e.event_key > 1, // Filter events
    };

    // Measure conversion.
    let report = pds.compute_report(report_request);

    // Look at the histogram stored in the report (unencrypted here).
    assert_eq!(report.bin_value, Some((event.event_key, 3.0)));
}
