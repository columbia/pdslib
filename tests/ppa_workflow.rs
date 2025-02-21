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
    // This demo represents what happens on a single device and
    // for managing the budget of a single querier
    // Scenario similar to https://arxiv.org/pdf/2405.16719, Section 3.3

    // Set up storage and Private Data Service.
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

    // Create an impression (event, with very basic metadata).
    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        event_key: 3,
    };

    // Save impression.
    pds.register_event(event.clone()).unwrap();

    // Next, a conversion happens and the querier prepares request parameters.

    // pdslib only needs the mechanism (noise distribution and scale), which
    // can be computed from the global sensitivity and global epsilon if needed.
    // TODO(https://github.com/columbia/pdslib/issues/23): potentially use two parameters
    // instead of a single `laplace_noise_scale`.
    let query_global_sensitivity = 100.0;
    let requested_epsilon = 1.0;
    let laplace_noise_scale = query_global_sensitivity / requested_epsilon;

    // Can depend on information available to the querier about this particular
    // conversion.
    let report_global_sensitivity = 70.0;

    // Relevant event filter, e.g. only attribute to an ad for Nike if event_key
    // is the advertiser ID + some campaign information.
    let is_relevant_event = |e: &SimpleEvent| e.event_key > 1;

    // Create a request to measure a conversion (report request).
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 4,
        attributable_value: report_global_sensitivity,
        laplace_noise_scale,
        is_relevant_event,
    };

    // Measure conversion.
    let report = pds.compute_report(report_request).unwrap();

    // Look at the histogram stored in the report (unencrypted here).
    assert_eq!(report.bin_value, Some((event.event_key, 70.0)));
}
