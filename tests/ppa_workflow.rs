mod common;

use common::logging;
use pdslib::{
    budget::traits::FilterStorage,
    events::{simple_event::SimpleEvent, traits::EventUris},
    pds::{
        aliases::{SimpleEventStorage, SimpleFilterStorage, SimplePds},
        quotas::StaticCapacities,
    },
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
    // This demo represents what happens on a single device and
    // for managing the budget of a single querier
    // Scenario similar to https://arxiv.org/pdf/2405.16719, Section 3.3

    // Set up storage and Private Data Service.
    let capacities = StaticCapacities::mock();
    let filters = SimpleFilterStorage::new(capacities)?;
    let events = SimpleEventStorage::new();
    let mut pds = SimplePds::new(filters, events);

    let sample_event_uris = EventUris::mock();
    let sample_report_uris = ReportRequestUris {
        trigger_uri: "shoes.com".to_string(),
        source_uris: vec!["blog.com".to_string()],
        intermediary_uris: vec!["search.engine.com".to_string()],
        querier_uris: vec!["adtech.com".to_string()],
    };

    // Create an impression (event, with very basic metadata).
    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        event_key: 3,
        uris: sample_event_uris.clone(),
    };

    // Save impression.
    pds.register_event(event.clone())?;

    // Next, a conversion happens and the querier prepares request parameters.

    // pdslib only needs the mechanism (noise distribution and scale), which
    // can be computed from the global sensitivity and global epsilon if needed.
    let query_global_sensitivity = 100.0;
    let requested_epsilon = 1.0;

    // Can depend on information available to the querier about this particular
    // conversion.
    let report_global_sensitivity = 70.0;

    // Relevant event filter, e.g. only attribute to an ad for Nike if event_key
    // is the advertiser ID + some campaign information.
    let is_relevant_event = SimpleRelevantEventSelector {
        lambda: |e: &SimpleEvent| e.event_key > 1,
    };

    // Create a request to measure a conversion (report request).
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 4,
        report_global_sensitivity,
        query_global_sensitivity,
        requested_epsilon,
        is_relevant_event,
        report_uris: sample_report_uris.clone(),
    };

    // Measure conversion.
    let report = pds.compute_report(&report_request)?;

    // Look at the histogram stored in the report (unencrypted here).
    assert_eq!(
        report
            .get(&report_request.report_uris.querier_uris[0].clone())
            .unwrap()
            .filtered_report
            .bin_value,
        Some((event.event_key, 70.0))
    );

    Ok(())
}
