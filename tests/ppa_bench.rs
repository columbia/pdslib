mod common;

use pdslib::{
    budget::traits::FilterStorage as _,
    events::{
        ppa_event::PpaEvent,
        traits::{EventStorage as _, EventUris},
    },
    pds::{
        aliases::{PpaEventStorage, PpaFilterStorage, PpaPds},
        quotas::StaticCapacities,
    },
    queries::{
        ppa_histogram::{
            PpaHistogramConfig, PpaHistogramRequest, PpaRelevantEventSelector,
            RequestedBuckets,
        },
        traits::ReportRequestUris,
    },
};

#[test]
#[ignore]
fn bench_compute_report() -> anyhow::Result<()> {
    let capacities = StaticCapacities::mock();
    let filters = PpaFilterStorage::<&str>::new(capacities)?;
    let events = PpaEventStorage::<&str>::new();
    let mut pds =
        PpaPds::<PpaFilterStorage<&str>, PpaEventStorage<&str>, &str>::new(
            filters, events,
        );

    let event_uris = EventUris {
        source_uri: "source",
        trigger_uris: vec!["trigger"],
        querier_uris: vec!["querier"],
    };
    let report_uris = ReportRequestUris {
        trigger_uri: "trigger",
        source_uris: vec!["source"],
        querier_uris: vec!["querier"],
    };

    // we start at 100 so we can subtract 100 without overflowing
    for epoch_id in 100..10000 {
        // add 3 events
        for event_id in 0..1000 {
            let event = PpaEvent {
                id: event_id,
                timestamp: 1000 + epoch_id * 100 + event_id,
                epoch_number: epoch_id,
                histogram_index: event_id,
                uris: event_uris.clone(),
                filter_data: 0,
            };
            pds.event_storage.add_event(event)?;
        }

        // compute the report for those 3 events
        let request_config = PpaHistogramConfig {
            start_epoch: epoch_id - 100,
            end_epoch: epoch_id + 1,
            attributable_value: 1.0,
            max_attributable_value: 2.0,
            requested_epsilon: 1.0,
            histogram_size: 1001,
        };
        let selector = PpaRelevantEventSelector {
            report_request_uris: report_uris.clone(),
            is_matching_event: Box::new(|_| true),
            requested_buckets: RequestedBuckets::AllBuckets,
        };
        let request = PpaHistogramRequest::new(&request_config, selector)?;

        let report = pds.compute_report(&request)?;

        assert!(!report.filtered_report.bin_values.is_empty())
    }

    Ok(())
}
