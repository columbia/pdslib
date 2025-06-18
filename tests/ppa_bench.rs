mod common;

use common::logging;
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
    logging::init_default_logging();

    let capacities = StaticCapacities::mock();
    let filters = PpaFilterStorage::new(capacities)?;
    let events = PpaEventStorage::new();
    let mut pds = PpaPds::<_>::new(filters, events);

    let event_uris = EventUris::mock();
    let report_uris = ReportRequestUris::mock();

    for i in 5..10000 {
        // add 3 events
        for j in 0..3 {
            let event = PpaEvent {
                id: j,
                timestamp: 1000 + i * 100 + j,
                epoch_number: i,
                histogram_index: j,
                uris: event_uris.clone(),
                filter_data: 0,
            };
            pds.event_storage.add_event(event)?;
        }

        // compute the report for those 3 events
        let request_config = PpaHistogramConfig {
            start_epoch: i - 5,
            end_epoch: i + 1,
            attributable_value: 1.0,
            max_attributable_value: 2.0,
            requested_epsilon: 1.0,
            histogram_size: 10,
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
