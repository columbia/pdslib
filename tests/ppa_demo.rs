mod common;

use std::collections::HashMap;

use common::logging;
use log::info;
use pdslib::{
    budget::traits::FilterStorage,
    events::{ppa_event::PpaEvent, traits::EventUris},
    pds::{
        aliases::{PpaEventStorage, PpaFilterStorage, PpaPds},
        quotas::StaticCapacities,
    },
    queries::{
        ppa_histogram::{
            PpaHistogramConfig, PpaHistogramRequest, PpaRelevantEventSelector,
        },
        traits::{EpochReportRequest, ReportRequestUris},
    },
};

#[test]
fn main() -> Result<(), anyhow::Error> {
    logging::init_default_logging();
    let capacities = StaticCapacities::mock();
    let filters = PpaFilterStorage::new(capacities)?;
    let events = PpaEventStorage::new();

    let mut pds = PpaPds::<_>::new(filters, events);

    let sample_event_uris = EventUris::mock();
    let event_uris_irrelevant_due_to_source = EventUris {
        source_uri: "blog_off_brand.com".to_string(),
        ..EventUris::mock()
    };
    let event_uris_irrelevant_due_to_trigger = EventUris {
        trigger_uris: vec!["shoes_off_brand.com".to_string()],
        ..EventUris::mock()
    };
    let event_uris_irrelevant_due_to_querier = EventUris {
        querier_uris: vec!["adtech_off_brand.com".to_string()],
        ..EventUris::mock()
    };

    let sample_report_request_uris = ReportRequestUris {
        trigger_uri: "shoes.com".to_string(),
        source_uris: vec!["blog.com".to_string()],
        intermediary_uris: vec!["search.engine.com".to_string()],
        querier_uris: vec!["adtech.com".to_string()],
    };

    let event1 = PpaEvent {
        id: 1,
        timestamp: 0,
        epoch_number: 1,
        histogram_index: 0x559, // 0x559 = "campaignCounts".to_string() | 0x400
        uris: sample_event_uris.clone(),
        filter_data: 1,
    };

    let event_irr_1 = PpaEvent {
        id: 1,
        timestamp: 0,
        epoch_number: 1,
        histogram_index: 0x559, // 0x559 = "campaignCounts".to_string() | 0x400
        uris: event_uris_irrelevant_due_to_source.clone(),
        filter_data: 1,
    };

    let event_irr_2 = PpaEvent {
        id: 1,
        timestamp: 0,
        epoch_number: 1,
        histogram_index: 0x559, // 0x559 = "campaignCounts".to_string() | 0x400
        uris: event_uris_irrelevant_due_to_trigger.clone(),
        filter_data: 1,
    };

    let event_irr_3 = PpaEvent {
        id: 1,
        timestamp: 0,
        epoch_number: 1,
        histogram_index: 0x559, // 0x559 = "campaignCounts".to_string() | 0x400
        uris: event_uris_irrelevant_due_to_querier.clone(),
        filter_data: 1,
    };

    pds.register_event(event1.clone())?;
    pds.register_event(event_irr_1.clone()).unwrap();
    pds.register_event(event_irr_2.clone()).unwrap();
    pds.register_event(event_irr_3.clone()).unwrap();

    // Test basic attribution
    let request1 = PpaHistogramRequest::new(
        &PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            attributable_value: 32768.0,
            max_attributable_value: 65536.0,
            requested_epsilon: 1.0,
            histogram_size: 2048,
        },
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
            bucket_intermediary_mapping: HashMap::new(),
        }, // Not filtering yet.
    )
    .unwrap();

    let report1 = pds.compute_report(&request1).unwrap();
    info!("Report1: {:?}", report1);
    let bin_values1 = &report1
        .get(&request1.report_uris().querier_uris[0])
        .unwrap()
        .filtered_report
        .bin_values;

    // One event attributed to the binary OR of the source keypiece and trigger
    // keypiece = 0x159 | 0x400
    assert!(bin_values1.contains_key(&0x559));
    println!("Report1: {:?}", bin_values1.len());
    assert_eq!(bin_values1.get(&0x559), Some(&32768.0));

    // Test error case when requested_epsilon is 0.
    let request2 = PpaHistogramRequest::new(
        &PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            attributable_value: 32768.0,
            max_attributable_value: 65536.0,
            requested_epsilon: 0.0, // This should fail.
            histogram_size: 2048,
        },
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
            bucket_intermediary_mapping: HashMap::new(),
        }, // Not filtering yet.
    );
    assert!(request2.is_err());

    let request3 = PpaHistogramRequest::new(
        &PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            attributable_value: 32768.0,
            max_attributable_value: 65536.0,
            requested_epsilon: 1.0,
            histogram_size: 2048,
        },
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data != 1
            }),
            bucket_intermediary_mapping: HashMap::new(),
        }, // Not filtering yet.
    )
    .unwrap();

    let report3 = pds.compute_report(&request3).unwrap();
    info!("Report3: {:?}", report3);

    // No event attributed because the lambda logic filters out the only
    // qualified event.
    assert_eq!(
        report3
            .get(&request3.report_uris().querier_uris[0])
            .unwrap()
            .filtered_report
            .bin_values
            .len(),
        0
    );

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events

    Ok(())
}
