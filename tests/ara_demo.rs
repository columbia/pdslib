mod common;

use common::logging;
use log::info;
use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
        traits::EventUris,
    },
    pds::epoch_pds::{EpochPrivateDataService, StaticCapacities},
    queries::{
        ppa_histogram::{
            AttributionLogic, PpaHistogramRequest, PpaRelevantEventSelector,
        },
        traits::ReportRequestUris,
    },
};

#[test]
fn main() -> Result<(), anyhow::Error> {
    logging::init_default_logging();
    let events =
        HashMapEventStorage::<PpaEvent, PpaRelevantEventSelector>::new();
    let capacities = StaticCapacities::mock();
    let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
        HashMapFilterStorage::new(capacities)?;

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget::Epsilon(3.0),
        _phantom_request: std::marker::PhantomData::<PpaHistogramRequest>,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

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
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        1.0,
        2048,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
        }, // Not filtering yet.
        AttributionLogic::LastTouch,
    )
    .unwrap();

    let report1 = pds.compute_report(&request1).unwrap();
    info!("Report1: {:?}", report1);
    let bin_values1 = &report1.filtered_report.bin_values;

    // One event attributed to the binary OR of the source keypiece and trigger
    // keypiece = 0x159 | 0x400
    assert!(bin_values1.contains_key(&0x559));
    println!("Report1: {:?}", bin_values1.len());
    assert_eq!(bin_values1.get(&0x559), Some(&32768.0));

    // Test error case when requested_epsilon is 0.
    let request2 = PpaHistogramRequest::new(
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        0.0, // This should fail.
        2048,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
        }, // Not filtering yet.
        AttributionLogic::LastTouch,
    );
    assert!(request2.is_err());

    // Test metadata relevant event logic check rejects.
    let request3 = PpaHistogramRequest::new(
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        1.0,
        2048,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data != 1
            }),
        }, // Not filtering yet.
        AttributionLogic::LastTouch,
    )
    .unwrap();

    let report3 = pds.compute_report(&request3).unwrap();
    info!("Report3: {:?}", report3);

    // No event attributed because the lambda logic filters out the only
    // qualified event.
    assert_eq!(report3.filtered_report.bin_values.len(), 0);

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events

    Ok(())
}
