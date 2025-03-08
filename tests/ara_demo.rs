use log::info;
use std::collections::HashMap;

use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent, traits::EventUris
    },
    pds::epoch_pds::{EpochPrivateDataService, StaticCapacities},
    queries::{
        ppa_histogram::{PpaRelevantEventSelector, PpaHistogramRequest, AttributionLogic}, traits::ReportRequestUris
    },
    util::logging,
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
        trigger_uris: vec!["shoes.com".to_string()],
        querier_uris: vec!["shoes.com".to_string(), "adtech.com".to_string()],
    };
    let event_uris_irrelevant_due_to_trigger = EventUris {
        source_uri: "blog.com".to_string(),
        trigger_uris: vec!["shoes_off_brand.com".to_string()],
        querier_uris: vec!["shoes.com".to_string(), "adtech.com".to_string()],
    };
    let event_uris_irrelevant_due_to_querier = EventUris {
        source_uri: "blog.com".to_string(),
        trigger_uris: vec!["shoes.com".to_string()],
        querier_uris: vec!["adtech.com".to_string()],
    };
    let sample_report_request_uris = ReportRequestUris::mock();

    // Test similar to https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration
    let mut sources1 = HashMap::new();
    sources1.insert("campaignCounts".to_string(), 0x159);
    sources1.insert("geoValue".to_string(), 0x5);

    let event1 = PpaEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1.clone(),
        uris: sample_event_uris.clone(),
    };

    let event_irr_1 = PpaEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1.clone(),
        uris: event_uris_irrelevant_due_to_source.clone(),
    };

    let event_irr_2 = PpaEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1.clone(),
        uris: event_uris_irrelevant_due_to_trigger.clone(),
    };

    let event_irr_3 = PpaEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1.clone(),
        uris: event_uris_irrelevant_due_to_querier.clone(),
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
        "campaignCounts".to_string(),
        0x400,
        PpaRelevantEventSelector {
            filters: HashMap::new(),
            report_request_uris: sample_report_request_uris.clone(),
        }, // Not filtering yet.
        AttributionLogic::LastTouch,
    ).unwrap();

    let report1 = pds.compute_report(&request1).unwrap();
    info!("Report1: {:?}", report1);

    // One event attributed to the binary OR of the source keypiece and trigger
    // keypiece = 0x159 | 0x400
    assert!(report1.bin_values.contains_key(&0x559));
    println!("Report1: {:?}", report1.bin_values.len());
    assert_eq!(report1.bin_values.get(&0x559), Some(&32768.0));

    // Test error case when requested_epsilon is 0.
    let request2 = PpaHistogramRequest::new(
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        0.0, // This should fail.
        "campaignCounts".to_string(),
        0x400,
        PpaRelevantEventSelector {
            filters: HashMap::new(),
            report_request_uris: sample_report_request_uris.clone(),
        }, // Not filtering yet.
        AttributionLogic::LastTouch,
    );
    assert!(request2.is_err());

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events

    Ok(())
}
