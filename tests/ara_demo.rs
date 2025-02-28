use std::collections::HashMap;

use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
    },
    events::{
        ara_event::AraEvent, hashmap_event_storage::HashMapEventStorage,
        traits::EventUris,
    },
    pds::epoch_pds::EpochPrivateDataService,
    queries::{
        ara_histogram::{AraHistogramRequest, AraRelevantEventSelector},
        traits::ReportRequestUris,
    },
};

#[test]
fn main() {
    let events =
        HashMapEventStorage::<AraEvent, AraRelevantEventSelector>::new();
    let filters: HashMapFilterStorage<usize, PureDPBudgetFilter, PureDPBudget> =
        HashMapFilterStorage::new();

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget::Epsilon(3.0),
        _phantom_request: std::marker::PhantomData::<AraHistogramRequest>,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

    let sample_event_uris = EventUris {
        source_uri: "blog.com".to_string(),
        trigger_uris: vec!["shoes.com".to_string()],
        querier_uris: vec!["shoes.com".to_string(), "adtech.com".to_string()],
    };
    let sample_report_uris = ReportRequestUris {
        trigger_uri: "shoes.com".to_string(),
        source_uris: vec!["blog.com".to_string()],
        querier_uris: vec!["adtech.com".to_string()],
    };

    // Test similar to https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration
    let mut sources1 = HashMap::new();
    sources1.insert("campaignCounts".to_string(), 0x159);
    sources1.insert("geoValue".to_string(), 0x5);

    let event1 = AraEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1,
        uris: sample_event_uris.clone(),
    };

    pds.register_event(event1.clone()).unwrap();

    // Test basic attribution
    let request1_construction = AraHistogramRequest::new(
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        1.0,
        "campaignCounts".to_string(),
        0x400,
        AraRelevantEventSelector {
            filters: HashMap::new(),
        }, // Not filtering yet.
        sample_report_uris.clone(),
    );

    match request1_construction {
        Ok(request1) => {
            let report1 = pds.compute_report(request1).unwrap();
            println!("Report1: {:?}", report1);

            // One event attributed to the binary OR of the source keypiece and trigger
            // keypiece = 0x159 | 0x400
            assert!(report1.bin_values.contains_key(&0x559));
            assert_eq!(report1.bin_values.get(&0x559), Some(&32768.0));
        },
        Err(e) => {
            println!("Failed to create AraHistogramRequest: {}", e);
        },
    };

    // Test error case when requested_epsilon is 0.
    let request1_construction = AraHistogramRequest::new(
        1,
        2,
        32768.0,
        65536.0,
        65536.0,
        0.0,  // This should fail.
        "campaignCounts".to_string(),
        0x400,
        AraRelevantEventSelector {
            filters: HashMap::new(),
        }, // Not filtering yet.
        sample_report_uris.clone(),
    );

    match request1_construction {
        Ok(request1) => {
            let report1 = pds.compute_report(request1).unwrap();
            println!("Report1: {:?}", report1);

            // One event attributed to the binary OR of the source keypiece and trigger
            // keypiece = 0x159 | 0x400
            assert!(report1.bin_values.contains_key(&0x559));
            assert_eq!(report1.bin_values.get(&0x559), Some(&32768.0));
        },
        Err(e) => {
            println!("Failed to create AraHistogramRequest: {}", e);
        },
    };

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events
}
