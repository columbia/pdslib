use std::collections::HashMap;

use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
    },
    events::{ara_event::AraEvent, hashmap_event_storage::HashMapEventStorage},
    pds::epoch_pds::EpochPrivateDataService,
    queries::ara_histogram::{AraHistogramRequest, AraRelevantEventSelector},
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

    // Test similar to https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration
    let mut sources1 = HashMap::new();
    sources1.insert("campaignCounts".to_string(), 0x159);
    sources1.insert("geoValue".to_string(), 0x5);

    let event1 = AraEvent {
        id: 1,
        epoch_number: 1,
        aggregatable_sources: sources1,
    };

    pds.register_event(event1.clone()).unwrap();

    // Test basic attribution
    let request1 = AraHistogramRequest {
        start_epoch: 1,
        end_epoch: 2,
        per_event_attributable_value: 32768.0,
        attributable_value: 65536.0,
        noise_scale: 65536.0,
        source_key: "campaignCounts".to_string(),
        trigger_keypiece: 0x400,
        filters: AraRelevantEventSelector {
            filters: HashMap::new(),
        }, // Not filtering yet.
    };

    let report1 = pds.compute_report(request1).unwrap();
    println!("Report1: {:?}", report1);

    // One event attributed to the binary OR of the source keypiece and trigger
    // keypiece = 0x159 | 0x400
    assert!(report1.bin_values.contains_key(&0x559));
    assert_eq!(report1.bin_values.get(&0x559), Some(&32768.0));

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events
}
