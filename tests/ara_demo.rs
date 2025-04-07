mod common;

use common::logging;
use log::info;
use pdslib::{
    budget::{
        hashmap_filter_storage::HashMapFilterStorage,
        pure_dp_filter::{PureDPBudgetFilter, PureDPBudget},
        traits::FilterStorage,
    },
    events::{
        hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
        traits::EventUris,
    },
    pds::epoch_pds::{EpochPrivateDataService, StaticCapacities, PdsReportResult, FilterId},
    queries::{
        ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector, PpaHistogramConfig, create_querier_bucket_mapping},
        traits::ReportRequestUris,
    },
};
use core::panic;
use std::collections::HashMap;

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
    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 2,
        report_global_sensitivity: 32768.0,
        query_global_sensitivity: 65536.0,
        requested_epsilon: 1.0,
        histogram_size: 2048,
        is_optimization_query: false
    };
    let request1 = PpaHistogramRequest::new(
        config,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
            querier_bucket_mapping: HashMap::new(),
        }, // Not filtering yet.
    )
    .unwrap();

    let report1 = pds.compute_report(&request1).unwrap();
    match report1 {
        PdsReportResult::Regular(pds_report) => {
            info!("Report1: {:?}", pds_report);
            let bin_values1 = &pds_report.filtered_report.bin_values;

            // One event attributed to the binary OR of the source keypiece and trigger
            // keypiece = 0x159 | 0x400
            assert!(bin_values1.contains_key(&0x559));
            println!("Report1: {:?}", bin_values1.len());
            assert_eq!(bin_values1.get(&0x559), Some(&32768.0));
        },
        PdsReportResult::Optimization(_) => {
            // Handle the Optimization case if needed
            panic!("This should never happen because we are not using optimization queries here.");
        }
    }

    // Test error case when requested_epsilon is 0.
    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 2,
        report_global_sensitivity: 32768.0,
        query_global_sensitivity: 65536.0,
        requested_epsilon: 0.0, // This should fail.
        histogram_size: 2048,
        is_optimization_query: false
    };
    let request2 = PpaHistogramRequest::new(
        config,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
            querier_bucket_mapping: HashMap::new(),
        }, // Not filtering yet.
    );
    assert!(request2.is_err());

    // Test metadata relevant event logic check rejects.
    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 2,
        report_global_sensitivity: 32768.0,
        query_global_sensitivity: 65536.0,
        requested_epsilon: 1.0,
        histogram_size: 2048,
        is_optimization_query: false
    };
    let request3 = PpaHistogramRequest::new(
        config,
        PpaRelevantEventSelector {
            report_request_uris: sample_report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data != 1
            }),
            querier_bucket_mapping: HashMap::new(),
        }, // Not filtering yet.
    )
    .unwrap();

    let report3 = pds.compute_report(&request3).unwrap();
    match report3 {
        PdsReportResult::Regular(pds_report) => {
            info!("Report: {:?}", pds_report);
            
            // No event attributed because the lambda logic filters out the only
            // qualified event.
            assert_eq!(pds_report.filtered_report.bin_values.len(), 0);
        },
        PdsReportResult::Optimization(_) => {
            // Handle the Optimization case if needed
            panic!("This should never happen because we are not using optimization queries here.");
        }
    }

    // TODO(https://github.com/columbia/pdslib/issues/8): add more tests when we have multiple events

    Ok(())
}

#[test]
fn test_optimization_queries() -> Result<(), anyhow::Error> {    
    // Initialize PDS with events storage and filter storage
    let events = HashMapEventStorage::<PpaEvent, PpaRelevantEventSelector>::new();
    let capacities = StaticCapacities::mock();
    let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
        HashMapFilterStorage::new(capacities)?;

    let mut pds = EpochPrivateDataService {
        filter_storage: filters,
        event_storage: events,
        _phantom_request: std::marker::PhantomData::<PpaHistogramRequest>,
        _phantom_error: std::marker::PhantomData::<anyhow::Error>,
    };

    info!("1. Setting up single event with multiple queriers");
    
    // Create event URIs for a single event with multiple queriers
    let event_uris = EventUris {
        source_uri: "blog.com".to_string(),
        trigger_uris: vec!["shoes.com".to_string()],
        querier_uris: vec!["adtech1.com".to_string(), "adtech2.com".to_string()],
    };

    // Create report request URIs with the same queriers
    let report_request_uris = ReportRequestUris {
        trigger_uri: "shoes.com".to_string(),
        source_uris: vec!["blog.com".to_string()], 
        querier_uris: vec!["adtech1.com".to_string(), "adtech2.com".to_string()],
    };

    // Register a single event
    let event = PpaEvent {
        id: 1,
        timestamp: 100,
        epoch_number: 1,
        histogram_index: 1, // Bucket 1
        uris: event_uris.clone(),
        filter_data: 1,
    };

    pds.register_event(event.clone())?;
    
    info!("Registered event: id={}, epoch={}, histogram_index={}, source={}", 
         event.id, event.epoch_number, event.histogram_index, event.uris.source_uri);
    
    // Create querier bucket mapping using the helper function
    info!("2. Creating querier bucket mapping");
    let mappings = vec![
        ("adtech1.com".to_string(), vec![1]),
        ("adtech2.com".to_string(), vec![1]),
    ];
    let querier_bucket_mapping = create_querier_bucket_mapping(mappings);
    
    info!("Querier bucket mapping: {:?}", querier_bucket_mapping);
    
    // Create the optimization query
    let optimization_config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 1,
        report_global_sensitivity: 100.0, 
        query_global_sensitivity: 200.0,
        requested_epsilon: 1.0,
        histogram_size: 3,
        is_optimization_query: true, // This is the key difference
    };
    
    let optimization_request = PpaHistogramRequest::new(
        optimization_config.clone(),
        PpaRelevantEventSelector {
            report_request_uris: report_request_uris.clone(),
            is_matching_event: Box::new(|event_filter_data: u64| event_filter_data == 1),
            querier_bucket_mapping,
        },
    ).unwrap();
    
    // Verify the request is properly set up
    assert!(optimization_request.is_optimization_query(), 
           "Request should be recognized as an optimization query");
    assert!(!optimization_request.get_querier_bucket_mapping().is_empty(),
           "Querier bucket mapping should be available");
    
    info!("3. Computing optimization query");
    let optimization_result = pds.compute_report(&optimization_request)?;
    
    // Handle the result
    match optimization_result {
        PdsReportResult::Regular(_) => {
            panic!("Expected Optimization report but got Regular!");
        },
        PdsReportResult::Optimization(querier_reports) => {
            info!("Received optimization report with {} querier reports", querier_reports.len());

            print!("Querier reports: {:?}", querier_reports);
            
            if !querier_reports.is_empty() {
                // Success! Process the reports.
                info!("SUCCESS: Generated querier reports!");
                
                // Detailed examination of the querier reports
                info!("5. Examining querier reports in detail");
                info!("Complete querier reports map: {:?}", querier_reports);
                
                // Verify both queriers received reports
                assert!(querier_reports.contains_key("adtech1.com"), "adtech1.com should have a report");
                assert!(querier_reports.contains_key("adtech2.com"), "adtech2.com should have a report");
                assert_eq!(querier_reports.len(), 2, "Should have exactly 2 querier reports");
                
                // Check adtech1.com report
                let adtech1_report = querier_reports.get("adtech1.com").unwrap();
                info!("Report for adtech1.com:");
                info!("  Filtered bins: {:?}", adtech1_report.filtered_report.bin_values);
                info!("  Unfiltered bins: {:?}", adtech1_report.unfiltered_report.bin_values);
                info!("  OOB filters: {:?}", adtech1_report.oob_filters);
                
                // Verify adtech1.com report content
                assert!(adtech1_report.filtered_report.bin_values.contains_key(&1), 
                    "adtech1 report should contain bucket 1");
                assert_eq!(adtech1_report.filtered_report.bin_values.get(&1), Some(&100.0),
                        "Bucket 1 should have value 100.0 for adtech1");
                assert_eq!(adtech1_report.filtered_report.bin_values.len(), 1,
                        "adtech1 report should have exactly 1 bucket");
                assert!(adtech1_report.oob_filters.is_empty(),
                    "adtech1 report should not have any OOB filters");
                
                // Check adtech2.com report
                let adtech2_report = querier_reports.get("adtech2.com").unwrap();
                info!("Report for adtech2.com:");
                info!("  Filtered bins: {:?}", adtech2_report.filtered_report.bin_values);
                info!("  Unfiltered bins: {:?}", adtech2_report.unfiltered_report.bin_values);
                info!("  OOB filters: {:?}", adtech2_report.oob_filters);
                
                // Verify adtech2.com report content
                assert!(adtech2_report.filtered_report.bin_values.contains_key(&1), 
                    "adtech2 report should contain bucket 1");
                assert_eq!(adtech2_report.filtered_report.bin_values.get(&1), Some(&100.0),
                        "Bucket 1 should have value 100.0 for adtech2");
                assert_eq!(adtech2_report.filtered_report.bin_values.len(), 1,
                        "adtech2 report should have exactly 1 bucket");
                assert!(adtech2_report.oob_filters.is_empty(),
                    "adtech2 report should not have any OOB filters");
                
                // Compare the two reports - they should be identical in this simple case
                assert_eq!(adtech1_report.filtered_report.bin_values, adtech2_report.filtered_report.bin_values,
                        "Both queriers should have identical bin values as they both access the same bucket");
                
                info!("6. Testing privacy budget consumption");
                
                // Check NC filter state for each querier
                let filter_id_adtech1 = FilterId::Nc(1, "adtech1.com".to_string());
                let filter_id_adtech2 = FilterId::Nc(1, "adtech2.com".to_string());
                
                // Initialize filters if needed
                if !pds.filter_storage.is_initialized(&filter_id_adtech1)? {
                    pds.filter_storage.new_filter(filter_id_adtech1.clone())?;
                }
                if !pds.filter_storage.is_initialized(&filter_id_adtech2)? {
                    pds.filter_storage.new_filter(filter_id_adtech2.clone())?;
                }
                
                // Check remaining budget
                let remaining_budget_adtech1 = pds.filter_storage.remaining_budget(&filter_id_adtech1)?;
                let remaining_budget_adtech2 = pds.filter_storage.remaining_budget(&filter_id_adtech2)?;
                
                info!("Remaining budget for adtech1.com: {:?}", remaining_budget_adtech1);
                info!("Remaining budget for adtech2.com: {:?}", remaining_budget_adtech2);
                
                // Verify each querier consumed some privacy budget
                match remaining_budget_adtech1 {
                    PureDPBudget::Epsilon(eps) => {
                        assert!(eps < 1.0, "adtech1.com should have consumed some privacy budget");
                    },
                    _ => panic!("Expected finite budget consumption for adtech1.com"),
                }
                
                match remaining_budget_adtech2 {
                    PureDPBudget::Epsilon(eps) => {
                        assert!(eps < 1.0, "adtech2.com should have consumed some privacy budget");
                    },
                    _ => panic!("Expected finite budget consumption for adtech2.com"),
                }
            } else {
                panic!("No querier reports survived the filtering process!");
            }
        }
    }
    
    Ok(())
}
