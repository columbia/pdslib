use std::collections::HashMap;

use super::quotas::*;
#[cfg(feature = "experimental")]
use crate::{
    budget::pure_dp_filter::PureDPBudget,
    pds::{
        aliases::{
            PpaEventStorage, PpaPds, SimpleEventStorage, SimpleFilterStorage,
            SimplePds,
        },
        quotas::FilterId::*,
    },
    queries::traits::PassivePrivacyLossRequest,
};
use crate::{
    budget::traits::FilterStorage,
    events::{
        ppa_event::PpaEvent, relevant_events::RelevantEvents, traits::EventUris,
    },
    pds::aliases::{PpaFilterStorage, PpaPdsCore},
    queries::{
        ppa_histogram::{
            PpaHistogramConfig, PpaHistogramRequest, PpaRelevantEventSelector,
        },
        traits::{EpochReportRequest, ReportRequestUris},
    },
    util::tests::init_default_logging,
};

#[test]
#[cfg(feature = "experimental")]
fn test_account_for_passive_privacy_loss() -> Result<(), anyhow::Error> {
    let capacities: StaticCapacities<FilterId, PureDPBudget> =
        StaticCapacities::mock();
    let filters = SimpleFilterStorage::new(capacities)?;
    let events = SimpleEventStorage::new();
    let mut pds = SimplePds::new(filters, events);

    let uris = ReportRequestUris::mock();

    // First request should succeed
    let request = PassivePrivacyLossRequest {
        epoch_ids: vec![1, 2, 3],
        privacy_budget: PureDPBudget::from(0.2),
        uris: uris.clone(),
    };
    let result = pds.account_for_passive_privacy_loss(request)?;
    assert_eq!(result, PdsFilterStatus::Continue);

    // Second request with same budget should succeed (2.0 total)
    let request = PassivePrivacyLossRequest {
        epoch_ids: vec![1, 2, 3],
        privacy_budget: PureDPBudget::from(0.3),
        uris: uris.clone(),
    };
    let result = pds.account_for_passive_privacy_loss(request)?;
    assert_eq!(result, PdsFilterStatus::Continue);

    // Verify remaining budgets
    for epoch_id in 1..=3 {
        // we consumed 0.5 so far
        let expected_budgets = vec![
            (
                FilterId::PerQuerier(epoch_id, uris.querier_uris[0].clone()),
                0.5,
            ),
            (FilterId::Global(epoch_id), 19.5),
            (
                FilterId::TriggerQuota(epoch_id, uris.trigger_uri.clone()),
                1.0,
            ),
        ];

        assert_remaining_budgets(
            &mut pds.core.filter_storage,
            &expected_budgets,
        )?;
    }

    // Attempting to consume more should fail.
    let request = PassivePrivacyLossRequest {
        epoch_ids: vec![2, 3],
        privacy_budget: PureDPBudget::from(2.0),
        uris: uris.clone(),
    };
    let result = pds.account_for_passive_privacy_loss(request)?;
    assert!(matches!(result, PdsFilterStatus::OutOfBudget(_)));
    if let PdsFilterStatus::OutOfBudget(oob_filters) = result {
        assert!(oob_filters
            .contains(&FilterId::PerQuerier(2, uris.querier_uris[0].clone())));
    }

    // Consume from just one epoch.
    let request = PassivePrivacyLossRequest {
        epoch_ids: vec![3],
        privacy_budget: PureDPBudget::from(0.5),
        uris: uris.clone(),
    };
    let result = pds.account_for_passive_privacy_loss(request)?;
    assert_eq!(result, PdsFilterStatus::Continue);

    // Verify remaining budgets
    for epoch_id in 1..=2 {
        let expected_budgets = vec![
            (PerQuerier(epoch_id, uris.querier_uris[0].clone()), 0.5),
            (Global(epoch_id), 19.5),
            (TriggerQuota(epoch_id, uris.trigger_uri.clone()), 1.0),
        ];

        assert_remaining_budgets(
            &mut pds.core.filter_storage,
            &expected_budgets,
        )?;
    }

    // epoch 3's PerQuerier and TriggerQuota should be out of budget
    let remaining = pds
        .core
        .filter_storage
        .remaining_budget(&PerQuerier(3, uris.querier_uris[0].clone()))?;
    assert_eq!(remaining, PureDPBudget::from(0.0));

    Ok(())
}

#[track_caller]
#[cfg(feature = "experimental")]
fn assert_remaining_budgets<FS: FilterStorage<Budget = PureDPBudget>>(
    filter_storage: &mut FS,
    expected_budgets: &[(FS::FilterId, f64)],
) -> Result<(), FS::Error> {
    for (filter_id, expected_budget) in expected_budgets {
        let remaining = filter_storage.remaining_budget(filter_id)?;
        assert_eq!(
            remaining,
            PureDPBudget::from(*expected_budget),
            "Remaining budget for {:?} is not as expected",
            filter_id
        );
    }
    Ok(())
}

/// TODO: test this on the real `compute_report`, not just passive privacy
/// loss.
#[test]
#[cfg(feature = "experimental")]
fn test_budget_rollback_on_depletion() -> Result<(), anyhow::Error> {
    // PDS with several filters
    let capacities: StaticCapacities<FilterId, PureDPBudget> =
        StaticCapacities::new(
            PureDPBudget::from(1.0),  // PerQuerier
            PureDPBudget::from(20.0), // Global
            PureDPBudget::from(2.0),  // TriggerQuota
            PureDPBudget::from(5.0),  // SourceQuota
        );

    let filters = SimpleFilterStorage::new(capacities)?;
    let events = SimpleEventStorage::new();
    let mut pds = SimplePds::new(filters, events);

    // Create a sample request uris with multiple queriers
    let mut uris = ReportRequestUris::mock();
    uris.querier_uris = vec![
        "querier1.example.com".to_string(),
        "querier2.example.com".to_string(),
    ];

    // Initialize all filters for epoch 1
    let epoch_id = 1;
    let filter_ids = vec![
        FilterId::Global(epoch_id),
        FilterId::PerQuerier(epoch_id, uris.querier_uris[0].clone()),
        FilterId::PerQuerier(epoch_id, uris.querier_uris[1].clone()),
        FilterId::TriggerQuota(epoch_id, uris.trigger_uri.clone()),
        FilterId::SourceQuota(epoch_id, uris.source_uris[0].clone()),
    ];

    // Record initial budgets
    let mut initial_budgets = HashMap::new();
    for filter_id in &filter_ids {
        initial_budgets.insert(
            filter_id.clone(),
            pds.core.filter_storage.remaining_budget(filter_id)?,
        );
    }

    // Set up a request that will succeed for most filters but fail for one
    // Make the PerQuerier filter for querier1 have only 0.5 epsilon left
    pds.core.filter_storage.try_consume(
        &FilterId::PerQuerier(epoch_id, uris.querier_uris[0].clone()),
        &PureDPBudget::from(0.5),
    )?;

    // Now attempt a deduction that requires 0.7 epsilon
    // This should fail because querier1's PerQuerier filter only has 0.5 left
    let request = PassivePrivacyLossRequest {
        epoch_ids: vec![epoch_id],
        privacy_budget: PureDPBudget::from(0.7),
        uris: uris.clone(),
    };

    let result = pds.account_for_passive_privacy_loss(request)?;
    assert!(matches!(result, PdsFilterStatus::OutOfBudget(_)));
    if let PdsFilterStatus::OutOfBudget(oob_filters) = result {
        assert!(oob_filters.contains(&FilterId::PerQuerier(
            1,
            "querier1.example.com".to_string()
        )));
    }

    // Check that all other filters were not modified
    // First verify that querier1's PerQuerier filter still has 0.5 epsilon
    assert_eq!(
        pds.core
            .filter_storage
            .remaining_budget(&FilterId::PerQuerier(
                epoch_id,
                uris.querier_uris[0].clone()
            ))?,
        PureDPBudget::from(0.5),
        "Filter that was insufficient should still have its partial budget"
    );

    // Then verify the other filters still have their original budgets
    for filter_id in &filter_ids {
        // Skip the querier1 PerQuerier filter we already checked
        if matches!(filter_id, FilterId::PerQuerier(_, uri) if uri == &uris.querier_uris[0])
        {
            continue;
        }

        let current_budget =
            pds.core.filter_storage.remaining_budget(filter_id)?;
        let initial_budget = initial_budgets.get(filter_id).unwrap();

        assert_eq!(
            current_budget, *initial_budget,
            "Filter {:?} budget changed when it shouldn't have",
            filter_id
        );
    }

    Ok(())
}

#[test]
#[cfg(feature = "experimental")]
fn test_cross_report_optimization() -> Result<(), anyhow::Error> {
    // Create PDS with mock capacities
    let capacities = StaticCapacities::mock();
    let filters = PpaFilterStorage::new(capacities)?;
    let events = PpaEventStorage::new();
    let mut pds = PpaPds::<_>::new(filters, events);

    // Create test URIs
    let source_uri = "blog.example.com".to_string();
    let beneficiary_uri = "shoes.example.com".to_string();
    let trigger_uri = "shoes.example.com".to_string();
    let intermediary_uri1 = "r1.ex".to_string();
    let intermediary_uri2 = "r2.ex".to_string();
    let intermediary_uri3 = "r3.ex".to_string();

    // Create event URIs with appropriate intermediaries
    let event_uris = EventUris {
        source_uri: source_uri.clone(),
        trigger_uris: vec![trigger_uri.clone()],
        querier_uris: vec![beneficiary_uri.clone()],
        intermediary_uris: vec![
            intermediary_uri1.clone(),
            intermediary_uri2.clone(),
            intermediary_uri3.clone(),
        ],
    };

    // Create report request URIs
    let report_request_uris = ReportRequestUris {
        trigger_uri: trigger_uri.clone(),
        source_uris: vec![source_uri.clone()],
        querier_uris: vec![beneficiary_uri.clone()],
        intermediary_uris: vec![
            intermediary_uri1.clone(),
            intermediary_uri2.clone(),
            intermediary_uri3.clone(),
        ],
    };

    // Register an early event with bucket 1 - this should be overridden by
    // last-touch attribution
    let early_event = PpaEvent {
        id: 1,
        timestamp: 100,
        epoch_number: 1,
        histogram_index: 1, // r1.ex bucket
        uris: event_uris.clone(),
        filter_data: 1,
    };
    pds.register_event(early_event.clone())?;

    // The event that should be attributed (latest timestamp in epoch 1)
    // We'll use a histogram index that's covered by both intermediaries (3)
    let main_event = PpaEvent {
        id: 2,
        timestamp: 200, /* Later timestamp so this event is picked by
                         * last-touch */
        epoch_number: 1,
        histogram_index: 2, // A bucket that will be kept and read by r2.ex
        uris: event_uris.clone(),
        filter_data: 1,
    };
    pds.register_event(main_event.clone())?;
    // Create intermediary bucket mapping
    // Both intermediaries have access to bucket 3, so they'll both get data
    // from the same event
    let bucket_intermediary_mapping = HashMap::from([
        (1, intermediary_uri1.clone()), // r1.ex gets buckets 1
        (2, intermediary_uri2.clone()), // r2.ex gets buckets 2
        (3, intermediary_uri3.clone()), // r3.ex gets buckets 3
    ]);
    // Create histogram request with optimization query flag set to true
    let config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 2,
        attributable_value: 100.0,
        max_attributable_value: 200.0,
        requested_epsilon: 1.0,
        histogram_size: 4, // Ensure we have space for bucket 3
    };

    let request = PpaHistogramRequest::new(
        &config,
        PpaRelevantEventSelector {
            report_request_uris,
            is_matching_event: Box::new(|event_filter_data: u64| {
                event_filter_data == 1
            }),
            bucket_intermediary_mapping,
        },
    )
    .map_err(|e| anyhow::anyhow!("Failed to create request: {}", e))?;
    // Initialize and check the initial beneficiary's PerQuerier filter
    let beneficiary_filter_id =
        FilterId::PerQuerier(1, beneficiary_uri.clone());
    let initial_budget = pds
        .core
        .filter_storage
        .remaining_budget(&beneficiary_filter_id)?;

    // Process the request
    let report_result = pds.compute_report(&request)?;

    // Verify the result is an Optimization report
    // Verify we have reports for both intermediaries
    assert_eq!(
        report_result.len(),
        3,
        "Expected reports for 2 intermediaries"
    );

    // Verify r1.ex's report has bucket 1
    let r1_report = report_result
        .get(&intermediary_uri1)
        .expect("Missing report for r1.ex");
    let r1_bins = &r1_report.filtered_report.bin_values;
    assert!(r1_bins.is_empty(), "1 bucket for r1.ex should have been filtered out by last-touch attribution");

    // Verify r2.ex's report has bucket 2
    let r2_report = report_result
        .get(&intermediary_uri2)
        .expect("Missing report for r2.ex");
    let r2_bins = &r2_report.filtered_report.bin_values;
    assert_eq!(r2_bins.len(), 1, "Expected 1 bucket for r2.ex");
    assert!(r2_bins.contains_key(&2), "Expected bucket 3 for r2.ex");

    // Intermediary r2 receives the value from the main event
    assert_eq!(
        r2_bins.get(&2),
        Some(&100.0),
        "Incorrect value for r2.ex bucket 3"
    );

    // Verify r3.ex's report has bucket 3
    let r3_report = report_result
        .get(&intermediary_uri3)
        .expect("Missing report for r3.ex");
    let r3_bins = &r3_report.filtered_report.bin_values;
    assert!(r3_bins.is_empty(), "1 bucket for r1.ex should have been filtered out by last-touch attribution");

    // Verify the privacy budget was deducted only once
    // Despite two reports being generated (one for each intermediary)
    let post_budget = pds
        .core
        .filter_storage
        .remaining_budget(&beneficiary_filter_id)?;

    if initial_budget.is_finite() && post_budget.is_finite() {
        let deduction = initial_budget - post_budget;

        // Verify budget was actually deducted
        assert!(
            deduction == 0.5,
            "Expected budget deduction but got {deduction}",
        );

        // Calculate what would be deducted with vs. without
        // optimization
        let expected_single_deduction =
            config.attributable_value / config.max_attributable_value;

        // Verify deduction is close to single event (cross-report
        // optimization working)
        assert!(
            deduction == expected_single_deduction,
            "Budget deduction indicates optimization is not working"
        );
    } else {
        panic!("Expected finite budget deduction");
    }
    Ok(())
}

#[test]
fn test_cross_report_optim_oob() -> Result<(), anyhow::Error> {
    init_default_logging();

    let capacities = StaticCapacities::new(0.0, 0.0, 0.0, 0.0);
    let filters = PpaFilterStorage::new(capacities)?;
    let mut pds = PpaPdsCore::<_>::new(filters);

    let event = PpaEvent {
        id: 1,
        timestamp: 0,
        epoch_number: 1,
        histogram_index: 0,
        uris: EventUris::mock(),
        filter_data: 1,
    };

    let mut events_per_epoch = HashMap::new();
    events_per_epoch.insert(1, vec![event]);
    let relevant_events = RelevantEvents::from_mapping(events_per_epoch);

    let request_config = PpaHistogramConfig {
        start_epoch: 1,
        end_epoch: 1,
        attributable_value: 100.0,
        max_attributable_value: 100.0,
        requested_epsilon: 0.0001,
        histogram_size: 5,
    };

    let mut report_uris = ReportRequestUris::mock();
    let querier_uri = report_uris.querier_uris[0].clone();
    report_uris.intermediary_uris.push(querier_uri.clone());

    let always_relevant_selector = PpaRelevantEventSelector {
        report_request_uris: report_uris,
        is_matching_event: Box::new(|_: u64| true),
        bucket_intermediary_mapping: HashMap::from([(0, querier_uri.clone())]),
    };
    let request =
        PpaHistogramRequest::new(&request_config, always_relevant_selector)?;

    let report = pds.compute_report(&request, relevant_events)?;

    // the report will have run out of budget, so it should be null
    let querier_uri = &request.report_uris().querier_uris[0];
    let report = &report[querier_uri];

    for bin_value in report.filtered_report.bin_values.values() {
        assert_eq!(
            *bin_value, 0.0,
            "Report should be null: {:?}",
            report.filtered_report.bin_values
        );
    }

    Ok(())
}
