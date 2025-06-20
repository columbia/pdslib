#[cfg(feature = "experimental")]
use crate::{
    budget::{pure_dp_filter::PureDPBudget, traits::FilterStorage},
    pds::quotas::{FilterId, PdsFilterStatus, StaticCapacities},
    pds::{
        aliases::{SimpleEventStorage, SimpleFilterStorage, SimplePds},
        quotas::FilterId::*,
    },
    queries::traits::PassivePrivacyLossRequest,
    queries::traits::ReportRequestUris,
    util::hashmap::HashMap,
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
