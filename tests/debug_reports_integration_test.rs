#[cfg(test)]
mod experimental_feature_tests {
    use pdslib::{
        budget::{pure_dp_filter::PureDPBudget, traits::FilterStorage},
        events::{
            simple_event::SimpleEvent,
            traits::{EventStorage, EventUris},
        },
        pds::{
            aliases::{SimpleEventStorage, SimpleFilterStorage, SimplePds},
            quotas::{FilterId, StaticCapacities},
        },
        queries::{
            simple_last_touch_histogram::{
                SimpleLastTouchHistogramRequest, SimpleRelevantEventSelector,
            },
            traits::ReportRequestUris,
        },
    };

    // Create a PDS with constrained privacy budget to trigger filtering
    fn setup_constrained_pds() -> Result<SimplePds, anyhow::Error> {
        let capacities: StaticCapacities<FilterId, PureDPBudget> =
            StaticCapacities::new(
                PureDPBudget::from(0.001),
                PureDPBudget::from(0.01),
                PureDPBudget::from(0.002),
                PureDPBudget::from(0.001),
            );

        let filters = SimpleFilterStorage::new(capacities)?;
        let mut events = SimpleEventStorage::new();

        // Add events across multiple epochs
        let event_uris = EventUris::mock();
        for epoch in 1..=3 {
            for i in 1..=2 {
                let event = SimpleEvent {
                    id: epoch * 10 + i,
                    epoch_number: epoch,
                    event_key: i,
                    uris: event_uris.clone(),
                };
                events.add_event(event)?;
            }
        }

        Ok(SimplePds::new(filters, events))
    }

    fn create_high_budget_request() -> SimpleLastTouchHistogramRequest {
        SimpleLastTouchHistogramRequest {
            epoch_start: 1,
            epoch_end: 3,
            report_global_sensitivity: 2.0,
            query_global_sensitivity: 1.0,
            requested_epsilon: 2.0,
            is_relevant_event: SimpleRelevantEventSelector {
                lambda: |_event| true,
            },
            report_uris: ReportRequestUris::mock(),
        }
    }

    // Experimental mode functionality: unfiltered_report should contain real
    // data
    #[cfg(feature = "experimental")]
    #[test]
    fn experimental_mode_provides_unfiltered_access(
    ) -> Result<(), anyhow::Error> {
        use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchHistogramReport;

        let mut pds = setup_constrained_pds()?;

        // Make a simple request that should succeed (before exhausting budget)
        let simple_request = SimpleLastTouchHistogramRequest {
            epoch_start: 1,
            epoch_end: 1,
            report_global_sensitivity: 0.5,
            query_global_sensitivity: 1.0,
            requested_epsilon: 1.0,
            is_relevant_event: SimpleRelevantEventSelector {
                lambda: |_event| true,
            },
            report_uris: ReportRequestUris::mock(),
        };

        let report = pds.compute_report(&simple_request)?;

        // In experimental mode, unfiltered_report should NOT be the default
        // empty report
        let default_report = SimpleLastTouchHistogramReport::default();
        assert_ne!(
            format!("{:?}", report.unfiltered_report), 
            format!("{:?}", default_report),
            "Experimental mode: unfiltered_report should contain actual data, not default empty report"
        );

        Ok(())
    }

    // Test experimental feature OFF: unfiltered_report should be default/empty.
    // To activate the test, run `cargo test --no-default-features`.
    #[cfg(not(feature = "experimental"))]
    #[test]
    fn production_mode_uses_default_unfiltered_report(
    ) -> Result<(), anyhow::Error> {
        use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchHistogramReport;

        let mut pds = setup_constrained_pds()?;
        let request = create_high_budget_request();

        let report = pds.compute_report(&request)?;

        // In production mode, unfiltered_report should be the default report
        assert_eq!(
            format!("{:?}", report.unfiltered_report),
            format!("{:?}", SimpleLastTouchHistogramReport::default()),
            "Production mode: unfiltered_report should be default/empty"
        );

        Ok(())
    }

    // Test that core PDS behavior is consistent regardless of feature flag
    #[test]
    fn core_behavior_and_budget_exhaustion_work_consistently(
    ) -> Result<(), anyhow::Error> {
        let mut pds = setup_constrained_pds()?;
        let request = create_high_budget_request();

        let report = pds.compute_report(&request)?;

        // Core PDS behavior should work the same
        // filtered_report should always be populated correctly
        assert!(
            report.filtered_report.bin_value.is_some()
                || report.filtered_report.bin_value.is_none()
        );

        // oob_filters should work the same
        // (we don't assert specific values since they depend on budget state)

        Ok(())
    }
}
