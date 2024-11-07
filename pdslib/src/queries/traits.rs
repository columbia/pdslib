// TODO: traits for attribution fn maybe?

use std::fmt::Debug;

// TODO: another trait for queries, that combine reports?

/// Trait for an epoch-based query.
pub trait ReportRequest: Debug {
    type EpochId;
    type EpochEvents: Debug;
    type Report: Debug;
    type PrivacyBudget;
    type ReportGlobalSensitivity;

    // TODO: add function to compute report

    fn get_epoch_ids(&self) -> Vec<Self::EpochId>;

    // TODO: split this out to AttributionFunction if
    // we want to keep the same attribution function but use a different accounting.
    fn compute_report(
        &self,
        all_epoch_events: &Vec<Self::EpochEvents>, // TODO: maybe take a mapping from epoch Ids to epoch events?
    ) -> Self::Report;

    fn get_single_epoch_individual_sensitivity(&self, report: &Self::Report, is_gaussian: bool) -> f64 {
        // Returns 0 if not filled with value
        0.0
    }

    fn get_global_sensitivity(&self) -> f64 {
        0.0
    }

    fn get_requested_epsilon(&self) -> f64 {
        0.0
    }
}
