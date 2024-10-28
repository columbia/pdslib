// TODO: traits for attribution fn maybe?

use std::fmt::Debug;

// TODO: another trait for queries, that combine reports?

/// Trait for an epoch-based query.
pub trait ReportRequest: Debug {
    type EpochId;
    type EpochEvents: Debug;
    type Report: Debug;
    type PrivacyBudget;

    // TODO: add function to compute report

    fn get_epoch_ids(&self) -> Vec<Self::EpochId>;

    // TODO: split this out to AttributionFunction if
    // we want to keep the same attribution function but use a different accounting.
    fn compute_report(
        &self,
        all_epoch_events: &Vec<Self::EpochEvents>, // TODO: maybe take a mapping from epoch Ids to epoch events?
    ) -> Self::Report;

    /// NOTE: more efficient to compute all the budgets at once?
    /// But seems cleaner to have the budget only depend on one event.
    /// Refactor if this is too inefficient.
    fn compute_individual_budget(
        &self,
        epoch_events: &Self::EpochEvents,
    ) -> Self::PrivacyBudget;
}
