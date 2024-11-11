// TODO: traits for attribution fn maybe?

use std::collections::HashMap;
use std::fmt::Debug;

use crate::queries::simple_last_touch_histogram::NormType;

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
        all_epoch_events: &HashMap<Self::EpochId, Self::EpochEvents>, // TODO: maybe take a mapping from epoch Ids to epoch events?  // COMMENT(Mark): I think what we need is an IndexMap since HashMap doesn't preserve insertion order.
    ) -> Self::Report;

    fn get_single_epoch_individual_sensitivity(
        &self,
        _report: &Self::Report,
        _norm_type: NormType,
    ) -> f64;

    fn get_global_sensitivity(&self) -> f64;

    fn get_noise_scale(&self) -> f64;
}
