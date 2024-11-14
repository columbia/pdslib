// TODO: traits for attribution fn maybe?

use std::collections::HashMap;
use std::fmt::Debug;

use crate::events::traits::{EpochEvents, EpochId};
use crate::mechanisms::NormType;

/// Trait for report types returned by a device (in plaintext). Must implement a
/// default variant for null reports, so devices with errors or no budget
/// left are still sending something (and are thus indistinguishable from other devices once reports are encrypted).
/// TODO: marker trait for now, might add aggregation methods later.
pub trait Report: Debug + Default {}

/// Trait for a generic query.
pub trait Query: Debug {
    type Report: Report;
}

/// Trait for an epoch-based query.
pub trait EpochQuery: Query {
    type EpochId: EpochId;
    type EpochEvents: EpochEvents;
    type PrivacyBudget;
    type ReportGlobalSensitivity;

    /// Returns the list of epoch IDs, in the order the attribution should run.
    fn get_epoch_ids(&self) -> Vec<Self::EpochId>;

    /// Computes the report for the given request and epoch events.
    fn compute_report(
        &self,
        all_epoch_events: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Self::Report;

    /// Computes the individual sensitivity for the query when the report is computed over a single epoch.
    fn get_single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64;

    /// Computes the global sensitivity for the query.
    fn get_global_sensitivity(&self) -> f64;

    /// Retrieves the scale of the noise that will be added by the aggregator.
    fn get_noise_scale(&self) -> f64;
}
