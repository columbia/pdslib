use std::{collections::HashMap, fmt::Debug};

use crate::{
    events::traits::{EpochEvents, EpochId},
    mechanisms::{NoiseScale, NormType},
};

/// Trait for report types returned by a device (in plaintext). Must implement a
/// default variant for null reports, so devices with errors or no budget
/// left are still sending something (and are thus indistinguishable from other
/// devices once reports are encrypted).  
///
/// TODO(https://github.com/columbia/pdslib/issues/20): marker trait for now, might add aggregation methods later.
pub trait Report: Debug + Default {}

/// Trait for a generic query.
pub trait ReportRequest: Debug {
    type Report: Report;
}

/// Trait for an epoch-based query.
pub trait EpochReportRequest: ReportRequest {
    type EpochId: EpochId;
    type EpochEvents: EpochEvents;
    type RelevantEventSelector;
    type PrivacyBudget;
    type ReportGlobalSensitivity;

    /// Returns the list of requested epoch IDs, in the order the attribution
    /// should run.
    fn get_epoch_ids(&self) -> Vec<Self::EpochId>;

    /// Returns the selector for relevant events for the query. The selector
    /// can be passed to the event storage to retrieve only the relevant events.
    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector;

    /// Computes the report for the given request and epoch events.
    fn compute_report(
        &self,
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Self::Report;

    /// Computes the individual sensitivity for the query when the report is
    /// computed over a single epoch.
    fn get_single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64;

    /// Computes the global sensitivity for the query.
    fn get_report_global_sensitivity(&self) -> f64;

    /// Retrieves the scale of the noise that will be added by the aggregator.
    fn get_noise_scale(&self) -> NoiseScale;
}

/// Type for passive privacy loss accounting. Uniform over all epochs for now.
#[derive(Debug)]
pub struct PassivePrivacyLossRequest<EI: EpochId, PrivacyBudget> {
    pub epoch_ids: Vec<EI>,
    pub privacy_budget: PrivacyBudget,
}
