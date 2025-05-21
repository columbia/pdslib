use std::{collections::HashMap, fmt::Debug};

use crate::{
    events::{
        relevant_events::RelevantEvents,
        traits::{EpochId, Event, RelevantEventSelector, Uri},
    },
    mechanisms::{NoiseScale, NormType},
};

#[derive(Debug)]
pub struct QueryComputeResult<U, R> {
    pub bucket_uri_map: HashMap<u64, U>,
    pub uri_report_map: HashMap<U, R>,
}

impl<U, R> QueryComputeResult<U, R> {
    // Example methods, if you need them
    pub fn new(
        bucket_uri_map: HashMap<u64, U>,
        uri_report_map: HashMap<U, R>,
    ) -> Self {
        Self {
            bucket_uri_map,
            uri_report_map,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReportRequestUris<U> {
    /// URI that triggered the report
    pub trigger_uri: U,

    /// Source URIs that can be used to compute the report
    pub source_uris: Vec<U>,

    /// Intermediary URIs that are embedded in the source/trigger sites
    /// and will receive encrypted reports
    pub intermediary_uris: Vec<U>,

    /// Queriers that will receive a report
    pub querier_uris: Vec<U>,
}

/// Trait for report types returned by a device (in plaintext). Must implement a
/// default variant for null reports, so devices with errors or no budget
/// left are still sending something (and are thus indistinguishable from other
/// devices once reports are encrypted). Aggregation methods can be defined by
/// callers.
pub trait Report: Debug + Default {}

/// Trait for an epoch-based query.
pub trait EpochReportRequest: Debug {
    type Uri: Uri;
    type EpochId: EpochId;
    type Event: Event<Uri = Self::Uri, EpochId = Self::EpochId>;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;
    type PrivacyBudget;
    type Report: Report;

    fn report_uris(&self) -> &ReportRequestUris<Self::Uri>;

    /// Returns the list of requested epoch IDs, in the order the attribution
    /// should run.
    fn epoch_ids(&self) -> Vec<Self::EpochId>;

    /// Returns the selector for relevant events for the query. The selector
    /// can be passed to the event storage to retrieve only the relevant events.
    fn relevant_event_selector(&self) -> &Self::RelevantEventSelector;

    /// Computes the report for the given request and epoch events.
    fn compute_report(
        &self,
        relevant_events: &RelevantEvents<Self::Event>,
    ) -> QueryComputeResult<Self::Uri, Self::Report>;

    /// Computes the individual sensitivity for the query when the report is
    /// computed over a single epoch.
    fn single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64;

    /// Computes the individual sensitivity for the query when the report is
    /// computed over a single epoch-source.
    fn single_epoch_source_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64;

    /// Computes the global sensitivity for the query.
    fn report_global_sensitivity(&self) -> f64;

    /// Retrieves the scale of the noise that will be added by the aggregator.
    fn noise_scale(&self) -> NoiseScale;
}

/// Type for passive privacy loss accounting. Uniform over all epochs for now.
#[derive(Debug)]
pub struct PassivePrivacyLossRequest<EI: EpochId, U, PrivacyBudget> {
    pub epoch_ids: Vec<EI>,
    pub privacy_budget: PrivacyBudget,
    pub uris: ReportRequestUris<U>,
}
