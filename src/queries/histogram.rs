use std::{collections::{HashMap, HashSet}, fmt::Debug, hash::Hash};

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::traits::{EpochEvents, EpochId, Event, RelevantEventSelector},
    mechanisms::{NoiseScale, NormType},
    queries::traits::{EpochReportRequest, Report, ReportRequestUris},
};

#[derive(Debug, Clone)]
pub struct HistogramReport<BucketKey> {
    pub bin_values: HashMap<BucketKey, f64>,
}

/// Trait for bucket keys.
pub trait BucketKey: Debug + Hash + Eq + Clone {}

/// Default type for bucket keys.
impl BucketKey for usize {}

/// Default histogram has no bins (null report).
impl<BK> Default for HistogramReport<BK> {
    fn default() -> Self {
        Self {
            bin_values: HashMap::new(),
        }
    }
}

impl<BK: BucketKey> Report for HistogramReport<BK> {}

/// [Experimental] Trait for generic histogram requests. Any type satisfying
/// this interface will be callable as a valid ReportRequest with the right
/// accounting. Following the formalism from https://arxiv.org/pdf/2405.16719, Thm 18.
/// Can be instantiated by ARA-style queries in particular.
pub trait HistogramRequest: Debug {
    type EpochId: EpochId;
    type EpochEvents: EpochEvents;
    type Event: Event;
    type BucketKey: BucketKey;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;

    /// Returns the ids of the epochs that are relevant for this query.
    /// Typically a range of epochs.
    fn epochs_ids(&self) -> Vec<Self::EpochId>;

    /// Returns the query global sensitivity which is the maximum change that
    /// can be made by a device-epoch to the output of a query over a batch of
    /// reports.
    fn query_global_sensitivity(&self) -> f64;

    /// Returns the global privacy budget requested by a query over a batch of
    /// reports.
    fn requested_epsilon(&self) -> f64;

    /// Returns the Laplace noise scale added after summing all the reports.
    fn laplace_noise_scale(&self) -> f64;

    /// Returns the maximum attributable value, i.e. the maximum L1 norm of an
    /// attributed histogram.
    fn report_global_sensitivity(&self) -> f64;

    /// Returns a selector object, that can be passed to the event storage to
    /// retrieve relevant events. The selector can also output a boolean
    /// indicating whether a single event is relevant.
    fn relevant_event_selector(&self) -> &Self::RelevantEventSelector;

    /// Returns the histogram bucket key (bin) for a given event.
    fn bucket_key(&self, event: &Self::Event) -> Self::BucketKey;

    /// Attributes a value to each event in `relevant_events_per_epoch`, which
    /// will be obtained by retrieving *relevant* events from the event
    /// storage. Events can point to the relevant_events_per_epoch, hence
    /// the lifetime.
    fn event_values<'a>(
        &self,
        relevant_events_per_epoch: &'a HashMap<
            Self::EpochId,
            Self::EpochEvents,
        >,
    ) -> Vec<(&'a Self::Event, f64)>;

    fn report_uris(&self) -> ReportRequestUris<String>;

    /// Returns whether this is an optimization query (has split reports)
    fn is_optimization_query(&self) -> bool {
        false // Default implementation returns false
    }
    
    /// Gets the querier bucket mapping for filtering histograms
    fn get_histogram_querier_bucket_mapping(&self) -> Option<&HashMap<String, HashSet<Self::BucketKey>>> {
        None // Default implementation returns None
    }

    /// Returns the mapping from bucket keys to their respective site URIs.
    fn get_bucket_site_mapping<'a>(&self, 
        relevant_events_per_epoch: &'a HashMap<Self::EpochId, Self::EpochEvents>
    ) -> HashMap<usize, String>;
    
    /// Filter a histogram for a specific querier
    fn filter_histogram_for_querier(
        &self,
        _: &HashMap<Self::BucketKey, f64>,
        _: &String,
        _: &HashMap<Self::EpochId, Self::EpochEvents>,
        _: Option<&HashMap<String, PureDPBudget>>,
        _: Option<&HashMap<String, PureDPBudget>>,
    ) -> Option<HashMap<Self::BucketKey, f64>> {
        None // Default implementation returns None
    }
}

/// We implement the EpochReportRequest trait, so any type that implements
/// HistogramRequest can be used as an EpochReportRequest.
impl<H: HistogramRequest + 'static> EpochReportRequest for H {
    type EpochId = H::EpochId;
    type Event = H::Event;
    type EpochEvents = H::EpochEvents;
    type PrivacyBudget = PureDPBudget;
    type RelevantEventSelector = H::RelevantEventSelector; // Use the full request as the selector.
    type Report = HistogramReport<<H as HistogramRequest>::BucketKey>;
    type Uri = String;
    type BucketKey = H::BucketKey;

    fn report_uris(&self) -> ReportRequestUris<String> {
        self.report_uris()
    }

    /// Re-expose some methods
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/19): any cleaner inheritance?
    fn epoch_ids(&self) -> Vec<H::EpochId> {
        self.epochs_ids()
    }

    fn relevant_event_selector(&self) -> &H::RelevantEventSelector {
        self.relevant_event_selector()
    }

    fn noise_scale(&self) -> NoiseScale {
        // Note that the noise scale equals query_global_sensitiviity divided by
        // the requested epsilon.
        NoiseScale::Laplace(
            self.query_global_sensitivity() / self.requested_epsilon(),
        )
    }

    /// Computes the report by attributing values to events, and then summing
    /// events by bucket.
    fn compute_report(
        &self,
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Self::Report {
        let mut bin_values: HashMap<H::BucketKey, f64> = HashMap::new();

        let mut total_value: f64 = 0.0;
        let event_values = self.event_values(relevant_events_per_epoch);

        // The event_values function selects the relevant events and assigns
        // values according to the requested attribution logic, so we
        // should be able to aggregate values directly. For example, in the case
        // of LastTouch logic, only the last value for an event will be
        // selected, so the sum is juat that singular value.
        //
        // The order matters, since events that are attributed last might be
        // dropped by the contribution cap.
        //
        // TODO(https://github.com/columbia/pdslib/issues/19):  Use an ordered map for relevant_events_per_epoch?
        for (event, value) in event_values {
            total_value += value;
            if total_value > self.report_global_sensitivity() {
                // Return partial attribution to stay within the cap.
                return HistogramReport { bin_values };
            }
            let bin = self.bucket_key(event);
            *bin_values.entry(bin).or_default() += value;
        }

        HistogramReport { bin_values }
    }

    /// Computes individual sensitivity in the single epoch case.
    fn single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64 {
        match norm_type {
            NormType::L1 => report.bin_values.values().sum(),
            NormType::L2 => {
                let sum_squares: f64 =
                    report.bin_values.values().map(|x| x * x).sum();
                sum_squares.sqrt()
            }
        }
    }

    /// Computes individual sensitivity in the single epoch-source case.
    fn single_epoch_source_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64 {
        self.single_epoch_individual_sensitivity(report, norm_type)
    }

    /// Computes the global sensitivity, useful for the multi-epoch case.
    /// See https://arxiv.org/pdf/2405.16719, Thm. 18
    fn report_global_sensitivity(&self) -> f64 {
        // NOTE: if we have only one possible bin (histogram in R instead or
        // R^m), then we can remove the factor 2. But this constraint is
        // not enforceable with HashMap<BucketKey, f64>, so for
        // use-cases that have one bin we should use a custom type
        // similar to `SimpleLastTouchHistogramReport` with Option<BucketKey,
        // f64>.
        2.0 * self.report_global_sensitivity()
    }

    /// TODO(https://github.com/columbia/pdslib/issues/55): For now, assume no optimization queries for simple
    /// last touch histogram queries, so default to the respective Null implementation.

    /// Returns whether this is an optimization query.
    /// Default implementation returns false.
    fn is_optimization_query(&self) -> bool {
        self.is_optimization_query()
    }

    /// Returns the mapping from querier URIs to their respective bucket mappings.
    /// Default implementation returns None.
    fn get_querier_bucket_mapping(&self) -> Option<&HashMap<Self::Uri, HashSet<Self::BucketKey>>> {
        self.get_histogram_querier_bucket_mapping()
    }

    /// Filters the report for a specific querier.
    /// Default implementation returns None.
    fn filter_report_for_querier(
        &self,
        report: &Self::Report,
        querier_uri: &Self::Uri,
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
        epoch_site_privacy_losses: Option<&HashMap<Self::Uri, PureDPBudget>>, // Privacy loss per site
        site_bucket_map: Option<&HashMap<Self::Uri, PureDPBudget>>, // Event source site to bucket mapping
    ) -> Option<Self::Report> {
        // Delegate to the HistogramRequest implementation
        self.filter_histogram_for_querier(
            &report.bin_values,
            querier_uri,
            relevant_events_per_epoch,
            epoch_site_privacy_losses,
            site_bucket_map,
        )
        .map(|
            filtered_bins
        | HistogramReport { bin_values: filtered_bins }
        )
    }
}
