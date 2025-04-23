use std::{
    collections::{HashMap, HashSet},
    vec,
};

use anyhow::{bail, Result};

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::{
        hashmap_event_storage::VecEpochEvents,
        ppa_event::PpaEvent,
        traits::{RelevantEventSelector, Uri},
    },
    mechanisms::NoiseScale,
    queries::{
        histogram::{BucketKey, HistogramReport, HistogramRequest},
        traits::{EpochReportRequest, ReportRequestUris},
    },
};

type PpaBucketKey = usize;
type PpaEpochId = usize;
type PpaEpochEvents<U> = VecEpochEvents<PpaEvent<U>>;

pub struct PpaRelevantEventSelector<U: Uri = String> {
    pub report_request_uris: ReportRequestUris<U>,
    pub is_matching_event: Box<dyn Fn(u64) -> bool>,
    pub bucket_intermediary_mapping: HashMap<usize, U>,
}

impl<U: Uri> std::fmt::Debug for PpaRelevantEventSelector<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PpaRelevantEventSelector")
            .field("report_request_uris", &self.report_request_uris)
            .finish_non_exhaustive()
    }
}

/// For compatibility with PPA spec that uses two parameters (epsilon, query
/// global sensitivity) instead of directly Laplace noise scale.
#[derive(Debug, Clone)]
pub struct PpaHistogramConfig {
    pub start_epoch: usize,
    pub end_epoch: usize,

    /// Conversion value that is spread across events for this conversion.
    pub attributable_value: f64,

    /// Maximum conversion value across all reports in the batch.
    pub max_attributable_value: f64,

    /// Budget spent on the batch, considering the max_attributable_value.
    pub requested_epsilon: f64,
    pub histogram_size: usize,
}

/// Alternative configuration that directly provides Laplace noise scale.
/// Should be easier to use and have less footguns than the spec-compatible
/// configuration.
#[derive(Debug, Clone)]
pub struct DirectPpaHistogramConfig {
    pub start_epoch: usize,
    pub end_epoch: usize,
    /// Conversion value that is spread across events
    pub attributable_value: f64,
    pub laplace_noise_scale: f64,
    pub histogram_size: usize,
}

#[derive(Debug, Clone)]
pub enum AttributionLogic {
    LastTouch,
}

impl<U: Uri> RelevantEventSelector for PpaRelevantEventSelector<U> {
    type Event = PpaEvent<U>;

    fn is_relevant_event(&self, event: &Self::Event) -> bool {
        // Condition 1: Event's source URI should be in the allowed list by the
        // report request source URIs.
        let source_match = self
            .report_request_uris
            .source_uris
            .contains(&event.uris.source_uri);

        // Condition 2: Every querier URI from the report must be in the event’s
        // querier URIs. TODO: We might change Condition 2 eventually
        // when we support split reports, where one querier is
        // authorized but not others.
        let querier_match = self
            .report_request_uris
            .querier_uris
            .iter()
            .all(|uri| event.uris.querier_uris.contains(uri));

        // Condition 3: The report’s trigger URI should be allowed by the event
        // trigger URIs.
        let trigger_match = event
            .uris
            .trigger_uris
            .contains(&self.report_request_uris.trigger_uri);

        source_match
            && querier_match
            && trigger_match
            && (self.is_matching_event)(event.filter_data)
    }
}

#[derive(Debug)]
pub struct PpaHistogramRequest<U: Uri = String> {
    start_epoch: usize,
    end_epoch: usize,
    /// Conversion value that is spread across events
    attributable_value: f64,
    laplace_noise_scale: f64,
    histogram_size: usize,
    relevant_event_selector: PpaRelevantEventSelector<U>,
    logic: AttributionLogic,
}

impl<U: Uri> PpaHistogramRequest<U> {
    /// Constructs a new `PpaHistogramRequest` with PPA-style parameters.
    /// `relevant_event_selector` are known as `filters` in the PPA spec, but
    /// this is an overloaded term.
    /// Takes sensitivity as an input to reverse-engineer the attributable
    /// value.
    pub fn new(
        config: PpaHistogramConfig,
        relevant_event_selector: PpaRelevantEventSelector<U>,
    ) -> Result<Self> {
        if config.requested_epsilon <= 0.0 {
            bail!("epsilon scale must be > 0");
        }
        if config.attributable_value < 0.0
            || config.max_attributable_value < 0.0
        {
            bail!("sensitivity values must be >= 0");
        }
        if config.histogram_size == 0 {
            bail!("histogram_size must be greater than 0");
        }

        // Sensitivity for a histogram with multiple bins.
        let query_global_sensitivity = config.max_attributable_value * 2.0;
        let laplace_noise_scale =
            query_global_sensitivity / config.requested_epsilon;

        Ok(Self {
            start_epoch: config.start_epoch,
            end_epoch: config.end_epoch,
            attributable_value: config.attributable_value,
            laplace_noise_scale,
            histogram_size: config.histogram_size,
            relevant_event_selector,
            logic: AttributionLogic::LastTouch,
        })
    }

    /// Constructs a new `PpaHistogramRequest` with direct Laplace noise scale.
    pub fn new_direct(
        config: DirectPpaHistogramConfig,
        relevant_event_selector: PpaRelevantEventSelector<U>,
    ) -> Result<Self> {
        if config.attributable_value <= 0.0 {
            bail!("attributable_value must be > 0");
        }
        if config.laplace_noise_scale <= 0.0 {
            bail!("laplace_noise_scale must be > 0");
        }
        if config.histogram_size == 0 {
            bail!("histogram_size must be greater than 0");
        }
        Ok(Self {
            start_epoch: config.start_epoch,
            end_epoch: config.end_epoch,
            attributable_value: config.attributable_value,
            laplace_noise_scale: config.laplace_noise_scale,
            histogram_size: config.histogram_size,
            relevant_event_selector,
            logic: AttributionLogic::LastTouch,
        })
    }

    pub fn get_bucket_intermediary_mapping(&self) -> &HashMap<usize, U> {
        &self.relevant_event_selector.bucket_intermediary_mapping
    }

    // Helper to check if a bucket is for a specific intermediary
    pub fn is_bucket_for_intermediary(
        &self,
        bucket_key: usize,
        intermediary_uri: &U,
    ) -> bool {
        match self
            .relevant_event_selector
            .bucket_intermediary_mapping
            .get(&bucket_key)
        {
            Some(intermediary) => intermediary == intermediary_uri,
            None => false,
        }
    }
}

impl<U: Uri> HistogramRequest for PpaHistogramRequest<U> {
    type BucketKey = PpaBucketKey;
    type HistogramEvent = PpaEvent<U>;
    type HistogramEpochId = PpaEpochId;
    type HistogramEpochEvents = PpaEpochEvents<U>;
    type HistogramUri = U;

    fn bucket_key(&self, event: &Self::HistogramEvent) -> Self::BucketKey {
        // Bucket key validation.
        if event.histogram_index >= self.histogram_size {
            log::warn!(
                "Invalid bucket key {}: exceeds histogram size {}. Event id: {}",
                event.histogram_index,
                self.histogram_size,
                event.id
            );
        }

        event.histogram_index
    }

    fn event_values<'a>(
        &self,
        relevant_events_per_epoch: &'a HashMap<PpaEpochId, PpaEpochEvents<U>>,
    ) -> Vec<(&'a PpaEvent<U>, f64)> {
        let mut event_values = vec![];

        match self.logic {
            // Only attribution logic supported for now.
            AttributionLogic::LastTouch => {
                for relevant_events in relevant_events_per_epoch.values() {
                    if let Some(last_impression) = relevant_events.last() {
                        if last_impression.histogram_index < self.histogram_size
                        {
                            event_values.push((
                                last_impression,
                                self.attributable_value,
                            ));
                        } else {
                            // Log error for dropped events
                            log::error!(
                                "Dropping event with id {} due to invalid bucket key {}",
                                last_impression.id,
                                last_impression.histogram_index
                            );
                        }
                    }
                }
            }
        }

        event_values
    }

    fn get_bucket_intermediary_mapping(&self) -> Option<&HashMap<usize, U>> {
        Some(&self.relevant_event_selector.bucket_intermediary_mapping)
    }

    fn filter_report_for_intermediary(
        &self,
        report: &HistogramReport<Self::BucketKey>,
        intermediary_uri: &U,
        _relevant_events_per_epoch: &HashMap<PpaEpochId, PpaEpochEvents<U>>,
    ) -> Option<HistogramReport<Self::BucketKey>> {
        // Collect all usize keys whose value matches intermediary_uri
        let intermediary_buckets: HashSet<usize> = self
            .relevant_event_selector
            .bucket_intermediary_mapping
            .iter()
            .filter_map(|(bucket_id, uri)| {
                if uri == intermediary_uri {
                    Some(*bucket_id)
                } else {
                    None
                }
            })
            .collect();

        // If none matched, return None; otherwise, filter and return Some(...)
        if intermediary_buckets.is_empty() {
            None
        } else {
            let filtered_bins = filter_histogram_for_intermediary(
                &report.bin_values,
                &intermediary_buckets,
            );
            Some(HistogramReport {
                bin_values: filtered_bins,
            })
        }
    }

    fn attributable_value(&self) -> f64 {
        self.attributable_value
    }

    fn histogram_report_uris(&self) -> ReportRequestUris<Self::HistogramUri> {
        self.relevant_event_selector.report_request_uris.clone()
    }
}

impl<U: Uri> EpochReportRequest for PpaHistogramRequest<U> {
    type Uri = U;
    type EpochId = PpaEpochId;
    type Event = PpaEvent<U>;
    type EpochEvents = PpaEpochEvents<U>;
    type RelevantEventSelector = PpaRelevantEventSelector<U>;
    type PrivacyBudget = PureDPBudget;
    type Report = HistogramReport<PpaBucketKey>;

    fn epoch_ids(&self) -> Vec<Self::EpochId> {
        (self.start_epoch..=self.end_epoch).rev().collect()
    }

    fn report_global_sensitivity(&self) -> f64 {
        self.histogram_report_global_sensitivity()
    }

    fn relevant_event_selector(&self) -> &Self::RelevantEventSelector {
        &self.relevant_event_selector
    }

    fn report_uris(&self) -> ReportRequestUris<Self::Uri> {
        self.relevant_event_selector.report_request_uris.clone()
    }

    fn compute_report(
        &self,
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> super::traits::QueryComputeResult<Self::Uri, Self::Report> {
        self.compute_histogram_report(relevant_events_per_epoch)
    }

    fn single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: crate::mechanisms::NormType,
    ) -> f64 {
        self.histogram_single_epoch_individual_sensitivity(report, norm_type)
    }

    fn single_epoch_source_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: crate::mechanisms::NormType,
    ) -> f64 {
        self.histogram_single_epoch_source_individual_sensitivity(
            report, norm_type,
        )
    }

    fn noise_scale(&self) -> crate::mechanisms::NoiseScale {
        NoiseScale::Laplace(self.laplace_noise_scale)
    }
}

// Utility function to filter histogram
pub fn filter_histogram_for_intermediary<BK: BucketKey>(
    full_histogram: &HashMap<BK, f64>,
    intermediary_buckets: &HashSet<BK>,
) -> HashMap<BK, f64> {
    full_histogram
        .iter()
        .filter_map(|(key, value)| {
            if intermediary_buckets.contains(key) {
                Some((key.clone(), *value))
            } else {
                None
            }
        })
        .collect()
}
