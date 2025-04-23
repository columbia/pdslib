use std::collections::HashMap;

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::{
        hashmap_event_storage::VecEpochEvents, simple_event::SimpleEvent,
        traits::RelevantEventSelector,
    },
    mechanisms::{NoiseScale, NormType},
    queries::traits::{
        EpochReportRequest, QueryComputeResult, Report, ReportRequestUris,
    },
};

#[derive(Debug)]
pub struct SimpleLastTouchHistogramRequest {
    pub epoch_start: usize,
    pub epoch_end: usize,
    pub report_global_sensitivity: f64,
    pub query_global_sensitivity: f64,
    pub requested_epsilon: f64,
    pub is_relevant_event: SimpleRelevantEventSelector,
    pub report_uris: ReportRequestUris<String>,
}

#[derive(Clone, Copy)]
pub struct SimpleRelevantEventSelector {
    pub lambda: fn(&SimpleEvent) -> bool,
}

impl RelevantEventSelector for SimpleRelevantEventSelector {
    type Event = SimpleEvent;

    fn is_relevant_event(&self, event: &Self::Event) -> bool {
        (self.lambda)(event)
    }
}

impl std::fmt::Debug for SimpleRelevantEventSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleRelevantEventSelector")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimpleLastTouchHistogramReport {
    // Value attributed to one bin or None if no attribution
    pub bin_value: Option<(
        usize, // Bucket key (which is just event_key for now)
        f64,   // Attributed value
    )>,
}

impl Report for SimpleLastTouchHistogramReport {}

impl EpochReportRequest for SimpleLastTouchHistogramRequest {
    type EpochId = usize;
    type Event = SimpleEvent;
    type EpochEvents = VecEpochEvents<SimpleEvent>;
    type PrivacyBudget = PureDPBudget;
    type RelevantEventSelector = SimpleRelevantEventSelector;
    type Report = SimpleLastTouchHistogramReport;
    type Uri = String;

    fn report_uris(&self) -> ReportRequestUris<String> {
        self.report_uris.clone()
    }

    fn epoch_ids(&self) -> Vec<Self::EpochId> {
        let range = self.epoch_start..=self.epoch_end;
        range.rev().collect()
    }

    fn relevant_event_selector(&self) -> &Self::RelevantEventSelector {
        &self.is_relevant_event
    }

    fn compute_report(
        &self,
        relevant_epochs_per_epoch: &HashMap<usize, Self::EpochEvents>,
    ) -> QueryComputeResult<Self::Uri, Self::Report> {
        // Browse epochs in the order given by `epoch_ids, most recent
        // epoch first. Within each epoch, we assume that events are
        // stored in the order that they occured
        for epoch_id in self.epoch_ids() {
            if let Some(relevant_events) =
                relevant_epochs_per_epoch.get(&epoch_id)
            {
                if let Some(last_impression) = relevant_events.last() {
                    // `last_impression` is the most recent relevant impression
                    // from the most recent non-empty epoch.
                    let event_key = last_impression.event_key;
                    let attributed_value = self.report_global_sensitivity;

                    // Just use event_key as the bucket key.
                    // See `ppaa_histogram` for a more general impression_key ->
                    // bucket_key mapping.
                    return QueryComputeResult::new(
                        HashMap::new(),
                        HashMap::from([(
                            self.report_uris
                                .querier_uris
                                .first()
                                .unwrap()
                                .clone(),
                            SimpleLastTouchHistogramReport {
                                bin_value: Some((event_key, attributed_value)),
                            },
                        )]),
                    );
                }
            }
        }

        // No impressions were found so we return a report with a None bucket.
        QueryComputeResult::new(
            HashMap::new(),
            HashMap::from([(
                self.report_uris.querier_uris.first().unwrap().clone(),
                SimpleLastTouchHistogramReport { bin_value: None },
            )]),
        )
    }

    fn single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64 {
        // Report has at most one non-zero bin, so L1 and L2 norms are the same.
        let attributed_value = match report.bin_value {
            Some((_, av)) => av,
            None => 0.0,
        };
        match norm_type {
            NormType::L1 => attributed_value.abs(),
            NormType::L2 => attributed_value.abs(),
        }
    }

    fn single_epoch_source_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64 {
        self.single_epoch_individual_sensitivity(report, norm_type)
    }

    fn report_global_sensitivity(&self) -> f64 {
        self.report_global_sensitivity
    }

    fn noise_scale(&self) -> NoiseScale {
        NoiseScale::Laplace(
            self.query_global_sensitivity / self.requested_epsilon,
        )
    }
}
