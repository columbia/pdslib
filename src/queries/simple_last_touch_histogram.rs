use std::collections::HashMap;

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::{
        hashmap_event_storage::VecEpochEvents, simple_event::SimpleEvent,
        traits::RelevantEventSelector,
    },
    mechanisms::{NoiseScale, NormType},
    queries::traits::{EpochReportRequest, Report, ReportRequest},
};

#[derive(Debug)]
pub struct SimpleLastTouchHistogramRequest {
    pub epoch_start: usize,
    pub epoch_end: usize,
    pub attributable_value: f64,
    pub laplace_noise_scale: f64,
    pub is_relevant_event: fn(&SimpleEvent) -> bool,
}

pub struct SimpleRelevantEventSelector {
    pub lambda: fn(&SimpleEvent) -> bool,
}

impl RelevantEventSelector for SimpleRelevantEventSelector {
    type Event = SimpleEvent;

    fn is_relevant_event(&self, event: &SimpleEvent) -> bool {
        (self.lambda)(event)
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

impl ReportRequest for SimpleLastTouchHistogramRequest {
    type Report = SimpleLastTouchHistogramReport;
}

impl EpochReportRequest for SimpleLastTouchHistogramRequest {
    type EpochId = usize;
    type EpochEvents = VecEpochEvents<SimpleEvent>;
    type PrivacyBudget = PureDPBudget;
    type ReportGlobalSensitivity = f64;
    type RelevantEventSelector = SimpleRelevantEventSelector;

    fn get_epoch_ids(&self) -> Vec<Self::EpochId> {
        let range = self.epoch_start..=self.epoch_end;
        range.rev().collect()
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        SimpleRelevantEventSelector {
            lambda: self.is_relevant_event,
        }
    }

    fn compute_report(
        &self,
        relevant_epochs_per_epoch: &HashMap<usize, Self::EpochEvents>,
    ) -> Self::Report {
        // Browse epochs in the order given by `get_epoch_ids, most recent
        // epoch first. Within each epoch, we assume that events are
        // stored in the order that they occured
        for epoch_id in self.get_epoch_ids() {
            if let Some(relevant_events) =
                relevant_epochs_per_epoch.get(&epoch_id)
            {
                if let Some(last_impression) = relevant_events.last() {
                    // `last_impression` is the most recent relevant impression
                    // from the most recent non-empty epoch.
                    let event_key = last_impression.event_key;
                    let attributed_value = self.attributable_value;

                    // Just use event_key as the bucket key.
                    // See `ara_histogram` for a more general impression_key ->
                    // bucket_key mapping.
                    return SimpleLastTouchHistogramReport {
                        bin_value: Some((event_key, attributed_value)),
                    };
                }
            }
        }

        // No impressions were found so we return a report with a None bucket.
        SimpleLastTouchHistogramReport { bin_value: None }
    }

    fn get_single_epoch_individual_sensitivity(
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

    fn get_report_global_sensitivity(&self) -> f64 {
        self.attributable_value
    }

    fn get_noise_scale(&self) -> NoiseScale {
        NoiseScale::Laplace(self.laplace_noise_scale)
    }
}
