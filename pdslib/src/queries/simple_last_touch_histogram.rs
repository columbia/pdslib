use std::collections::HashMap;

use crate::budget::pure_dp_filter::PureDPBudget;
use crate::events::simple_events::SimpleEpochEvents;
use crate::queries::traits::ReportRequest;
// TODO: relevant events?

#[derive(Debug)]
pub struct SimpleLastTouchHistogramRequest {
    pub epoch_start: usize,
    pub epoch_end: usize,
    pub attributable_value: f64,
    pub noise_scale: f64,
}

#[derive(Debug, Clone, Default)]
pub struct SimpleLastTouchHistogramReport {
    // Value attributed to one bin or None if no attribution
    pub attributed_value: Option<(
        usize, // Epoch ID
        usize, // Event ID
        f64,   // Attributed value
    )>,
}

#[derive(PartialEq)]
pub enum NormType {
    L1,
    L2,
}

impl ReportRequest for SimpleLastTouchHistogramRequest {
    type EpochId = usize;
    type EpochEvents = SimpleEpochEvents;
    type Report = SimpleLastTouchHistogramReport;
    type PrivacyBudget = PureDPBudget;
    type ReportGlobalSensitivity = f64;

    fn get_epoch_ids(&self) -> Vec<Self::EpochId> {
        let range = self.epoch_start..=self.epoch_end;
        range.rev().collect()
    }

    fn compute_report(
        &self,
        all_epoch_events: &HashMap<usize, Self::EpochEvents>,
    ) -> Self::Report {
        // TODO: Browse epochs in the order given by `get_epoch_ids`.
        // We assume that all_epoch_events is always stored in the order that
        // they occured
        for epoch_id in self.get_epoch_ids() {
            // For now, we assume that all the events are relevant, so we just
            // need to check the most recent one. TODO: eventually
            // add the notion of "relevant events" to the `SimpleEvent` struct,
            // and browse all the events from `epoch_events` instead of the last
            // one.
            if let Some(epoch_events) = all_epoch_events.get(&epoch_id) {
                if let Some(last_impression) = epoch_events.last() {
                    if last_impression.epoch_number > self.epoch_end
                        || last_impression.epoch_number < self.epoch_start
                    {
                        continue;
                    }

                    // TODO: allow ReportRequest to give a custom impression_key
                    // -> bucket_key mapping. Also potentially depending on the
                    // conversion key. Check how ARA implements it with the
                    // source/trigger keypiece.
                    let event_id = last_impression.event_key;
                    let attributed_value = self.attributable_value;

                    return SimpleLastTouchHistogramReport {
                        attributed_value: Some((
                            epoch_id,
                            event_id,
                            attributed_value,
                        )),
                    };
                }
            }
        }

        // No impressions were found so we return a report with a None bucket.
        SimpleLastTouchHistogramReport {
            attributed_value: None,
        }
    }

    fn get_single_epoch_individual_sensitivity(
        &self,
        report: &Self::Report,
        norm_type: NormType,
    ) -> f64 {
        if norm_type == NormType::L1 {
            // L2 norm.
            match report.attributed_value {
                Some((_, _, av)) => {
                    let av_abs = av.abs();
                    return av_abs;
                }
                None => {
                    return 0.0;
                }
            }
        } else if norm_type == NormType::L2 {
            // L1 norm.
            match report.attributed_value {
                Some((_, _, av)) => {
                    let av_abs = av.abs();
                    return (av_abs * av_abs).sqrt();
                }
                None => {
                    return 0.0;
                }
            }
        } else {
            panic!("Unsupported norm type.");
        }
    }

    fn get_global_sensitivity(&self) -> f64 {
        return self.attributable_value;
    }

    fn get_noise_scale(&self) -> f64 {
        return self.noise_scale;
    }
}
