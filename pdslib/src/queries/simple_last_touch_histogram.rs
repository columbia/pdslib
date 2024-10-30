use crate::budget::pure_dp_filter::PureDPBudget;
use crate::events::simple_events::SimpleEpochEvents;
use crate::queries::traits::ReportRequest;

// TODO: relevant events?

#[derive(Debug)]
pub struct SimpleLastTouchHistogramRequest {
    pub epoch_start: usize,
    pub epoch_end: usize,
    pub attributable_value: f64,
}

#[derive(Debug)]
pub struct SimpleLastTouchHistogramReport {
    // Value attributed to one bin or None if no attribution
    pub attributed_value: Option<(String, f64)>,
}

impl ReportRequest for SimpleLastTouchHistogramRequest {
    type EpochId = usize;
    type EpochEvents = SimpleEpochEvents;
    type Report = SimpleLastTouchHistogramReport;
    type PrivacyBudget = PureDPBudget;

    fn get_epoch_ids(&self) -> Vec<Self::EpochId> {
        let range = self.epoch_start..=self.epoch_end;
        range.collect()
    }

    fn compute_report(
        &self,
        all_epoch_events: &Vec<Self::EpochEvents>,
    ) -> Self::Report 
    {
        // We assume that all_epoch_events is always stored in the order that they occured
        for epoch_events in all_epoch_events.iter().rev() {
            if let Some(last_impression) = epoch_events.last() {
                if last_impression.epoch_number > self.epoch_end || last_impression.epoch_number < self.epoch_start {
                    continue;
                }
                let impression_epoch_number = last_impression.epoch_number;
                let impression_id = last_impression.id;

                let bucket_key = format!("{}_{}", impression_id, impression_epoch_number);
                let bucket_value = last_impression.value.min(self.attributable_value);
             
                return SimpleLastTouchHistogramReport {
                    attributed_value: Some((bucket_key, bucket_value)),
                };
            }
        }

        // No impressions were found so we return a report with a zero-value bucket.
        let bucket_key = format!("{}_{}", 0, 0);
        SimpleLastTouchHistogramReport {
            attributed_value: Some((bucket_key, 0.0)),
        }
    }

    fn compute_individual_budget(
        &self,
        epoch_events: &Self::EpochEvents,
    ) -> Self::PrivacyBudget {
        // TODO: implement for real
        PureDPBudget { epsilon: 0.0 }
    }
}
