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
    ) -> Self::Report {
        // TODO: implement for real
        SimpleLastTouchHistogramReport {
            attributed_value: None,
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
