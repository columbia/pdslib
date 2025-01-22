use crate::budget::pure_dp_filter::PureDPBudget;
use crate::events::traits::RelevantEventSelector;
use crate::events::traits::{EpochEvents, EpochId, Event};
use crate::mechanisms::NormType;
use crate::queries::traits::{EpochReportRequest, Report, ReportRequest};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct HistogramReport<BucketKey> {
    pub bin_values: HashMap<BucketKey, f64>,
}

/// Trait for bucket keys.
pub trait BucketKey: Debug + Hash + Eq {}

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

pub trait HistogramRequest: Debug {
    type EpochId: EpochId;
    type EpochEvents: EpochEvents;
    type Event: Event;
    type BucketKey: BucketKey;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;
    fn get_epochs(&self) -> Vec<Self::EpochId>;

    fn get_noise_scale(&self) -> f64;

    fn get_attributable_value(&self) -> f64;

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector;

    fn get_bucket_key(&self, event: &Self::Event) -> Self::BucketKey;

    /// Events can point to the all_epoch_events, hence the lifetime.
    fn get_values<'a>(
        &self,
        all_epoch_events: &'a HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Vec<(&'a Self::Event, f64)>;
}

impl<H: HistogramRequest> ReportRequest for H {
    type Report = HistogramReport<<H as HistogramRequest>::BucketKey>;
}

/// Any type that implements HistogramRequest can be used as an
/// EpochReportRequest.
impl<H: HistogramRequest> EpochReportRequest for H {
    type EpochId = H::EpochId;
    type EpochEvents = H::EpochEvents;
    type PrivacyBudget = PureDPBudget;
    type ReportGlobalSensitivity = f64;
    type RelevantEventSelector = H::RelevantEventSelector; // Use the full request as the selector.

    // TODO: inherit these things?
    fn get_epoch_ids(&self) -> Vec<Self::EpochId> {
        self.get_epochs()
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        self.get_relevant_event_selector()
    }

    fn compute_report(
        &self,
        all_epoch_events: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Self::Report {
        let mut bin_values: HashMap<H::BucketKey, f64> = HashMap::new();
        let mut total_value: f64 = 0.0;
        let event_values = self.get_values(all_epoch_events);

        // TODO: the order matters, use an ordered map?
        for (event, value) in event_values {
            total_value += value;
            if total_value > self.get_attributable_value() {
                // Return partial attribution to stay within the cap.
                return HistogramReport { bin_values };
            }
            let bin = self.get_bucket_key(event);
            *bin_values.entry(bin).or_default() += value;
        }

        HistogramReport { bin_values }
    }

    // TODO: double check this
    fn get_single_epoch_individual_sensitivity(
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

    /// See https://arxiv.org/pdf/2405.16719, Thm. 18
    fn get_global_sensitivity(&self) -> f64 {
        // TODO: if we have only one bin then we can remove the factor 2
        2.0 * self.get_attributable_value()
    }

    fn get_noise_scale(&self) -> f64 {
        self.get_noise_scale()
    }
}
