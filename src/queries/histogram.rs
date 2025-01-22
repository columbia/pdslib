use crate::budget::pure_dp_filter::PureDPBudget;
use crate::events::traits::{EpochEvents, EpochId, Event};
use crate::mechanisms::NormType;
use crate::queries::traits::{EpochReportRequest, Report, ReportRequest};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

// TODO: check if we can simplify
// TODO: make generic over event type.
#[derive(Debug)]
pub struct HistogramRequest<EpochId, EpochEvents, Event, BucketKey> {
    pub epochs: Vec<EpochId>,
    pub attributable_value: f64,
    pub noise_scale: f64,
    pub is_relevant_event: fn(&Event) -> bool,
    pub get_bin: fn(&Event) -> BucketKey,
    pub get_values: fn(&HashMap<EpochId, EpochEvents>) -> HashMap<&Event, f64>,
}

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

impl<EI, EE, E, BK> ReportRequest for HistogramRequest<EI, EE, E, BK>
where
    EI: EpochId,
    EE: EpochEvents,
    E: Event,
    BK: BucketKey,
{
    type Report = HistogramReport<BK>;
}

impl<EI, EE, E, BK> EpochReportRequest for HistogramRequest<EI, EE, E, BK>
where
    EI: EpochId,
    EE: EpochEvents,
    E: Event,
    BK: BucketKey,
{
    type EpochId = EI;
    type EpochEvents = EE;
    type PrivacyBudget = PureDPBudget;
    type ReportGlobalSensitivity = f64;
    type RelevantEventSelector = fn(&E) -> bool;

    fn get_epoch_ids(&self) -> Vec<Self::EpochId> {
        self.epochs.clone()
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        self.is_relevant_event
    }

    fn compute_report(
        &self,
        all_epoch_events: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Self::Report {
        let mut bin_values: HashMap<BK, f64> = HashMap::new();
        let mut total_value: f64 = 0.0;
        let event_values = (self.get_values)(all_epoch_events);

        for (event, value) in event_values {
            total_value += value;
            if total_value > self.attributable_value {
                // Invalid attribution function, return empty report.
                // Could also return partial attribution.
                return HistogramReport::default();
            }
            let bin = (self.get_bin)(event);
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
        2.0 * self.attributable_value
    }

    fn get_noise_scale(&self) -> f64 {
        self.noise_scale
    }
}
