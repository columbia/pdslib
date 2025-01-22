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

/// Trait for generic histogram requests. Any type satisfying this interface
/// will be callable as a valid ReportRequest with the right accounting.
/// Following the formalism from https://arxiv.org/pdf/2405.16719, Thm 18.
/// Can be instantiated by ARA-style queries in particular.
pub trait HistogramRequest: Debug {
    type EpochId: EpochId;
    type EpochEvents: EpochEvents;
    type Event: Event;
    type BucketKey: BucketKey;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;

    /// Returns the ids of the epochs that are relevant for this query.
    /// Typically a range of epochs.
    fn get_epochs_ids(&self) -> Vec<Self::EpochId>;

    /// Returns the Laplace noise scale added after summing all the reports.
    fn get_noise_scale(&self) -> f64;

    /// Returns the maximum attributable value, i.e. the maximum L1 norm of an
    /// attributed histogram.
    fn get_attributable_value(&self) -> f64;

    /// Returns a selector object, that can be passed to the event storage to
    /// retrieve relevant events. The selector can also output a boolean
    /// indicating whether a single event is relevant.
    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector;

    /// Returns the histogram bucket key (bin) for a given event.
    fn get_bucket_key(&self, event: &Self::Event) -> Self::BucketKey;

    /// Attribuets a value to each event in `all_epoch_events`, which will be
    /// obtained by retrieving *relevant* events from the event storage.
    /// Events can point to the all_epoch_events, hence the lifetime.
    fn get_values<'a>(
        &self,
        all_epoch_events: &'a HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Vec<(&'a Self::Event, f64)>;
}

impl<H: HistogramRequest> ReportRequest for H {
    type Report = HistogramReport<<H as HistogramRequest>::BucketKey>;
}

/// We implement the EpochReportRequest trait, so any type that implements
/// HistogramRequest can be used as an EpochReportRequest.
impl<H: HistogramRequest> EpochReportRequest for H {
    type EpochId = H::EpochId;
    type EpochEvents = H::EpochEvents;
    type PrivacyBudget = PureDPBudget;
    type ReportGlobalSensitivity = f64;
    type RelevantEventSelector = H::RelevantEventSelector; // Use the full request as the selector.

    /// Re-expose some methods (TODO: any cleaner inheritance?)
    fn get_epoch_ids(&self) -> Vec<H::EpochId> {
        self.get_epochs_ids()
    }

    fn get_relevant_event_selector(&self) -> H::RelevantEventSelector {
        self.get_relevant_event_selector()
    }

    fn get_noise_scale(&self) -> f64 {
        self.get_noise_scale()
    }

    /// Computes the report by attributing values to events, and then summing
    /// events by bucket.
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

    /// Computes individual sensitivity in the single epoch case.
    /// TODO: double check this
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

    /// Computes the global sensitivity, useful for the multi-epoch case.
    /// See https://arxiv.org/pdf/2405.16719, Thm. 18
    fn get_global_sensitivity(&self) -> f64 {
        // TODO: if we have only one bin then we can remove the factor 2
        2.0 * self.get_attributable_value()
    }
}
