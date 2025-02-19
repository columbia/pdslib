//! [Experimental] ARA-style requests, that mirror https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md

use std::{collections::HashMap, vec};

use crate::{
    events::{
        ara_event::AraEvent, hashmap_event_storage::VecEpochEvents,
        traits::RelevantEventSelector,
    },
    queries::histogram::HistogramRequest,
};

#[derive(Debug, Clone)]
pub struct AraRelevantEventSelector {
    pub filters: HashMap<String, Vec<String>>,
    // TODO(https://github.com/columbia/pdslib/issues/8): add this if we drop events without the right source key
    // source_key: String,
}

/// Select events using ARA-style filters.
/// See https://github.com/WICG/attribution-reporting-api/blob/main/EVENT.md#optional-attribution-filters
impl RelevantEventSelector for AraRelevantEventSelector {
    type Event = AraEvent;

    fn is_relevant_event(&self, _event: &AraEvent) -> bool {
        // TODO(https://github.com/columbia/pdslib/issues/8): add filters to events too, and implement ARA filtering
        true
    }
}

/// An instantiation of HistogramRequest that mimics ARA's types.
/// The request corresponds to a trigger event in ARA.
/// For now, each event is mapped to a single bucket, unlike ARA which supports
/// packed queries (which can be emulated by running multiple queries).
///
/// TODO(https://github.com/columbia/pdslib/issues/8): what is "nonMatchingKeyIdsIgnored"?
#[derive(Debug)]
pub struct AraHistogramRequest {
    pub start_epoch: usize,
    pub end_epoch: usize,
    pub per_event_attributable_value: f64, /* ARA can attribute to multiple
                                            * events */
    pub attributable_value: f64, /* E.g. 2^16 in ARA, with scaling as
                                  * post-processing */
    pub noise_scale: f64,
    pub source_key: String,
    pub trigger_keypiece: usize,
    pub filters: AraRelevantEventSelector,
}

/// See https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration.
impl HistogramRequest for AraHistogramRequest {
    type EpochId = usize;
    type EpochEvents = VecEpochEvents<AraEvent>;
    type Event = AraEvent;
    type BucketKey = usize;
    type RelevantEventSelector = AraRelevantEventSelector;

    fn get_epochs_ids(&self) -> Vec<Self::EpochId> {
        (self.start_epoch..=self.end_epoch).rev().collect()
    }

    fn get_laplace_noise_scale(&self) -> f64 {
        self.noise_scale
    }

    fn get_attributable_value(&self) -> f64 {
        self.attributable_value
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        self.filters.clone()
    }

    fn get_bucket_key(&self, event: &AraEvent) -> Self::BucketKey {
        // TODO(https://github.com/columbia/pdslib/issues/8):
        // What does ARA do when the source key is not present?
        // For now I still attribute with 0 for the source keypiece, but
        // I could treat the event as irrelevant too.
        let source_keypiece = event
            .aggregatable_sources
            .get(&self.source_key)
            .copied()
            .unwrap_or(0);
        source_keypiece | self.trigger_keypiece
    }

    /// Returns the same value for each relevant event. Will be capped by
    /// `compute_report`. An alternative would be to pick one event, or
    /// split the attribution cap uniformly.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/8): Double check with
    /// Chromium logic.
    fn get_values<'a>(
        &self,
        relevant_events_per_epoch: &'a HashMap<
            Self::EpochId,
            Self::EpochEvents,
        >,
    ) -> Vec<(&'a Self::Event, f64)> {
        let mut event_values = vec![];

        for relevant_events in relevant_events_per_epoch.values() {
            for event in relevant_events.iter() {
                event_values.push((event, self.per_event_attributable_value));
            }
        }
        event_values
    }
}
