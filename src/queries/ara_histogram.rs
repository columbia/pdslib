use crate::events::traits::RelevantEventSelector;
use crate::{
    events::ara_event::AraEvent, events::hashmap_event_storage::VecEpochEvents,
    queries::histogram::HistogramRequest,
};

use std::collections::HashMap;
use std::vec;

#[derive(Debug, Clone)]
struct AraRelevantEventSelector {
    filters: HashMap<String, Vec<String>>,
    // source_key: String, // TODO: add this if we drop events without the right source key
}

/// Select events using ARA-style filters.
/// See https://github.com/WICG/attribution-reporting-api/blob/main/EVENT.md#optional-attribution-filters
impl RelevantEventSelector for AraRelevantEventSelector {
    type Event = AraEvent;

    fn is_relevant_event(&self, event: &AraEvent) -> bool {
        // TODO: add filters to events too, and implement ARA filtering
        true
    }
}

/// An instantiation of HistogramRequest that mimics ARA's types.
/// The request corresponds to a trigger event in ARA.
/// For now, each event is mapped to a single bucket, unlike ARA which supports
/// packed queries (which can be emulated by running multiple queries).
/// TODO: what is "nonMatchingKeyIdsIgnored"?
#[derive(Debug)]
struct AraHistogramRequest {
    start_epoch: usize,
    end_epoch: usize,
    per_event_attributable_value: f64, // ARA can attribute to multiple events
    attributable_value: f64, // E.g. 2^16 in ARA, with scaling as post-processing
    noise_scale: f64,
    source_key: String,
    trigger_keypiece: usize,
    filters: AraRelevantEventSelector,
}

/// See https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration.
impl HistogramRequest for AraHistogramRequest {
    type EpochId = usize;
    type EpochEvents = VecEpochEvents<AraEvent>;
    type Event = AraEvent;
    type BucketKey = usize;
    type RelevantEventSelector = AraRelevantEventSelector;

    fn get_epochs(&self) -> Vec<Self::EpochId> {
        (self.start_epoch..=self.end_epoch).rev().collect()
    }

    fn get_noise_scale(&self) -> f64 {
        self.noise_scale
    }

    fn get_attributable_value(&self) -> f64 {
        self.attributable_value
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        self.filters.clone()
    }

    fn get_bucket_key(&self, event: &AraEvent) -> Self::BucketKey {
        // TODO: What does ARA do when the source key is not present?
        // For now I still attribute with 0 for the source keypiece, but
        // I could treat the event as irrelevant too.
        let source_keypiece = event
            .aggregatable_sources
            .get(&self.source_key)
            .copied()
            .unwrap_or(0);
        let bucket_key = source_keypiece | self.trigger_keypiece;
        bucket_key
    }

    /// Returns the same value for each relevant event. Will be capped by `compute_report`.
    /// An alternative would be to pick one event, or split the attribution cap uniformly.
    /// TODO: Double check with Chromium logic.
    fn get_values<'a>(
        &self,
        all_epoch_events: &'a HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> Vec<(&'a Self::Event, f64)> {
        let mut event_values = vec![];

        for epoch_events in all_epoch_events.values() {
            for event in epoch_events.iter() {
                event_values.push((event, self.per_event_attributable_value));
            }
        }
        event_values
    }
}
