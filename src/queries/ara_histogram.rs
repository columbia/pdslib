use crate::events::traits::RelevantEventSelector;
use crate::{
    events::ara_event::AraEvent, events::hashmap_event_storage::VecEpochEvents,
    queries::histogram::HistogramRequest,
};

use std::collections::HashMap;

/// An instantiation of HistogramRequest that mimics ARA's types.
/// The request corresponds to a trigger event in ARA.
/// Which key piece to query. Each event is mapped to a single bucket.
/// TODO: support multiple source keys, eventually?
/// TODO: what is "nonMatchingKeyIdsIgnored"?
#[derive(Debug)]
struct AraHistogramRequest {
    start_epoch: usize,
    end_epoch: usize,
    attributable_value: f64,
    noise_scale: f64,
    source_key: String,
    trigger_keypiece: usize,
}

struct AraRelevantEventSelector {
    source_key: String,
}

impl RelevantEventSelector for AraRelevantEventSelector {
    type Event = AraEvent;

    fn is_relevant_event(&self, event: &AraEvent) -> bool {
        // TODO: add filters too
        event.aggregatable_sources.contains_key(&self.source_key)
    }
}

impl HistogramRequest for AraHistogramRequest {
    type EpochId = usize;
    type EpochEvents = VecEpochEvents<AraEvent>;
    type Event = AraEvent;
    type BucketKey = usize;
    type RelevantEventSelector = AraRelevantEventSelector;

    fn get_epochs(&self) -> Vec<Self::EpochId> {
        (self.start_epoch..=self.end_epoch).collect()
    }

    fn get_noise_scale(&self) -> f64 {
        self.noise_scale
    }

    fn get_attributable_value(&self) -> f64 {
        self.attributable_value
    }

    fn get_relevant_event_selector(&self) -> Self::RelevantEventSelector {
        AraRelevantEventSelector {
            source_key: self.source_key.clone(), // TODO: extract?
        }
    }

    fn get_bucket_key(&self, event: &AraEvent) -> Self::BucketKey {
        // Called only on relevant events, which have the source key.
        let source_keypiece = event
            .aggregatable_sources
            .get(&self.source_key)
            .copied()
            .unwrap_or(0);
        let bucket_key = source_keypiece | self.trigger_keypiece;
        bucket_key
    }

    fn get_values(
        &self,
        all_epoch_events: &HashMap<Self::EpochId, Self::EpochEvents>,
    ) -> HashMap<&Self::Event, f64> {
        // TODO: implement. How does ARA do this?
        todo!()
    }
}
