use crate::{
    events::ara_event::AraEvent, events::event_storage::VecEpochEvents,
    queries::histogram::HistogramRequest,
};

use std::collections::HashMap;

/// An instantiation of HistogramRequest that mimics ARA's types.
/// The request corresponds to a trigger event in ARA.
type AraHistogramRequest =
    HistogramRequest<usize, VecEpochEvents<AraEvent>, AraEvent, usize>;

/// Which key piece to query. Each event is mapped to a single bucket.
/// TODO: support multiple source keys.
impl AraHistogramRequest {
    fn new(
        start_epoch: usize,
        end_epoch: usize,
        attributable_value: f64,
        noise_scale: f64,
        is_relevant_event: fn(&AraEvent) -> bool,
        get_bin: fn(&AraEvent) -> usize,
        get_values: fn(
            &HashMap<usize, VecEpochEvents<AraEvent>>,
        ) -> HashMap<&AraEvent, f64>,
    ) -> Self {
        Self {
            epochs: (start_epoch..=end_epoch).collect(),
            attributable_value,
            noise_scale,
            is_relevant_event,
            get_bin,
            get_values,
        }
    }
}
