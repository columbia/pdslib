use crate::events::traits::{Event, EventUris};

/// A barebones event type for testing and demo purposes. See ara_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,
    pub uris: EventUris<String>,
}

impl Event for SimpleEvent {
    type EpochId = usize;
    type Uri = String;
    type HistogramIndex = usize;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> EventUris<String> {
        self.uris.clone()
    }

    fn histogram_index(&self) -> Self::HistogramIndex {
        0  // Default to 0 for unimplemented histogram index 
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_event() {
        let event = SimpleEvent {
            id: 1,
            epoch_number: 1,
            event_key: 3,
            uris: EventUris::mock(),
        };
        assert_eq!(event.id, 1);
    }
}
