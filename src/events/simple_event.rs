use http::Uri;

use crate::events::traits::Event;

use super::traits::EventUris;

/// A barebones event type for testing and demo purposes. See ara_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,
    pub uris: EventUris,
}

impl Event for SimpleEvent {
    type EpochId = usize;

    fn get_epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> EventUris {
        self.uris.clone()
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
            uris: EventUris {
                source_uri: Uri::from_static("https://example.com"),
                trigger_uris: vec![],
                querier_uris: vec![],
            },
        };
        assert_eq!(event.id, 1);
    }
}
