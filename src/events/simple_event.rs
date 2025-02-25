use http::Uri;

use crate::events::traits::Event;

/// A barebones event type for testing and demo purposes. See ara_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,

    pub source_uri: Uri,
    pub trigger_uris: Vec<Uri>,
    pub querier_uris: Vec<Uri>,
}

impl Event for SimpleEvent {
    type EpochId = usize;

    fn get_epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }
    
    fn source_uri(&self) -> http::Uri {
        self.source_uri.clone()
    }
    
    fn trigger_uris(&self) -> Vec<http::Uri> {
        self.trigger_uris.clone()
    }
    
    fn querier_uris(&self) -> Vec<http::Uri> {
        self.querier_uris.clone()
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

            source_uri: "https://example.com".parse().unwrap(),
            trigger_uris: vec![],
            querier_uris: vec![],
        };
        assert_eq!(event.id, 1);
    }
}
