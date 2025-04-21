use std::fmt::Debug;

use crate::events::traits::{Event, EventUris};

use super::traits::Uri;

/// A barebones event type for testing and demo purposes. See ara_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent<U: Uri = String> {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,
    pub uris: EventUris<U>,
}

impl<U: Uri> Event for SimpleEvent<U> {
    type EpochId = usize;
    type Uri = U;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> EventUris<U> {
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
            uris: EventUris::mock(),
        };
        assert_eq!(event.id, 1);
    }
}
