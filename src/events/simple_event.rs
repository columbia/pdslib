use std::fmt::Debug;

use super::traits::Uri;
use crate::events::traits::{Event, EventUris};

/// A barebones event type for testing and demo purposes. See ppa_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent<U: Uri = String> {
    pub id: u64,
    pub epoch_number: u64,
    pub event_key: u64,
    pub uris: EventUris<U>,
}

impl<U: Uri> Event for SimpleEvent<U> {
    type EpochId = u64;
    type Uri = U;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> &EventUris<U> {
        &self.uris
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
