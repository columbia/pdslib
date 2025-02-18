use crate::events::traits::Event;

/// A barebones event type for testing and demo purposes. See ara_event for a
/// richer type.
#[derive(Debug, Clone)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,
}

impl Event for SimpleEvent {
    type EpochId = usize;

    fn get_epoch_id(&self) -> Self::EpochId {
        self.epoch_number
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
        };
        assert_eq!(event.id, 1);
    }
}
