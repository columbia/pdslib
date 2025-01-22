use crate::events::traits::Event;

/// TODO: add enough things to run basic queries and filter by attributes.
/// use https://github.com/patcg/meetings/blob/main/2024/09/27-tpac/Privacy-Preserving%20Attribution%20Proposed%20Roadmap.pdf
#[derive(Debug, Clone)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub event_key: usize,
    // TODO: consider adding timestamp
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
