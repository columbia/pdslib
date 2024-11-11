use crate::events::traits::{Event, EventStorage};
use std::collections::HashMap;

use super::traits::EpochEvents;

// TODO: add enough things to run basic queries and filter by attributes.
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

// NOTE: wrap in a struct if we need to implement more traits on this.
pub type SimpleEpochEvents = Vec<SimpleEvent>;

impl EpochEvents for SimpleEpochEvents {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

// TODO: if we have other event types, we could make this a generic, like the filter hashmap.
#[derive(Debug)]
pub struct SimpleEventStorage {
    pub epochs: HashMap<usize, SimpleEpochEvents>,
}

impl SimpleEventStorage {
    pub fn new() -> SimpleEventStorage {
        SimpleEventStorage {
            epochs: HashMap::new(),
        }
    }
}

impl EventStorage for SimpleEventStorage {
    type Event = SimpleEvent;
    type EpochEvents = SimpleEpochEvents; // TODO: use a pointer and add lifetime? Or just copy for now, nice to edit inplace anyway.

    fn add_event(
        &mut self,
        event: Self::Event,
        // querier_id: Self::QuerierId,
    ) -> Result<(), ()> {
        let epoch_id = event.get_epoch_id();
        let epoch = self.epochs.entry(epoch_id).or_insert(Vec::new());
        epoch.push(event);
        Ok(())
    }

    fn get_epoch_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> Option<Self::EpochEvents> {
        self.epochs.get(&epoch_id).cloned()
    }

    fn get_event_count(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> usize {
        self.get_epoch_events(&epoch_id)
            .map_or(0, |events| events.len())
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
