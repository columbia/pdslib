use crate::events::traits::{EventStorage};
use std::collections::HashMap;

// TODO: add enough things to run basic queries and filter by attributes.
#[derive(Debug)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub value: usize,
}


// TODO: if we have other event types, we could make this a generic, like the filter hashmap.
#[derive(Debug)]
pub struct SimpleEventStorage {
    pub epochs: HashMap<usize, Vec<SimpleEvent>>,
}

// TODO: define epoch type if that helps.
impl SimpleEventStorage {
    pub fn new() -> SimpleEventStorage {
        SimpleEventStorage {
            epochs: HashMap::new(),
        }
    }
}

impl EventStorage for SimpleEventStorage {
    type Event = SimpleEvent;
    type QuerierId = (); // Only one querier for now
    type EpochId = usize;

    fn add_event(&mut self, event: Self::Event, epoch_id: Self::EpochId, querier_id: Self::QuerierId) -> Result<(), ()> {
        let epoch = self.epochs.entry(epoch_id).or_insert(Vec::new());
        epoch.push(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_event() {
        let event = SimpleEvent { id: 1, epoch_number: 1, value: 3};
        assert_eq!(event.id, 1);
    }
}