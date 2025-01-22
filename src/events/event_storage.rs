use crate::events::traits::EventStorage;
use crate::events::traits::{EpochEvents, Event};

use std::collections::HashMap;

pub type VecEpochEvents<E: Event> = Vec<E>;

impl<E: Event> EpochEvents for VecEpochEvents<E> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// A simple in-memory event storage. Stores a mapping of epoch id to epoch
/// events, where each epoch events is just a vec of events.
/// Clones events when asked to retrieve events for an epoch.
#[derive(Debug)]
pub struct HashMapEventStorage<E: Event> {
    epochs: HashMap<E::EpochId, VecEpochEvents<E>>,
}

impl<E: Event> HashMapEventStorage<E> {
    pub fn new() -> Self {
        Self {
            epochs: HashMap::new(),
        }
    }
}

impl<E: Event + Clone> EventStorage for HashMapEventStorage<E> {
    type Event = E;
    type EpochEvents = VecEpochEvents<E>;
    type RelevantEventSelector = fn(&E) -> bool;

    fn add_event(&mut self, event: E) -> Result<(), ()> {
        let epoch_id = event.get_epoch_id();
        let epoch = self.epochs.entry(epoch_id).or_default();
        epoch.push(event);
        Ok(())
    }

    fn get_epoch_events(
        &self,
        epoch_id: &E::EpochId,
        is_relevant_event: &Self::RelevantEventSelector,
    ) -> Option<Self::EpochEvents> {
        // Return relevant events for a given epoch_id
        self.epochs.get(&epoch_id).map(|events| {
            events
                .iter()
                .filter(|event| is_relevant_event(event))
                .cloned()
                .collect()
        })
    }
}
