use std::hash::BuildHasher;

use crate::{
    events::traits::{Event, EventStorage, RelevantEventSelector},
    util::hashmap::{HashMap, RandomState},
};

/// A simple in-memory event storage. Stores a mapping of epoch id to epoch
/// events, where each epoch events is just a vec of events.
/// Clones events when asked to retrieve events for an epoch.
#[derive(Debug)]
pub struct HashMapEventStorage<E: Event, S = RandomState>
where
    S: BuildHasher,
{
    pub epochs: HashMap<E::EpochId, Vec<E>, S>,
}

impl<E: Event, S: BuildHasher + Default> Default for HashMapEventStorage<E, S> {
    fn default() -> Self {
        Self {
            epochs: HashMap::with_hasher(S::default()),
        }
    }
}

/// Simple in-memory event storage. Stores a mapping of epoch id to events
/// in that epoch.
impl<E: Event, S: BuildHasher + Default> HashMapEventStorage<E, S> {
    pub fn new() -> Self {
        Self::with_hashmap_capacity(0)
    }

    pub fn with_hashmap_capacity(hashmap_capacity: usize) -> Self {
        Self {
            epochs: HashMap::with_capacity_and_hasher(
                hashmap_capacity,
                S::default(),
            ),
        }
    }
}

impl<E, S> EventStorage for HashMapEventStorage<E, S>
where
    E: Event + Clone,
    S: BuildHasher + Default,
{
    type Event = E;
    type Error = anyhow::Error;

    fn add_event(&mut self, event: E) -> Result<(), Self::Error> {
        let epoch_id = event.epoch_id();
        let epoch = self.epochs.entry(epoch_id).or_default();
        epoch.push(event);
        Ok(())
    }

    fn events_for_epoch(
        &mut self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> Result<impl Iterator<Item = Self::Event>, Self::Error> {
        let events = self.epochs.get(epoch_id).cloned().unwrap_or_default();

        let iterator = events.into_iter();
        Ok(iterator)
    }

    fn relevant_events_for_epoch(
        &mut self,
        epoch_id: &<Self::Event as Event>::EpochId,
        relevant_event_selector: &impl RelevantEventSelector<Event = Self::Event>,
    ) -> Result<impl Iterator<Item = Self::Event>, Self::Error> {
        // more efficient implementation that only clones relevant events
        // instead of all events for the epoch.
        let events = self
            .epochs
            .get(epoch_id)
            .map(|events| events.as_slice())
            .unwrap_or_else(|| &[]);

        let iterator = events
            .iter()
            .filter(|event| relevant_event_selector.is_relevant_event(event))
            .cloned();

        Ok(iterator)
    }
}
