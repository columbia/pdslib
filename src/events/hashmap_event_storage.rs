use crate::{
    events::traits::{Event, EventStorage},
    util::hashmap::HashMap,
};

/// A simple in-memory event storage. Stores a mapping of epoch id to epoch
/// events, where each epoch events is just a vec of events.
/// Clones events when asked to retrieve events for an epoch.
#[derive(Debug, Default)]
pub struct HashMapEventStorage<E: Event> {
    epochs: HashMap<E::EpochId, Vec<E>>,
}

/// Simple in-memory event storage. Stores a mapping of epoch id to events
/// in that epoch.
impl<E: Event> HashMapEventStorage<E> {
    pub fn new() -> Self {
        Self {
            epochs: HashMap::new(),
        }
    }
}

impl<E> EventStorage for HashMapEventStorage<E>
where
    E: Event + Clone,
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
}
