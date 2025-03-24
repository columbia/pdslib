use std::{collections::HashMap, marker::PhantomData};

use crate::events::traits::{
    EpochEvents, EpochSourceEventsResult, Event, EventStorage,
    RelevantEventSelector,
};

pub type VecEpochEvents<E> = Vec<E>;

impl<E: Event> EpochEvents for VecEpochEvents<E> {
    fn new() -> Self {
        Vec::new()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

/// A simple in-memory event storage. Stores a mapping of epoch id to epoch
/// events, where each epoch events is just a vec of events.
/// Clones events when asked to retrieve events for an epoch.
#[derive(Debug, Default)]
pub struct HashMapEventStorage<E: Event, RES: RelevantEventSelector<Event = E>>
{
    epochs: HashMap<E::EpochId, VecEpochEvents<E>>,
    _phantom: PhantomData<RES>,
}

impl<E: Event, RES: RelevantEventSelector<Event = E>>
    HashMapEventStorage<E, RES>
{
    pub fn new() -> Self {
        Self {
            epochs: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

impl<E, RES> EventStorage for HashMapEventStorage<E, RES>
where
    E: Event + Clone,
    RES: RelevantEventSelector<Event = E>,
{
    type Uri = E::Uri;
    type Event = E;
    type EpochEvents = VecEpochEvents<E>;
    type RelevantEventSelector = RES;
    type Error = anyhow::Error;

    fn add_event(&mut self, event: E) -> Result<(), Self::Error> {
        let epoch_id = event.epoch_id();
        let epoch = self.epochs.entry(epoch_id).or_default();
        epoch.push(event);
        Ok(())
    }

    fn relevant_epoch_events(
        &self,
        epoch_id: &E::EpochId,
        selector: &RES,
    ) -> Result<Option<VecEpochEvents<E>>, Self::Error> {
        // Return relevant events for a given epoch_id
        // TODO: instead of returning an empty Vec, return None?
        let events = self.epochs.get(epoch_id).map(|events| {
            events
                .iter()
                .filter(|event| selector.is_relevant_event(event))
                .cloned()
                .collect()
        });
        Ok(events)
    }

    fn relevant_epoch_source_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
        selector: &Self::RelevantEventSelector,
    ) -> EpochSourceEventsResult<Self::Uri, Self::EpochEvents, Self::Error>
    {
        // Return relevant events for a given epoch_id
        // TODO: instead of returning an empty Vec, return None?
        let events_map = self.epochs.get(epoch_id).map(|events| {
            events
                .iter()
                .filter(|event| selector.is_relevant_event(event))
                .cloned()
                .fold(
                    HashMap::new(),
                    |mut acc: HashMap<<E as Event>::Uri, Vec<E>>, event| {
                        let key = event.event_uris().source_uri.clone();
                        acc.entry(key).or_default().push(event);
                        acc
                    },
                )
        });
        Ok(events_map)
    }
}
