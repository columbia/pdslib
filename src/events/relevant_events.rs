use std::collections::{HashMap, HashSet};

use super::traits::{Event, EventStorage, RelevantEventSelector};

/// A struct that holds relevant events for a set of epochs.
///
/// Can be constructed either from an `EventStorage`, or directly from a
/// mapping of relevant events per epoch.
pub struct RelevantEvents<E: Event> {
    pub events_per_epoch: HashMap<E::EpochId, Vec<E>>,
}

impl<E: Event> RelevantEvents<E> {
    /// Fetches and filters relevant events from the given event storage,
    /// for the specified epochs.
    pub fn from_event_storage<ES>(
        event_storage: &ES,
        epoch_ids: &[E::EpochId],
        selector: &impl RelevantEventSelector<Event = E>,
    ) -> Result<Self, ES::Error>
    where
        ES: EventStorage<Event = E>,
    {
        let mut events_per_epoch = HashMap::new();

        for epoch_id in epoch_ids {
            // fetch all events at that epoch from storage
            let events = event_storage
                .events_for_epoch(epoch_id)?
                // filter relevant events using the selector
                .filter(|event| selector.is_relevant_event(event))
                .collect();

            // store the events in the map
            events_per_epoch.insert(epoch_id.clone(), events);
        }

        let this = Self::from_mapping(events_per_epoch);
        Ok(this)
    }

    /// Constructs a `RelevantEvents` instance directly from a mapping of
    /// epochs, to relevant events for each of those epochs.
    pub fn from_mapping(events_per_epoch: HashMap<E::EpochId, Vec<E>>) -> Self {
        Self { events_per_epoch }
    }

    /// Get the relevant events for a specific epoch.
    pub fn for_epoch(&self, epoch_id: &E::EpochId) -> &[E] {
        self.events_per_epoch
            .get(epoch_id)
            .map(|events| events.as_slice())
            .unwrap_or_default()
    }

    /// Get the relevant events for a specific epoch, as well as source URI.
    pub fn for_epoch_and_source(
        &self,
        epoch_id: &E::EpochId,
        source: &E::Uri,
    ) -> Vec<&E> {
        let events_for_epoch = self.for_epoch(epoch_id);

        // filter events for the given source
        let events_for_source: Vec<&E> = events_for_epoch
            .iter()
            .filter(|event| &event.event_uris().source_uri == source)
            .collect();

        events_for_source
    }

    /// Get the set of unique source URIs for relevant events in the given epoch.
    pub fn sources_for_epoch(&self, epoch_id: &E::EpochId) -> HashSet<E::Uri> {
        let events_for_epoch = self.for_epoch(epoch_id);

        // collect unique source URIs for the given epoch
        let sources: HashSet<E::Uri> = events_for_epoch
            .iter()
            .map(|event| event.event_uris().source_uri)
            .collect();
        sources
    }

    /// Drop and forget the given epoch and all its events.
    pub fn drop_epoch(&mut self, epoch_id: &E::EpochId) {
        self.events_per_epoch.remove(epoch_id);
    }
}
