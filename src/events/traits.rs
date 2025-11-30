use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
};

use crate::events::uri_set::UriSet;

/// Marker trait with bounds for epoch identifiers.
pub trait EpochId: Clone + Copy + Debug + Eq + Hash {}

/// Implement EpochId for all eligible types
impl<T: Clone + Copy + Debug + Eq + Hash> EpochId for T {}

/// Marker trait for URIs.
pub trait Uri: Hash + Eq + Clone + Debug {}

/// Implement URI for all eligible types
impl<T: Hash + Eq + Clone + Debug> Uri for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventUris<U: Uri> {
    /// URI of the entity that registered this event.
    pub source_uri: U,

    /// URI of entities that can trigger the computation of a report
    pub trigger_uris: UriSet<U>,

    /// URI of entities that can receive reports that include this event.
    pub querier_uris: UriSet<U>,
}

impl<U: Uri> Hash for EventUris<U> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_uri.hash(state);
        self.trigger_uris.iter().for_each(|uri| uri.hash(state));
        self.querier_uris.iter().for_each(|uri| uri.hash(state));
    }
}

/// Event with an associated epoch.
pub trait Event: Debug + Clone {
    type EpochId: EpochId;
    type Uri: Uri;

    fn epoch_id(&self) -> Self::EpochId;

    fn event_uris(&self) -> &EventUris<Self::Uri>;
}

/// Selector that can tag relevant events one by one or in bulk.
/// Can carry some immutable state.
pub trait RelevantEventSelector {
    type Event: Event;

    /// Checks whether a single event is relevant. Storage implementations
    /// don't have to use this method, they can also implement their own
    /// bulk retrieval functionality on the type implementing this trait.
    fn is_relevant_event(&self, event: &Self::Event) -> bool;
}

/// Interface to store events and retrieve them by epoch.
pub trait EventStorage {
    type Event: Event;
    type Error;

    /// Stores a new event.
    fn add_event(&mut self, event: Self::Event) -> Result<(), Self::Error>;

    /// Retrieves all events for a given epoch.
    fn events_for_epoch(
        &mut self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> Result<impl Iterator<Item = Self::Event>, Self::Error>;

    /// Retrieves relevant events for a specific epoch, filtered by the
    /// provided selector.
    fn relevant_events_for_epoch(
        &mut self,
        epoch_id: &<Self::Event as Event>::EpochId,
        relevant_event_selector: &impl RelevantEventSelector<Event = Self::Event>,
    ) -> Result<impl Iterator<Item = Self::Event>, Self::Error> {
        // This is the default implementation. It can be inefficient because
        // it retrieves (and potentially clones) ALL events for the epoch
        // before filtering out the irrelevant ones.
        // Storage implementations should override this method if they can
        // perform the filtering more efficiently (e.g. before allocation).
        let iter = self
            .events_for_epoch(epoch_id)?
            .filter(|event| relevant_event_selector.is_relevant_event(event));
        Ok(iter)
    }
}
