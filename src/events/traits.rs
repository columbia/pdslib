use std::{collections::HashMap, fmt::Debug, hash::Hash};

/// Marker trait with bounds for epoch identifiers.
pub trait EpochId: Hash + Eq + Clone + Debug {}

/// Implement EpochId for all eligible types
impl<T: Hash + Eq + Clone + Debug> EpochId for T {}

/// Marker trait for URIs.
pub trait Uri: Hash + Eq + Clone + Debug {}

/// Implement URI for all eligible types
impl<T: Hash + Eq + Clone + Debug> Uri for T {}

pub type EpochEventsMap<U, E> = HashMap<U, E>;
pub type EpochSourceEventsResult<U, E, Err> =
    Result<Option<EpochEventsMap<U, E>>, Err>;

#[derive(Debug, Clone)]
pub struct EventUris<U> {
    /// URI of the entity that registered this event.
    pub source_uri: U,

    /// URI of entities that can trigger the computation of a report
    pub trigger_uris: Vec<U>,

    /// URI of entities that are embedded in the source/trigger sites
    /// and can receive reports that include this event.
    pub intermediary_uris: Vec<U>,

    /// URI of entities that can receive reports that include this event.
    pub querier_uris: Vec<U>,
}

/// Event with an associated epoch.
/// TODO(https://github.com/columbia/pdslib/issues/61): investigate clone.
pub trait Event: Debug + Clone {
    type EpochId: EpochId;
    type Uri: Uri;

    fn epoch_id(&self) -> Self::EpochId;

    fn event_uris(&self) -> EventUris<Self::Uri>;
}

/// Collection of events for a given epoch.
pub trait EpochEvents: Debug {
    type Event: Event;

    fn new() -> Self;

    fn iter(&self) -> impl Iterator<Item = &Self::Event>;

    fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    fn push(&mut self, event: Self::Event);
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
    type Uri;
    type Event: Event<Uri = Self::Uri>;
    type EpochEvents: EpochEvents;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;
    type Error;

    /// Stores a new event.
    fn add_event(&mut self, event: Self::Event) -> Result<(), Self::Error>;

    /// Retrieves all relevant events for a given epoch.
    fn relevant_epoch_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
        relevant_event_selector: &Self::RelevantEventSelector,
    ) -> Result<Option<Self::EpochEvents>, Self::Error>;

    /// Retrieves all relevant events for a given epoch.
    fn relevant_epoch_source_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
        relevant_event_selector: &Self::RelevantEventSelector,
    ) -> EpochSourceEventsResult<Self::Uri, Self::EpochEvents, Self::Error>;
}
