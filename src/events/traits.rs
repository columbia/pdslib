use std::{collections::HashMap, fmt::Debug, hash::Hash};

use crate::util::shared_types::Uri;

/// Marker trait with bounds for epoch identifiers.
pub trait EpochId: Hash + std::cmp::Eq + Clone + Debug {}

/// Default EpochId
impl EpochId for usize {}

pub type EpochEventsMap<U, E> = HashMap<U, E>;
pub type EpochSiteEventsResult<U, E, Err> =
    Result<Option<EpochEventsMap<U, E>>, Err>;

#[derive(Debug, Clone)]
pub struct EventUris<U: Uri> {
    /// URI of the entity that registered this event.
    pub source_uri: U,

    /// URI of entities that can trigger the computation of a report
    pub trigger_uris: Vec<U>,

    /// URI of entities that can receive reports that include this event.
    pub querier_uris: Vec<U>,
}

/// Event with an associated epoch.
pub trait Event: Debug {
    type EpochId: EpochId;
    type Uri: Uri;
    // TODO(https://github.com/columbia/pdslib/issues/18): add source/trigger information for Big Bird / Level 2.

    fn epoch_id(&self) -> Self::EpochId;

    fn event_uris(&self) -> EventUris<Self::Uri>;
}

/// Collection of events for a given epoch.
pub trait EpochEvents: Debug {
    fn new() -> Self;

    fn is_empty(&self) -> bool;
}

/// Selector that can tag relevant events one by one or in bulk.
/// Can carry some immutable state.
///
/// TODO: do we really need a separate trait? We could also add
/// `is_relevant_event` directly to the `ReportRequest` trait, and pass the
/// whole request to the `EventStorage` when needed.
pub trait RelevantEventSelector {
    type Event: Event;

    /// Checks whether a single event is relevant. Storage implementations
    /// don't have to use this method, they can also implement their own
    /// bulk retrieval functionality on the type implementing this trait.
    fn is_relevant_event(&self, event: &Self::Event) -> bool;
}

/// Interface to store events and retrieve them by epoch.
pub trait EventStorage {
    type Uri: Uri;
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
    ) -> EpochSiteEventsResult<Self::Uri, Self::EpochEvents, Self::Error>;
}
