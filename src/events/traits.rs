use std::{fmt::Debug, hash::Hash};

/// Marker trait with bounds for epoch identifiers.
pub trait EpochId: Hash + std::cmp::Eq + Clone + Debug {}

/// Default EpochId
impl EpochId for usize {}

/// Event with an associated epoch.
pub trait Event: Debug {
    type EpochId: EpochId;
    // TODO(https://github.com/columbia/pdslib/issues/18): add source/trigger information for Big Bird / Level 2.

    fn get_epoch_id(&self) -> Self::EpochId;
}

/// Collection of events for a given epoch.
pub trait EpochEvents: Debug {
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
    type Event: Event;
    type EpochEvents: EpochEvents;
    type RelevantEventSelector: RelevantEventSelector<Event = Self::Event>;
    type Error;

    /// Stores a new event.
    fn add_event(&mut self, event: Self::Event) -> Result<(), Self::Error>;

    /// Retrieves all relevant events for a given epoch.
    fn get_relevant_epoch_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
        relevant_event_selector: &Self::RelevantEventSelector,
    ) -> Result<Option<Self::EpochEvents>, Self::Error>;
}
