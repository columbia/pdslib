use std::fmt::Debug;
use std::hash::Hash;

///  Marker trait with bounds for epoch identifiers.
pub trait EpochId: Hash + std::cmp::Eq + Clone {}

/// Event with an associated epoch.
pub trait Event: Debug {
    type EpochId: EpochId;
    // TODO: add identifier for the first-party who issued this event?

    fn get_epoch_id(&self) -> Self::EpochId;
}

/// Collection of events for a given epoch.
pub trait EpochEvents: Debug {
    fn is_empty(&self) -> bool;
}

/// Interface to store events and retrieve them by epoch.
pub trait EventStorage {
    type Event: Event;
    type EpochEvents: EpochEvents;

    /// Stores a new event.
    fn add_event(&mut self, event: Self::Event) -> Result<(), ()>;

    /// Retrieves all events for a given epoch.
    /// TODO: allow to filter relevant events for a query?
    fn get_epoch_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> Option<Self::EpochEvents>;
}
