use std::fmt::Debug;

pub trait Event: Debug {
    type EpochId;
    // TODO: add first-party source?

    fn get_epoch_id(&self) -> Self::EpochId;
}

pub trait EpochEvents: Debug {
    fn is_empty(&self) -> bool;
}

pub trait EventStorage {
    type Event: Event;
    type EpochEvents: EpochEvents;

    fn add_event(&mut self, event: Self::Event) -> Result<(), ()>;

    // TODO: allow to filter relevant events for a query?
    fn get_epoch_events(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> Option<Self::EpochEvents>;

    fn get_event_count(
        &self,
        epoch_id: &<Self::Event as Event>::EpochId,
    ) -> usize;
}
