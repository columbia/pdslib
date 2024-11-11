use std::fmt::Debug;

pub trait Event: Debug {
    type EpochId;
    fn get_epoch_id(&self) -> Self::EpochId;
}

pub trait EpochEvents: Debug {
    fn is_empty(&self) -> bool;
}

// TODO: do we need epochs? Could we use an abstact notion of Event Key instead?
pub trait EventStorage {
    type Event: Event;
    type EpochEvents: EpochEvents;
    // type QuerierId;

    fn add_event(
        &mut self,
        event: Self::Event,
        // epoch_id: Self::Event::EpochId,
        // querier_id: Self::QuerierId,
    ) -> Result<(), ()>;

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
