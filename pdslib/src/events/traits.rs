// TODO: do we need epochs? Could we use an abstact notion of Event Key instead?
pub trait EventStorage {
    type Event; // TODO: no need for a trait bound? 
    type EpochId;
    type QuerierId;

    // fn new() -> Self;

    fn add_event(&mut self, event: Self::Event, epoch_id: Self::EpochId, querier_id: Self::QuerierId) -> Result<(), ()>;
}