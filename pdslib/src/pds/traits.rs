pub trait PrivateDataService {
    type Budget; // Budget type
    type EpochEvents;
    type EpochId; // Epoch ID
    type Event;
    type Report; // TODO: is this going to be a union type over possible reports? Sounds pretty resonable. Or trait with dynamic dispatch?
    type ReportRequest;

    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    fn register_epoch_capacity(&mut self, epoch_id: Self::EpochId, capacity: Self::Budget) -> Result<(), ()>;

    // TODO: where do we restrict the list of supported query types? Maybe allow them all to run for now, an return null reports if not supported?
    fn compute_report(&mut self, request: Self::ReportRequest) -> Self::Report;
}
