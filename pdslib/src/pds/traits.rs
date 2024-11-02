pub trait PrivateDataService {
    type Event;
    type ReportRequest;
    type EpochEvents;
    type Report; // TODO: is this going to be a union type over possible reports? Sounds pretty resonable. Or trait with dynamic dispatch?

    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    // TODO: where do we restrict the list of supported query types? Maybe allow them all to run for now, an return null reports if not supported?
    fn compute_report(&mut self, request: Self::ReportRequest) -> Self::Report;

    // Computes the individual privacy loss for a given set of events.
    fn compute_individual_privacy_loss(&self, request: &Self::ReportRequest, epoch_events: &Self::EpochEvents, epoch_events_count: usize, computed_attribution: &Self::Report) -> f64;
}
