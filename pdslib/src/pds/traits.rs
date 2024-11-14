pub trait PrivateDataService {
    type Budget; // Budget type
    type EpochEvents;
    type EpochId; // Epoch ID
    type Event;
    type ReportRequest; // union type over possible queries
    type Report;

    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    fn compute_report(&mut self, request: Self::ReportRequest) -> Self::Report;
}
