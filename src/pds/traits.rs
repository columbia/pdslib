use crate::queries::traits::ReportRequest;

/// Trait for a generic private data service.
pub trait PrivateDataService {
    /// The type of events that the service can register.
    type Event;

    /// The type of queries the service can handle.
    type Request: ReportRequest;

    /// Registers a new event.
    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    /// Computes a report for the given query.
    fn compute_report(
        &mut self,
        request: Self::Request,
    ) -> <Self::Request as ReportRequest>::Report;
}
