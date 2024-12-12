use crate::queries::traits::Query;

/// Trait for a generic private data service.
pub trait PrivateDataService {
    /// The type of events that the service can register.
    type Event;

    /// The type of queries the service can handle.
    type Query: Query;

    /// Registers a new event.
    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    /// Computes a report for the given query.
    fn compute_report(
        &mut self,
        query: Self::Query,
    ) -> <Self::Query as Query>::Report;
}
