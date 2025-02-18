use crate::queries::traits::ReportRequest;

/// Trait for a generic private data service.
pub trait PrivateDataService {
    /// The type of events that the service can register.
    type Event;

    /// The type of report requests the service can handle.
    type Request: ReportRequest;

    /// Special request type for passive privacy loss accounting.
    type PassivePrivacyLossRequest;

    /// Errors.
    type Error;

    /// Registers a new event.
    fn register_event(&mut self, event: Self::Event)
        -> Result<(), Self::Error>;

    /// Computes a report for the given report request.
    fn compute_report(
        &mut self,
        request: Self::Request,
    ) -> <Self::Request as ReportRequest>::Report;

    /// [Experimental] Accounts for passive privacy loss. Can fail if the implementation has
    /// an error, but failure must not leak the state of the filters.
    /// TODO: what are the semantics of passive loss queries that go over the filter
    /// capacity? See https://github.com/columbia/pdslib/issues/16.
    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), Self::Error>;
}
