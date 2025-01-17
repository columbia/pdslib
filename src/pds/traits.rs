use crate::queries::traits::ReportRequest;

/// Trait for a generic private data service.
pub trait PrivateDataService {
    /// The type of events that the service can register.
    type Event;

    /// The type of report requests the service can handle.
    type Request: ReportRequest;

    /// Special request type for passive privacy loss accounting.
    type PassivePrivacyLossRequest;

    /// Registers a new event.
    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    /// Computes a report for the given report request.
    fn compute_report(
        &mut self,
        request: Self::Request,
    ) -> <Self::Request as ReportRequest>::Report;

    /// Accounts for passive privacy loss. Can fail if the implementation has
    /// an error, but failure must not leak the state of the filters.
    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), ()>;
}
