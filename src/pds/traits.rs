use crate::{
    budget::traits::FilterStorageError, events::traits::EventStorageError,
    queries::traits::ReportRequest,
};

pub trait PDSError {
    type FilterStorageError: FilterStorageError;
    type EventStorageError: EventStorageError;

    fn from_filter_storage_error(
        error: <Self as PDSError>::FilterStorageError,
    ) -> Self;
    fn from_event_storage_error(
        error: <Self as PDSError>::EventStorageError,
    ) -> Self;

    fn as_filter_storage_error(&self) -> Option<&Self::FilterStorageError>;
    fn as_event_storage_error(&self) -> Option<&Self::EventStorageError>;
}

/// Trait for a generic private data service.
pub trait PrivateDataService {
    /// The type of events that the service can register.
    type Event;

    /// The type of report requests the service can handle.
    type Request: ReportRequest;

    /// Special request type for passive privacy loss accounting.
    type PassivePrivacyLossRequest;

    /// Errors.
    type Error: PDSError;

    /// Registers a new event.
    fn register_event(&mut self, event: Self::Event)
        -> Result<(), Self::Error>;

    /// Computes a report for the given report request.
    fn compute_report(
        &mut self,
        request: Self::Request,
    ) -> Result<<Self::Request as ReportRequest>::Report, Self::Error>;

    /// [Experimental] Accounts for passive privacy loss. Can fail if the
    /// implementation has an error, but failure must not leak the state of
    /// the filters.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/16): what are the semantics of passive loss queries that go over the filter
    /// capacity?
    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), Self::Error>;
}
