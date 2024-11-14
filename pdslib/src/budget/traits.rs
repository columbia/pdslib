// TODO: maybe Budget trait, and Filter<T: Budget> if we need?

use thiserror::Error;

/// Error returned when trying to consume from a filter.
#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Out of budget")]
    OutOfBudget,
}

/// Trait for a privacy filter.
pub trait Filter<T> {
    /// Initializes a new filter with a given capacity.
    fn new(capacity: T) -> Self;

    /// Tries to consume a given budget from the filter. In the formalism from https://arxiv.org/abs/1605.08294, Ok(()) corresponds to CONTINUE, and Err(FilterError::OutOfBudget) corresponds to HALT..
    fn try_consume(&mut self, budget: &T) -> Result<(), FilterError>;
}

/// Error returned when trying to interact with a filter storage.
#[derive(Error, Debug)]
pub enum FilterStorageError {
    #[error(transparent)]
    FilterError(#[from] FilterError),
    #[error("Filter does not exist")]
    FilterDoesNotExist,
    #[error("Cannot initialize new filter")]
    CannotInitializeFilter,
}

/// Trait for an interface or object that maintains a collection of filters.
pub trait FilterStorage {
    type FilterId;
    type Budget;

    /// Initializes a new filter with an associated filter ID and capacity.
    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
        capacity: Self::Budget,
    ) -> Result<(), FilterStorageError>;

    /// Checks if filter `filter_id` is initialized.
    fn is_initialized(&mut self, filter_id: &Self::FilterId) -> bool;

    /// Tries to consume a given budget from the filter with ID `filter_id`.
    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<(), FilterStorageError>;
}
