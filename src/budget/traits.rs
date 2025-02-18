use thiserror::Error;

/// Trait for privacy budgets
pub trait Budget: Clone {
    // For now just a marker trait requiring Clone
}

/// Error returned when trying to consume from a filter.
#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Out of budget")]
    OutOfBudget,
}

/// Trait for a privacy filter.
pub trait Filter<T: Budget> {
    /// Initializes a new filter with a given capacity.
    fn new(capacity: T) -> Self;

    /// Tries to consume a given budget from the filter.
    /// In the formalism from https://arxiv.org/abs/1605.08294, Ok(()) corresponds to CONTINUE, and Err(FilterError::OutOfBudget) corresponds to HALT.
    fn try_consume(&mut self, budget: &T) -> Result<(), FilterError>;

    /// [Experimental] Gets the remaining budget for this filter.
    /// WARNING: this method is for local visualization only.
    /// Its output should not be shared outside the device.
    fn get_remaining_budget(&self) -> T;
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
    type Budget: Budget;

    /// Initializes a new filter with an associated filter ID and capacity.
    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
        capacity: Self::Budget,
    ) -> Result<(), FilterStorageError>;

    /// Checks if filter `filter_id` is initialized.
    fn is_initialized(&mut self, filter_id: &Self::FilterId) -> bool;

    /// Tries to consume a given budget from the filter with ID `filter_id`.
    /// Returns an error if the filter does not exist, the caller can then
    /// decide to create a new filter.
    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<(), FilterStorageError>;

    /// Gets the remaining budget for a filter.
    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, FilterStorageError>;
}
