use std::fmt::Debug;


/// Trait for privacy budgets
pub trait Budget: Clone {
    // For now just a marker trait requiring Clone
}

/// Error returned when trying to consume from a filter.
pub trait FilterError: Debug {
    fn is_out_of_budget(&self) -> bool;
}

/// Trait for a privacy filter.
pub trait Filter<T: Budget> {
    type Error: FilterError;

    /// Initializes a new filter with a given capacity.
    fn new(capacity: T) -> Self;

    /// Tries to consume a given budget from the filter.
    /// In the formalism from https://arxiv.org/abs/1605.08294, Ok(()) corresponds to CONTINUE, and Err(FilterError::OutOfBudget) corresponds to HALT.
    fn check_and_consume(&mut self, budget: &T) -> Result<(), Self::Error>;

    /// [Experimental] Gets the remaining budget for this filter.
    /// WARNING: this method is for local visualization only.
    /// Its output should not be shared outside the device.
    fn get_remaining_budget(&self) -> T;
}

/// Error returned when trying to interact with a filter storage.
pub trait FilterStorageError:
    From<<Self as FilterStorageError>::FilterError> + Debug
{
    type FilterError: FilterError;

    fn is_filter_error(&self) -> Option<&Self::FilterError>;
    fn is_filter_does_not_exist(&self) -> bool;
    fn is_cannot_initialize_filter(&self) -> bool;

    /// Helper method
    fn is_out_of_budget(&self) -> bool {
        self.is_filter_error().is_some_and(|e| e.is_out_of_budget())
    }
}

/// Trait for an interface or object that maintains a collection of filters.
pub trait FilterStorage {
    type FilterId;
    type Budget: Budget;
    type Error: FilterStorageError;

    /// Initializes a new filter with an associated filter ID and capacity.
    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
        capacity: Self::Budget,
    ) -> Result<(), Self::Error>;

    /// Checks if filter `filter_id` is initialized.
    fn is_initialized(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<bool, Self::Error>;

    /// Tries to consume a given budget from the filter with ID `filter_id`.
    /// Returns an error if the filter does not exist, the caller can then
    /// decide to create a new filter.
    fn check_and_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<(), Self::Error>;

    /// Gets the remaining budget for a filter.
    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error>;
}
