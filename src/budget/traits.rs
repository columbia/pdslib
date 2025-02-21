/// Trait for privacy budgets
pub trait Budget: Clone {
    // For now just a marker trait requiring Clone
}

/// Trait for a privacy filter.
pub trait Filter<T: Budget> {
    type Error;

    /// Initializes a new filter with a given capacity.
    fn new(capacity: T) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Tries to consume a given budget from the filter.
    /// In the formalism from https://arxiv.org/abs/1605.08294, Ok(()) corresponds to CONTINUE, and Err(FilterError::OutOfBudget) corresponds to HALT.
    fn check_and_consume(
        &mut self,
        budget: &T,
    ) -> Result<FilterStatus, Self::Error>;

    /// [Experimental] Gets the remaining budget for this filter.
    /// WARNING: this method is for local visualization only.
    /// Its output should not be shared outside the device.
    fn get_remaining_budget(&self) -> Result<T, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStatus {
    Continue,
    OutOfBudget,
}

/// Trait for an interface or object that maintains a collection of filters.
pub trait FilterStorage {
    type FilterId;
    type Budget: Budget;
    type Error;

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
    ) -> Result<FilterStatus, Self::Error>;

    /// Gets the remaining budget for a filter.
    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error>;
}
