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

    /// Checks if the filter has enough budget without consuming
    fn can_consume(&self, budget: &T) -> Result<bool, Self::Error>;

    /// Attempts to consume the budget if sufficient.
    /// TODO(https://github.com/columbia/pdslib/issues/39): Simplify the logic, as OOB event should not happen within this function now.
    /// Tries to consume a given budget from the filter.
    /// In the formalism from https://arxiv.org/abs/1605.08294,
    /// Continue corresponds to CONTINUE, and OutOfBudget corresponds to HALT.
    fn try_consume(&mut self, budget: &T) -> Result<FilterStatus, Self::Error>;

    /// [Experimental] Gets the remaining budget for this filter.
    /// WARNING: this method is for local visualization only.
    /// Its output should not be shared outside the device.
    fn remaining_budget(&self) -> Result<T, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterStatus {
    Continue,
    OutOfBudget,
}

pub trait FilterCapacities {
    type FilterId;
    type Budget: Budget;
    type Error;

    fn capacity(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error>;
}

/// Trait for an interface or object that maintains a collection of filters.
pub trait FilterStorage {
    type FilterId: std::fmt::Debug;
    type Budget: Budget;
    type Capacities: FilterCapacities<
        Budget = Self::Budget,
        Error = Self::Error,
    >;
    type Error;

    /// Create a new filter storage with the given capacities for new filters.
    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Initializes a new filter with an associated filter ID.
    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
    ) -> Result<(), Self::Error>;

    /// Checks if filter `filter_id` is initialized.
    fn is_initialized(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<bool, Self::Error>;

    /// Creates a new filter if not initialized.
    fn ensure_filter(
        &mut self,
        filter_id: Self::FilterId,
    ) -> Result<(), Self::Error> {
        if !self.is_initialized(&filter_id)? {
            self.new_filter(filter_id)?;
        }
        Ok(())
    }

    /// Check if budget can be consumed without modifying state
    fn can_consume(
        &self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<bool, Self::Error>;

    /// Attempts to consume the budget if sufficient.
    /// TODO(https://github.com/columbia/pdslib/issues/39): Simplify the logic, as OOB event should not happen within this function now.
    /// Tries to consume a given budget from the filter with ID `filter_id`.
    /// Returns an error if the filter does not exist, the caller can then
    /// decide to create a new filter.
    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<FilterStatus, Self::Error>;

    /// Convenience function that routes to either can_consume or try_consume
    fn maybe_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
        dry_run: bool,
    ) -> Result<FilterStatus, Self::Error> {
        if dry_run {
            match self.can_consume(filter_id, budget)? {
                true => Ok(FilterStatus::Continue),
                false => Ok(FilterStatus::OutOfBudget),
            }
        } else {
            self.try_consume(filter_id, budget)
        }
    }

    /// Gets the remaining budget for a filter.
    fn remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error>;
}
