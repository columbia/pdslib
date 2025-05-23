use std::fmt::Debug;

/// Trait for privacy budgets
pub trait Budget: Clone + Debug {
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
    fn can_consume(&self, budget: &T) -> Result<FilterStatus, Self::Error>;

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
    type FilterId: Debug;
    type Budget: Budget;
    type Filter: Filter<Self::Budget, Error = Self::Error>;
    type Capacities: FilterCapacities<
        FilterId = Self::FilterId,
        Budget = Self::Budget,
        Error = Self::Error,
    >;
    type Error;

    /// Create a new filter storage with the given capacities for new filters.
    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Get the capacities object that was passed to the constructor.
    fn capacities(&self) -> &Self::Capacities;

    /// Get the filter with the given ID from the storage.
    /// Returns None if the filter has not been set yet.
    fn get_filter(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<Option<Self::Filter>, Self::Error>;

    /// Store the filter with the given ID in the storage.
    fn set_filter(
        &mut self,
        filter_id: &Self::FilterId,
        filter: Self::Filter,
    ) -> Result<(), Self::Error>;

    /// Get the filter with the given ID from the storage, or return a new one
    /// with default capacity if it does not exist.
    fn get_filter_or_new(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Filter, Self::Error> {
        let filter = match self.get_filter(filter_id)? {
            Some(filter) => filter,
            None => {
                let capacity = self.capacities().capacity(filter_id)?;
                Self::Filter::new(capacity)?
            }
        };
        Ok(filter)
    }

    /// Check if budget can be consumed from the given filter,
    /// without modifying state.
    fn can_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<FilterStatus, Self::Error> {
        self.get_filter_or_new(filter_id)?.can_consume(budget)
    }

    /// Attempts to consume the budget if sufficient.
    /// Tries to consume a given budget from the filter with ID `filter_id`.
    /// If the filter does not yet exist, it is created with the default,
    /// capacity, then consumed from and stored.
    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<FilterStatus, Self::Error> {
        let mut filter = self.get_filter_or_new(filter_id)?;
        let status = filter.try_consume(budget)?;
        self.set_filter(filter_id, filter)?;
        Ok(status)
    }

    /// Gets the remaining budget for a filter.
    /// WARNING: this method is for testing and local visualization only.
    fn remaining_budget(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        let filter = self.get_filter(filter_id)?;
        let budget = match filter {
            Some(filter) => filter.remaining_budget()?,
            None => self.capacities().capacity(filter_id)?,
        };
        Ok(budget)
    }
}
