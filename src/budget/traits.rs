use std::fmt::Debug;

/// Trait for privacy budgets
pub trait Budget: Clone + Debug {
    // For now just a marker trait requiring Clone
}

/// Trait for a privacy filter.
pub trait Filter<B: Budget> {
    type Error;

    /// Initializes a new filter with a given capacity.
    fn new(capacity: B) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Checks if the filter has enough budget without consuming
    fn can_consume(&self, budget: &B) -> Result<FilterStatus, Self::Error>;

    /// Attempts to consume the budget if sufficient.
    /// TODO(https://github.com/columbia/pdslib/issues/39): Simplify the logic, as OOB event should not happen within this function now.
    /// Tries to consume a given budget from the filter.
    /// In the formalism from https://arxiv.org/abs/1605.08294,
    /// Continue corresponds to CONTINUE, and OutOfBudget corresponds to HALT.
    fn try_consume(&mut self, budget: &B) -> Result<FilterStatus, Self::Error>;

    /// [Experimental] Gets the remaining budget for this filter.
    /// WARNING: this method is for local visualization only.
    /// Its output should not be shared outside the device.
    fn remaining_budget(&self) -> Result<B, Self::Error>;
}

/// Trait for a filter that can release budget over time.
pub trait ReleaseFilter<B: Budget>: Filter<B> {
    /// Gets the current capacity of the filter.
    fn get_capacity(&self) -> Result<B, Self::Error>;

    /// Updates the capacity of the filter.
    fn set_capacity(&mut self, capacity: B) -> Result<(), Self::Error>;

    /// Only release up to the capacity. `release` becomes a no-op once the
    /// unlocked budget reaches capacity.
    fn release(&mut self, budget_to_unlock: &B) -> Result<(), Self::Error>;
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
    /// Note: for the privacy proof to be valid, get_filter() must always
    /// return exactly what was set by set_filter().
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

    fn edit_filter_if_exists<R>(
        &mut self,
        filter_id: &Self::FilterId,
        f: impl FnOnce(&mut Self::Filter) -> Result<R, Self::Error>,
    ) -> Result<Option<R>, Self::Error> {
        if let Some(mut filter) = self.get_filter(filter_id)? {
            let r = f(&mut filter)?;
            self.set_filter(filter_id, filter)?;
            return Ok(Some(r));
        }
        Ok(None)
    }

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

    /// Edit the filter with the given ID, creating a new one if it does not
    /// exist.
    fn edit_filter_or_new<R>(
        &mut self,
        filter_id: &Self::FilterId,
        f: impl FnOnce(&mut Self::Filter) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let mut filter = self.get_filter_or_new(filter_id)?;
        let r = f(&mut filter)?;
        self.set_filter(filter_id, filter)?;
        Ok(r)
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
