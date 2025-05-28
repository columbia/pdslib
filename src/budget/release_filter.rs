use anyhow::Result;
use serde::Serialize;

use super::{
    pure_dp_filter::PureDPBudget,
    traits::{Filter, FilterStatus, ReleaseFilter},
};

/// [Experimental] A pure DP filter that has additional functionality to release
/// budget over time. TODO: Could frame as another trait if we want to have
/// release for other types of filters. TODO: refactor a bit too.
#[derive(Debug, Clone, Serialize)]
pub struct PureDPBudgetReleaseFilter {
    pub consumed: PureDPBudget,
    pub unlocked: PureDPBudget,
    pub capacity: PureDPBudget,
}

impl Filter<PureDPBudget> for PureDPBudgetReleaseFilter {
    type Error = anyhow::Error;

    fn new(capacity: PureDPBudget) -> Result<Self, Self::Error> {
        let this = Self {
            consumed: 0.0,
            unlocked: 0.0,
            capacity,
        };
        Ok(this)
    }

    fn can_consume(
        &self,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        // Infinite filters accept all requests, even if they are infinite too.
        if self.capacity == f64::INFINITY {
            return Ok(FilterStatus::Continue);
        }

        if budget == &f64::INFINITY {
            // Finite capacity can't allow infinite requests
            return Ok(FilterStatus::OutOfBudget);
        }

        let can_consume = self.consumed + budget <= self.unlocked;
        match can_consume {
            true => Ok(FilterStatus::Continue),
            false => Ok(FilterStatus::OutOfBudget),
        }
    }

    fn try_consume(
        &mut self,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        let status = self.can_consume(budget)?;

        if status == FilterStatus::Continue {
            // If we can consume, update the consumed budget.
            self.consumed += *budget;
        }

        Ok(status)
    }

    fn remaining_budget(&self) -> Result<PureDPBudget, anyhow::Error> {
        let remaining = self.capacity - self.consumed;
        Ok(remaining)
    }
}

impl ReleaseFilter<PureDPBudget> for PureDPBudgetReleaseFilter {
    fn set_capacity(
        &mut self,
        capacity: PureDPBudget,
    ) -> Result<(), Self::Error> {
        self.capacity = capacity;
        Ok(())
    }

    fn release(
        &mut self,
        budget_to_unlock: &PureDPBudget,
    ) -> Result<(), Self::Error> {
        if self.capacity == f64::INFINITY {
            // Infinite filters can be released infinitely
            self.unlocked += budget_to_unlock;
        } else {
            self.unlocked = self.capacity.min(self.unlocked + budget_to_unlock);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_dp_budget_release_filter() -> Result<(), anyhow::Error> {
        let mut filter = PureDPBudgetReleaseFilter::new(1.0)?;

        // No budget initially
        assert_eq!(filter.try_consume(&0.5)?, FilterStatus::OutOfBudget);

        // Unlock some budget
        filter.release(&0.7)?;
        assert_eq!(filter.try_consume(&0.5)?, FilterStatus::Continue);
        assert_eq!(filter.try_consume(&0.3)?, FilterStatus::OutOfBudget);

        // Unlock the rest
        filter.release(&2.0)?;
        assert_eq!(filter.try_consume(&0.6)?, FilterStatus::OutOfBudget);
        assert_eq!(filter.try_consume(&0.3)?, FilterStatus::Continue);

        Ok(())
    }
}
