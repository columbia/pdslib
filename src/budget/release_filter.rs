use anyhow::{bail, Error, Result};
use log::debug;
use serde::Serialize;

use super::{
    pure_dp_filter::PureDPBudget,
    traits::{Filter, FilterStatus},
};

/// [Experimental] A pure DP filter that has additional functionality to release budget over time.
/// TODO: Could frame as another trait if we want to have release for other types of filters.
/// TODO: refactor a bit too.
#[derive(Debug, Serialize)]
pub struct PureDPBudgetReleaseFilter {
    pub consumed: f64, // Internal value, not bothering with infinity.
    pub unlocked: f64,
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

    fn can_consume(&self, budget: &PureDPBudget) -> Result<bool, Self::Error> {
        match budget {
            PureDPBudget::Infinite => {
                // Infinite requests are accepted iff capacity is infinite
                Ok(self.capacity == PureDPBudget::Infinite)
            }
            PureDPBudget::Epsilon(requested) => {
                Ok(self.consumed + *requested <= self.unlocked)
            }
        }
    }

    fn try_consume(
        &mut self,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        debug!(
            "Filter: {:?}. We need to consume this much budget {:?}",
            self, budget
        );

        // Infinite filters accept all requests, even if they are infinite too.
        if self.capacity == PureDPBudget::Infinite {
            return Ok(FilterStatus::Continue);
        }

        let status = match budget {
            PureDPBudget::Epsilon(requested) => {
                if self.consumed + *requested <= self.unlocked {
                    self.consumed += *requested;
                    FilterStatus::Continue
                } else {
                    // TODO: maybe a different status?
                    FilterStatus::OutOfBudget
                }
            }
            // Infinite requests on finite filters are always rejected
            PureDPBudget::Infinite => FilterStatus::OutOfBudget,
        };

        Ok(status)
    }

    fn remaining_budget(&self) -> Result<PureDPBudget, anyhow::Error> {
        match self.capacity {
            PureDPBudget::Infinite => Ok(PureDPBudget::Infinite),
            PureDPBudget::Epsilon(capacity) => {
                let remaining = capacity - self.consumed;
                Ok(PureDPBudget::Epsilon(remaining))
            }
        }
    }
}

impl PureDPBudgetReleaseFilter {
    /// Only release up to the capacity. `release` becomes a no-op once the unlocked budget reaches capacity.
    pub fn release(&mut self, budget_to_unlock: f64) {
        match self.capacity {
            PureDPBudget::Infinite => {
                // Infinite filters can be released infinitely
                self.unlocked += budget_to_unlock;
            }
            PureDPBudget::Epsilon(capacity) => {
                self.unlocked = capacity.min(self.unlocked + budget_to_unlock);
            }
        };
    }
}
