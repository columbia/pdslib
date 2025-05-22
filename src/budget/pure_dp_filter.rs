use core::f64;

use log::{debug, warn};
use serde::Serialize;

use crate::budget::traits::{Budget, Filter, FilterStatus};

/// A simple floating-point budget for pure differential privacy, with support
/// for infinite budget
///
/// Infinite budget can be used for noiseless testing queries and to deactivate
/// filters by setting their capacity to `PureDPBudget::Infinite`. We use a
/// simple f64 for epsilon and ignore floating point arithmetic issues.
///
/// TODO(https://github.com/columbia/pdslib/issues/14): use OpenDP accountant (even though it seems
///     to also use f64) or move to a positive rational type or fixed point.
///     We could also generalize to RDP/zCDP.
pub type PureDPBudget = f64;

impl Budget for PureDPBudget {}

/// A filter for pure differential privacy.
#[derive(Debug, Clone, Serialize)]
pub struct PureDPBudgetFilter {
    pub consumed: PureDPBudget,
    pub capacity: Option<PureDPBudget>, // None = infinite budget
}

impl Filter<PureDPBudget> for PureDPBudgetFilter {
    type Error = anyhow::Error;

    fn new(capacity: PureDPBudget) -> Result<Self, Self::Error> {
        let this = Self {
            consumed: 0.0,
            capacity: Some(capacity),
        };
        Ok(this)
    }

    fn can_consume(&self, budget: &PureDPBudget) -> Result<bool, Self::Error> {
        match self.capacity {
            None => Ok(true),
            Some(capacity) => {
                let remaining = capacity - self.consumed;
                let diff = (remaining - budget).abs();

                if diff < 1e-9 && diff > 0.0 {
                    warn!(
                        "can_consume: difference between remaining budget ({}) and requested budget ({}) is very small, diff = {}",
                        remaining, budget, diff
                    );
                }

                Ok(self.consumed + budget <= capacity)
            }
        }
    }

    fn try_consume(
        &mut self,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        debug!("The budget consumed in this epoch is {:?}, budget capacity for this epoch is  {:?}, and we need to consume this much budget {:?}", self.consumed, self.capacity, budget);

        // Check that we have enough budget and if yes, deduct in place.
        let status = match self.capacity {
            None => {
                // Infinite capacity
                self.consumed += budget;
                FilterStatus::Continue
            }
            Some(capacity) => {
                let remaining = capacity - self.consumed;
                let diff = (remaining - budget).abs();

                if diff < 1e-9 && diff > 0.0 {
                    warn!(
                        "try_consume: difference between remaining budget ({}) and requested budget ({}) is very small, diff = {}",
                        remaining, budget, diff
                    );
                }

                if self.consumed + budget <= capacity {
                    self.consumed += budget;
                    FilterStatus::Continue
                } else {
                    FilterStatus::OutOfBudget
                }
            }
        };

        Ok(status)
    }

    fn remaining_budget(&self) -> Result<PureDPBudget, anyhow::Error> {
        match self.capacity {
            None => Ok(f64::INFINITY),
            Some(capacity) => Ok(capacity - self.consumed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_dp_budget_filter() -> Result<(), anyhow::Error> {
        let mut filter = PureDPBudgetFilter::new(1.0)?;
        assert_eq!(filter.try_consume(&0.5)?, FilterStatus::Continue);
        assert_eq!(filter.try_consume(&0.6)?, FilterStatus::OutOfBudget);

        // Test infinite capacity
        let mut infinite_filter = PureDPBudgetFilter {
            consumed: 0.0,
            capacity: None,
        };
        assert_eq!(
            infinite_filter.try_consume(&100.0)?,
            FilterStatus::Continue
        );

        Ok(())
    }
}
