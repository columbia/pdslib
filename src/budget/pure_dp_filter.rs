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
#[derive(Debug, Clone, PartialEq)]
pub enum PureDPBudget {
    /// Infinite budget, for filters with no set capacity, or requests that
    /// don't add any noise
    Infinite,

    /// Finite pure DP epsilon
    Epsilon(f64),
}

impl Budget for PureDPBudget {}

/// A filter for pure differential privacy.
#[derive(Debug)]
pub struct PureDPBudgetFilter {
    pub remaining_budget: PureDPBudget,
}

impl Filter<PureDPBudget> for PureDPBudgetFilter {
    type Error = anyhow::Error;

    fn new(capacity: PureDPBudget) -> Result<Self, Self::Error> {
        let this = Self {
            remaining_budget: capacity,
        };
        Ok(this)
    }

    fn check_and_consume(
        &mut self,
        budget: &PureDPBudget,
    ) -> Result<FilterStatus, Self::Error> {
        println!("The budget that remains in this epoch is {:?}, and we need to consume this much budget {:?}", self.remaining_budget, budget);

        // Check that we have enough budget and if yes, deduct in place.
        // We check `Infinite` manually instead of implementing `PartialOrd` and
        // `SubAssign` because we just need this in filters, not to
        // compare or subtract arbitrary budgets.
        let status = match self.remaining_budget {
            // Infinite filters accept all requests, even if they are infinite
            // too.
            PureDPBudget::Infinite => FilterStatus::Continue,
            PureDPBudget::Epsilon(remaining_epsilon) => match budget {
                PureDPBudget::Epsilon(requested_epsilon) => {
                    if *requested_epsilon <= remaining_epsilon {
                        self.remaining_budget = PureDPBudget::Epsilon(
                            remaining_epsilon - *requested_epsilon,
                        );
                        FilterStatus::Continue
                    } else {
                        FilterStatus::OutOfBudget
                    }
                }
                // Infinite requests on finite filters are always rejected
                _ => FilterStatus::OutOfBudget,
            },
        };

        Ok(status)
    }

    fn get_remaining_budget(&self) -> Result<PureDPBudget, anyhow::Error> {
        Ok(self.remaining_budget.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_dp_budget_filter() {
        let mut filter =
            PureDPBudgetFilter::new(PureDPBudget::Epsilon(1.0)).unwrap();
        assert_eq!(
            filter
                .check_and_consume(&PureDPBudget::Epsilon(0.5))
                .unwrap(),
            FilterStatus::Continue
        );
        assert_eq!(
            filter
                .check_and_consume(&PureDPBudget::Epsilon(0.6))
                .unwrap(),
            FilterStatus::OutOfBudget
        );
    }
}
