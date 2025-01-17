pub use crate::budget::traits::{Budget, Filter, FilterError};

#[derive(Debug, Clone)]
pub struct PureDPBudget {
    pub epsilon: f64,
}

impl Budget for PureDPBudget {}

// TODO: Check whether we can reuse the OpenDP accountant if we want to use RDP/zCDP, without having to execute a measurement on real data. Check out the `compose` function here: https://docs.rs/opendp/latest/opendp/measures/struct.ZeroConcentratedDivergence.html, check if they offer filters directly.

#[derive(Debug)]
pub struct PureDPBudgetFilter {
    pub remaining_budget: PureDPBudget,
}

impl Filter<PureDPBudget> for PureDPBudgetFilter {
    fn new(capacity: PureDPBudget) -> Self {
        Self {
            remaining_budget: capacity,
        }
    }

    fn try_consume(
        &mut self,
        budget: &PureDPBudget,
    ) -> Result<(), FilterError> {
        println!("The budget that remains in this epoch is {:?}, and we need to consume this much budget {:?}", self.remaining_budget.epsilon, budget.epsilon);
        if budget.epsilon <= self.remaining_budget.epsilon {
            self.remaining_budget.epsilon -= budget.epsilon;
            Ok(())
        } else {
            Err(FilterError::OutOfBudget)
        }
    }

    fn get_remaining_budget(&self) -> PureDPBudget {
        self.remaining_budget.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_dp_budget_filter() {
        let mut filter = PureDPBudgetFilter::new(PureDPBudget { epsilon: 1.0 });
        assert!(filter.try_consume(&PureDPBudget { epsilon: 0.5 }).is_ok());
        assert!(filter.try_consume(&PureDPBudget { epsilon: 0.6 }).is_err());
    }
}
