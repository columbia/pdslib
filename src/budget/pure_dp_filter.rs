pub use crate::budget::traits::{Budget, Filter, FilterError};

/// A simple floating-point budget for pure differential privacy.
///
/// TODO(https://github.com/columbia/pdslib/issues/14): use OpenDP accountant
#[derive(Debug, Clone)]
pub struct PureDPBudget {
    pub epsilon: f64,
}

impl Budget for PureDPBudget {}

/// A filter for pure differential privacy.
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
