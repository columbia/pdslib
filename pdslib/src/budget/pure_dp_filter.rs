pub use crate::budget::traits::Filter;
#[derive(Debug)]
pub struct PureDPBudget {
    pub epsilon: f64,
}

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

    fn try_consume(&mut self, budget: PureDPBudget) -> Result<(), ()> {
        println!("The budget that remains in this epoch is {:?}, and we need to consume this much budget {:?}", self.remaining_budget.epsilon, budget.epsilon);
        if budget.epsilon <= self.remaining_budget.epsilon {
            self.remaining_budget.epsilon -= budget.epsilon;
            Ok(())
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_dp_budget_filter() {
        let mut filter = PureDPBudgetFilter::new(PureDPBudget { epsilon: 1.0 });
        assert!(filter.try_consume(PureDPBudget { epsilon: 0.5 }).is_ok());
        assert!(filter.try_consume(PureDPBudget { epsilon: 0.6 }).is_err());
    }
}
