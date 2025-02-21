use std::{collections::HashMap, marker::PhantomData};

use anyhow::Context;

use crate::budget::traits::{Budget, Filter, FilterStatus, FilterStorage};

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug, Default)]
pub struct HashMapFilterStorage<K, F, Budget> {
    filters: HashMap<K, F>,
    _marker: PhantomData<Budget>,
}

impl<K, F, Budget> HashMapFilterStorage<K, F, Budget> {
    pub fn new() -> Self {
        Self {
            filters: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<K, F, B> FilterStorage for HashMapFilterStorage<K, F, B>
where
    B: Budget,
    F: Filter<B, Error = anyhow::Error>,
    K: Eq + std::hash::Hash,
{
    type FilterId = K;
    type Budget = B;
    type Error = anyhow::Error;

    fn new_filter(
        &mut self,
        filter_id: K,
        capacity: B,
    ) -> Result<(), Self::Error> {
        let filter = F::new(capacity)?;
        self.filters.insert(filter_id, filter);
        Ok(())
    }

    fn is_initialized(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<bool, Self::Error> {
        Ok(self.filters.contains_key(filter_id))
    }

    fn check_and_consume(
        &mut self,
        filter_id: &K,
        budget: &B,
    ) -> Result<FilterStatus, Self::Error> {
        let filter = self
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;
        filter.check_and_consume(budget)
    }

    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        let filter = self
            .filters
            .get(filter_id)
            .context("Filter does not exist")?;
        filter.get_remaining_budget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};

    #[test]
    fn test_hash_map_filter_storage() {
        let mut storage: HashMapFilterStorage<
            usize,
            PureDPBudgetFilter,
            PureDPBudget,
        > = HashMapFilterStorage::new();
        storage.new_filter(1, PureDPBudget::Epsilon(1.0)).unwrap();
        assert_eq!(
            storage
                .check_and_consume(&1, &PureDPBudget::Epsilon(0.5))
                .unwrap(),
            FilterStatus::Continue
        );
        assert_eq!(
            storage
                .check_and_consume(&1, &PureDPBudget::Epsilon(0.6))
                .unwrap(),
            FilterStatus::OutOfBudget
        );

        // Filter 2 does not exist
        assert!(storage
            .check_and_consume(&3, &PureDPBudget::Epsilon(0.2))
            .is_err());
    }
}
