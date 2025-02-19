use std::{collections::HashMap, marker::PhantomData};

use crate::budget::traits::{
    Budget, Filter, FilterStorage, FilterStorageError,
};

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug)]
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
    F: Filter<B>,
    K: Eq + std::hash::Hash,
{
    type FilterId = K;
    type Budget = B;

    fn new_filter(
        &mut self,
        filter_id: K,
        capacity: B,
    ) -> Result<(), FilterStorageError> {
        let filter = F::new(capacity);
        self.filters.insert(filter_id, filter);
        Ok(())
    }

    fn is_initialized(&mut self, filter_id: &Self::FilterId) -> bool {
        self.filters.contains_key(filter_id)
    }

    fn try_consume(
        &mut self,
        filter_id: &K,
        budget: &B,
    ) -> Result<(), FilterStorageError> {
        let filter = self
            .filters
            .get_mut(filter_id)
            .ok_or(FilterStorageError::FilterDoesNotExist)?;
        filter.try_consume(budget)?;
        Ok(())
    }

    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, FilterStorageError> {
        let filter = self
            .filters
            .get(filter_id)
            .ok_or(FilterStorageError::FilterDoesNotExist)?;
        Ok(filter.get_remaining_budget())
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
        assert!(storage.try_consume(&1, &PureDPBudget::Epsilon(0.5)).is_ok());
        assert!(storage
            .try_consume(&1, &PureDPBudget::Epsilon(0.6))
            .is_err());

        // Filter 2 does not exist
        assert!(storage
            .try_consume(&3, &PureDPBudget::Epsilon(0.2))
            .is_err());
    }
}
