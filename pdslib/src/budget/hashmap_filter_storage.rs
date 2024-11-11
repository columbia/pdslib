use crate::budget::traits::{Filter, FilterResult, FilterStorage};
use std::collections::HashMap;
use std::marker::PhantomData;

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

impl<K, F, Budget> FilterStorage for HashMapFilterStorage<K, F, Budget>
where
    F: Filter<Budget>,
    K: Eq + std::hash::Hash,
{
    type FilterId = K;
    type Budget = Budget;
    type Filter = F;

    fn new_filter(&mut self, filter_id: K, capacity: Budget) -> Result<(), ()> {
        let filter = F::new(capacity);
        self.filters.insert(filter_id, filter);
        Ok(())
    }

    fn get_filter(&mut self, filter_id: &K) -> Option<&F> {
        self.filters.get(&filter_id)
    }

    // TODO: PDS will be in charge of creating filters when missing?
    fn try_consume(
        &mut self,
        filter_id: &K,
        budget: Budget,
    ) -> Result<FilterResult, ()> {
        let filter = self.filters.get_mut(filter_id).ok_or(())?;
        Ok(filter.try_consume(budget))
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
        storage
            .new_filter(1, PureDPBudget { epsilon: 1.0 })
            .unwrap();
        assert!(storage
            .try_consume(&1, PureDPBudget { epsilon: 0.5 })
            .unwrap()
            .is_ok());
        assert!(storage
            .try_consume(&1, PureDPBudget { epsilon: 0.6 })
            .unwrap()
            .is_err());

        // Filter 2 does not exist
        assert!(storage
            .try_consume(&3, PureDPBudget { epsilon: 0.2 })
            .is_err());
    }
}
