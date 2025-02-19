use std::{collections::HashMap, marker::PhantomData};

use thiserror::Error;

use crate::budget::traits::{
    Budget, Filter, FilterStorage, FilterStorageError,
};

use super::traits::FilterError;

/// Error returned when trying to consume from a filter.
#[derive(Error, Debug)]
pub enum SimpleFilterError {
    #[error("Out of budget")]
    OutOfBudget,
}

impl FilterError for SimpleFilterError {
    fn is_out_of_budget(&self) -> bool {
        matches!(self, SimpleFilterError::OutOfBudget)
    }
}

/// Error returned when trying to interact with a filter storage.
#[derive(Error, Debug)]
pub enum SimpleFilterStorageError {
    #[error(transparent)]
    Filter(SimpleFilterError),
    #[error("Filter does not exist")]
    FilterDoesNotExist,
    #[error("Cannot initialize new filter")]
    CannotInitializeFilter,
}

impl FilterStorageError for SimpleFilterStorageError {
    type FilterError = SimpleFilterError;

    fn is_filter_error(&self) -> Option<&Self::FilterError> {
        match self {
            SimpleFilterStorageError::Filter(e) => Some(e),
            _ => None,
        }
    }

    fn is_filter_does_not_exist(&self) -> bool {
        matches!(self, SimpleFilterStorageError::FilterDoesNotExist)
    }

    fn is_cannot_initialize_filter(&self) -> bool {
        matches!(self, SimpleFilterStorageError::CannotInitializeFilter)
    }
}

impl From<SimpleFilterError> for SimpleFilterStorageError {
    fn from(e: SimpleFilterError) -> Self {
        SimpleFilterStorageError::Filter(e)
    }
}

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
    F: Filter<B, Error = SimpleFilterError>,
    K: Eq + std::hash::Hash,
{
    type FilterId = K;
    type Budget = B;
    type Error = SimpleFilterStorageError;

    fn new_filter(
        &mut self,
        filter_id: K,
        capacity: B,
    ) -> Result<(), Self::Error> {
        let filter = F::new(capacity);
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
    ) -> Result<(), Self::Error> {
        let filter = self
            .filters
            .get_mut(filter_id)
            .ok_or(SimpleFilterStorageError::FilterDoesNotExist)?;
        filter.check_and_consume(budget)?;
        Ok(())
    }

    fn get_remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        let filter = self
            .filters
            .get(filter_id)
            .ok_or(SimpleFilterStorageError::FilterDoesNotExist)?;
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
        assert!(storage
            .check_and_consume(&1, &PureDPBudget::Epsilon(0.5))
            .is_ok());
        assert!(storage
            .check_and_consume(&1, &PureDPBudget::Epsilon(0.6))
            .is_err());

        // Filter 2 does not exist
        assert!(storage
            .check_and_consume(&3, &PureDPBudget::Epsilon(0.2))
            .is_err());
    }
}
