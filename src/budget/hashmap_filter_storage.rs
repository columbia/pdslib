use std::{collections::HashMap, fmt::Debug, hash::Hash};

use anyhow::Context;
use serde::{ser::SerializeStruct, Serialize};

use crate::budget::traits::{
    Filter, FilterCapacities, FilterStatus, FilterStorage,
};

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug, Default)]
pub struct HashMapFilterStorage<F, C>
where
    C: FilterCapacities,
    F: Filter<C::Budget>,
{
    capacities: C,
    filters: HashMap<C::FilterId, F>,
}

impl<F, C, FID> Serialize for HashMapFilterStorage<F, C>
where
    C: FilterCapacities<FilterId = FID> + Serialize,
    F: Filter<C::Budget> + Serialize,
    FID: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state =
            serializer.serialize_struct("HashMapFilterStorage", 2)?;
        state.serialize_field("capacities", &self.capacities)?;
        state.serialize_field("filters", &self.filters)?;
        state.end()
    }
}

impl<F, C> FilterStorage for HashMapFilterStorage<F, C>
where
    F: Filter<C::Budget, Error = anyhow::Error>,
    C: FilterCapacities<Error = anyhow::Error>,
    C::FilterId: Clone + Eq + Hash + Debug,
{
    type FilterId = C::FilterId;
    type Budget = C::Budget;
    type Capacities = C;
    type Error = anyhow::Error;

    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let this = Self {
            capacities,
            filters: HashMap::new(),
        };
        Ok(this)
    }

    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
    ) -> Result<(), Self::Error> {
        let capacity = self.capacities.capacity(&filter_id)?;
        let filter = F::new(capacity)?;
        self.filters.insert(filter_id, filter);

        Ok(())
    }

    fn is_initialized(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<bool, Self::Error> {
        let entry = self.filters.get_mut(filter_id);
        Ok(entry.is_some())
    }

    fn can_consume(
        &self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<bool, Self::Error> {
        let filter = self
            .filters
            .get(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.can_consume(budget)
    }

    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<FilterStatus, Self::Error> {
        let filter = self
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.try_consume(budget)
    }

    fn remaining_budget(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        let filter = self
            .filters
            .get(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.remaining_budget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        budget::pure_dp_filter::PureDPBudgetFilter,
        pds::quotas::{FilterId, StaticCapacities},
    };

    #[test]
    fn test_hash_map_filter_storage() -> Result<(), anyhow::Error> {
        let capacities = StaticCapacities::mock();
        let mut storage: HashMapFilterStorage<PureDPBudgetFilter, _> =
            HashMapFilterStorage::new(capacities)?;

        let fid: FilterId<i32, ()> = FilterId::C(1);
        storage.new_filter(fid.clone())?;
        assert_eq!(storage.try_consume(&fid, &10.0)?, FilterStatus::Continue);
        assert_eq!(
            storage.try_consume(&fid, &11.0)?,
            FilterStatus::OutOfBudget,
        );

        // Filter C(2) does not exist
        assert!(storage.try_consume(&FilterId::C(2), &1.0).is_err());

        Ok(())
    }
}
