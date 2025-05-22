use std::{collections::HashMap, fmt::Debug, hash::Hash};

use serde::{ser::SerializeStruct, Serialize};

use crate::budget::traits::{Filter, FilterCapacities, FilterStorage};

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
    F: Filter<C::Budget, Error = anyhow::Error> + Clone,
    C: FilterCapacities<Error = anyhow::Error>,
    C::FilterId: Clone + Eq + Hash + Debug,
{
    type FilterId = C::FilterId;
    type Filter = F;
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

    fn capacities(&self) -> &Self::Capacities {
        &self.capacities
    }

    fn get_filter(
        &mut self,
        filter_id: &Self::FilterId,
    ) -> Result<Option<Self::Filter>, Self::Error> {
        let filter = self.filters.get(filter_id).cloned();
        Ok(filter)
    }

    fn set_filter(
        &mut self,
        filter_id: &Self::FilterId,
        filter: Self::Filter,
    ) -> Result<(), Self::Error> {
        self.filters.insert(filter_id.clone(), filter);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        budget::{pure_dp_filter::PureDPBudgetFilter, traits::FilterStatus},
        pds::quotas::{FilterId, StaticCapacities},
    };

    #[test]
    fn test_hash_map_filter_storage() -> Result<(), anyhow::Error> {
        let capacities = StaticCapacities::mock();
        let mut storage: HashMapFilterStorage<PureDPBudgetFilter, _> =
            HashMapFilterStorage::new(capacities)?;

        let fid: FilterId<i32, ()> = FilterId::C(1);
        assert_eq!(storage.try_consume(&fid, &10.0)?, FilterStatus::Continue);
        assert_eq!(
            storage.try_consume(&fid, &11.0)?,
            FilterStatus::OutOfBudget,
        );

        Ok(())
    }
}
