use std::{
    collections::hash_map::Entry,
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

use serde::{Serialize, ser::SerializeStruct};

use crate::{
    budget::traits::{Filter, FilterCapacities, FilterStorage},
    util::hashmap::{HashMap, RandomState},
};

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug)]
pub struct HashMapFilterStorage<F, C, S = RandomState>
where
    C: FilterCapacities,
    F: Filter<C::Budget>,
    S: BuildHasher + Default,
{
    pub capacities: C,
    pub filters: HashMap<C::FilterId, F, S>,
}

impl<F, C, S> Default for HashMapFilterStorage<F, C, S>
where
    C: FilterCapacities + Default,
    F: Filter<C::Budget>,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self {
            capacities: C::default(),
            filters: HashMap::with_hasher(S::default()),
        }
    }
}

impl<F, C, FID, S> Serialize for HashMapFilterStorage<F, C, S>
where
    C: FilterCapacities<FilterId = FID> + Serialize,
    F: Filter<C::Budget> + Serialize,
    FID: Serialize + Eq + Hash + Debug,
    S: BuildHasher + Default,
    HashMap<FID, F, S>: Serialize,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        let mut state =
            serializer.serialize_struct("HashMapFilterStorage", 2)?;
        state.serialize_field("capacities", &self.capacities)?;
        state.serialize_field("filters", &self.filters)?;
        state.end()
    }
}

impl<F, C, S> HashMapFilterStorage<F, C, S>
where
    C: FilterCapacities,
    F: Filter<C::Budget>,
    S: BuildHasher + Default,
{
    pub fn with_hashmap_capacity(
        capacities: C,
        hashmap_capacity: usize,
    ) -> Self {
        Self {
            capacities,
            filters: HashMap::with_capacity_and_hasher(
                hashmap_capacity,
                S::default(),
            ),
        }
    }
}

impl<F, C, S> FilterStorage for HashMapFilterStorage<F, C, S>
where
    F: Filter<C::Budget, Error = anyhow::Error> + Clone,
    C: FilterCapacities<Error = anyhow::Error>,
    C::FilterId: Clone + Eq + Hash + Debug,
    S: BuildHasher + Default,
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
        Ok(Self::with_hashmap_capacity(capacities, 0))
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

    /// Optimized version using Entry API
    fn edit_filter_or_new<R>(
        &mut self,
        filter_id: &Self::FilterId,
        f: impl FnOnce(&mut Self::Filter) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let entry = self.filters.entry(filter_id.clone());

        let filter = match entry {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                let capacity = self.capacities.capacity(filter_id)?;
                vacant.insert(Self::Filter::new(capacity)?)
            }
        };

        f(filter)
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

        let fid: FilterId<i32, ()> = FilterId::Global(1);
        assert_eq!(storage.try_consume(&fid, &10.0)?, FilterStatus::Continue);
        assert_eq!(
            storage.try_consume(&fid, &11.0)?,
            FilterStatus::OutOfBudget,
        );

        Ok(())
    }
}
