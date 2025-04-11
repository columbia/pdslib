use std::{collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData};

use anyhow::Context;
use log::debug;

use crate::budget::traits::{
    Budget, Filter, FilterCapacities, FilterStatus, FilterStorage,
};

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug, Default)]
pub struct HashMapFilterStorage<FID, F, B, C> {
    pub capacities: C,

    /// TODO: make this field private again eventually. MAde it public for
    /// hacky serialization.
    pub filters: HashMap<FID, F>,
    _marker: PhantomData<B>,
}

impl<FID, F, B, C> FilterStorage for HashMapFilterStorage<FID, F, B, C>
where
    FID: Clone + Eq + Hash + Debug,
    F: Filter<B, Error = anyhow::Error>,
    B: Budget,
    C: FilterCapacities<FilterId = FID, Budget = B, Error = anyhow::Error>,
{
    type FilterId = FID;
    type Budget = B;
    type Capacities = C;
    type Error = anyhow::Error;

    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let this = Self {
            capacities,
            filters: HashMap::new(),
            _marker: PhantomData,
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

    fn is_initialized(&mut self, filter_id: &FID) -> Result<bool, Self::Error> {
        let entry = self.filters.get_mut(filter_id);
        Ok(entry.is_some())
    }

    fn can_consume(
        &self,
        filter_id: &FID,
        budget: &B,
    ) -> Result<bool, Self::Error> {
        let filter = self
            .filters
            .get(filter_id)
            .context("Filter for epoch not initialized")?;

        debug!(
            "Check if we can consume {:?} from filter {:?}: {:?}",
            budget, filter_id, filter
        );

        filter.can_consume(budget)
    }

    fn try_consume(
        &mut self,
        filter_id: &FID,
        budget: &B,
    ) -> Result<FilterStatus, Self::Error> {
        let filter = self
            .filters
            .get_mut(filter_id)
            .context("Filter for epoch not initialized")?;

        debug!(
            "Trying to consume {:?} from filter {:?}: {:?}",
            budget, filter_id, filter
        );

        filter.try_consume(budget)
    }

    fn remaining_budget(
        &self,
        filter_id: &FID,
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
        budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        pds::epoch_pds::{FilterId, StaticCapacities},
    };

    #[test]
    fn test_hash_map_filter_storage() -> Result<(), anyhow::Error> {
        let capacities = StaticCapacities::mock();
        let mut storage: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
            HashMapFilterStorage::new(capacities)?;

        let fid: FilterId<_, String> = FilterId::C(1);
        storage.new_filter(fid.clone())?;
        assert_eq!(
            storage.try_consume(&fid, &PureDPBudget::Epsilon(10.0))?,
            FilterStatus::Continue
        );
        assert_eq!(
            storage.try_consume(&fid, &PureDPBudget::Epsilon(11.0))?,
            FilterStatus::OutOfBudget,
        );

        // Filter C(2) does not exist
        assert!(storage
            .try_consume(&FilterId::C(2), &PureDPBudget::Epsilon(1.0))
            .is_err());

        Ok(())
    }
}
