use std::{
    collections::HashMap,
    marker::PhantomData,
};

use anyhow::Context;

use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{
            Budget, Filter, FilterCapacities, FilterStatus, FilterStorage,
        },
    },
    pds::epoch_pds::FilterId,
};

type HashMapFilterId = FilterId<usize, String>;

pub struct StaticCapacities<B> {
    pub nc_capacity: B,
    pub c_capacity: B,
    pub qconv_capacity: B,
}

impl StaticCapacities<PureDPBudget> {
    /// Sample capacitiy values for testing.
    pub fn mock() -> Self {
        Self {
            nc_capacity: PureDPBudget::Epsilon(1.0),
            c_capacity: PureDPBudget::Epsilon(20.0),
            qconv_capacity: PureDPBudget::Epsilon(1.0),
        }
    }
}

impl<B: Budget> FilterCapacities for StaticCapacities<B> {
    type Budget = B;
    type Error = anyhow::Error;

    fn nc_capacity(&self) -> Result<Self::Budget, Self::Error> {
        Ok(self.nc_capacity.clone())
    }

    fn c_capacity(&self) -> Result<Self::Budget, Self::Error> {
        Ok(self.c_capacity.clone())
    }

    fn qconv_capacity(&self) -> Result<Self::Budget, Self::Error> {
        Ok(self.qconv_capacity.clone())
    }
}

/// Simple implementation of FilterStorage using a HashMap.
/// Works for any Filter that implements the Filter trait.
#[derive(Debug, Default)]
pub struct HashMapFilterStorage<C, F, Budget> {
    capacities: C,
    nc: HashMap<HashMapFilterId, F>,
    c: HashMap<HashMapFilterId, F>,
    qconv: HashMap<HashMapFilterId, F>,
    _marker: PhantomData<Budget>,
}

impl<C, F, B> FilterStorage for HashMapFilterStorage<C, F, B>
where
    B: Budget,
    C: FilterCapacities<Budget = B, Error = anyhow::Error>,
    F: Filter<B, Error = anyhow::Error>,
{
    type EpochId = usize;
    type Uri = String;
    type Budget = B;
    type Capacities = C;
    type Error = anyhow::Error;

    fn new(capacities: Self::Capacities) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let this = Self {
            capacities,
            nc: HashMap::new(),
            c: HashMap::new(),
            qconv: HashMap::new(),
            _marker: PhantomData,
        };
        Ok(this)
    }

    fn new_filter(
        &mut self,
        filter_id: HashMapFilterId,
    ) -> Result<(), Self::Error> {
        let nc_capacity = self.capacities.nc_capacity()?;
        let c_capacity = self.capacities.c_capacity()?;
        let qconv_capacity = self.capacities.qconv_capacity()?;

        let nc_filter = F::new(nc_capacity)?;
        let c_filter = F::new(c_capacity)?;
        let qconv_filter = F::new(qconv_capacity)?;

        self.nc.insert(filter_id.clone(), nc_filter);
        self.c.insert(filter_id.clone(), c_filter);
        self.qconv.insert(filter_id, qconv_filter);

        Ok(())
    }

    fn is_initialized(
        &mut self,
        filter_id: &HashMapFilterId,
    ) -> Result<bool, Self::Error> {
        let entry = self.get_filter_mut(filter_id);
        Ok(entry.is_some())
    }

    fn check_and_consume(
        &mut self,
        filter_id: &HashMapFilterId,
        budget: &B,
    ) -> Result<FilterStatus, Self::Error> {
        let filter = self
            .get_filter_mut(filter_id)
            .context("Filter for epoch not initialized")?;

        filter.check_and_consume(budget)
    }

    fn remaining_budget(
        &self,
        filter_id: &HashMapFilterId,
    ) -> Result<Self::Budget, Self::Error> {
        let filter = self
            .get_filter(filter_id)
            .context("Filter for epoch not initialized")?;
        // Return 0 if filter not initialized?

        filter.remaining_budget()
    }
}

impl<C, F, B> HashMapFilterStorage<C, F, B>
where
    B: Budget,
    C: FilterCapacities<Budget = B, Error = anyhow::Error>,
    F: Filter<B, Error = anyhow::Error>,
{
    fn get_filter(&self, filter_id: &HashMapFilterId) -> Option<&F> {
        let map = match &filter_id {
            FilterId::Nc(..) => &self.nc,
            FilterId::C(..) => &self.c,
            FilterId::QConv(..) => &self.qconv,
        };
        map.get(filter_id)
    }

    fn get_filter_mut(
        &mut self,
        filter_id: &HashMapFilterId,
    ) -> Option<&mut F> {
        let map = match &filter_id {
            FilterId::Nc(..) => &mut self.nc,
            FilterId::C(..) => &mut self.c,
            FilterId::QConv(..) => &mut self.qconv,
        };
        map.get_mut(filter_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};

    #[test]
    fn test_hash_map_filter_storage() -> Result<(), anyhow::Error> {
        let capacities = StaticCapacities {
            nc_capacity: PureDPBudget::Epsilon(1.0),
            c_capacity: PureDPBudget::Epsilon(20.0),
            qconv_capacity: PureDPBudget::Epsilon(1.0),
        };
        let mut storage: HashMapFilterStorage<_, PureDPBudgetFilter, _> =
            HashMapFilterStorage::new(capacities)?;

        let fid = FilterId::C(1);
        storage.new_filter(fid.clone())?;
        assert_eq!(
            storage.check_and_consume(&fid, &PureDPBudget::Epsilon(10.0))?,
            FilterStatus::Continue
        );
        assert_eq!(
            storage.check_and_consume(&fid, &PureDPBudget::Epsilon(11.0))?,
            FilterStatus::OutOfBudget
        );

        // Filter C(2) does not exist
        assert!(storage
            .check_and_consume(&FilterId::C(2), &PureDPBudget::Epsilon(1.0))
            .is_err());

        Ok(())
    }
}
