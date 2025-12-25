use std::fmt::Debug;

use crate::{
    actions::traits::{ActionId, ActionStorage},
    events::traits::{EpochId, Uri},
    util::hashmap::{HashMap, HashSet},
};

#[derive(Debug, Clone)]
pub struct UserActionState<E: EpochId, U: Uri> {
    pub accessed_sites: HashMap<E, HashSet<U>>,
}

impl<E: EpochId, U: Uri> Default for UserActionState<E, U> {
    fn default() -> Self {
        Self {
            accessed_sites: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct HashMapActionStorage<AID: ActionId, E: EpochId, U: Uri> {
    pub actions: HashMap<AID, UserActionState<E, U>>,
    pub quota_limit: Option<usize>,
}

impl<AID: ActionId, E: EpochId, U: Uri> HashMapActionStorage<AID, E, U> {
    pub fn new(quota_limit: Option<usize>) -> Self {
        Self {
            actions: HashMap::new(),
            quota_limit,
        }
    }
}

impl<AID, E, U> ActionStorage for HashMapActionStorage<AID, E, U>
where
    AID: ActionId,
    E: EpochId,
    U: Uri,
{
    type ActionId = AID;
    type EpochId = E;
    type Uri = U;
    type Error = anyhow::Error;

    fn try_record_site(
        &mut self,
        action_id: Self::ActionId,
        epoch: Self::EpochId,
        site: &Self::Uri,
    ) -> Result<bool, Self::Error> {
        let state = self.actions.entry(action_id).or_default();
        let epoch_sites = state.accessed_sites.entry(epoch).or_default();

        if epoch_sites.contains(site) {
            return Ok(true);
        }

        if let Some(quota_limit) = self.quota_limit
            && epoch_sites.len() >= quota_limit
        {
            return Ok(false);
        }

        epoch_sites.insert(site.clone());
        Ok(true)
    }
}
