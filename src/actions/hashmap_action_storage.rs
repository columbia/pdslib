use std::{fmt::Debug, hash::BuildHasher};

use crate::{
    actions::traits::{ActionId, ActionStorage},
    events::traits::{EpochId, Uri},
    util::hashmap::{HashMap, HashSet, RandomState},
};

#[derive(Debug, Clone)]
pub struct UserActionState<E: EpochId, U: Uri, S: BuildHasher = RandomState> {
    pub accessed_sites: HashMap<E, HashSet<U, S>, S>,
}

impl<E: EpochId, U: Uri, S: BuildHasher + Default> Default
    for UserActionState<E, U, S>
{
    fn default() -> Self {
        Self {
            accessed_sites: HashMap::with_hasher(S::default()),
        }
    }
}

#[derive(Debug)]
pub struct HashMapActionStorage<
    AID: ActionId,
    E: EpochId,
    U: Uri,
    S: BuildHasher = RandomState,
> {
    pub actions: HashMap<AID, UserActionState<E, U, S>, S>,
    pub quota_limit: Option<usize>,
}

impl<AID: ActionId, E: EpochId, U: Uri, S: BuildHasher + Default> Default
    for HashMapActionStorage<AID, E, U, S>
{
    fn default() -> Self {
        Self {
            actions: HashMap::with_hasher(S::default()),
            quota_limit: None,
        }
    }
}

impl<AID: ActionId, E: EpochId, U: Uri, S: BuildHasher + Default>
    HashMapActionStorage<AID, E, U, S>
{
    pub fn new(quota_limit: Option<usize>) -> Self {
        Self::with_hashmap_capacity(quota_limit, 0)
    }

    pub fn with_hashmap_capacity(
        quota_limit: Option<usize>,
        hashmap_capacity: usize,
    ) -> Self {
        Self {
            actions: HashMap::with_capacity_and_hasher(
                hashmap_capacity,
                S::default(),
            ),
            quota_limit,
        }
    }
}

impl<AID, E, U, S> ActionStorage for HashMapActionStorage<AID, E, U, S>
where
    AID: ActionId,
    E: EpochId,
    U: Uri,
    S: BuildHasher + Default,
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
        let epoch_sites = state
            .accessed_sites
            .entry(epoch)
            .or_insert_with(|| HashSet::with_hasher(S::default()));

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
