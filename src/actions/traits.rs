use std::{fmt::Debug, hash::Hash};

use crate::events::traits::{EpochId, Uri};

/// Marker trait for user-action context identifiers
pub trait ActionId: Copy + Eq + Hash + Debug {}
impl<T: Copy + Eq + Hash + Debug> ActionId for T {}

pub trait ActionStorage {
    type ActionId: ActionId;
    type EpochId: EpochId;
    type Uri: Uri;
    type Error: Debug;

    /// Checks if a site can be recorded for the given action and epoch.
    /// Returns Ok(true) if allowed (and records it), Ok(false) if
    /// quota exceeded.
    fn try_record_site(
        &mut self,
        action_id: Self::ActionId,
        epoch: Self::EpochId,
        site: &Self::Uri,
    ) -> Result<bool, Self::Error>;
}
