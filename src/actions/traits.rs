use std::{fmt::Debug, hash::Hash};

use crate::events::traits::{EpochId, Uri};

/// Marker trait for Action identifiers (e.g., u64, UUID).
pub trait ActionId: Copy + Eq + Hash + Debug {}
impl<T: Copy + Eq + Hash + Debug> ActionId for T {}

pub trait ActionStorage {
    type ActionId: ActionId;
    type EpochId: EpochId;
    type Uri: Uri;
    type Error: Debug;

    /// Checks if an impression site can be recorded for the given action.
    /// Returns Ok(true) if allowed (and records it), Ok(false) if quota
    /// exceeded.
    fn try_record_impression_site(
        &mut self,
        action_id: Self::ActionId,
        site: &Self::Uri,
    ) -> Result<bool, Self::Error>;

    /// Checks if a conversion site can be recorded for the given action and
    /// epoch. Returns Ok(true) if allowed (and records it), Ok(false) if
    /// quota exceeded.
    fn try_record_conversion_site(
        &mut self,
        action_id: Self::ActionId,
        epoch: Self::EpochId,
        site: &Self::Uri,
    ) -> Result<bool, Self::Error>;
}
