use std::fmt::Debug;

use super::traits::Uri;
use crate::{
    actions::traits::ActionId,
    events::traits::{Event, EventUris},
    queries::ppa_histogram::{PpaBucketKey, PpaEpochId, PpaFilterData},
};

/// Impression event
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PpaEvent<U: Uri = String, A: ActionId = u64> {
    /// Event ID, e.g., counter or random ID. Unused in Firefox but kept for
    /// debugging purposes.
    pub id: u64,

    /// Used to order events for last-touch attribution.
    pub timestamp: u64,
    pub epoch_number: PpaEpochId,
    pub histogram_index: PpaBucketKey,
    pub user_action_id: Option<A>,
    pub uris: EventUris<U>,

    /// This field can contain bit-packed information about campaigns, ads, or
    /// other attributes that the relevant event selector can use to
    /// determine relevance. Note: Unlike Firefox's implementation which
    /// has explicit campaign_id or ad_id fields, the PPA spec uses
    /// filter_data as a more generic mechanism for filtering events.
    pub filter_data: PpaFilterData,
}

impl<U: Uri, A: ActionId> Event for PpaEvent<U, A> {
    type EpochId = PpaEpochId;
    type Uri = U;
    type ActionId = A;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> &EventUris<U> {
        &self.uris
    }

    fn user_action_id(&self) -> Option<Self::ActionId> {
        self.user_action_id
    }
}
