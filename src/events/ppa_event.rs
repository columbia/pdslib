use std::fmt::Debug;

use super::traits::Uri;
use crate::{
    events::traits::{Event, EventUris},
    queries::ppa_histogram::{PpaBucketKey, PpaEpochId, PpaFilterData},
};

/// Impression event
#[derive(Debug, Clone)]
pub struct PpaEvent<U: Uri = String> {
    /// Event ID, e.g., counter or random ID. Unused in Firefox but kept for
    /// debugging purposes.
    pub id: u64,

    /// Timestamp, also for debugging purposes.
    pub timestamp: u64,
    // TODO(later): Use the timestamp to determine last-touch attribution
    pub epoch_number: PpaEpochId,

    pub histogram_index: PpaBucketKey,

    pub uris: EventUris<U>,

    /// This field can contain bit-packed information about campaigns, ads, or
    /// other attributes that the relevant event selector can use to
    /// determine relevance. Note: Unlike Firefox's implementation which
    /// has explicit campaign_id or ad_id fields, the PPA spec uses
    /// filter_data as a more generic mechanism for filtering events.
    pub filter_data: PpaFilterData,
}

impl<U: Uri> Event for PpaEvent<U> {
    type EpochId = PpaEpochId;
    type Uri = U;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> &EventUris<U> {
        &self.uris
    }
}
