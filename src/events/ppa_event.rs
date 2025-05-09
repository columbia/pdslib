use std::fmt::Debug;

use super::traits::Uri;
use crate::events::traits::{Event, EventUris};

/// Impression event
#[derive(Debug, Clone)]
pub struct PpaEvent<U: Uri = String> {
    /// Event ID, e.g., counter or random ID. Unused in Firefox but kept for
    /// debugging purposes.
    pub id: u64,

    /// Timestamp, also for debugging purposes.
    pub timestamp: u64,

    pub epoch_number: u64,

    pub histogram_index: u64,

    pub uris: EventUris<U>,

    /// This field can contain bit-packed information about campaigns, ads, or
    /// other attributes that the relevant event selector can use to
    /// determine relevance. Note: Unlike Firefox's implementation which
    /// has explicit campaign_id or ad_id fields, the PPA spec uses
    /// filter_data as a more generic mechanism for filtering events.
    pub filter_data: u64,
}

impl<U: Uri> Event for PpaEvent<U> {
    type EpochId = u64;
    type Uri = U;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> &EventUris<U> {
        &self.uris
    }
}
