use crate::events::traits::{Event, EventUris};

/// Impression event
#[derive(Debug, Clone)]
pub struct PpaEvent {
    /// Event ID, e.g., counter or random ID. Unused in Firefox but kept for
    /// debugging purposes.
    pub id: usize,

    /// Timestamp, also for debugging purposes.
    pub timestamp: u64,

    pub epoch_number: usize,

    pub histogram_index: usize,

    pub uris: EventUris<String>,

    /// This field can contain bit-packed information about campaigns, ads, or
    /// other attributes that the relevant event selector can use to
    /// determine relevance. Note: Unlike Firefox's implementation which
    /// has explicit campaign_id or ad_id fields, the PPA spec uses
    /// filter_data as a more generic mechanism for filtering events.
    pub filter_data: u64,
}

impl Event for PpaEvent {
    type EpochId = usize;
    type Uri = String;
    type HistogramIndex = usize;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> EventUris<String> {
        self.uris.clone()
    }

    fn histogram_index(&self) -> Self::HistogramIndex {
        self.histogram_index
    }
}
