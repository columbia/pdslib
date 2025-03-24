use crate::events::traits::{Event, EventUris};

/// Source event for ARA-style callers such as Chromium.
/// Mimics the fields from https://source.chromium.org/chromium/chromium/src/+/main:content/browser/attribution_reporting/attribution_reporting.proto.
///
/// TODO(https://github.com/columbia/pdslib/issues/8): add other fields as needed by callers, e.g. filters.
#[derive(Debug, Clone)]
pub struct PpaEvent {
    // Unused but kept for debugging purposes - can be filled with counter or random ID
    pub id: usize,
    pub epoch_number: usize,
    pub histogram_index: usize,
    pub uris: EventUris<String>,
    // Note: Unlike Firefox's implementation which has explicit campaign_id or ad_id fields,
    // the PPA spec uses filter_data as a more generic mechanism for filtering events.
    // This field can contain bit-packed information about campaigns, ads, or other attributes
    // that the relevant event selector can use to determine relevance.
    pub filter_data: u64,
}

impl Event for PpaEvent {
    type EpochId = usize;
    type Uri = String;

    fn epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn event_uris(&self) -> EventUris<String> {
        self.uris.clone()
    }
}
