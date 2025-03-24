use crate::events::traits::{Event, EventUris};

/// Source event for ARA-style callers such as Chromium.
/// Mimics the fields from https://source.chromium.org/chromium/chromium/src/+/main:content/browser/attribution_reporting/attribution_reporting.proto.
///
/// TODO(https://github.com/columbia/pdslib/issues/8): add other fields as needed by callers, e.g. filters.
#[derive(Debug, Clone)]
pub struct PpaEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub histogram_index: usize,
    pub uris: EventUris<String>,
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
