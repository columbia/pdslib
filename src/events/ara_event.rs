use std::collections::HashMap;

use crate::events::traits::Event;

/// Source event for ARA-style callers such as Chromium.
/// Mimics the fields from https://source.chromium.org/chromium/chromium/src/+/main:content/browser/attribution_reporting/attribution_reporting.proto.
///
/// TODO(https://github.com/columbia/pdslib/issues/8): add other fields as needed by callers, e.g. filters.
#[derive(Debug, Clone)]
pub struct AraEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub aggregatable_sources: HashMap<String, usize>,
}

impl Event for AraEvent {
    type EpochId = usize;

    fn get_epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }
}
