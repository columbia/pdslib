use std::collections::HashMap;

use http::Uri;

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

    pub source_uri: Uri,
    pub trigger_uris: Vec<Uri>,
    pub querier_uris: Vec<Uri>,
}

impl Event for AraEvent {
    type EpochId = usize;

    fn get_epoch_id(&self) -> Self::EpochId {
        self.epoch_number
    }

    fn source_uri(&self) -> Uri {
        self.source_uri.clone()
    }

    fn trigger_uris(&self) -> Vec<Uri> {
        self.trigger_uris.clone()
    }

    fn querier_uris(&self) -> Vec<Uri> {
        self.querier_uris.clone()
    }
}
