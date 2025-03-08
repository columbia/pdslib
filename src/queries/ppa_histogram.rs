//! [Experimental] ARA-style requests, that mirror https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md

use std::{collections::HashMap, vec};

use crate::{
    events::{
        ppa_event::PpaEvent, hashmap_event_storage::VecEpochEvents,
        traits::RelevantEventSelector,
    },
    queries::{histogram::HistogramRequest, traits::ReportRequestUris},
};

#[derive(Debug, Clone)]
pub struct PpaRelevantEventSelector {
    pub filters: HashMap<String, Vec<String>>,
    // TODO(https://github.com/columbia/pdslib/issues/8): add this if we drop events without the right source key
    // source_key: String,
    pub report_request_uris: ReportRequestUris<String>,
}

#[derive(Debug, Clone)]
pub enum AttributionLogic {
    LastTouch,
}

/// Select events using ARA-style filters.
/// See https://github.com/WICG/attribution-reporting-api/blob/main/EVENT.md#optional-attribution-filters

/// TODO: But additionally we might also want to filter based on metadata. Right now, any event that matches all the 3
/// URiIs is deemed relevant. But what about a query that only cares about impressions for product_a? This is what 
/// filterData is about in PPA. We will need to find out how it works exactly. Otherwise, a simple example would be
/// what we've done for Simple Histogram where we pass a lambda function, e.g. to keep events with a certain value 
/// of event_key.
impl RelevantEventSelector for PpaRelevantEventSelector {
    type Event = PpaEvent;

    fn is_relevant_event(&self, event: &PpaEvent) -> bool {
        // Condition 1: Event's source URI should be in the allowed list by the report request source URIs.
        let source_match = self.report_request_uris
            .source_uris
            .contains(&event.uris.source_uri);

        // Condition 2: Every querier URI from the report must be in the event’s querier URIs.
        // TODO: We might change Condition 2 eventually when we support split reports, where one querier is
        // authorized but not others.
        let querier_match = self.report_request_uris
            .querier_uris
            .iter()
            .all(|uri| event.uris.querier_uris.contains(uri));

        // Condition 3: The report’s trigger URI should be allowed by the event trigger URIs.
        let trigger_match = event.uris
            .trigger_uris
            .contains(&self.report_request_uris.trigger_uri);

        source_match && querier_match && trigger_match
    }
}

/// An instantiation of HistogramRequest that mimics ARA's types.
/// The request corresponds to a trigger event in ARA.
/// For now, each event is mapped to a single bucket, unlike ARA which supports
/// packed queries (which can be emulated by running multiple queries).
///
/// TODO(https://github.com/columbia/pdslib/issues/8): what is "nonMatchingKeyIdsIgnored"?
#[derive(Debug)]
pub struct PpaHistogramRequest {
    start_epoch: usize,
    end_epoch: usize,
    per_event_attributable_value: f64, /* ARA can attribute to multiple
                                            * events */
    report_global_sensitivity: f64, /* E.g. 2^16 in ARA, with scaling as
                                  * post-processing */
    query_global_sensitivity: f64,
    requested_epsilon: f64,
    source_key: String,
    trigger_keypiece: usize,
    filters: PpaRelevantEventSelector,
    logic: AttributionLogic,
}

impl PpaHistogramRequest {
    /// Constructs a new `PpaHistogramRequest`, validating that:
    /// - `requested_epsilon` is > 0.
    /// - `per_event_attributable_value`, `report_global_sensitivity` and 
    ///   `query_global_sensitivity` are non-negative.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        start_epoch: usize,
        end_epoch: usize,
        per_event_attributable_value: f64,
        report_global_sensitivity: f64,
        query_global_sensitivity: f64,
        requested_epsilon: f64,
        source_key: String,
        trigger_keypiece: usize,
        filters: PpaRelevantEventSelector,
        logic: AttributionLogic,
    ) -> Result<Self, &'static str> {
        if requested_epsilon <= 0.0 {
            return Err("requested_epsilon must be greater than 0");
        }
        if per_event_attributable_value < 0.0 ||
            report_global_sensitivity < 0.0 ||
            query_global_sensitivity < 0.0 {
            return Err("sensitivity values must be non-negative");
        }
        Ok(Self {
            start_epoch,
            end_epoch,
            per_event_attributable_value,
            report_global_sensitivity,
            query_global_sensitivity,
            requested_epsilon,
            source_key,
            trigger_keypiece,
            filters,
            logic,
        })
    }
}


/// See https://github.com/WICG/attribution-reporting-api/blob/main/AGGREGATE.md#attribution-trigger-registration.
impl HistogramRequest for PpaHistogramRequest {
    type EpochId = usize;
    type EpochEvents = VecEpochEvents<PpaEvent>;
    type Event = PpaEvent;
    type BucketKey = usize;
    type RelevantEventSelector = PpaRelevantEventSelector;

    fn epochs_ids(&self) -> Vec<Self::EpochId> {
        (self.start_epoch..=self.end_epoch).rev().collect()
    }

    fn query_global_sensitivity(&self) -> f64 {
        self.query_global_sensitivity
    }

    fn requested_epsilon(&self) -> f64 {
        self.requested_epsilon
    }

    fn laplace_noise_scale(&self) -> f64 {
        self.query_global_sensitivity / self.requested_epsilon
    }

    fn report_global_sensitivity(&self) -> f64 {
        self.report_global_sensitivity
    }

    fn relevant_event_selector(&self) -> Self::RelevantEventSelector {
        Self::RelevantEventSelector{
            filters: self.filters.filters.clone(),
            report_request_uris: self.filters.report_request_uris.clone(),
        }
    }

    // fn attribution_logic(&self) -> Self::AttributionLogic {
    //     self.logic.clone()
    // }

    fn bucket_key(&self, event: &PpaEvent) -> Self::BucketKey {
        // TODO(https://github.com/columbia/pdslib/issues/8):
        // What does ARA do when the source key is not present?
        // For now I still attribute with 0 for the source keypiece, but
        // I could treat the event as irrelevant too.
        let source_keypiece = event
            .aggregatable_sources
            .get(&self.source_key)
            .copied()
            .unwrap_or(0);
        source_keypiece | self.trigger_keypiece
    }

    /// Returns the same value for each relevant event. Will be capped by
    /// `compute_report`. An alternative would be to pick one event, or
    /// split the attribution cap uniformly.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/8): Double check with
    /// Chromium logic.
    fn event_values<'a>(
        &self,
        relevant_events_per_epoch: &'a HashMap<
            Self::EpochId,
            Self::EpochEvents,
        >,
    ) -> Vec<(&'a Self::Event, f64)> {
        let mut event_values = vec![];

        match self.logic {
            AttributionLogic::LastTouch => {
                for relevant_events in relevant_events_per_epoch.values() {
                    if let Some(last_impression) = relevant_events.last() {
                        event_values.push((last_impression, self.per_event_attributable_value));
                    }
                }
            }
            // Other attribution logic not supported yet.
        }

        event_values
    }

    fn report_uris(&self) -> ReportRequestUris<String> {
        self.filters.report_request_uris.clone()
    }
}
