use std::{collections::{HashMap, HashSet}, vec};

use crate::{
    events::{
        hashmap_event_storage::VecEpochEvents, ppa_event::PpaEvent,
        traits::RelevantEventSelector,
    },
    queries::{histogram::HistogramRequest, traits::ReportRequestUris},
};

pub struct PpaRelevantEventSelector {
    pub report_request_uris: ReportRequestUris<String>,
    pub is_matching_event: Box<dyn Fn(u64) -> bool>,
    pub querier_bucket_mapping: HashMap<String, HashSet<usize>>,
}

impl std::fmt::Debug for PpaRelevantEventSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PpaRelevantEventSelector")
            .field("report_request_uris", &self.report_request_uris)
            .field("querier_bucket_mapping", &self.querier_bucket_mapping)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub enum AttributionLogic {
    LastTouch,
}

impl RelevantEventSelector for PpaRelevantEventSelector {
    type Event = PpaEvent;

    fn is_relevant_event(&self, event: &PpaEvent) -> bool {
        // Condition 1: Event's source URI should be in the allowed list by the
        // report request source URIs.
        let source_match = self
            .report_request_uris
            .source_uris
            .contains(&event.uris.source_uri);

        // Condition 2: Every querier URI from the report must be in the event’s
        // querier URIs. TODO: We might change Condition 2 eventually
        // when we support split reports, where one querier is
        // authorized but not others.
        let querier_match = self
            .report_request_uris
            .querier_uris
            .iter()
            .all(|uri| event.uris.querier_uris.contains(uri));

        // Condition 3: The report’s trigger URI should be allowed by the event
        // trigger URIs.
        let trigger_match = event
            .uris
            .trigger_uris
            .contains(&self.report_request_uris.trigger_uri);

        source_match
            && querier_match
            && trigger_match
            && (self.is_matching_event)(event.filter_data)
    }
}

#[derive(Debug)]
pub struct PpaHistogramRequest {
    start_epoch: usize,
    end_epoch: usize,
    report_global_sensitivity: f64,
    query_global_sensitivity: f64,
    requested_epsilon: f64,
    histogram_size: usize,
    filters: PpaRelevantEventSelector,
    logic: AttributionLogic,
    is_optimization_query: bool,
}

impl PpaHistogramRequest {
    /// Constructs a new `PpaHistogramRequest`, validating that:
    /// - `requested_epsilon` is > 0.
    /// - `report_global_sensitivity` and `query_global_sensitivity` are
    ///   non-negative.
    pub fn new(
        start_epoch: usize,
        end_epoch: usize,
        report_global_sensitivity: f64,
        query_global_sensitivity: f64,
        requested_epsilon: f64,
        histogram_size: usize,
        filters: PpaRelevantEventSelector,
        is_optimization_query: bool,
    ) -> Result<Self, &'static str> {
        if requested_epsilon <= 0.0 {
            return Err("requested_epsilon must be greater than 0");
        }
        if report_global_sensitivity < 0.0 || query_global_sensitivity < 0.0 {
            return Err("sensitivity values must be non-negative");
        }
        if histogram_size == 0 {
            return Err("histogram_size must be greater than 0");
        }
        Ok(Self {
            start_epoch,
            end_epoch,
            report_global_sensitivity,
            query_global_sensitivity,
            requested_epsilon,
            histogram_size,
            filters,
            logic: AttributionLogic::LastTouch,
            is_optimization_query,
        })
    }

    // Helper method to check if a bucket is for a specific querier
    pub fn is_bucket_for_querier(&self, bucket_key: usize, querier_uri: &str) -> bool {
        match self.filters.querier_bucket_mapping.get(querier_uri) {
            Some(bucket_set) => bucket_set.contains(&bucket_key),
            None => false,
        }
    }
    
    // Helper method to check if this is an optimization query
    pub fn is_optimization_query(&self) -> bool {
        self.is_optimization_query
    }
    
    // Helper method to get querier bucket mapping
    pub fn get_querier_bucket_mapping(&self) -> &HashMap<String, HashSet<usize>> {
        &self.filters.querier_bucket_mapping
    }
}

// Util function to create querier bucket mapping
pub fn create_querier_bucket_mapping(
    mappings: Vec<(String, Vec<usize>)>,
) -> HashMap<String, HashSet<usize>> {
    mappings
        .into_iter()
        .map(|(uri, buckets)| (uri, buckets.into_iter().collect()))
        .collect()
}

// Util function to filter histogram reports for specific queriers
pub fn filter_histogram_for_querier<BK: std::hash::Hash + Eq + Clone>(
    full_histogram: &HashMap<BK, f64>,
    querier_buckets: &HashSet<BK>,
) -> HashMap<BK, f64> {
    full_histogram
        .iter()
        .filter_map(|(key, value)| {
            if querier_buckets.contains(key) {
                Some((key.clone(), *value))
            } else {
                None
            }
        })
        .collect()
}

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

    fn relevant_event_selector(&self) -> &Self::RelevantEventSelector {
        &self.filters
    }

    fn bucket_key(&self, event: &PpaEvent) -> Self::BucketKey {
        // Bucket key validation.
        if event.histogram_index >= self.histogram_size {
            log::warn!(
                "Invalid bucket key {}: exceeds histogram size {}. Event id: {}",
                event.histogram_index,
                self.histogram_size,
                event.id
            );
        }

        event.histogram_index
    }

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
                        if last_impression.histogram_index < self.histogram_size {
                            // For optimization queries, check if the bucket belongs to any querier
                            // For regular queries, include all buckets
                            let include_bucket = !self.is_optimization_query || 
                                self.filters.report_request_uris.querier_uris.iter().any(|uri| 
                                    self.is_bucket_for_querier(last_impression.histogram_index, uri)
                                );
                                
                            if include_bucket {
                                event_values.push((
                                    last_impression,
                                    self.report_global_sensitivity,
                                ));
                            }
                        } else {
                            // Log error for dropped events
                            log::error!(
                                "Dropping event with id {} due to invalid bucket key {}",
                                last_impression.id,
                                last_impression.histogram_index
                            );
                        }
                    }
                }
            } // Other attribution logic not supported yet.
        }

        event_values
    }

    fn report_uris(&self) -> ReportRequestUris<String> {
        self.filters.report_request_uris.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_querier_bucket_mapping() {
        let mappings = vec![
            ("meta.ex".to_string(), vec![0, 1]),
            ("google.ex".to_string(), vec![1, 2]),
        ];

        let mapping = create_querier_bucket_mapping(mappings);
        
        assert!(mapping.get("meta.ex").unwrap().contains(&0));
        assert!(mapping.get("meta.ex").unwrap().contains(&1));
        assert!(!mapping.get("meta.ex").unwrap().contains(&2));

        assert!(!mapping.get("google.ex").unwrap().contains(&0));
        assert!(mapping.get("google.ex").unwrap().contains(&1));
        assert!(mapping.get("google.ex").unwrap().contains(&2));
    }
    
    #[test]
    fn test_filter_histogram_for_querier() {
        let full_histogram: HashMap<usize, f64> = [
            (0, 10.0),
            (1, 20.0),
            (2, 30.0),
        ].iter().cloned().collect();

        let meta_buckets: HashSet<usize> = [0, 1].iter().cloned().collect();
        let google_buckets: HashSet<usize> = [1, 2].iter().cloned().collect();

        let meta_histogram = filter_histogram_for_querier(&full_histogram, &meta_buckets);
        let google_histogram = filter_histogram_for_querier(&full_histogram, &google_buckets);

        assert_eq!(meta_histogram.len(), 2);
        assert_eq!(meta_histogram.get(&0), Some(&10.0));
        assert_eq!(meta_histogram.get(&1), Some(&20.0));
        assert_eq!(meta_histogram.get(&2), None);

        assert_eq!(google_histogram.len(), 2);
        assert_eq!(google_histogram.get(&0), None);
        assert_eq!(google_histogram.get(&1), Some(&20.0));
        assert_eq!(google_histogram.get(&2), Some(&30.0));
    }
}
