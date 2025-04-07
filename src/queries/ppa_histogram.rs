use std::{collections::{HashMap, HashSet}, vec};

use crate::{
    events::{
        hashmap_event_storage::VecEpochEvents, ppa_event::PpaEvent,
        traits::RelevantEventSelector,
    },
    queries::{histogram::HistogramRequest, traits::ReportRequestUris},
    budget::pure_dp_filter::PureDPBudget,
};

pub struct PpaHistogramConfig {
    pub start_epoch: usize,
    pub end_epoch: usize,
    pub report_global_sensitivity: f64,
    pub query_global_sensitivity: f64,
    pub requested_epsilon: f64,
    pub histogram_size: usize,
    pub is_optimization_query: bool,
}

pub struct PpaRelevantEventSelector {
    pub report_request_uris: ReportRequestUris<String>,
    pub is_matching_event: Box<dyn Fn(u64) -> bool>,
    pub querier_bucket_mapping: HashMap<String, HashSet<usize>>,
}

/// Struct for bucket event source site mapping.
pub struct BucketSiteMapping {
    // Maps bucket keys to impression site URIs
    pub bucket_to_site: HashMap<usize, String>,
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
        config: PpaHistogramConfig,
        filters: PpaRelevantEventSelector,
    ) -> Result<Self, &'static str> {
        if config.requested_epsilon <= 0.0 {
            return Err("requested_epsilon must be greater than 0");
        }
        if config.report_global_sensitivity < 0.0 || config.query_global_sensitivity < 0.0 {
            return Err("sensitivity values must be non-negative");
        }
        if config.histogram_size == 0 {
            return Err("histogram_size must be greater than 0");
        }
        Ok(Self {
            start_epoch: config.start_epoch,
            end_epoch: config.end_epoch,
            report_global_sensitivity: config.report_global_sensitivity,
            query_global_sensitivity: config.query_global_sensitivity,
            requested_epsilon: config.requested_epsilon,
            histogram_size: config.histogram_size,
            filters,
            logic: AttributionLogic::LastTouch,
            is_optimization_query: config.is_optimization_query,
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

    pub fn create_bucket_site_mapping(&self, events: &[PpaEvent]) -> BucketSiteMapping {
        let mut mapping = HashMap::new();
        
        for event in events {
            mapping.insert(
                event.histogram_index, 
                event.uris.source_uri.clone()
            );
        }
        
        BucketSiteMapping {
            bucket_to_site: mapping
        }
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
            }
        }

        event_values
    }

    fn report_uris(&self) -> ReportRequestUris<String> {
        self.filters.report_request_uris.clone()
    }

    fn is_optimization_query(&self) -> bool {
        self.is_optimization_query
    }

    fn get_histogram_querier_bucket_mapping(&self) -> Option<&HashMap<String, HashSet<Self::BucketKey>>> {
        Some(&self.filters.querier_bucket_mapping)
    }

    // Get a mapping from bucket IDs to site URIs
    fn get_bucket_site_mapping(&self, 
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>
    ) -> HashMap<usize, String> {
        let mut mapping = HashMap::new();
        
        // For each relevant site-level event, get the bucket ID to source site URI mapping.
        for events in relevant_events_per_epoch.values() {
            for event in events.iter() {
                mapping.insert(
                    event.histogram_index, 
                    event.uris.source_uri.clone()
                );
            }
        }
        
        mapping
    }

    fn filter_histogram_for_querier(
        &self,
        bin_values: &HashMap<Self::BucketKey, f64>,
        querier_uri: &String,
        relevant_events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
        epoch_site_privacy_losses: Option<&HashMap<String, PureDPBudget>>,
        available_site_budgets: Option<&HashMap<String, PureDPBudget>>,
    ) -> Option<HashMap<Self::BucketKey, f64>> {
        // If no epoch site privacy losses or available site budgets are provided,
        // we can just filter by the querier bucket mapping
        if let (
            Some(epoch_site_privacy_losses_map),
            Some(available_site_budgets_map)
        ) = (
            epoch_site_privacy_losses,
            available_site_budgets
        ) {
            // Get the buckets for this querier
            let querier_buckets =
                self.filters.querier_bucket_mapping.get(querier_uri)?;
            
            // Start by filtering based on bucket mapping
            let mut filtered_bins = HashMap::new();
            
            // We need a bucket-to-site mapping.
            let bucket_site_mapping = self.get_bucket_site_mapping(relevant_events_per_epoch);
            
            // Filter by both bucket mapping and site-level privacy budgets
            for (bucket, value) in bin_values {
                // Check if this bucket is assigned to this querier. Continue if not
                if !querier_buckets.contains(bucket) {
                    continue;
                }
                
                // Check if the corresponding site is within budget
                if let Some(site_uri) = bucket_site_mapping.get(bucket) {
                    if let (Some(required_budget), Some(available_budget)) = (
                        epoch_site_privacy_losses_map.get(site_uri), 
                        available_site_budgets_map.get(site_uri)
                    ) {
                        match (required_budget, available_budget) {
                            // Check if the site has enough budget
                            (PureDPBudget::Epsilon(req), PureDPBudget::Epsilon(avail)) => {
                                if *req <= *avail {
                                    filtered_bins.insert(*bucket, *value);
                                }
                            }
                            (_, PureDPBudget::Infinite) => {
                                // Infinite available budget, always include
                                filtered_bins.insert(*bucket, *value);
                            }
                            (PureDPBudget::Infinite, _) => {
                                // Infinite budget to consume, never include
                            }
                        }
                    } else {
                        // If we don't have budget information, include the bucket
                        print!("Warning: No budget information for site {}. Including bucket {} in the report.", site_uri, bucket);
                    }
                } else {
                    // If we don't know which site this bucket belongs to, include it
                    print!("Warning: No site mapping for bucket {}. Including it in the report.", bucket);
                }
            }
            
            if filtered_bins.is_empty() {
                None
            } else {
                Some(filtered_bins)
            }
        } else {
            // Handle the case where either is None
            // First check if the querier URI exists in the mapping
            match self.filters.querier_bucket_mapping.get(querier_uri) {
                Some(querier_buckets) => {
                    // Now use the helper function with the retrieved bucket set
                    Some(filter_histogram_for_querier(
                        bin_values,
                        querier_buckets,
                    ))
                },
                None => None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::traits::EventUris;

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
    fn test_filter_histogram_for_querier_basic() {
        // Create a sample histogram
        let histogram: HashMap<usize, f64> = [
            (0, 10.0), // Meta bucket
            (1, 20.0), // Shared bucket
            (2, 30.0), // Google bucket
        ].iter().cloned().collect();
        
        // Create querier bucket mappings
        let mappings = vec![
            ("meta.ex".to_string(), vec![0, 1]),
            ("google.ex".to_string(), vec![1, 2]),
        ];
        let mapping = create_querier_bucket_mapping(mappings);
        
        // Create a PPA histogram request with the mappings
        let filter_data_matcher = Box::new(|_: u64| true); // Simple matcher that accepts all
        let request = create_test_request(mapping.clone(), filter_data_matcher, true);
        
        // Test filtering for meta.ex
        let filtered_meta = request.filter_histogram_for_querier(
            &histogram,
            &"meta.ex".to_string(),
            &HashMap::new(), // Empty events map
            None, // No privacy budgets
            None, // No available budgets
        );
        
        assert!(filtered_meta.is_some());
        let filtered_meta = filtered_meta.unwrap();
        assert_eq!(filtered_meta.len(), 2);
        assert_eq!(filtered_meta.get(&0), Some(&10.0));
        assert_eq!(filtered_meta.get(&1), Some(&20.0));
        assert_eq!(filtered_meta.get(&2), None);
        
        // Test filtering for google.ex
        let filtered_google = request.filter_histogram_for_querier(
            &histogram,
            &"google.ex".to_string(),
            &HashMap::new(),
            None,
            None,
        );
        
        assert!(filtered_google.is_some());
        let filtered_google = filtered_google.unwrap();
        assert_eq!(filtered_google.len(), 2);
        assert_eq!(filtered_google.get(&0), None);
        assert_eq!(filtered_google.get(&1), Some(&20.0));
        assert_eq!(filtered_google.get(&2), Some(&30.0));
    }

    #[test]
    fn test_filter_histogram_for_querier_no_matching_buckets() {
        // Create a sample histogram
        let histogram: HashMap<usize, f64> = [
            (3, 10.0),
            (4, 20.0),
            (5, 30.0),
        ].iter().cloned().collect();
        
        // Create querier bucket mappings
        let mappings = vec![
            ("meta.ex".to_string(), vec![0, 1]),
            ("google.ex".to_string(), vec![1, 2]),
        ];
        let mapping = create_querier_bucket_mapping(mappings);
        
        // Create a PPA histogram request with the mappings
        let filter_data_matcher = Box::new(|_: u64| true);
        let request = create_test_request(mapping.clone(), filter_data_matcher, true);
        
        // Test filtering for meta.ex - should return None since no buckets match
        let filtered_meta = request.filter_histogram_for_querier(
            &histogram,
            &"meta.ex".to_string(),
            &HashMap::new(),
            None,
            None,
        );

        assert!(filtered_meta.unwrap().is_empty());
        
        // Test filtering for non-existent querier
        let filtered_unknown = request.filter_histogram_for_querier(
            &histogram,
            &"unknown.ex".to_string(),
            &HashMap::new(),
            None,
            None,
        );
        
        assert!(filtered_unknown.is_none());
    }

    #[test]
    fn test_filter_histogram_for_querier_with_privacy_budgets() {
        // Create a sample histogram
        let histogram: HashMap<usize, f64> = [
            (0, 10.0), // blog.ex bucket
            (1, 20.0), // news.ex bucket (insufficient budget)
            (2, 30.0), // social.ex bucket
        ].iter().cloned().collect();
        
        // Create querier bucket mappings
        let mappings = vec![
            ("meta.ex".to_string(), vec![0, 1, 2]),
        ];
        let mapping = create_querier_bucket_mapping(mappings);
        
        // Create a PPA histogram request with the mappings
        let filter_data_matcher = Box::new(|_: u64| true);
        let request = create_test_request(mapping.clone(), filter_data_matcher, true);
        
        // Create epoch-source privacy losses
        let mut site_privacy_losses = HashMap::new();
        site_privacy_losses.insert("blog.ex".to_string(), PureDPBudget::Epsilon(0.5));
        site_privacy_losses.insert("news.ex".to_string(), PureDPBudget::Epsilon(0.8));
        site_privacy_losses.insert("social.ex".to_string(), PureDPBudget::Epsilon(0.2));
        
        // Create available site budgets - news.ex has insufficient budget
        let mut available_budgets = HashMap::new();
        available_budgets.insert("blog.ex".to_string(), PureDPBudget::Epsilon(1.0));
        available_budgets.insert("news.ex".to_string(), PureDPBudget::Epsilon(0.5)); // Less than required 0.8
        available_budgets.insert("social.ex".to_string(), PureDPBudget::Epsilon(1.0));
        
        // Create bucket_site_mapping
        let mut bucket_site_mapping = HashMap::new();
        bucket_site_mapping.insert(0, "blog.ex".to_string());
        bucket_site_mapping.insert(1, "news.ex".to_string());
        bucket_site_mapping.insert(2, "social.ex".to_string());
        
        // Create a relevant events map with the bucket_site_mapping
        let mut events = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris {
                source_uri: "blog.ex".to_string(),
                trigger_uris: vec!["shoes.ex".to_string()],
                querier_uris: vec!["meta.ex".to_string()],
            },
            filter_data: 1,
        };
        
        let mut event_list = vec![events.clone()];
        
        events.histogram_index = 1;
        events.uris.source_uri = "news.ex".to_string();
        event_list.push(events.clone());
        
        events.histogram_index = 2;
        events.uris.source_uri = "social.ex".to_string();
        event_list.push(events.clone());
        
        let mut relevant_events_map = HashMap::new();
        relevant_events_map.insert(1, event_list);
        
        // Test filtering with privacy budgets
        let filtered = request.filter_histogram_for_querier(
            &histogram,
            &"meta.ex".to_string(),
            &relevant_events_map,
            Some(&site_privacy_losses),
            Some(&available_budgets),
        );
        
        assert!(filtered.is_some());
        let filtered = filtered.unwrap();
        
        // news.ex bucket (1) should be excluded due to insufficient budget
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered.get(&0), Some(&10.0)); // blog.ex has sufficient budget
        assert_eq!(filtered.get(&1), None);         // news.ex has insufficient budget
        assert_eq!(filtered.get(&2), Some(&30.0)); // social.ex has sufficient budget
    }

    // Util create a test request
    fn create_test_request(
        querier_bucket_mapping: HashMap<String, HashSet<usize>>,
        filter_data_matcher: Box<dyn Fn(u64) -> bool>,
        is_optimization_query: bool,
    ) -> PpaHistogramRequest {
        let report_request_uris = ReportRequestUris {
            trigger_uri: "shoes.ex".to_string(),
            source_uris: vec!["blog.ex".to_string(), "news.ex".to_string(), "social.ex".to_string()],
            querier_uris: querier_bucket_mapping.keys().cloned().collect(),
        };
        
        let config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            report_global_sensitivity: 32768.0,
            query_global_sensitivity: 65536.0,
            requested_epsilon: 1.0,
            histogram_size: 2048,
            is_optimization_query,
        };

        PpaHistogramRequest::new(
            config,
            
            PpaRelevantEventSelector {
                report_request_uris,
                is_matching_event: filter_data_matcher,
                querier_bucket_mapping,
            },
        ).unwrap()
    }
}