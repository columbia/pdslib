use std::collections::{HashMap, HashSet};

use crate::queries::traits::EpochReportRequest;

/// Trait for queries that support optimization (split reports)
pub trait OptimizableQuery: EpochReportRequest {
    /// Returns true if this query should be treated as an optimization query
    fn is_optimization_query(&self) -> bool;
    
    /// Returns the mapping from querier URIs to their allowed bucket keys
    fn get_querier_bucket_mapping(&self) -> Option<&HashMap<Self::Uri, HashSet<Self::BucketKey>>>;
    
    /// Filter events for a specific querier based on their allowed bucket keys
    fn filter_events_for_querier(
        &self, 
        events_per_epoch: &HashMap<Self::EpochId, Self::EpochEvents>,
        querier_uri: &Self::Uri
    ) -> HashMap<Self::EpochId, Self::EpochEvents>;
}

/// Extension trait for EpochReportRequest to support optimization queries
pub trait OptimizableQueryExt: EpochReportRequest {
    /// Returns self as an OptimizableQuery if supported
    fn as_optimizable(&self) -> Option<&dyn OptimizableQuery<
        EpochId = Self::EpochId,
        Event = Self::Event,
        EpochEvents = Self::EpochEvents,
        PrivacyBudget = Self::PrivacyBudget,
        RelevantEventSelector = Self::RelevantEventSelector,
        Report = Self::Report,
        Uri = Self::Uri,
        BucketKey = Self::BucketKey,
    >>;
}

// Default implementation that returns None
impl<Q: EpochReportRequest> OptimizableQueryExt for Q {
    fn as_optimizable(&self) -> Option<&dyn OptimizableQuery<
        EpochId = Self::EpochId,
        Event = Self::Event,
        EpochEvents = Self::EpochEvents,
        PrivacyBudget = Self::PrivacyBudget,
        RelevantEventSelector = Self::RelevantEventSelector,
        Report = Self::Report,
        Uri = Self::Uri,
        BucketKey = Self::BucketKey,
    >> {
        None
    }
}