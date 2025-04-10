use anyhow::Error;

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::traits::EventUris,
    pds::epoch_pds::StaticCapacities,
    queries::{
        ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
        traits::ReportRequestUris,
    },
};

// Sample mock values to reduce boilerplate in tests.

impl<FID> StaticCapacities<FID, PureDPBudget> {
    /// Sample capacitiy values for testing.
    pub fn mock() -> Self {
        Self::new(
            PureDPBudget::Epsilon(1.0),
            PureDPBudget::Epsilon(20.0),
            PureDPBudget::Epsilon(1.5),
            PureDPBudget::Epsilon(4.0),
        )
    }
}

impl EventUris<String> {
    /// Sample URIs for testing.
    pub fn mock() -> Self {
        Self {
            source_uri: "blog.com".to_string(),
            trigger_uris: vec!["shoes.com".to_string()],
            querier_uris: vec![
                "shoes.com".to_string(),
                "adtech.com".to_string(),
            ],
        }
    }
}

impl ReportRequestUris<String> {
    /// Sample URIs for testing.
    pub fn mock() -> Self {
        Self {
            trigger_uri: "shoes.com".to_string(),
            source_uris: vec!["blog.com".to_string()],
            querier_uris: vec!["adtech.com".to_string()],
        }
    }
}

impl PpaHistogramRequest {
    pub fn mock() -> Result<Self, Error> {
        PpaHistogramRequest::new(
            1,
            2,
            32768.0,
            65536.0,
            1.0,
            2048,
            PpaRelevantEventSelector {
                report_request_uris: ReportRequestUris::mock(),
                is_matching_event: Box::new(|_: u64| true),
            },
        )
    }
}
