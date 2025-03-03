use crate::{
    budget::{
        hashmap_filter_storage::StaticCapacities, pure_dp_filter::PureDPBudget,
    },
    events::traits::EventUris,
    queries::traits::ReportRequestUris,
};

// Sample mock values to reduce boilerplate in tests.

impl StaticCapacities<PureDPBudget> {
    /// Sample capacitiy values for testing.
    pub fn mock() -> Self {
        Self {
            nc_capacity: PureDPBudget::Epsilon(1.0),
            c_capacity: PureDPBudget::Epsilon(20.0),
            qtrigger_capacity: PureDPBudget::Epsilon(1.5),
        }
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
