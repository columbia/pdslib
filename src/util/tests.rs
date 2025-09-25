use crate::{
    budget::pure_dp_filter::PureDPBudget, events::traits::EventUris,
    pds::quotas::StaticCapacities, queries::traits::ReportRequestUris,
};

#[cfg(test)]
pub fn init_default_logging() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
    });
}

// Sample mock values to reduce boilerplate in tests.

impl<FID> StaticCapacities<FID, PureDPBudget> {
    /// Sample capacitiy values for testing.
    pub fn mock() -> Self {
        Self::new(
            PureDPBudget::from(1.0),
            PureDPBudget::from(20.0),
            PureDPBudget::from(1.5),
            PureDPBudget::from(4.0),
        )
    }
}

impl EventUris<String> {
    /// Sample URIs for testing.
    pub fn mock() -> Self {
        Self {
            source_uri: "blog.com".to_string(),
            trigger_uris: ["shoes.com".to_string()].into(),
            querier_uris: ["shoes.com".to_string(), "adtech.com".to_string()]
                .into(),
        }
    }
}

impl ReportRequestUris<String> {
    /// Sample URIs for testing.
    pub fn mock() -> Self {
        Self {
            trigger_uri: "shoes.com".to_string(),
            source_uris: ["blog.com".to_string()].into(),
            querier_uris: ["adtech.com".to_string()].into(),
        }
    }
}
