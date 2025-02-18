use std::collections::HashMap;
use thiserror::Error;

use crate::budget::pure_dp_filter::PureDPBudget;
use crate::budget::traits::{FilterError, FilterStorage, FilterStorageError};
use crate::events::traits::RelevantEventSelector;
use crate::events::traits::{EpochEvents, EpochId, Event, EventStorage};
use crate::mechanisms::{NoiseScale, NormType};
use crate::pds::traits::PrivateDataService;
use crate::queries::traits::{
    EpochReportRequest, PassivePrivacyLossRequest, ReportRequest,
};

/// Epoch-based private data service implementation, using generic filter
/// storage and event storage interfaces. We might want other implementations
/// eventually, but at first this implementation should cover most use cases,
/// as we can swap the types of events, filters and queries.
pub struct PrivateDataServiceImpl<
    FS: FilterStorage,
    ES: EventStorage,
    Q: EpochReportRequest,
> {
    /// Filter storage interface.
    pub filter_storage: FS,

    /// Event storage interface.
    pub event_storage: ES,

    /// Default capacity that will be used for all new epochs
    pub epoch_capacity: FS::Budget,

    /// Type of accepted queries.
    pub _phantom: std::marker::PhantomData<Q>,
}

#[derive(Debug, Error)]
pub enum PDSImplError {
    #[error("Failed to register event.")]
    EventRegistrationError,

    #[error("Failed to consume privacy budget from filter: {0}")]
    FilterConsumptionError(#[from] FilterStorageError),
}

impl<EI, E, EE, RES, FS, ES, Q> PrivateDataService
    for PrivateDataServiceImpl<FS, ES, Q>
where
    EI: EpochId,
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage<FilterId = EI, Budget = PureDPBudget>, /* NOTE: we'll want to support other budgets eventually */
    RES: RelevantEventSelector<Event = E>,
    ES: EventStorage<Event = E, EpochEvents = EE, RelevantEventSelector = RES>,
    Q: EpochReportRequest<
        EpochId = EI,
        EpochEvents = EE,
        RelevantEventSelector = RES,
    >,
{
    type Event = E;
    type Request = Q;
    type PassivePrivacyLossRequest =
        PassivePrivacyLossRequest<EI, PureDPBudget>;
    type Error = PDSImplError;

    fn register_event(&mut self, event: E) -> Result<(), PDSImplError> {
        println!("Registering event {:?}", event);
        self.event_storage
            .add_event(event)
            .map_err(|_| PDSImplError::EventRegistrationError)
    }

    fn compute_report(&mut self, request: Q) -> <Q as ReportRequest>::Report {
        println!("Computing report for request {:?}", request);
        // Collect events from event storage. If an epoch has no relevant
        // events, don't add it to the mapping.
        let mut all_relevant_events: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.get_relevant_event_selector();
        for epoch_id in request.get_epoch_ids() {
            if let Some(epoch_relevant_events) = self
                .event_storage
                .get_relevant_epoch_events(&epoch_id, &relevant_event_selector)
            {
                all_relevant_events.insert(epoch_id, epoch_relevant_events);
            }
        }
        let num_epochs: usize = all_relevant_events.len();

        let unbiased_report = request.compute_report(&all_relevant_events);

        for epoch_id in request.get_epoch_ids() {
            // Get the epoch events for the epoch_id in the report.
            let epoch_relevant_events = all_relevant_events.get(&epoch_id);

            // Compute the individual sensitivity for the relevant epoch.
            let individual_privacy_loss = self.compute_individual_privacy_loss(
                &request,
                epoch_relevant_events,
                &unbiased_report,
                num_epochs,
            );

            // Initialize filter if necessary.
            if !self.filter_storage.is_initialized(&epoch_id) {
                if self
                    .filter_storage
                    .new_filter(epoch_id.clone(), self.epoch_capacity.clone())
                    .is_err()
                {
                    return Default::default();
                }
            }

            // Try to consume budget from current epoch, drop events if OOB.
            match self
                .filter_storage
                .try_consume(&epoch_id, &individual_privacy_loss)
            {
                Ok(_) => {
                    // The budget is not depleted, keep events.
                }
                Err(FilterStorageError::FilterError(
                    FilterError::OutOfBudget,
                )) => {
                    // The budget is depleted, drop events.
                    all_relevant_events.remove(&epoch_id);
                }
                _ => {
                    // Return default report if anything else goes wrong.
                    return Default::default();
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report = request.compute_report(&all_relevant_events);
        filtered_report
    }

    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), PDSImplError> {
        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            // Initialize filter if necessary.
            if !self.filter_storage.is_initialized(&epoch_id) {
                self.filter_storage.new_filter(
                    epoch_id.clone(),
                    self.epoch_capacity.clone(),
                )?;
            }

            // Try to consume budget from current epoch.
            self.filter_storage
                .try_consume(&epoch_id, &request.privacy_budget)?;

            // TODO(https://github.com/columbia/pdslib/issues/16): semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(())
    }
}

/// Utility method for individual privacy loss computation.
/// TODO: generalize to other types of budget.
impl<EI, E, EE, FS, ES, Q> PrivateDataServiceImpl<FS, ES, Q>
where
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    Q: EpochReportRequest<EpochId = EI, EpochEvents = EE>,
{
    fn compute_individual_privacy_loss(
        &self,
        request: &Q,
        epoch_events: Option<&EE>,
        computed_attribution: &<Q as ReportRequest>::Report,
        num_epochs: usize,
    ) -> PureDPBudget {
        // Implement the logic to compute individual privacy loss
        // Case 1: Empty epoch_event.
        match epoch_events {
            None => {
                return PureDPBudget::Epsilon(0.0);
            }
            Some(epoch_events) => {
                if epoch_events.is_empty() {
                    return PureDPBudget::Epsilon(0.0);
                }
            }
        }

        let individual_sensitivity: f64;
        if num_epochs == 1 {
            // Case 2: Exactly one event in epoch_events, then individual
            // sensitivity is the one attribution value.
            individual_sensitivity = request
                .get_single_epoch_individual_sensitivity(
                    computed_attribution,
                    NormType::L1,
                );
        } else {
            // Case 3: Multiple events in epoch_events.
            individual_sensitivity = request.get_global_sensitivity();
        }

        let noise_scale = match request.get_noise_scale() {
            NoiseScale::Laplace(scale) => scale,
        };

        if noise_scale.abs() < f64::EPSILON {
            return PureDPBudget::Infinite;
        }
        return PureDPBudget::Epsilon(individual_sensitivity / noise_scale);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::hashmap_filter_storage::HashMapFilterStorage;
    use crate::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};
    use crate::events::hashmap_event_storage::HashMapEventStorage;
    use crate::queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest;
    use crate::queries::traits::PassivePrivacyLossRequest;

    #[test]
    fn test_account_for_passive_privacy_loss() {
        let filters: HashMapFilterStorage<
            usize,
            PureDPBudgetFilter,
            PureDPBudget,
        > = HashMapFilterStorage::new();
        let events = HashMapEventStorage::new();

        let mut pds = PrivateDataServiceImpl {
            filter_storage: filters,
            event_storage: events,
            epoch_capacity: PureDPBudget::Epsilon(3.0),
            _phantom: std::marker::PhantomData::<SimpleLastTouchHistogramRequest>,
        };

        // First request should succeed
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request);
        assert!(result.is_ok());

        // Second request with same budget should succeed (2.0 total)
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request);
        assert!(result.is_ok());

        // Verify remaining budgets
        for epoch_id in 1..=3 {
            let remaining = pds
                .filter_storage
                .get_remaining_budget(&epoch_id)
                .expect("Failed to get remaining budget");
            assert_eq!(remaining, PureDPBudget::Epsilon(1.0)); // 3.0 - 2.0 =
                                                               // 1.0 remaining
        }

        // Attempting to consume more should fail.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![2, 3],
            privacy_budget: PureDPBudget::Epsilon(2.0),
        };
        let result = pds.account_for_passive_privacy_loss(request);
        assert!(result.is_err());

        // Consume from just one epoch.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request);
        assert!(result.is_ok());

        // Verify remaining budgets
        for epoch_id in 1..=2 {
            let remaining = pds
                .filter_storage
                .get_remaining_budget(&epoch_id)
                .expect("Failed to get remaining budget");
            assert_eq!(remaining, PureDPBudget::Epsilon(1.0));
        }
        let remaining = pds
            .filter_storage
            .get_remaining_budget(&3)
            .expect("Failed to get remaining budget");
        assert_eq!(remaining, PureDPBudget::Epsilon(0.0));
    }
}
