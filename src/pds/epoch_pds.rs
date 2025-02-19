use std::collections::HashMap;

use thiserror::Error;

use crate::events::traits::EventStorageError;
use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStorage, FilterStorageError},
    },
    events::traits::{
        EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
    },
    mechanisms::{NoiseScale, NormType},
    pds::traits::PrivateDataService,
    queries::traits::{
        EpochReportRequest, PassivePrivacyLossRequest, ReportRequest,
    },
};

use super::traits::PDSError;

/// Epoch-based private data service implementation, using generic filter
/// storage and event storage interfaces. We might want other implementations
/// eventually, but at first this implementation should cover most use cases,
/// as we can swap the types of events, filters and queries.
pub struct EpochPrivateDataServiceImpl<
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
pub enum PDSImplError<E: EventStorageError, F: FilterStorageError> {
    #[error("Failed to register event.")]
    EventRegistrationError(E),

    #[error("Failed to consume privacy budget from filter: {0}")]
    FilterConsumptionError(F),
}

impl<E: EventStorageError, F: FilterStorageError> PDSError
    for PDSImplError<E, F>
{
    type EventStorageError = E;
    type FilterStorageError = F;

    fn from_event_storage_error(
        error: <Self as PDSError>::EventStorageError,
    ) -> Self {
        PDSImplError::EventRegistrationError(error)
    }

    fn from_filter_storage_error(
        error: <Self as PDSError>::FilterStorageError,
    ) -> Self {
        PDSImplError::FilterConsumptionError(error)
    }

    fn as_filter_storage_error(&self) -> Option<&Self::FilterStorageError> {
        match self {
            PDSImplError::FilterConsumptionError(e) => Some(e),
            _ => None,
        }
    }

    fn as_event_storage_error(&self) -> Option<&Self::EventStorageError> {
        match self {
            PDSImplError::EventRegistrationError(e) => Some(e),
            _ => None,
        }
    }
}

/// Implements the generic PDS interface for the epoch-based PDS.
///
/// TODO(https://github.com/columbia/pdslib/issues/21): support more than PureDP
/// TODO(https://github.com/columbia/pdslib/issues/18): handle multiple queriers
/// instead of assuming that there is a single querier and using filter_id =
/// epoch_id
impl<EI, E, EE, RES, FS, ES, Q> PrivateDataService
    for EpochPrivateDataServiceImpl<FS, ES, Q>
where
    EI: EpochId,
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage<FilterId = EI, Budget = PureDPBudget>,
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
    type Error = PDSImplError<ES::Error, FS::Error>;
    fn register_event(
        &mut self,
        event: E,
    ) -> Result<(), PDSImplError<ES::Error, FS::Error>> {
        println!("Registering event {:?}", event);
        self.event_storage
            .add_event(event)
            .map_err(PDSImplError::from_event_storage_error)
    }

    /// This function follows `compute_attribution_report` from the Cookie
    /// Monster Algorithm (https://arxiv.org/pdf/2405.16719, Code Listing 1)
    fn compute_report(
        &mut self,
        request: Q,
    ) -> Result<<Q as ReportRequest>::Report, Self::Error> {
        println!("Computing report for request {:?}", request);
        // Collect events from event storage. If an epoch has no relevant
        // events, don't add it to the mapping.
        let mut relevant_events_per_epoch: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.get_relevant_event_selector();
        for epoch_id in request.get_epoch_ids() {
            let epoch_relevant_events = self
                .event_storage
                .get_relevant_epoch_events(&epoch_id, &relevant_event_selector)
                .map_err(PDSImplError::from_event_storage_error)?;

            if let Some(epoch_relevant_events) = epoch_relevant_events {
                relevant_events_per_epoch.insert(epoch_id, epoch_relevant_events);
            }
        }

        // Compute the raw report, useful for debugging and accounting.
        let num_epochs: usize = relevant_events_per_epoch.len();
        let unbiased_report =
            request.compute_report(&relevant_events_per_epoch);

        // Browse epochs in the attribution window
        for epoch_id in request.get_epoch_ids() {
            // Step 1. Get relevant events for the current epoch `epoch_id`.
            let epoch_relevant_events =
                relevant_events_per_epoch.get(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
            let individual_privacy_loss = self.compute_individual_privacy_loss(
                &request,
                epoch_relevant_events,
                &unbiased_report,
                num_epochs,
            );

            // Initialize filter if necessary.
            // TODO(https://github.com/columbia/pdslib/issues/18): handle multiple queriers.
            self.initialize_filter_if_necessary(&epoch_id)?;

            // Step 3. Try to consume budget from current epoch, drop events if OOB.
            match self
                .filter_storage
                .check_and_consume(&epoch_id, &individual_privacy_loss)
            {
                Ok(_) => {
                    // The budget is not depleted, keep events.
                }
                Err(e) if e.is_out_of_budget() => {
                    // The budget is depleted, drop events.
                    relevant_events_per_epoch.remove(&epoch_id);
                }
                _ => {
                    // Return default report if anything else goes wrong.
                    return Ok(Default::default());
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report = request.compute_report(&relevant_events_per_epoch);
        Ok(filtered_report)
    }

    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), Self::Error> {
        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            self.initialize_filter_if_necessary(&epoch_id)?;

            // Try to consume budget from current epoch.
            self.filter_storage
                .check_and_consume(&epoch_id, &request.privacy_budget)
                .map_err(PDSImplError::from_filter_storage_error)?;

            // TODO(https://github.com/columbia/pdslib/issues/16): semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(())
    }
}

/// Utility methods for the epoch-based PDS implementation.
impl<EI, E, EE, FS, ES, Q> EpochPrivateDataServiceImpl<FS, ES, Q>
where
    EI: EpochId,
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage<FilterId = EI>,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    Q: EpochReportRequest<EpochId = EI, EpochEvents = EE>,
{
    fn initialize_filter_if_necessary(
        &mut self,
        epoch_id: &EI,
    ) -> Result<(), PDSImplError<ES::Error, FS::Error>> {
        let filter_initialized =
            self.filter_storage
                .is_initialized(epoch_id)
                .map_err(PDSImplError::from_filter_storage_error)?;
        if !filter_initialized {
            let create_filter_result = self
                .filter_storage
                .new_filter(epoch_id.clone(), self.epoch_capacity.clone());

            if create_filter_result.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Pure DP individual privacy loss, following
    /// `compute_individual_privacy_loss` from Code Listing 1 in Cookie Monster (https://arxiv.org/pdf/2405.16719).
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/21): generic budget.
    fn compute_individual_privacy_loss(
        &self,
        request: &Q,
        epoch_relevant_events: Option<&EE>,
        computed_attribution: &<Q as ReportRequest>::Report,
        num_epochs: usize,
    ) -> PureDPBudget {
        // Case 1: Epoch with no relevant events
        match epoch_relevant_events {
            None => {
                return PureDPBudget::Epsilon(0.0);
            }
            Some(epoch_events) => {
                if epoch_events.is_empty() {
                    return PureDPBudget::Epsilon(0.0);
                }
            }
        }

        let individual_sensitivity = match num_epochs {
            1 => {
                // Case 2: One epoch.
                request.get_single_epoch_individual_sensitivity(
                    computed_attribution,
                    NormType::L1,
                )
            }
            _ => {
                // Case 3: Multiple epochs.
                request.get_report_global_sensitivity()
            }
        };

        let NoiseScale::Laplace(noise_scale) = request.get_noise_scale();

        // Treat near-zero noise scales as non-private, i.e. requesting infinite
        // budget, which can only go through if filters are also set to
        // infinite capacity, e.g. for debugging. The machine precision
        // `f64::EPSILON` is not related to privacy.
        if noise_scale.abs() < f64::EPSILON {
            return PureDPBudget::Infinite;
        }

        // In Cookie Monster, we have `query_global_sensitivity` / `requested_epsilon` instead
        // of just `noise_scale`.
        // TODO(https://github.com/columbia/pdslib/issues/23): potentially use two parameters
        // instead of a single `noise_scale`.
        PureDPBudget::Epsilon(individual_sensitivity / noise_scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        budget::{
            hashmap_filter_storage::HashMapFilterStorage,
            pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        },
        events::hashmap_event_storage::HashMapEventStorage,
        queries::{
            simple_last_touch_histogram::SimpleLastTouchHistogramRequest,
            traits::PassivePrivacyLossRequest,
        },
    };

    #[test]
    fn test_account_for_passive_privacy_loss() {
        let filters: HashMapFilterStorage<
            usize,
            PureDPBudgetFilter,
            PureDPBudget,
        > = HashMapFilterStorage::new();
        let events = HashMapEventStorage::new();

        let mut pds = EpochPrivateDataServiceImpl {
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
