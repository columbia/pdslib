use std::collections::HashMap;

use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStatus, FilterStorage},
    },
    events::traits::{
        EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
    },
    mechanisms::{NoiseScale, NormType},
    queries::traits::{
        EpochReportRequest, PassivePrivacyLossRequest, ReportRequest,
    },
};

/// Epoch-based private data service, using generic filter
/// storage and event storage interfaces.
///
/// TODO(https://github.com/columbia/pdslib/issues/18): handle multiple queriers
/// instead of assuming that there is a single querier and using filter_id =
/// epoch_id
pub struct EpochPrivateDataService<
    FS: FilterStorage,
    ES: EventStorage,
    Q: EpochReportRequest,
    ERR: From<FS::Error> + From<ES::Error>,
> {
    /// Filter storage interface.
    pub filter_storage: FS,

    /// Event storage interface.
    pub event_storage: ES,

    /// Default capacity that will be used for all new epochs
    pub epoch_capacity: FS::Budget,

    /// Type of accepted queries.
    pub _phantom_request: std::marker::PhantomData<Q>,

    /// Type of errors.
    pub _phantom_error: std::marker::PhantomData<ERR>,
}

/// API for the epoch-based PDS.
///
/// TODO(https://github.com/columbia/pdslib/issues/21): support more than PureDP
/// TODO(https://github.com/columbia/pdslib/issues/22): simplify trait bounds?
impl<EI, E, EE, RES, FS, ES, Q, ERR> EpochPrivateDataService<FS, ES, Q, ERR>
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
    ERR: From<FS::Error> + From<ES::Error>,
{
    /// Registers a new event.
    pub fn register_event(&mut self, event: E) -> Result<(), ERR> {
        println!("Registering event {:?}", event);
        self.event_storage.add_event(event)?;
        Ok(())
    }

    /// Computes a report for the given report request.
    /// This function follows `compute_attribution_report` from the Cookie
    /// Monster Algorithm (https://arxiv.org/pdf/2405.16719, Code Listing 1)
    pub fn compute_report(
        &mut self,
        request: Q,
    ) -> Result<<Q as ReportRequest>::Report, ERR> {
        println!("Computing report for request {:?}", request);

        // Collect events from event storage. If an epoch has no relevant
        // events, don't add it to the mapping.
        let mut relevant_events_per_epoch: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.get_relevant_event_selector();
        for epoch_id in request.get_epoch_ids() {
            let epoch_relevant_events =
                self.event_storage.get_relevant_epoch_events(
                    &epoch_id,
                    &relevant_event_selector,
                )?;

            if let Some(epoch_relevant_events) = epoch_relevant_events {
                relevant_events_per_epoch
                    .insert(epoch_id, epoch_relevant_events);
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

            // Step 3. Try to consume budget from current epoch, drop events if
            // OOB.
            match self
                .filter_storage
                .check_and_consume(&epoch_id, &individual_privacy_loss)
            {
                Ok(FilterStatus::Continue) => {
                    // The budget is not depleted, keep events.
                }
                Ok(FilterStatus::OutOfBudget) => {
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
        let filtered_report =
            request.compute_report(&relevant_events_per_epoch);
        Ok(filtered_report)
    }

    /// [Experimental] Accounts for passive privacy loss. Can fail if the
    /// implementation has an error, but failure must not leak the state of
    /// the filters.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/16): what are the semantics of passive loss queries that go over the filter
    /// capacity?
    pub fn account_for_passive_privacy_loss(
        &mut self,
        request: PassivePrivacyLossRequest<EI, PureDPBudget>,
    ) -> Result<FilterStatus, ERR> {
        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            self.initialize_filter_if_necessary(&epoch_id)?;

            // Try to consume budget from current epoch.
            let filter_status = self
                .filter_storage
                .check_and_consume(&epoch_id, &request.privacy_budget)?;

            if filter_status == FilterStatus::OutOfBudget {
                return Ok(FilterStatus::OutOfBudget);
            }

            // TODO(https://github.com/columbia/pdslib/issues/16): semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(FilterStatus::Continue)
    }

    fn initialize_filter_if_necessary(
        &mut self,
        epoch_id: &EI,
    ) -> Result<(), ERR> {
        let filter_initialized =
            self.filter_storage.is_initialized(epoch_id)?;

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

        // In Cookie Monster, we have `query_global_sensitivity` /
        // `requested_epsilon` instead of just `noise_scale`.
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

        let mut pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            epoch_capacity: PureDPBudget::Epsilon(3.0),
            _phantom_request: std::marker::PhantomData::<
                SimpleLastTouchHistogramRequest,
            >,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };

        // First request should succeed
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request).unwrap();
        assert_eq!(result, FilterStatus::Continue);

        // Second request with same budget should succeed (2.0 total)
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request).unwrap();
        assert_eq!(result, FilterStatus::Continue);

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
        let result = pds.account_for_passive_privacy_loss(request).unwrap();
        assert_eq!(result, FilterStatus::OutOfBudget);

        // Consume from just one epoch.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![3],
            privacy_budget: PureDPBudget::Epsilon(1.0),
        };
        let result = pds.account_for_passive_privacy_loss(request).unwrap();
        assert_eq!(result, FilterStatus::Continue);

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
