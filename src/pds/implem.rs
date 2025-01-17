use crate::budget::pure_dp_filter::PureDPBudget;
use crate::budget::traits::{FilterError, FilterStorage, FilterStorageError};
use crate::events::traits::{EpochEvents, EpochId, Event, EventStorage};
use crate::mechanisms::NormType;
use crate::pds::traits::PrivateDataService;
use crate::queries::traits::{
    EpochReportRequest, PassivePrivacyLossRequest, ReportRequest,
};
use std::collections::HashMap;

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

impl<EI, E, EE, RES, FS, ES, Q> PrivateDataService
    for PrivateDataServiceImpl<FS, ES, Q>
where
    EI: EpochId,
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage<FilterId = EI, Budget = PureDPBudget>, /* NOTE: we'll want to support other budgets eventually */
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

    fn register_event(&mut self, event: E) -> Result<(), ()> {
        println!("Registering event {:?}", event);
        self.event_storage.add_event(event)
    }

    fn compute_report(&mut self, request: Q) -> <Q as ReportRequest>::Report {
        println!("Computing report for request {:?}", request);
        // Collect events from event storage. If an epoch has no relevant
        // events, don't add it to the mapping.
        let mut map_of_events_set_over_epochs: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.get_relevant_event_selector();
        for epoch_id in request.get_epoch_ids() {
            if let Some(epoch_events) = self
                .event_storage
                .get_epoch_events(&epoch_id, &relevant_event_selector)
            {
                map_of_events_set_over_epochs.insert(epoch_id, epoch_events);
            }
        }
        let num_epochs: usize = map_of_events_set_over_epochs.len();

        let unbiased_report =
            request.compute_report(&map_of_events_set_over_epochs);

        for epoch_id in request.get_epoch_ids() {
            // Get the epoch events for the epoch_id in the report.
            let set_of_events_for_relevant_epoch =
                map_of_events_set_over_epochs.get(&epoch_id);

            // Compute the individual sensitivity for the relevant epoch.
            let individual_privacy_loss = self.compute_individual_privacy_loss(
                &request,
                set_of_events_for_relevant_epoch,
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
                    map_of_events_set_over_epochs.remove(&epoch_id);
                }
                _ => {
                    // Return default report if anything else goes wrong.
                    return Default::default();
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report =
            request.compute_report(&map_of_events_set_over_epochs);
        filtered_report
    }

    fn account_for_passive_privacy_loss(
        &mut self,
        request: Self::PassivePrivacyLossRequest,
    ) -> Result<(), ()> {
        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            // Initialize filter if necessary.
            if !self.filter_storage.is_initialized(&epoch_id) {
                self.filter_storage
                    .new_filter(epoch_id.clone(), self.epoch_capacity.clone())
                    .map_err(|_| ())?;
            }

            // Try to consume budget from current epoch.
            self.filter_storage
                .try_consume(&epoch_id, &request.privacy_budget)
                .map_err(|_| ())?;
        }
        Ok(())
    }
}

/// Utility methods for individual privacy loss computation.
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
                return PureDPBudget { epsilon: 0.0 };
            }
            Some(epoch_events) => {
                if epoch_events.is_empty() {
                    return PureDPBudget { epsilon: 0.0 };
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

        return PureDPBudget {
            epsilon: request.get_noise_scale() * individual_sensitivity,
        };
    }
}
