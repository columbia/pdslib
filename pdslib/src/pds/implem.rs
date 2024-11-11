use crate::budget::pure_dp_filter::PureDPBudget;
use crate::budget::traits::FilterStorage;
use crate::events::traits::{EpochEvents, Event, EventStorage};
use crate::pds::traits::PrivateDataService;
use crate::queries::simple_last_touch_histogram::NormType;
use crate::queries::traits::ReportRequest;
use std::collections::HashMap;
use std::hash::Hash;

/// Epoch-based private data service implementation, using generic filter
/// storage and event storage interfaces. We might want other implementations
/// eventually, but at first this implementation should cover most use cases,
/// as we can swap the types of events, filters and queries.
pub struct PrivateDataServiceImpl<
    Filters: FilterStorage,
    Events: EventStorage,
    RR: ReportRequest,
> {
    pub filter_storage: Filters,
    pub event_storage: Events,
    pub _phantom: std::marker::PhantomData<RR>, // Store the type of accepted queries.
}

impl<FS, ES, E, RR, EI, EE> PrivateDataService
    for PrivateDataServiceImpl<FS, ES, RR>
where
    EI: Hash + std::cmp::Eq + Clone,
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage<FilterId = EI, Budget = PureDPBudget>,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>,
    RR::Report: Default,
{
    type Budget = <FS as FilterStorage>::Budget;
    type EpochEvents = EE;
    type EpochId = EI;
    type Event = E;
    type Report = RR::Report;
    type ReportRequest = RR;

    fn register_event(&mut self, event: E) -> Result<(), ()> {
        println!("Registering event {:?}", event);
        self.event_storage.add_event(event)
    }

    fn register_epoch_capacity(
        &mut self,
        epoch_id: Self::EpochId,
        capacity: Self::Budget,
    ) -> Result<(), ()> {
        let mut res = Err(());
        if !self.filter_storage.get_filter(&epoch_id).is_none() {
            // The filter has already been set.
            return res;
        }

        // The filter has not been set yet, so we initialize new filter.
        res = self.filter_storage.new_filter(epoch_id, capacity);

        res
    }

    fn compute_report(&mut self, request: Self::ReportRequest) -> Self::Report {
        println!("Computing report for request {:?}", request);
        // Collect events from event storage.
        let mut map_of_events_set_over_epochs: HashMap<Self::EpochId, EE> =
            HashMap::new();
        for epoch_id in request.get_epoch_ids() {
            if let Some(epoch_events) =
                self.event_storage.get_epoch_events(&epoch_id)
            {
                map_of_events_set_over_epochs.insert(epoch_id, epoch_events); // TODO: else, push empty evc or actually None? COMMENT(Mark): Think it works better to push empty vec.
            }
        }
        let num_epochs: usize = map_of_events_set_over_epochs.len();

        // TODO: ensure types match.
        let unbiased_report =
            request.compute_report(&map_of_events_set_over_epochs);

        // TODO: compute individual sensitivity for each epoch, consume from filters; return null for
        // that part of the report if budget depleted.
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

            // Finalize report values based on budget consumptions.
            // Get the filter_id and filter_budget from the unbiased_report.
            if self.filter_storage.get_filter(&epoch_id).is_none() {
                // The filter is not set yet, so should be an error case.
                // For now, treat as error and return default report.
                // TODO: initialize new filter with default budget.
                return Default::default();
            }
            // If epoch_id is in the filter storage, then we try to consume budget from the filter for the filter_id set to the current epoch.
            match self
                .filter_storage
                .try_consume(&epoch_id, individual_privacy_loss)
            {
                Ok(Ok(())) => {
                    // The budget is not depleted, keep events.
                }
                Ok(Err(())) => {
                    // The budget is depleted / current reuqets requires more budget than the currently remaining, then drop events.
                    map_of_events_set_over_epochs.remove(&epoch_id);
                }
                Err(_) => {
                    // TODO: raise error properly
                    panic!("Storage failed to call filter.");
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report =
            request.compute_report(&map_of_events_set_over_epochs);
        filtered_report
    }
}

impl<FS, ES, E, RR, EI, EE> PrivateDataServiceImpl<FS, ES, RR>
where
    E: Event<EpochId = EI>,
    EE: EpochEvents,
    FS: FilterStorage,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>,
{
    fn compute_individual_privacy_loss(
        &self,
        request: &RR,
        epoch_events: Option<&EE>,
        computed_attribution: &RR::Report,
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
            // Case 2: Exactly one event in epoch_events, then individual sensitivity is the one attribution value.
            individual_sensitivity = request
                .get_single_epoch_individual_sensitivity(
                    computed_attribution,
                    NormType::L1,
                );
        } else {
            // Case 3: Multiple events in epoch_events.
            individual_sensitivity = request.get_global_sensitivity();
        }

        // TODO: allow other types of budgets, e.g. with type bound
        return PureDPBudget {
            epsilon: request.get_noise_scale() * individual_sensitivity,
        };
    }
}
