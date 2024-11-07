use crate::budget::traits::FilterStorage;
use crate::events::traits::{Event, EventStorage};
use crate::pds::traits::PrivateDataService;
use crate::queries::traits::ReportRequest;
use crate::events::simple_events::SimpleEvent;

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
    // Q: Query, // TODO: maybe particular type?
    FS: FilterStorage,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    E: Event<EpochId = EI>,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>, EE: std::fmt::Debug + Clone + AsRef<[SimpleEvent]>,
{
    type Event = E;
    type ReportRequest = RR;
    type Report = RR::Report;
    type EpochEvents = EE;

    fn register_event(&mut self, event: E) -> Result<(), ()> {
        println!("Registering event {:?}", event);
        self.event_storage.add_event(event)
    }

    fn compute_report(&mut self, request: Self::ReportRequest) -> Self::Report {
        print!("Computing report for request {:?}", request);
        // TODO: collect events from event storage.
        // It means the request should give a list of epochs.

        let mut all_epoch_events: Vec<_> = vec![];
        for epoch_id in request.get_epoch_ids() {
            // TODO: ensure epochs match.
            let epoch_events = self.event_storage.get_epoch_events(&epoch_id);
            if let Some(epoch_events) = epoch_events {
                all_epoch_events.push(epoch_events.clone()); // TODO: else, push empty evc or actually None? COMMENT(Mark): Think it works better to push empty evc.
            }
        }

        // TODO: ensure types match.
        let unbiased_report = request.compute_report(&all_epoch_events);

        // TODO: compute individual sensitivity for each epoch, consume from filters; return null for
        // that part of the report if budget depleted.
        // NOTE: for debugging, we'd like an unbiased report. Use a tuple then?
        for epoch_events in all_epoch_events.iter() {
            let individual_sensitivity = self.compute_individual_privacy_loss(&request, epoch_events, &unbiased_report);
            println!("Individual sensitivity: {:?}", individual_sensitivity);
        }

        // TODO: return the report that is desired. Temporarily returning unbiased_report to compile successfully.
        unbiased_report
    }

    // fn check_and_consume(&self, epoch_data: &EE, budget: &mut f64) -> Option<EE> {
    //     let privacy_loss = self.compute_individual_privacy_loss(epoch_data);
    //     if *budget >= privacy_loss {
    //         *budget -= privacy_loss;
    //         Some(epoch_data.clone()) // Return the original data if budget is sufficient
    //     } else {
    //         None // Return None if budget is depleted
    //     }
    // }
}

impl<FS, ES, E, RR, EI, EE> PrivateDataServiceImpl<FS, ES, RR>
where
    FS: FilterStorage,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    E: Event<EpochId = EI>,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>, EE: std::fmt::Debug + Clone + AsRef<[SimpleEvent]>,
{
    fn compute_individual_privacy_loss(&self, request: &RR, epoch_events: &EE, computed_attribution: &RR::Report) -> f64 {
        // Implement the logic to compute individual privacy loss
        let events_slice = epoch_events.as_ref().to_vec();

        // Case 1: Empty epoch_event.
        if events_slice.is_empty() {
            return 0.0;
        }

        let individual_sensitivity: f64;
        if events_slice.len() == 1 {
            // Case 2: Exactly one event in epoch_events, then individual sensitivity is the one attribution value.
            individual_sensitivity = request.get_single_epoch_individual_sensitivity(computed_attribution, false);
        }
        else {
            // Case 3: Multiple events in epoch_events.
            individual_sensitivity = request.get_global_sensitivity();
        }
        return request.get_requested_epsilon() * individual_sensitivity / request.get_global_sensitivity();
    }
}
