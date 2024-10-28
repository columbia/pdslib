use crate::budget::traits::FilterStorage;
use crate::events::traits::{Event, EventStorage};
use crate::pds::traits::PrivateDataService;
use crate::queries::traits::ReportRequest;

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
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>,
{
    type Event = E;
    type ReportRequest = RR;
    type Report = RR::Report;

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
            let epoch_events = self.event_storage.get_epoch_events(epoch_id);
            if let Some(epoch_events) = epoch_events {
                all_epoch_events.push(epoch_events); // TODO: else, push empty evc or actually None?
            }
        }

        // TODO: ensure types match.
        let unbiased_report = request.compute_report(&all_epoch_events);

        // TODO: compute individual budgets for each epoch, consume from filters, compute biased report.
        // NOTE: for debugging, we'd like an unbiased report. Use a tuple then?

        // TODO: return the report that is desired. Temporarily returning unbiased_report to compile successfully.
        unbiased_report
    }
}
