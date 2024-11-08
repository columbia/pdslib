use crate::budget::traits::FilterStorage;
use crate::events::simple_events::SimpleEvent;
use crate::events::traits::{Event, EventStorage};
use crate::pds::traits::PrivateDataService;
use crate::queries::simple_last_touch_histogram::NormType;
use crate::queries::simple_last_touch_histogram::SimpleLastTouchHistogramReport;
use crate::queries::traits::ReportRequest;
use core::num;
use std::hash::Hash;
use indexmap::IndexMap;

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
    EI: Hash + std::cmp::Eq + AsAny,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE> + std::any::Any + std::fmt::Debug,
    RR::Report: AsAny + From<SimpleLastTouchHistogramReport> + Default,
    EE: AsAny + std::fmt::Debug + Clone + AsRef<[SimpleEvent]>,
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
        println!("Computing report for request {:?}", request);
        // TODO: collect events from event storage.
        // It means the request should give a list of epochs.

        // Default return value.
        let default_report: Self::Report = SimpleLastTouchHistogramReport {
            attributed_value: None,
        }.into();

        // `events_of_all_epochs` is a vector of vectors of events, where each vector of events corresponds to an epoch.
        let mut map_of_events_set_over_epochs: IndexMap<usize, EE> = IndexMap::new();
        for epoch_id in request.get_epoch_ids() {
            // TODO: ensure epochs match.
            if let Some(&epoch_id_in_usize) = epoch_id.as_any().downcast_ref::<usize>() {
                if let Some(epoch_events) = self.event_storage.get_epoch_events(&epoch_id) {
                    map_of_events_set_over_epochs.insert(epoch_id_in_usize, epoch_events);  // TODO: else, push empty evc or actually None? COMMENT(Mark): Think it works better to push empty vec.
                }
            }
        }
        let num_epochs: usize = map_of_events_set_over_epochs.len();

        // TODO: ensure types match.
        let unbiased_report = request.compute_report(&map_of_events_set_over_epochs);

        if let Some(unbiased_report_parsed) = unbiased_report.as_any().downcast_ref::<SimpleLastTouchHistogramReport>() {
            // TODO: compute individual sensitivity for each epoch, consume from filters; return null for
            // that part of the report if budget depleted.
            // NOTE: for debugging, we'd like an unbiased report. Use a tuple then?
            if let Some((epoch_id, _, _)) = unbiased_report_parsed.attributed_value {
                // Get the epoch events for the epoch_id in the report.
                let set_of_events_for_relevant_epoch = map_of_events_set_over_epochs.get(&epoch_id).unwrap();

                // Compute the individual sensitivity for the relevant epoch.
                let individual_sensitivity = self.compute_individual_privacy_loss(
                    &request,
                    set_of_events_for_relevant_epoch,
                    &unbiased_report,
                    num_epochs,
                );
                println!("Individual sensitivity: {:?}", individual_sensitivity);

                // TODO(mark): Check if the individual sensitivity is less than the remaining budget.
                // Note that, when this goes over, it should rather be clearing the relevant events than clearing the report.
                // But as it stands right now, it may make sense to try to return Null report by searching up by the epoch_id.

                // self.filter_storage.try_consume(individual_sensitivity);
                return SimpleLastTouchHistogramReport {
                    attributed_value: unbiased_report_parsed.attributed_value.clone(),
                }.into();
            } else {
                println!("No attributed value in the unbiased report. Treat as None.");
            }
        } else {
            println!("Data cannot be casted to SimpleLastTouchHistogramReport type. Treat as None.");
        }

        default_report
    }
}

impl<FS, ES, E, RR, EI, EE> PrivateDataServiceImpl<FS, ES, RR>
where
    FS: FilterStorage,
    ES: EventStorage<Event = E, EpochEvents = EE>,
    E: Event<EpochId = EI>,
    RR: ReportRequest<EpochId = EI, EpochEvents = EE>,
    EE: std::fmt::Debug + Clone + AsRef<[SimpleEvent]>,
{
    fn compute_individual_privacy_loss(&self, request: &RR, epoch_events: &EE, computed_attribution: &RR::Report, num_epochs: usize) -> f64 {
        // Implement the logic to compute individual privacy loss
        // Case 1: Empty epoch_event.
        if epoch_events.as_ref().to_vec().is_empty() {
            return 0.0;
        }

        let individual_sensitivity: f64;
        if num_epochs == 1 {
            // Case 2: Exactly one event in epoch_events, then individual sensitivity is the one attribution value.
            individual_sensitivity = request.get_single_epoch_individual_sensitivity(computed_attribution, NormType::L1);
        }
        else {
            // Case 3: Multiple events in epoch_events.
            individual_sensitivity = request.get_global_sensitivity();
        }
        return request.get_noise_scale() * individual_sensitivity;
    }
}

// Cast from the generic Self::Report to SimpleLastTouchHistogramReport.
pub trait AsAny {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl AsAny for usize {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
