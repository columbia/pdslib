use crate::budget::traits::FilterStorage;
use crate::events::traits::EventStorage;
use crate::queries::traits::Query;
use crate::pds::traits::PrivateDataService;


/// Epoch-based private data service implementation, using generic filter storage and event storage interfaces.
pub struct PrivateDataServiceImpl<Filters: FilterStorage, Events: EventStorage>
{
    pub filter_storage: Filters,
    pub event_storage: Events,
}

impl <Q, FS, ES, E> PrivateDataService<E, Q> for PrivateDataServiceImpl<FS, ES>
where 
    Q: Query,
    FS: FilterStorage,
    ES: EventStorage<Event=E>,
{
    fn register_event(&mut self, event: E) -> Result<(), ()> {
        // TODO: events need to have an epoch number, maybe querier id too.

        todo!();

        // self.event_storage.add_event(event, 0, ());
        // Ok(())
    }

    fn compute_report(&mut self, request: Q::ReportRequest) -> Q::Report {

        todo!()
    }
}