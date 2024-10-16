use crate::events::traits::Event;
use crate::queries::traits::Query;



pub trait PrivateDataService<E: Event, Q: Query> {
    fn register_event(&mut self, event: E) -> Result<(), ()>;

    fn compute_report(&mut self, request: Q::ReportRequest) -> Q::Report;
}