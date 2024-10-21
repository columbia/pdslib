use crate::queries::traits::Query;

pub trait PrivateDataService<Event, Q: Query>
where 
{
    fn register_event(&mut self, event: Event) -> Result<(), ()>;

    fn compute_report(&mut self, request: Q::ReportRequest) -> Q::Report;
}