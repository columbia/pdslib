use crate::queries::traits::ReportRequest;

pub trait PrivateDataService {
    type Event;
    // type ReportRequest;
    // type Report; // TODO: is this going to be a union type over possible reports? Sounds pretty resonable. Or trait with dynamic dispatch?

    fn register_event(&mut self, event: Self::Event) -> Result<(), ()>;

    // TODO: where do we restrict the list of supported query types? Maybe allow them all to run for now, an return null reports if not supported?
    fn compute_report<R: ReportRequest>(&mut self, request: R) -> R::Report;
}
