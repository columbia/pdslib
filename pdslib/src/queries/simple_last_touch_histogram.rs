pub use crate::queries::traits::Query;

pub struct SimpleLastTouchHistogramQuery;


pub struct SimpleLastTouchHistogramReport {
    // Value attributed to one bin or zero if no attribution
    pub attributed_value: Option<(String, f64)>,
}

// TODO: takes a request, and computes the actual output
pub struct SimpleLastTouchAttribution;

// TODO: relevant events?
#[derive(Debug)]
pub struct SimpleLastTouchAttributionReportRequest {
    pub attributable_value: f64,
}


impl Query for SimpleLastTouchHistogramQuery {
    type Report = SimpleLastTouchHistogramReport;
    type AttributionFunction = SimpleLastTouchAttribution;
    type ReportRequest = SimpleLastTouchAttributionReportRequest;
}
