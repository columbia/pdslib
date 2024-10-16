// TODO: traits for attribution fn maybe?

pub trait Query {
    type AttributionFunction;
    type Report;
    type ReportRequest;
}