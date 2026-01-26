use pyo3::prelude::*;

use pdslib::queries::ppa_histogram::{
    PpaRelevantEventSelector, RequestedBuckets,
};
use pdslib::queries::traits::ReportRequestUris;

use crate::report_request_uris::PyReportRequestUris;
use crate::requested_buckets::PyRequestedBuckets;

// Stores the parameters needed to build a PpaRelevantEventSelector.
//
// The Rust struct has `is_matching_event: Box<dyn Fn(u64) -> bool>` for flexible
// event filtering. We simplify this to `filter_data: Option<u64>`:
// - `None` = match all events (equivalent to `|_| true`)
// - `Some(x)` = match events where filter_data == x
//
// This covers the common use cases without GIL overhead from Python callables.
// A callable can be added later if complex matching is needed.
#[pyclass(name = "PpaRelevantEventSelector", unsendable)]
#[derive(Clone)]
pub struct PyPpaRelevantEventSelector {
    pub report_request_uris: ReportRequestUris<String>,
    pub filter_data: Option<u64>,
    pub requested_buckets: RequestedBuckets<u64>,
}

impl PyPpaRelevantEventSelector {
    pub fn into_inner(self) -> PpaRelevantEventSelector<String> {
        let is_matching_event: Box<dyn Fn(u64) -> bool> = match self.filter_data
        {
            Some(fd) => {
                Box::new(move |event_filter_data| event_filter_data == fd)
            }
            None => Box::new(|_| true),
        };

        PpaRelevantEventSelector {
            report_request_uris: self.report_request_uris,
            is_matching_event,
            requested_buckets: self.requested_buckets,
        }
    }
}

#[pymethods]
impl PyPpaRelevantEventSelector {
    #[new]
    #[pyo3(signature = (report_request_uris, filter_data, requested_buckets))]
    fn new(
        report_request_uris: PyReportRequestUris,
        filter_data: Option<u64>,
        requested_buckets: PyRequestedBuckets,
    ) -> Self {
        PyPpaRelevantEventSelector {
            report_request_uris: report_request_uris.inner,
            filter_data,
            requested_buckets: requested_buckets.inner,
        }
    }
}
