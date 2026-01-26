use pyo3::prelude::*;

mod event_uris;
mod filter_id;
mod pds;
mod ppa_event;
mod ppa_histogram_config;
mod ppa_histogram_request;
mod ppa_relevant_event_selector;
mod report_request_uris;
mod requested_buckets;
mod uri_set;

use event_uris::PyEventUris;
use pds::{PyPds, PyPdsReport};
use ppa_event::PyPpaEvent;
use ppa_histogram_config::{PyDirectPpaHistogramConfig, PyPpaHistogramConfig};
use ppa_histogram_request::PyPpaHistogramRequest;
use ppa_relevant_event_selector::PyPpaRelevantEventSelector;
use report_request_uris::PyReportRequestUris;
use requested_buckets::PyRequestedBuckets;
use uri_set::PyUriSet;

#[pymodule]
fn pdslib_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    m.add_class::<PyUriSet>()?;
    m.add_class::<PyEventUris>()?;
    m.add_class::<PyReportRequestUris>()?;
    m.add_class::<PyPpaEvent>()?;
    m.add_class::<PyPpaHistogramConfig>()?;
    m.add_class::<PyDirectPpaHistogramConfig>()?;
    m.add_class::<PyRequestedBuckets>()?;
    m.add_class::<PyPpaRelevantEventSelector>()?;
    m.add_class::<PyPpaHistogramRequest>()?;
    m.add_class::<PyPds>()?;
    m.add_class::<PyPdsReport>()?;
    Ok(())
}
