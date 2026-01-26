use pyo3::prelude::*;

mod event_uris;
mod report_request_uris;
mod uri_set;

use event_uris::PyEventUris;
use report_request_uris::PyReportRequestUris;
use uri_set::PyUriSet;

#[pymodule]
fn pdslib_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    m.add_class::<PyUriSet>()?;
    m.add_class::<PyEventUris>()?;
    m.add_class::<PyReportRequestUris>()?;
    Ok(())
}
