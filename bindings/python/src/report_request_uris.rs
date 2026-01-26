use pyo3::prelude::*;

use pdslib::queries::traits::ReportRequestUris;

use crate::uri_set::PyUriSet;

#[pyclass(name = "ReportRequestUris", unsendable)]
pub struct PyReportRequestUris {
    pub inner: ReportRequestUris<String>,
}

#[pymethods]
impl PyReportRequestUris {
    #[new]
    fn new(
        trigger_uri: String,
        source_uris: PyUriSet,
        querier_uris: PyUriSet,
    ) -> Self {
        PyReportRequestUris {
            inner: ReportRequestUris {
                trigger_uri,
                source_uris: source_uris.inner,
                querier_uris: querier_uris.inner,
            },
        }
    }
}
