use pyo3::prelude::*;

use pdslib::queries::ppa_histogram::PpaHistogramRequest;

use crate::ppa_histogram_config::{PyDirectPpaHistogramConfig, PyPpaHistogramConfig};
use crate::ppa_relevant_event_selector::PyPpaRelevantEventSelector;

#[pyclass(name = "PpaHistogramRequest", unsendable)]
pub struct PyPpaHistogramRequest {
    pub inner: PpaHistogramRequest<String>,
}

#[pymethods]
impl PyPpaHistogramRequest {
    #[staticmethod]
    fn new_direct(
        config: PyDirectPpaHistogramConfig,
        relevant_event_selector: PyPpaRelevantEventSelector,
    ) -> PyResult<Self> {
        let request = PpaHistogramRequest::new_direct(
            config.inner,
            relevant_event_selector.into_inner(),
        )
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;

        Ok(PyPpaHistogramRequest { inner: request })
    }

    #[staticmethod]
    fn new(
        config: PyPpaHistogramConfig,
        relevant_event_selector: PyPpaRelevantEventSelector,
    ) -> PyResult<Self> {
        let request = PpaHistogramRequest::new(
            &config.inner,
            relevant_event_selector.into_inner(),
        )
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;

        Ok(PyPpaHistogramRequest { inner: request })
    }
}
