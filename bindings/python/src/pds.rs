use pyo3::prelude::*;

use pdslib::budget::traits::FilterStorage;
use pdslib::pds::aliases::{PpaEventStorage, PpaFilterStorage, PpaPds};
use pdslib::pds::quotas::StaticCapacities;

use crate::ppa_event::PyPpaEvent;
use crate::ppa_histogram_request::PyPpaHistogramRequest;

#[pyclass(name = "Pds", unsendable)]
pub struct PyPds {
    inner: PpaPds,
}

#[pymethods]
impl PyPds {
    #[new]
    fn new(
        per_querier_budget: f64,
        global_budget: f64,
        trigger_quota: f64,
        source_quota: f64,
    ) -> PyResult<Self> {
        let capacities = StaticCapacities::new(
            per_querier_budget.into(),
            global_budget.into(),
            trigger_quota.into(),
            source_quota.into(),
        );

        let filters = PpaFilterStorage::new(capacities).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })?;

        let events = PpaEventStorage::new();

        Ok(PyPds {
            inner: PpaPds::new(filters, events),
        })
    }

    fn register_event(&mut self, event: PyPpaEvent) -> PyResult<()> {
        self.inner.register_event(event.inner).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn compute_report(
        &mut self,
        request: &PyPpaHistogramRequest,
    ) -> PyResult<PyPdsReport> {
        let report =
            self.inner.compute_report(&request.inner).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
            })?;

        Ok(PyPdsReport {
            bin_values: report
                .filtered_report
                .bin_values
                .into_iter()
                .collect(),
        })
    }
}

/// Report returned by Pds containing histogram bin values.
#[pyclass(name = "PdsReport", unsendable)]
pub struct PyPdsReport {
    /// Histogram bin values after budget filtering is applied.
    /// Epochs that exceeded their budget are excluded.
    #[pyo3(get)]
    pub bin_values: Vec<(u64, f64)>,
}
