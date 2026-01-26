use pyo3::prelude::*;

use pdslib::budget::traits::FilterStorage;
use pdslib::pds::aliases::{PpaEventStorage, PpaFilterStorage, PpaPds};
use pdslib::pds::quotas::StaticCapacities;

use crate::filter_id::PyFilterId;
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
            filtered_bin_values: report
                .filtered_report
                .bin_values
                .into_iter()
                .collect(),
            unfiltered_bin_values: report
                .unfiltered_report
                .bin_values
                .into_iter()
                .collect(),
            oob_filters: report
                .oob_filters
                .into_iter()
                .map(|inner| PyFilterId { inner })
                .collect(),
        })
    }
}

/// Report returned by Pds containing histogram bin values and budget information.
#[pyclass(name = "PdsReport", unsendable)]
pub struct PyPdsReport {
    /// Histogram bin values after budget filtering is applied.
    /// Epochs that exceeded their budget are excluded.
    #[pyo3(get)]
    pub filtered_bin_values: Vec<(u64, f64)>,

    /// Histogram bin values before budget filtering.
    /// Useful for debugging to see what the report would be without privacy constraints.
    #[pyo3(get)]
    pub unfiltered_bin_values: Vec<(u64, f64)>,

    /// List of filters that were out of budget during the atomic check.
    /// Empty if all filters had sufficient budget.
    #[pyo3(get)]
    pub oob_filters: Vec<PyFilterId>,
}
