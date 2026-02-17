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
    ) -> anyhow::Result<Self> {
        let capacities = StaticCapacities::new(
            per_querier_budget.into(),
            global_budget.into(),
            trigger_quota.into(),
            source_quota.into(),
        );

        let filters = PpaFilterStorage::new(capacities)?;
        let events = PpaEventStorage::new();

        Ok(PyPds {
            inner: PpaPds::new(filters, events),
        })
    }

    fn register_event(&mut self, event: PyPpaEvent) -> anyhow::Result<()> {
        self.inner.register_event(event.inner)?;
        Ok(())
    }

    fn compute_report(
        &mut self,
        request: &PyPpaHistogramRequest,
    ) -> anyhow::Result<PyPdsReport> {
        let report = self.inner.compute_report(&request.inner)?;

        Ok(PyPdsReport {
            bin_values: report
                .filtered_report
                .bin_values
                .into_iter()
                .collect(),
            #[cfg(feature = "experimental")]
            unfiltered_bin_values: report
                .unfiltered_report
                .bin_values
                .into_iter()
                .collect(),
        })
    }
}

#[pyclass(name = "PdsReport", unsendable)]
pub struct PyPdsReport {
    #[pyo3(get)]
    pub bin_values: Vec<(u64, f64)>,

    #[cfg(feature = "experimental")]
    #[pyo3(get)]
    pub unfiltered_bin_values: Vec<(u64, f64)>,
}
