use pyo3::prelude::*;

use pdslib::pds::quotas::FilterId;

#[pyclass(name = "FilterId", unsendable)]
#[derive(Clone)]
pub struct PyFilterId {
    pub inner: FilterId<u64, String>,
}

#[pymethods]
impl PyFilterId {
    #[getter]
    fn filter_type(&self) -> &'static str {
        match &self.inner {
            FilterId::PerQuerier(_, _) => "PerQuerier",
            FilterId::Global(_) => "Global",
            FilterId::TriggerQuota(_, _) => "TriggerQuota",
            FilterId::SourceQuota(_, _) => "SourceQuota",
        }
    }

    #[getter]
    fn epoch_id(&self) -> u64 {
        *self.inner.epoch_id()
    }

    #[getter]
    fn uri(&self) -> Option<String> {
        self.inner.uri().cloned()
    }
}

impl From<FilterId<u64, String>> for PyFilterId {
    fn from(inner: FilterId<u64, String>) -> Self {
        PyFilterId { inner }
    }
}
