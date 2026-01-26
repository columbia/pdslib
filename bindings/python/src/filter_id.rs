use pyo3::prelude::*;

use pdslib::pds::quotas::FilterId;

#[pyclass(name = "FilterId", unsendable)]
#[derive(Clone)]
pub struct PyFilterId {
    pub inner: FilterId<u64, String>,
}

impl From<FilterId<u64, String>> for PyFilterId {
    fn from(inner: FilterId<u64, String>) -> Self {
        PyFilterId { inner }
    }
}
