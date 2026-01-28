use pyo3::prelude::*;

use pdslib::queries::ppa_histogram::RequestedBuckets;

#[pyclass(name = "RequestedBuckets", unsendable)]
#[derive(Clone)]
pub struct PyRequestedBuckets {
    pub inner: RequestedBuckets<u64>,
}

#[pymethods]
impl PyRequestedBuckets {
    #[staticmethod]
    fn all_buckets() -> Self {
        PyRequestedBuckets {
            inner: RequestedBuckets::AllBuckets,
        }
    }

    #[staticmethod]
    fn specific_buckets(buckets: Vec<u64>) -> Self {
        PyRequestedBuckets {
            inner: buckets.into(),
        }
    }
}
