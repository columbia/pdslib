use pyo3::prelude::*;

use pdslib::events::uri_set::UriSet;

#[pyclass(name = "UriSet", unsendable)]
#[derive(Clone)]
pub struct PyUriSet {
    pub inner: UriSet<String>,
}

#[pymethods]
impl PyUriSet {
    #[new]
    fn new(uris: Vec<String>) -> Self {
        PyUriSet { inner: uris.into() }
    }
}
