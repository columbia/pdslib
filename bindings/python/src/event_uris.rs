use pyo3::prelude::*;

use pdslib::events::traits::EventUris;

use crate::uri_set::PyUriSet;

#[pyclass(name = "EventUris", unsendable)]
#[derive(Clone)]
pub struct PyEventUris {
    pub inner: EventUris<String>,
}

#[pymethods]
impl PyEventUris {
    #[new]
    fn new(
        source_uri: String,
        trigger_uris: PyUriSet,
        querier_uris: PyUriSet,
    ) -> Self {
        PyEventUris {
            inner: EventUris {
                source_uri,
                trigger_uris: trigger_uris.inner,
                querier_uris: querier_uris.inner,
            },
        }
    }
}
