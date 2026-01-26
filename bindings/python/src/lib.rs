use pyo3::prelude::*;

use pdslib::events::traits::EventUris;
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

#[pyclass(name = "EventUris", unsendable)]
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

#[pymodule]
fn pdslib_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    m.add_class::<PyUriSet>()?;
    m.add_class::<PyEventUris>()?;
    Ok(())
}
