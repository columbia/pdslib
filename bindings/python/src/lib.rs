use pyo3::prelude::*;

use pdslib::events::uri_set::UriSet;

#[pyclass(name = "UriSet", unsendable)]
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

#[pymodule]
fn pdslib_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    m.add_class::<PyUriSet>()?;
    Ok(())
}
