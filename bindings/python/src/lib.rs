use pyo3::prelude::*;

use pdslib::events::uri_set::UriSet;
#[pymodule]
fn pdslib_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    Ok(())
}
