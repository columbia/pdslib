use pyo3::prelude::*;

#[pymodule]
fn pdslib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    Ok(())
}
