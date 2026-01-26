use pyo3::prelude::*;

use pdslib::events::ppa_event::PpaEvent;

use crate::event_uris::PyEventUris;

#[pyclass(name = "PpaEvent", unsendable)]
pub struct PyPpaEvent {
    pub inner: PpaEvent<String>,
}

#[pymethods]
impl PyPpaEvent {
    #[new]
    fn new(
        id: u64,
        timestamp: u64,
        epoch_number: u64,
        histogram_index: u64,
        uris: PyEventUris,
        filter_data: u64,
    ) -> Self {
        PyPpaEvent {
            inner: PpaEvent {
                id,
                timestamp,
                epoch_number,
                histogram_index,
                uris: uris.inner,
                filter_data,
            },
        }
    }
}
