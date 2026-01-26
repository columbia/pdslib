use pyo3::prelude::*;

use pdslib::queries::ppa_histogram::{
    DirectPpaHistogramConfig, PpaHistogramConfig,
};

#[pyclass(name = "PpaHistogramConfig", unsendable)]
#[derive(Clone)]
pub struct PyPpaHistogramConfig {
    pub inner: PpaHistogramConfig,
}

#[pymethods]
impl PyPpaHistogramConfig {
    #[new]
    fn new(
        start_epoch: u64,
        end_epoch: u64,
        attributable_value: f64,
        max_attributable_value: f64,
        requested_epsilon: f64,
        histogram_size: u64,
    ) -> Self {
        PyPpaHistogramConfig {
            inner: PpaHistogramConfig {
                start_epoch,
                end_epoch,
                attributable_value,
                max_attributable_value,
                requested_epsilon,
                histogram_size,
            },
        }
    }
}

#[pyclass(name = "DirectPpaHistogramConfig", unsendable)]
#[derive(Clone)]
pub struct PyDirectPpaHistogramConfig {
    pub inner: DirectPpaHistogramConfig,
}

#[pymethods]
impl PyDirectPpaHistogramConfig {
    #[new]
    fn new(
        start_epoch: u64,
        end_epoch: u64,
        attributable_value: f64,
        laplace_noise_scale: f64,
        histogram_size: u64,
    ) -> Self {
        PyDirectPpaHistogramConfig {
            inner: DirectPpaHistogramConfig {
                start_epoch,
                end_epoch,
                attributable_value,
                laplace_noise_scale,
                histogram_size,
            },
        }
    }
}
