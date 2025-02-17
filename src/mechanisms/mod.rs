/// L1 and L2 norms.
pub enum NormType {
    L1,
    L2, // Unused for now
}

/// Noise scale for the mechanism. Currently only Laplace noise is supported.
pub enum NoiseScale {
    Laplace(f64), // b parameter for Lap(b)
}
