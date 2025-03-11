/// Marker trait for URIs.
use std::hash::Hash;
pub trait Uri: Clone + Eq + Hash {}
