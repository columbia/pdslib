#[cfg(not(feature = "fxhash"))]
pub use std::collections::hash_map::RandomState;

#[cfg(feature = "fxhash")]
pub use rustc_hash::FxBuildHasher as RandomState;

pub type HashMap<K, V, S = RandomState> = std::collections::HashMap<K, V, S>;
pub type HashSet<K, S = RandomState> = std::collections::HashSet<K, S>;
