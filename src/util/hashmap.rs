#[cfg(not(feature = "ahash"))]
pub use std::collections::{
    HashSet,
    hash_map::{HashMap, RandomState},
};

#[cfg(feature = "ahash")]
pub use ahash::{AHashMap as HashMap, AHashSet as HashSet, RandomState};
