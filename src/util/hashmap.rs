#[cfg(not(feature = "ahash"))]
pub use std::collections::{HashMap, HashSet};

#[cfg(feature = "ahash")]
pub use ahash::{AHashMap as HashMap, AHashSet as HashSet};
