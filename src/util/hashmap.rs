// Prevent multiple hash implementations from being enabled
#[cfg(all(feature = "ahash", feature = "foldhash"))]
compile_error!("Features 'ahash' and 'foldhash' are mutually exclusive and cannot be enabled together");

#[cfg(not(any(feature = "ahash", feature = "foldhash")))]
pub use std::collections::{HashMap, HashSet};

#[cfg(feature = "ahash")]
pub use ahash::{AHashMap as HashMap, AHashSet as HashSet};

#[cfg(feature = "foldhash")]
pub use super::foldhash_map::FHashMap as HashMap;
#[cfg(feature = "foldhash")]
pub use super::foldhash_set::FHashSet as HashSet;
