pub mod hashmap;
pub mod tests;

#[cfg(feature = "foldhash")]
mod foldhash_map;
#[cfg(feature = "foldhash")]
mod foldhash_set;
