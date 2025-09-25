use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    ops::Deref,
    rc::Rc,
};

use crate::{events::traits::Uri, util::hashmap::HashSet};

/// A set of URIs, with shared ownership and cheap cloning.
///
/// This is a memory optimization derived from the observation that many
/// events share the same set of URIs, e.g. all the possible sources for
/// a given conversion site, or events having the same trigger and querier
/// URIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriSet<U: Uri> {
    pub uris: Rc<HashSet<U>>,
}

impl<U: Uri> Hash for UriSet<U> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uris.iter().for_each(|uri| uri.hash(state));
    }
}

impl<U: Uri> Deref for UriSet<U> {
    type Target = HashSet<U>;
    fn deref(&self) -> &Self::Target {
        &self.uris
    }
}

impl<U: Uri, I: IntoIterator<Item = U>> From<I> for UriSet<U> {
    fn from(uris: I) -> Self {
        Self {
            uris: Rc::new(uris.into_iter().collect()),
        }
    }
}

impl<'a, U: Uri> IntoIterator for &'a UriSet<U> {
    type Item = &'a U;
    type IntoIter = std::collections::hash_set::Iter<'a, U>;
    fn into_iter(self) -> Self::IntoIter {
        self.uris.iter()
    }
}
