//! This file is adapted from ahash/hash_set.rs to use foldhash instead of
//! ahash.

use std::{
    collections::{hash_set, HashSet},
    fmt::{self, Debug},
    hash::{BuildHasher, Hash},
    iter::FromIterator,
    ops::{BitAnd, BitOr, BitXor, Deref, DerefMut, Sub},
};

use foldhash::fast::RandomState;
use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};

/// A [`HashSet`](std::collections::HashSet) using
/// [`RandomState`](crate::RandomState) to hash the items. (Requires the `std`
/// feature to be enabled.)
#[derive(Clone)]
pub struct FHashSet<T, S = RandomState>(HashSet<T, S>);

impl<T> From<HashSet<T, RandomState>> for FHashSet<T> {
    fn from(item: HashSet<T, RandomState>) -> Self {
        FHashSet(item)
    }
}

impl<T, const N: usize> From<[T; N]> for FHashSet<T>
where
    T: Eq + Hash,
{
    fn from(arr: [T; N]) -> Self {
        Self::from_iter(arr)
    }
}

impl<T> Into<HashSet<T, RandomState>> for FHashSet<T> {
    fn into(self) -> HashSet<T, RandomState> {
        self.0
    }
}

impl<T> FHashSet<T, RandomState> {
    /// This creates a hashset using [RandomState::default].
    /// See the documentation in [RandomSource] for notes about key strength.
    pub fn new() -> Self {
        FHashSet(HashSet::with_hasher(RandomState::default()))
    }

    /// This creates a hashset with the specified capacity using
    /// [RandomState::default]. See the documentation in [RandomSource] for
    /// notes about key strength.
    pub fn with_capacity(capacity: usize) -> Self {
        FHashSet(HashSet::with_capacity_and_hasher(
            capacity,
            RandomState::default(),
        ))
    }
}

impl<T, S> FHashSet<T, S>
where
    S: BuildHasher,
{
    pub fn with_hasher(hash_builder: S) -> Self {
        FHashSet(HashSet::with_hasher(hash_builder))
    }

    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        FHashSet(HashSet::with_capacity_and_hasher(capacity, hash_builder))
    }
}

impl<T, S> Deref for FHashSet<T, S> {
    type Target = HashSet<T, S>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, S> DerefMut for FHashSet<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T, S> PartialEq for FHashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    fn eq(&self, other: &FHashSet<T, S>) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T, S> Eq for FHashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
}

impl<T, S> BitOr<&FHashSet<T, S>> for &FHashSet<T, S>
where
    T: Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type Output = FHashSet<T, S>;

    fn bitor(self, rhs: &FHashSet<T, S>) -> FHashSet<T, S> {
        FHashSet(self.0.bitor(&rhs.0))
    }
}

impl<T, S> BitAnd<&FHashSet<T, S>> for &FHashSet<T, S>
where
    T: Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type Output = FHashSet<T, S>;

    fn bitand(self, rhs: &FHashSet<T, S>) -> FHashSet<T, S> {
        FHashSet(self.0.bitand(&rhs.0))
    }
}

impl<T, S> BitXor<&FHashSet<T, S>> for &FHashSet<T, S>
where
    T: Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type Output = FHashSet<T, S>;

    fn bitxor(self, rhs: &FHashSet<T, S>) -> FHashSet<T, S> {
        FHashSet(self.0.bitxor(&rhs.0))
    }
}

impl<T, S> Sub<&FHashSet<T, S>> for &FHashSet<T, S>
where
    T: Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type Output = FHashSet<T, S>;

    fn sub(self, rhs: &FHashSet<T, S>) -> FHashSet<T, S> {
        FHashSet(self.0.sub(&rhs.0))
    }
}

impl<T, S> Debug for FHashSet<T, S>
where
    T: Debug,
    S: BuildHasher,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl<T> FromIterator<T> for FHashSet<T, RandomState>
where
    T: Eq + Hash,
{
    /// This creates a hashset from the provided iterator using
    /// [RandomState::default]. See the documentation in [RandomSource] for
    /// notes about key strength.
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> FHashSet<T> {
        let mut inner = HashSet::with_hasher(RandomState::default());
        inner.extend(iter);
        FHashSet(inner)
    }
}

impl<'a, T, S> IntoIterator for &'a FHashSet<T, S> {
    type Item = &'a T;
    type IntoIter = hash_set::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        (&self.0).iter()
    }
}

impl<T, S> IntoIterator for FHashSet<T, S> {
    type Item = T;
    type IntoIter = hash_set::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T, S> Extend<T> for FHashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<'a, T, S> Extend<&'a T> for FHashSet<T, S>
where
    T: 'a + Eq + Hash + Copy,
    S: BuildHasher,
{
    #[inline]
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<T> Default for FHashSet<T, RandomState> {
    /// Creates an empty `FHashSet<T, S>` with the `Default` value for the
    /// hasher.
    #[inline]
    fn default() -> FHashSet<T, RandomState> {
        FHashSet(HashSet::default())
    }
}

impl<T> Serialize for FHashSet<T>
where
    T: Serialize + Eq + Hash,
{
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        self.deref().serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for FHashSet<T>
where
    T: Deserialize<'de> + Eq + Hash,
{
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let hash_set = HashSet::deserialize(deserializer);
        hash_set.map(|hash_set| Self(hash_set))
    }

    fn deserialize_in_place<D: Deserializer<'de>>(
        deserializer: D,
        place: &mut Self,
    ) -> Result<(), D::Error> {
        HashSet::deserialize_in_place(deserializer, place)
    }
}
