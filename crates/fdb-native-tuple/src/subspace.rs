//! Key subspaces: a raw byte prefix under which tuples are packed.

use crate::decode::decode;
use crate::error::{Error, Result};
use crate::tuple::{Tuple, TuplePack, pack_prefixed, pack_prefixed_with_versionstamp};

/// A region of the keyspace defined by a byte prefix.
///
/// Keys inside the subspace are the prefix followed by a packed tuple.
///
/// ```
/// use fdb_native_tuple::{Subspace, tuple};
///
/// let users = Subspace::from_tuple(&("app", "users")).unwrap();
/// let key = users.pack(&(42, "name")).unwrap();
/// assert!(users.contains(&key));
/// assert_eq!(users.unpack(&key).unwrap(), tuple![42, "name"]);
///
/// let profiles = users.subspace(&(42,)).unwrap();
/// assert_eq!(profiles.pack(&("name",)).unwrap(), users.pack(&(42, "name")).unwrap());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Subspace {
    prefix: Vec<u8>,
}

impl Subspace {
    /// Creates a subspace from a raw byte prefix.
    pub fn new(prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Creates a subspace whose prefix is the packed form of `tuple`.
    ///
    /// # Errors
    ///
    /// Fails if the tuple cannot be packed, for example if it contains an
    /// incomplete versionstamp.
    pub fn from_tuple<T: TuplePack + ?Sized>(tuple: &T) -> Result<Self> {
        Ok(Self {
            prefix: pack_prefixed(&[], &tuple.to_elements())?,
        })
    }

    /// Returns the raw prefix bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.prefix
    }

    /// Returns a nested subspace whose prefix is this prefix followed by the
    /// packed `tuple`.
    ///
    /// # Errors
    ///
    /// Fails if the tuple cannot be packed.
    pub fn subspace<T: TuplePack + ?Sized>(&self, tuple: &T) -> Result<Self> {
        Ok(Self {
            prefix: pack_prefixed(&self.prefix, &tuple.to_elements())?,
        })
    }

    /// Packs `tuple` into a key inside this subspace.
    ///
    /// # Errors
    ///
    /// Fails if the tuple cannot be packed, for example if it contains an
    /// incomplete versionstamp.
    pub fn pack<T: TuplePack + ?Sized>(&self, tuple: &T) -> Result<Vec<u8>> {
        pack_prefixed(&self.prefix, &tuple.to_elements())
    }

    /// Packs a tuple containing exactly one incomplete versionstamp into a
    /// key inside this subspace, for use with `SET_VERSIONSTAMPED_KEY`.
    ///
    /// The trailing 4-byte little-endian offset points at the versionstamp's
    /// transaction version and includes the subspace prefix.
    ///
    /// # Errors
    ///
    /// Fails unless the tuple contains exactly one incomplete versionstamp.
    pub fn pack_with_versionstamp<T: TuplePack + ?Sized>(&self, tuple: &T) -> Result<Vec<u8>> {
        pack_prefixed_with_versionstamp(&self.prefix, &tuple.to_elements())
    }

    /// Unpacks a key inside this subspace into its tuple.
    ///
    /// # Errors
    ///
    /// [`Error::KeyNotInSubspace`] if `key` does not start with the prefix;
    /// otherwise any decoding error from [`unpack`](crate::unpack).
    pub fn unpack(&self, key: &[u8]) -> Result<Tuple> {
        let rest = key
            .strip_prefix(self.prefix.as_slice())
            .ok_or(Error::KeyNotInSubspace)?;
        decode(rest)
    }

    /// Returns `true` if `key` starts with this subspace's prefix.
    pub fn contains(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }

    /// Returns the half-open key range `[begin, end)` of all keys in this
    /// subspace that extend `tuple`: `prefix + pack(tuple) + 0x00` up to
    /// `prefix + pack(tuple) + 0xff`.
    ///
    /// # Errors
    ///
    /// Fails if the tuple cannot be packed.
    pub fn range<T: TuplePack + ?Sized>(&self, tuple: &T) -> Result<(Vec<u8>, Vec<u8>)> {
        let packed = pack_prefixed(&self.prefix, &tuple.to_elements())?;
        Ok(range_of(packed))
    }

    /// Returns the key range of the whole subspace (the range of the empty tuple).
    pub fn range_all(&self) -> (Vec<u8>, Vec<u8>) {
        range_of(self.prefix.clone())
    }
}

pub(crate) fn range_of(packed: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let mut begin = packed.clone();
    begin.push(0x00);
    let mut end = packed;
    end.push(0xff);
    (begin, end)
}
