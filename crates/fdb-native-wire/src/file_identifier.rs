//! File identifiers (`flow/include/flow/FileIdentifier.h`).

use core::fmt;

/// A message type's file identifier: the little-endian `u32` at bytes 4..8 of every
/// message, plus the composition depth that decides how wrappers compose it.
///
/// Plain identifiers are below 2<sup>24</sup> and have depth 0. Wrapping a type in
/// one of the [`Wrapper`]s puts the wrapper's code above the identifier:
///
/// - depth 0: `(code << 24) | id`, depth 1 (`ErrorOr<Inner>` = `0x020fdb0a`);
/// - depth 1: `(code << 28) | id`, depth 2 (`ErrorOr<Optional<Inner>>` = `0x240fdb0a`);
/// - depth 2: no identifier ([`FileIdentifier::compose`] panics, which is a compile
///   error in a constant, like the C++ `static_assert`).
///
/// [`EnsureTable`](crate::EnsureTable) keeps its type's identifier
/// ([`FileIdentifier::ensure_table`]).
///
/// ```
/// use fdb_native_wire::{FileIdentifier, Wrapper};
///
/// const INNER: FileIdentifier = FileIdentifier::new(1039114);
/// const REPLY: FileIdentifier =
///     INNER.compose(Wrapper::Optional).ensure_table().compose(Wrapper::ErrorOr);
/// assert_eq!(REPLY.value(), 0x240f_db0a);
/// assert_eq!(REPLY.composition_depth(), 2);
/// assert_eq!(REPLY.checked_compose(Wrapper::Vector), None);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileIdentifier {
    value: u32,
    depth: u8,
}

/// The wrapper types that compose file identifiers, with their FDB codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Wrapper {
    /// `ReplyPromise<T>` (code 1).
    ReplyPromise = 1,
    /// `ErrorOr<T>` (code 2); [`ErrorOr`](crate::ErrorOr) here.
    ErrorOr = 2,
    /// `VectorRef<T>` (code 3).
    VectorRef = 3,
    /// `Optional<T>` (code 4); `Option<T>` here.
    Optional = 4,
    /// `std::vector<T>` (code 5); `Vec<T>` here.
    Vector = 5,
    /// `std::unordered_set<T>` (code 6).
    UnorderedSet = 6,
}

impl Wrapper {
    /// The wrapper's composition code.
    pub const fn code(self) -> u8 {
        self as u8
    }
}

impl FileIdentifier {
    /// A plain (depth 0) identifier.
    ///
    /// # Panics
    ///
    /// Panics if `value` is not below 2<sup>24</sup>; in a constant that is a compile error.
    pub const fn new(value: u32) -> FileIdentifier {
        match FileIdentifier::checked_new(value) {
            Some(id) => id,
            None => panic!("plain file identifiers must be below 2^24"),
        }
    }

    /// A plain identifier, or `None` if `value` is not below 2<sup>24</sup>.
    pub const fn checked_new(value: u32) -> Option<FileIdentifier> {
        if value < 1 << 24 {
            Some(FileIdentifier { value, depth: 0 })
        } else {
            None
        }
    }

    /// The identifier as written at bytes 4..8 of a message.
    pub const fn value(self) -> u32 {
        self.value
    }

    /// The number of wrappers composed into this identifier (0, 1 or 2).
    pub const fn composition_depth(self) -> u8 {
        self.depth
    }

    /// The identifier of `wrapper<T>` where `T` has this identifier.
    ///
    /// # Panics
    ///
    /// Panics where [`FileIdentifier::checked_compose`] returns `None`; in a constant that
    /// is a compile error.
    pub const fn compose(self, wrapper: Wrapper) -> FileIdentifier {
        match self.checked_compose(wrapper) {
            Some(id) => id,
            None => {
                panic!("types with more than two levels of composition have no file identifier")
            }
        }
    }

    /// The identifier of `wrapper<T>` where `T` has this identifier, or `None` if `T` is
    /// already two wrappers deep (or its identifier does not fit under the wrapper code).
    pub const fn checked_compose(self, wrapper: Wrapper) -> Option<FileIdentifier> {
        let shift = match self.depth {
            0 => 24,
            1 => 28,
            _ => return None,
        };
        if self.value >= 1 << shift {
            return None;
        }
        Some(FileIdentifier {
            value: ((wrapper.code() as u32) << shift) | self.value,
            depth: self.depth + 1,
        })
    }

    /// The identifier of `EnsureTable<T>` where `T` has this identifier: the same value.
    /// FDB declares `EnsureTable`'s composition depth as a `bool`, so a depth of 2 reads
    /// as 1 (and then fails to compose, since the value does not fit under a code).
    pub const fn ensure_table(self) -> FileIdentifier {
        FileIdentifier {
            value: self.value,
            depth: if self.depth > 0 { 1 } else { 0 },
        }
    }
}

impl fmt::Debug for FileIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileIdentifier({:#010x}, depth {})",
            self.value, self.depth
        )
    }
}

impl fmt::Display for FileIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#010x}", self.value)
    }
}

/// A type with a file identifier: a type that can be the root of a message passed to
/// [`encode`](crate::encode) and [`decode`](crate::decode).
///
/// Wrapper impls compose: `Option<T>`, `Vec<T>` and [`ErrorOr<T>`](crate::ErrorOr) compose
/// `T`'s identifier with [`Wrapper::Optional`], [`Wrapper::Vector`] and
/// [`Wrapper::ErrorOr`], and [`EnsureTable<T>`](crate::EnsureTable) passes it through.
pub trait FileIdentified {
    /// This type's identifier.
    const FILE_IDENTIFIER: FileIdentifier;
}

impl<T: FileIdentified + ?Sized> FileIdentified for &T {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER;
}

impl<T: FileIdentified + ?Sized> FileIdentified for Box<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER;
}

impl<T: FileIdentified> FileIdentified for Option<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::Optional);
}

impl<T: FileIdentified> FileIdentified for Vec<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::Vector);
}

#[cfg(test)]
mod tests {
    use super::*;

    const INNER: FileIdentifier = FileIdentifier::new(0x000f_db0a);

    #[test]
    fn composition_matches_fixtures() {
        assert_eq!(INNER.compose(Wrapper::ReplyPromise).value(), 0x010f_db0a);
        assert_eq!(INNER.compose(Wrapper::ErrorOr).value(), 0x020f_db0a);
        assert_eq!(INNER.compose(Wrapper::VectorRef).value(), 0x030f_db0a);
        assert_eq!(INNER.compose(Wrapper::Optional).value(), 0x040f_db0a);
        assert_eq!(INNER.compose(Wrapper::Vector).value(), 0x050f_db0a);
        assert_eq!(
            FileIdentifier::new(13300811)
                .compose(Wrapper::VectorRef)
                .value(),
            0x03ca_f44b
        );
        assert_eq!(
            FileIdentifier::new(2010442)
                .ensure_table()
                .compose(Wrapper::ErrorOr)
                .value(),
            0x021e_ad4a
        );
        let two = INNER
            .compose(Wrapper::Optional)
            .ensure_table()
            .compose(Wrapper::ErrorOr);
        assert_eq!(two.value(), 0x240f_db0a);
        assert_eq!(two.composition_depth(), 2);
        assert_eq!(two.checked_compose(Wrapper::ErrorOr), None);
        assert_eq!(two.ensure_table().composition_depth(), 1);
        assert_eq!(two.ensure_table().checked_compose(Wrapper::ErrorOr), None);
        assert_eq!(FileIdentifier::checked_new(1 << 24), None);
    }
}
