//! FoundationDB tuple and subspace layers.
//!
//! The tuple layer encodes ordered sequences of typed values into byte strings
//! whose lexicographic order matches the order of the tuples, which makes them
//! suitable as FoundationDB keys. The encoding follows the FoundationDB tuple
//! type-code specification and is byte-compatible with the other language
//! bindings.
//!
//! This crate is pure Rust, synchronous and performs no I/O.
//!
//! # Elements and tuples
//!
//! A [`Tuple`] is a sequence of dynamically typed [`Element`]s: null, byte
//! strings, unicode strings, nested tuples, integers (arbitrary precision up
//! to 255 bytes), 32 and 64-bit floats, booleans, UUIDs and 96-bit
//! versionstamps. Common Rust values convert with `Into<Element>`, and the
//! [`tuple!`] macro or Rust tuples can be used for convenience.
//!
//! ```
//! use fdb_native_tuple::{pack, unpack, tuple, Element};
//!
//! let key = pack(&("user", 42, true)).unwrap();
//! assert_eq!(key, b"\x02user\x00\x15\x2a\x27");
//!
//! let t = unpack(&key).unwrap();
//! assert_eq!(t, tuple!["user", 42, true]);
//! assert_eq!(t[1], Element::Int(42));
//! ```
//!
//! # Nulls and nesting
//!
//! Byte and unicode strings escape `0x00` as `0x00 0xff`. Inside a nested
//! tuple a null element is encoded as `0x00 0xff`, so the nested tuple's
//! terminating `0x00` is unambiguous:
//!
//! ```
//! use fdb_native_tuple::{pack, tuple, Element, Tuple};
//!
//! let nested = Tuple::from_elements(vec![Element::Null, Element::Tuple(Tuple::new())]);
//! let key = pack(&tuple![Element::Tuple(nested)]).unwrap();
//! assert_eq!(key, b"\x05\x00\xff\x05\x00\x00");
//! ```
//!
//! # Ordering
//!
//! `Element` and `Tuple` implement `Ord` such that comparing two values gives
//! the same result as comparing their packed bytes. Floats are ordered by
//! their encoded form (the IEEE total order, with `-0.0 < 0.0` and NaNs
//! ordered by sign and payload).
//!
//! ```
//! use fdb_native_tuple::{pack, tuple};
//!
//! let a = tuple![-1, "b"];
//! let b = tuple![0, "a"];
//! assert!(a < b);
//! assert!(pack(&a).unwrap() < pack(&b).unwrap());
//! ```
//!
//! # Large integers
//!
//! `i64` values use the compact integer codes. Everything else, including the
//! upper half of the `u64` range, is [`Element::BigInt`]; values that fit in
//! an `i64` are always normalised to [`Element::Int`].
//!
//! ```
//! use fdb_native_tuple::{pack, unpack, Element};
//!
//! let big = Element::from(u64::MAX);
//! let packed = pack(&(big.clone(),)).unwrap();
//! assert_eq!(packed, b"\x1c\xff\xff\xff\xff\xff\xff\xff\xff");
//! assert_eq!(unpack(&packed).unwrap()[0], big);
//! ```
//!
//! # Versionstamps
//!
//! A [`Versionstamp`] whose 10-byte transaction version is all `0xff` is
//! *incomplete*, exactly as in the Python, Java and Go bindings. A tuple with
//! exactly one incomplete versionstamp can be packed with
//! [`pack_with_versionstamp`]; the result ends with a 4-byte little-endian
//! offset of the stamp, ready for `SET_VERSIONSTAMPED_KEY`. Plain [`pack`]
//! rejects incomplete versionstamps, and [`unpack`] returns bytes containing
//! an all-`0xff` transaction version as an incomplete versionstamp, so the
//! output of `pack_with_versionstamp` minus its 4-byte suffix unpacks to the
//! original tuple.
//!
//! Only the 4-byte offset used from API version 520 is supported; the 2-byte
//! offset of older API versions is not. A client built on this crate (for
//! example the stack tester) must therefore use a minimum API version of 520.
//!
//! ```
//! use fdb_native_tuple::{pack_with_versionstamp, Versionstamp};
//!
//! let key = pack_with_versionstamp(&("log", Versionstamp::incomplete(7))).unwrap();
//! // 0x02 "log" 0x00, then 0x33 and the stamp at offset 6.
//! assert_eq!(&key[key.len() - 4..], &[6, 0, 0, 0]);
//! ```
//!
//! # Subspaces
//!
//! A [`Subspace`] prepends a fixed prefix to packed tuples and can unpack keys
//! it contains and produce key ranges.
//!
//! ```
//! use fdb_native_tuple::{Subspace, tuple};
//!
//! let events = Subspace::from_tuple(&("events",)).unwrap();
//! let key = events.pack(&(2024, "login")).unwrap();
//! assert_eq!(events.unpack(&key).unwrap(), tuple![2024, "login"]);
//!
//! // All keys that begin with ("events", 2024, ...):
//! let (begin, end) = events.range(&(2024,)).unwrap();
//! assert!(begin < key && key < end);
//! ```
//!
//! # Errors and strictness
//!
//! [`unpack`] is strict and returns [`Error`] on truncated or malformed input;
//! no function in this crate panics on any input. Unterminated strings, byte
//! strings and nested tuples are errors, which matches the Java binding; the
//! Python binding is more lenient and treats end of input as the terminator.
//! Strings must be valid UTF-8 (Python also decodes strictly). Integer
//! decoding accepts non-minimal encodings and normalises the value.
//!
//! # Differences from other bindings
//!
//! * `u64::MAX` and `-(2^64 - 1)` are packed with the 8-byte integer codes
//!   (`1c ff*8` and `0c 00*8`), as the specification, Java and Go do. Python
//!   packs exactly these two values with the long forms `1d 08 ff*8` and
//!   `0b f7 00*8`. Decoding accepts both.
//! * Nesting is limited to [`MAX_DEPTH`] (128) levels on both pack and unpack
//!   to keep recursion bounded; Python can nest deeper.
//! * Only the API version 520 and later versionstamp offset is supported (see
//!   above).

mod decode;
mod element;
mod encode;
mod error;
mod subspace;
mod tuple;

pub use element::{Element, Uuid, Versionstamp};
pub use error::{Error, Result};
pub use subspace::Subspace;
pub use tuple::{Tuple, TuplePack};

/// Maximum nesting depth of tuples within tuples (128), for both pack and unpack.
///
/// This is a deliberate limit that bounds recursion; the Python binding can
/// nest deeper.
pub const MAX_DEPTH: usize = 128;

/// Packs a tuple into a key.
///
/// ```
/// use fdb_native_tuple::pack;
///
/// // Test vector from the tuple layer specification.
/// assert_eq!(pack(&(-5551212,)).unwrap(), b"\x11\xabK\x93");
/// assert_eq!(pack(&(b"foo\x00bar",)).unwrap(), b"\x01foo\x00\xffbar\x00");
/// ```
///
/// # Errors
///
/// Fails with [`Error::IncompleteVersionstamp`] if the tuple contains an
/// incomplete versionstamp, [`Error::IntegerTooLarge`] for an integer wider
/// than 255 bytes, and [`Error::NestingTooDeep`].
pub fn pack<T: TuplePack + ?Sized>(tuple: &T) -> Result<Vec<u8>> {
    tuple::pack_prefixed(&[], &tuple.to_elements())
}

/// Packs a tuple containing exactly one incomplete versionstamp.
///
/// The returned bytes are the packed tuple followed by the 4-byte
/// little-endian position of the versionstamp's 10-byte transaction version
/// within the packed bytes, as expected by `SET_VERSIONSTAMPED_KEY` and
/// `SET_VERSIONSTAMPED_VALUE` from API version 520.
///
/// # Errors
///
/// [`Error::NoIncompleteVersionstamp`] or
/// [`Error::MultipleIncompleteVersionstamps`] unless there is exactly one
/// incomplete versionstamp.
pub fn pack_with_versionstamp<T: TuplePack + ?Sized>(tuple: &T) -> Result<Vec<u8>> {
    tuple::pack_prefixed_with_versionstamp(&[], &tuple.to_elements())
}

/// Unpacks a key into a [`Tuple`].
///
/// ```
/// use fdb_native_tuple::{unpack, tuple};
///
/// assert_eq!(unpack(b"\x02a\x00\x14").unwrap(), tuple!["a", 0]);
/// assert!(unpack(b"\x02a").is_err()); // unterminated string
/// ```
///
/// # Errors
///
/// Returns an [`Error`] for truncated data, unknown type codes, invalid UTF-8
/// in strings, or excessive nesting. Never panics.
pub fn unpack(bytes: &[u8]) -> Result<Tuple> {
    decode::decode(bytes)
}

/// Returns the key range `(begin, end)` covering all tuples that have `tuple`
/// as a strict prefix: `pack(tuple) + 0x00` up to `pack(tuple) + 0xff`.
///
/// ```
/// use fdb_native_tuple::range;
///
/// let (begin, end) = range(&("a",)).unwrap();
/// assert_eq!(begin, b"\x02a\x00\x00");
/// assert_eq!(end, b"\x02a\x00\xff");
/// ```
///
/// # Errors
///
/// As [`pack`].
pub fn range<T: TuplePack + ?Sized>(tuple: &T) -> Result<(Vec<u8>, Vec<u8>)> {
    Ok(subspace::range_of(pack(tuple)?))
}
