//! The dynamic [`Element`] model, plus [`Uuid`] and [`Versionstamp`].

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use num_bigint::BigInt;
use num_traits::ToPrimitive;

use crate::tuple::Tuple;

/// A 128-bit UUID, stored in network (big-endian) byte order.
///
/// This is a minimal newtype so the crate does not depend on a UUID library.
/// It sorts by its unsigned byte order, which matches the packed order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// Creates a UUID from its 16 bytes in RFC 4122 (network) order.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the 16 bytes in RFC 4122 (network) order.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns the 16 bytes in RFC 4122 (network) order.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl From<[u8; 16]> for Uuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

/// A 96-bit versionstamp: a 10-byte transaction version followed by a 2-byte
/// user version.
///
/// A versionstamp whose transaction version is ten `0xff` bytes is
/// *incomplete*: it is a placeholder that the database fills in at commit.
/// This is the convention of the other bindings, so [`Versionstamp::incomplete`]
/// and `Versionstamp::complete([0xff; 10], u)` are the same value, and the
/// bytes `33 ff*10 uu uu` always unpack to an incomplete versionstamp.
/// Incomplete versionstamps can only be packed with
/// [`pack_with_versionstamp`](crate::pack_with_versionstamp).
///
/// Ordering is the packed byte order: transaction version, then user version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versionstamp {
    tr_version: [u8; 10],
    user_version: u16,
}

/// Transaction version marking an incomplete versionstamp.
const UNSET_TR_VERSION: [u8; 10] = [0xff; 10];

impl Versionstamp {
    /// Creates a versionstamp from a transaction version and user version.
    ///
    /// A transaction version of ten `0xff` bytes yields an incomplete
    /// versionstamp, identical to [`Versionstamp::incomplete`].
    pub const fn complete(tr_version: [u8; 10], user_version: u16) -> Self {
        Self {
            tr_version,
            user_version,
        }
    }

    /// Creates an incomplete versionstamp with the given user version.
    pub const fn incomplete(user_version: u16) -> Self {
        Self {
            tr_version: UNSET_TR_VERSION,
            user_version,
        }
    }

    /// Returns `true` if the transaction version is known, that is, it is not
    /// ten `0xff` bytes.
    pub fn is_complete(&self) -> bool {
        self.tr_version != UNSET_TR_VERSION
    }

    /// Returns the transaction version, or `None` if incomplete.
    pub fn tr_version(&self) -> Option<&[u8; 10]> {
        self.is_complete().then_some(&self.tr_version)
    }

    /// Returns the user version.
    pub const fn user_version(&self) -> u16 {
        self.user_version
    }

    /// Returns the 12-byte packed form. An incomplete versionstamp has ten
    /// `0xff` bytes in place of the transaction version.
    pub fn to_bytes(&self) -> [u8; 12] {
        let mut out = [0; 12];
        out[..10].copy_from_slice(&self.tr_version);
        out[10..].copy_from_slice(&self.user_version.to_be_bytes());
        out
    }

    /// Creates a versionstamp from its 12-byte packed form. A transaction
    /// version of ten `0xff` bytes gives an incomplete versionstamp.
    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        let mut tr = [0; 10];
        tr.copy_from_slice(&bytes[..10]);
        Self::complete(tr, u16::from_be_bytes([bytes[10], bytes[11]]))
    }
}

/// One element of a tuple.
///
/// Equality, ordering and hashing follow the *packed* representation, so
/// `a.cmp(&b)` always equals the byte-wise comparison of the packed forms:
///
/// * floats and doubles are compared by their encoded (sign-flipped) bits,
///   which is the IEEE total order: `-NaN < -inf < ... < -0.0 < 0.0 < ... <
///   inf < NaN`, with distinct NaN payloads being distinct values;
/// * [`Element::Int`] and [`Element::BigInt`] compare numerically, so an
///   un-normalised `BigInt` equals the corresponding `Int`.
///
/// Elements produced by [`unpack`](crate::unpack) and by the `From`
/// conversions are normalised: integers that fit in `i64` are always
/// [`Element::Int`].
#[derive(Debug, Clone)]
pub enum Element {
    /// The null value (type code `0x00`).
    Null,
    /// A byte string (`0x01`).
    Bytes(Vec<u8>),
    /// A UTF-8 string (`0x02`).
    String(String),
    /// A nested tuple (`0x05`).
    Tuple(Tuple),
    /// An integer that fits in an `i64` (`0x0c..=0x1c`).
    Int(i64),
    /// An integer outside the `i64` range, up to 255 bytes of magnitude
    /// (`0x0c..=0x1c` for up to 8 bytes, `0x0b`/`0x1d` beyond). This includes
    /// the part of the `u64` range above `i64::MAX`.
    BigInt(BigInt),
    /// A 32-bit float (`0x20`). Compared and stored by bit pattern.
    Float(f32),
    /// A 64-bit float (`0x21`). Compared and stored by bit pattern.
    Double(f64),
    /// A boolean (`0x26` / `0x27`).
    Bool(bool),
    /// A UUID (`0x30`).
    Uuid(Uuid),
    /// A 96-bit versionstamp (`0x33`).
    Versionstamp(Versionstamp),
}

impl Element {
    /// Converts an arbitrary-precision integer, using [`Element::Int`] when
    /// the value fits in an `i64`.
    pub fn from_bigint(value: BigInt) -> Self {
        match value.to_i64() {
            Some(v) => Element::Int(v),
            None => Element::BigInt(value),
        }
    }

    /// Returns `true` for [`Element::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Element::Null)
    }

    /// Returns the string value, if this is a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Element::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the byte-string value, if this is a byte string.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Element::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Returns the value as an `i64`, if this is an integer that fits.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Element::Int(v) => Some(*v),
            Element::BigInt(v) => v.to_i64(),
            _ => None,
        }
    }

    /// Returns the nested tuple, if this is one.
    pub fn as_tuple(&self) -> Option<&Tuple> {
        match self {
            Element::Tuple(t) => Some(t),
            _ => None,
        }
    }

    /// Position of this element's type in the packed type-code order.
    fn rank(&self) -> u8 {
        match self {
            Element::Null => 0,
            Element::Bytes(_) => 1,
            Element::String(_) => 2,
            Element::Tuple(_) => 3,
            Element::Int(_) | Element::BigInt(_) => 4,
            Element::Float(_) => 5,
            Element::Double(_) => 6,
            Element::Bool(false) => 7,
            Element::Bool(true) => 8,
            Element::Uuid(_) => 9,
            Element::Versionstamp(_) => 10,
        }
    }
}

/// Order-preserving transform of IEEE bits (the same as the packed form).
fn f32_key(f: f32) -> u32 {
    let bits = f.to_bits();
    if bits >> 31 == 1 {
        !bits
    } else {
        bits | 0x8000_0000
    }
}

/// Order-preserving transform of IEEE bits (the same as the packed form).
fn f64_key(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | 0x8000_0000_0000_0000
    }
}

impl Ord for Element {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Element::Bytes(a), Element::Bytes(b)) => a.cmp(b),
            (Element::String(a), Element::String(b)) => a.cmp(b),
            (Element::Tuple(a), Element::Tuple(b)) => a.cmp(b),
            (Element::Int(a), Element::Int(b)) => a.cmp(b),
            (Element::Int(a), Element::BigInt(b)) => BigInt::from(*a).cmp(b),
            (Element::BigInt(a), Element::Int(b)) => a.cmp(&BigInt::from(*b)),
            (Element::BigInt(a), Element::BigInt(b)) => a.cmp(b),
            (Element::Float(a), Element::Float(b)) => f32_key(*a).cmp(&f32_key(*b)),
            (Element::Double(a), Element::Double(b)) => f64_key(*a).cmp(&f64_key(*b)),
            (Element::Uuid(a), Element::Uuid(b)) => a.cmp(b),
            (Element::Versionstamp(a), Element::Versionstamp(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl PartialOrd for Element {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Element {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Element {}

impl Hash for Element {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rank().hash(state);
        match self {
            Element::Null | Element::Bool(_) => {}
            Element::Bytes(b) => b.hash(state),
            Element::String(s) => s.hash(state),
            Element::Tuple(t) => t.hash(state),
            Element::Int(_) | Element::BigInt(_) => match self.as_i64() {
                Some(v) => v.hash(state),
                None => {
                    if let Element::BigInt(v) = self {
                        v.hash(state);
                    }
                }
            },
            Element::Float(f) => f32_key(*f).hash(state),
            Element::Double(f) => f64_key(*f).hash(state),
            Element::Uuid(u) => u.hash(state),
            Element::Versionstamp(v) => v.hash(state),
        }
    }
}

macro_rules! from_small_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Element {
            fn from(v: $t) -> Self {
                Element::Int(i64::from(v))
            }
        }
    )*};
}
from_small_int!(i8, i16, i32, i64, u8, u16, u32);

macro_rules! from_wide_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Element {
            fn from(v: $t) -> Self {
                match i64::try_from(v) {
                    Ok(v) => Element::Int(v),
                    Err(_) => Element::BigInt(BigInt::from(v)),
                }
            }
        }
    )*};
}
from_wide_int!(u64, i128, u128, usize, isize);

impl From<BigInt> for Element {
    fn from(v: BigInt) -> Self {
        Element::from_bigint(v)
    }
}

impl From<&str> for Element {
    fn from(v: &str) -> Self {
        Element::String(v.to_owned())
    }
}

impl From<String> for Element {
    fn from(v: String) -> Self {
        Element::String(v)
    }
}

impl From<&String> for Element {
    fn from(v: &String) -> Self {
        Element::String(v.clone())
    }
}

impl From<&[u8]> for Element {
    fn from(v: &[u8]) -> Self {
        Element::Bytes(v.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Element {
    fn from(v: &[u8; N]) -> Self {
        Element::Bytes(v.to_vec())
    }
}

impl From<Vec<u8>> for Element {
    fn from(v: Vec<u8>) -> Self {
        Element::Bytes(v)
    }
}

impl From<bool> for Element {
    fn from(v: bool) -> Self {
        Element::Bool(v)
    }
}

impl From<f32> for Element {
    fn from(v: f32) -> Self {
        Element::Float(v)
    }
}

impl From<f64> for Element {
    fn from(v: f64) -> Self {
        Element::Double(v)
    }
}

impl From<Uuid> for Element {
    fn from(v: Uuid) -> Self {
        Element::Uuid(v)
    }
}

impl From<Versionstamp> for Element {
    fn from(v: Versionstamp) -> Self {
        Element::Versionstamp(v)
    }
}

impl From<Tuple> for Element {
    fn from(v: Tuple) -> Self {
        Element::Tuple(v)
    }
}

impl<T: Into<Element>> From<Option<T>> for Element {
    /// `None` converts to [`Element::Null`].
    fn from(v: Option<T>) -> Self {
        v.map_or(Element::Null, Into::into)
    }
}
