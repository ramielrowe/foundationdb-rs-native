//! [`MutationType`]: the atomic operations of [`Transaction::atomic_op`](crate::Transaction::atomic_op)
//! (`FDBMutationType` of `fdb.options`).

use fdb_native_schema::domain::MutationType as WireMutationType;

/// An atomic operation (`FDBMutationType`, the `MutationType` scope of `fdb.options`), for
/// [`Transaction::atomic_op`](crate::Transaction::atomic_op).
///
/// The operations an application can ask for, with the codes and names of `fdb.options`
/// ([`code`](Self::code), [`name`](Self::name), [`from_code`](Self::from_code),
/// [`from_name`](Self::from_name)). What each does to a value is in [`crate::atomic`].
///
/// As for every binding at API version 510 and later, [`BitAnd`](Self::BitAnd) and
/// [`Min`](Self::Min) are sent as `AndV2` and `MinV2`: a key that is not set takes the
/// operand (the version 1 operations treated it as zeros).
///
/// ```
/// use fdb_native_client::MutationType;
///
/// assert_eq!(MutationType::Add.code(), 2);
/// assert_eq!(MutationType::from_name("bit_and"), Some(MutationType::BitAnd));
/// assert_eq!(MutationType::from_code(20), Some(MutationType::CompareAndClear));
/// assert_eq!(MutationType::ByteMax.name(), "BYTE_MAX");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MutationType {
    /// `ADD` (2): little-endian addition of the operand to the value (of the operand's
    /// length; [`atomic::add`](crate::atomic::add)).
    Add,
    /// `BIT_AND` (6, also `AND`): bitwise and ([`atomic::and_v2`](crate::atomic::and_v2)).
    BitAnd,
    /// `BIT_OR` (7, also `OR`): bitwise or ([`atomic::or`](crate::atomic::or)).
    BitOr,
    /// `BIT_XOR` (8, also `XOR`): bitwise xor ([`atomic::xor`](crate::atomic::xor)).
    BitXor,
    /// `APPEND_IF_FITS` (9): appends the operand, unless the value would then be longer than
    /// the value size limit ([`atomic::append_if_fits`](crate::atomic::append_if_fits)).
    AppendIfFits,
    /// `MAX` (12): the larger, as little-endian unsigned integers
    /// ([`atomic::max`](crate::atomic::max)).
    Max,
    /// `MIN` (13): the smaller, as little-endian unsigned integers
    /// ([`atomic::min_v2`](crate::atomic::min_v2)).
    Min,
    /// `SET_VERSIONSTAMPED_KEY` (14): see
    /// [`Transaction::set_versionstamped_key`](crate::Transaction::set_versionstamped_key).
    SetVersionstampedKey,
    /// `SET_VERSIONSTAMPED_VALUE` (15): see
    /// [`Transaction::set_versionstamped_value`](crate::Transaction::set_versionstamped_value).
    SetVersionstampedValue,
    /// `BYTE_MIN` (16): the lexicographically smaller
    /// ([`atomic::byte_min`](crate::atomic::byte_min)).
    ByteMin,
    /// `BYTE_MAX` (17): the lexicographically larger
    /// ([`atomic::byte_max`](crate::atomic::byte_max)).
    ByteMax,
    /// `COMPARE_AND_CLEAR` (20): clears the key if its value is the operand
    /// ([`atomic::compare_and_clear`](crate::atomic::compare_and_clear)).
    CompareAndClear,
}

impl MutationType {
    /// Every atomic operation, in code order. A slice: the enum is `#[non_exhaustive]`, and
    /// the list grows with it.
    pub const ALL: &'static [MutationType] = &[
        MutationType::Add,
        MutationType::BitAnd,
        MutationType::BitOr,
        MutationType::BitXor,
        MutationType::AppendIfFits,
        MutationType::Max,
        MutationType::Min,
        MutationType::SetVersionstampedKey,
        MutationType::SetVersionstampedValue,
        MutationType::ByteMin,
        MutationType::ByteMax,
        MutationType::CompareAndClear,
    ];

    /// The operation's code in `fdb.options` (`FDBMutationType`).
    pub const fn code(self) -> i32 {
        match self {
            MutationType::Add => 2,
            MutationType::BitAnd => 6,
            MutationType::BitOr => 7,
            MutationType::BitXor => 8,
            MutationType::AppendIfFits => 9,
            MutationType::Max => 12,
            MutationType::Min => 13,
            MutationType::SetVersionstampedKey => 14,
            MutationType::SetVersionstampedValue => 15,
            MutationType::ByteMin => 16,
            MutationType::ByteMax => 17,
            MutationType::CompareAndClear => 20,
        }
    }

    /// The operation of an `fdb.options` code, `None` for a code that is not an atomic
    /// operation.
    pub const fn from_code(code: i32) -> Option<MutationType> {
        Some(match code {
            2 => MutationType::Add,
            6 => MutationType::BitAnd,
            7 => MutationType::BitOr,
            8 => MutationType::BitXor,
            9 => MutationType::AppendIfFits,
            12 => MutationType::Max,
            13 => MutationType::Min,
            14 => MutationType::SetVersionstampedKey,
            15 => MutationType::SetVersionstampedValue,
            16 => MutationType::ByteMin,
            17 => MutationType::ByteMax,
            20 => MutationType::CompareAndClear,
            _ => return None,
        })
    }

    /// The operation's name in `fdb.options`, upper case (`"ADD"`, `"BIT_AND"`, ...), as the
    /// binding tester's `ATOMIC_OP` instruction names it.
    pub const fn name(self) -> &'static str {
        match self {
            MutationType::Add => "ADD",
            MutationType::BitAnd => "BIT_AND",
            MutationType::BitOr => "BIT_OR",
            MutationType::BitXor => "BIT_XOR",
            MutationType::AppendIfFits => "APPEND_IF_FITS",
            MutationType::Max => "MAX",
            MutationType::Min => "MIN",
            MutationType::SetVersionstampedKey => "SET_VERSIONSTAMPED_KEY",
            MutationType::SetVersionstampedValue => "SET_VERSIONSTAMPED_VALUE",
            MutationType::ByteMin => "BYTE_MIN",
            MutationType::ByteMax => "BYTE_MAX",
            MutationType::CompareAndClear => "COMPARE_AND_CLEAR",
        }
    }

    /// The operation of an `fdb.options` name, in any case, with the deprecated aliases
    /// `AND`, `OR` and `XOR`; `None` for another name.
    pub fn from_name(name: &str) -> Option<MutationType> {
        let name = name.to_ascii_uppercase();
        match name.as_str() {
            "AND" => Some(MutationType::BitAnd),
            "OR" => Some(MutationType::BitOr),
            "XOR" => Some(MutationType::BitXor),
            _ => MutationType::ALL
                .iter()
                .copied()
                .find(|op| op.name() == name),
        }
    }
}

/// The mutation type this client sends for the operation: the type of the same code, but
/// `AndV2` and `MinV2` for [`BitAnd`](MutationType::BitAnd) and [`Min`](MutationType::Min)
/// (API version 510 and later).
impl From<MutationType> for WireMutationType {
    fn from(op: MutationType) -> WireMutationType {
        match op {
            MutationType::Add => WireMutationType::ADD_VALUE,
            MutationType::BitAnd => WireMutationType::AND_V2,
            MutationType::BitOr => WireMutationType::OR,
            MutationType::BitXor => WireMutationType::XOR,
            MutationType::AppendIfFits => WireMutationType::APPEND_IF_FITS,
            MutationType::Max => WireMutationType::MAX,
            MutationType::Min => WireMutationType::MIN_V2,
            MutationType::SetVersionstampedKey => WireMutationType::SET_VERSIONSTAMPED_KEY,
            MutationType::SetVersionstampedValue => WireMutationType::SET_VERSIONSTAMPED_VALUE,
            MutationType::ByteMin => WireMutationType::BYTE_MIN,
            MutationType::ByteMax => WireMutationType::BYTE_MAX,
            MutationType::CompareAndClear => WireMutationType::COMPARE_AND_CLEAR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ALL` lists every variant (`from_name` depends on it): the match is exhaustive in the
    /// defining crate, so a new variant fails to compile here until it is given an index, and
    /// the index must then be in `ALL`.
    #[test]
    fn all_lists_every_variant() {
        fn index(op: MutationType) -> usize {
            match op {
                MutationType::Add => 0,
                MutationType::BitAnd => 1,
                MutationType::BitOr => 2,
                MutationType::BitXor => 3,
                MutationType::AppendIfFits => 4,
                MutationType::Max => 5,
                MutationType::Min => 6,
                MutationType::SetVersionstampedKey => 7,
                MutationType::SetVersionstampedValue => 8,
                MutationType::ByteMin => 9,
                MutationType::ByteMax => 10,
                MutationType::CompareAndClear => 11,
            }
        }
        const VARIANTS: usize = 12;
        let mut seen = [false; VARIANTS];
        for &op in MutationType::ALL {
            assert!(!seen[index(op)], "{op:?} twice");
            seen[index(op)] = true;
        }
        assert!(seen.iter().all(|&s| s), "ALL misses a variant: {seen:?}");
        assert_eq!(MutationType::ALL.len(), VARIANTS);
    }

    #[test]
    fn codes_and_names_round_trip() {
        for &op in MutationType::ALL {
            assert_eq!(MutationType::from_code(op.code()), Some(op));
            assert_eq!(MutationType::from_name(op.name()), Some(op));
            assert_eq!(
                MutationType::from_name(&op.name().to_ascii_lowercase()),
                Some(op)
            );
        }
        assert_eq!(MutationType::from_code(0), None);
        assert_eq!(MutationType::from_code(18), None);
        assert_eq!(MutationType::from_name("SET_VALUE"), None);
        assert_eq!(MutationType::from_name("or"), Some(MutationType::BitOr));
    }

    #[test]
    fn wire_types_are_the_v2_ones() {
        for &op in MutationType::ALL {
            let wire = WireMutationType::from(op);
            match op {
                MutationType::BitAnd => assert_eq!(wire, WireMutationType::AND_V2),
                MutationType::Min => assert_eq!(wire, WireMutationType::MIN_V2),
                _ => assert_eq!(i32::from(wire.0), op.code()),
            }
        }
    }
}
