//! The protocol version a message is read or written for.

use core::fmt;

/// An FDB protocol version (`ProtocolVersion` in `flow/ProtocolVersion.h`), as exchanged
/// in the FlowTransport connect packet.
///
/// The codec does not interpret it. [`Reader`](crate::Reader) and
/// [`Writer`](crate::Writer) carry it so that [`Decode`](crate::Decode) and
/// [`Encode`](crate::Encode) impls can pick the slot layout of the peer's version (FDB 8.0,
/// for example, removed fields from the middle of some messages).
///
/// The value is `versionWithFlags()`: the version in the low 60 bits and flags above them
/// (the object-serializer flag). **The derived `PartialEq`, `Ord` and `Hash` compare the whole
/// word, flags included**, whereas C++'s `==` and `<` compare `version()` without flags.
/// Compare through [`version`](ProtocolVersion::version) or
/// [`without_flags`](ProtocolVersion::without_flags) (or use
/// [`is_compatible`](ProtocolVersion::is_compatible)) when a peer's flagged version is
/// checked against a local one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(pub u64);

impl ProtocolVersion {
    /// The version's numeric value.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// FDB 7.4's `defaultProtocolVersionValue` (`flow/ProtocolVersions.cmake`).
    pub const V7_4: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_7400_0000);
    /// FDB 8.0's `defaultProtocolVersionValue`.
    pub const V8_0: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_8000_0000);
    /// `ProtocolVersion::minValidProtocolVersion`: the oldest version `isValid()`.
    pub const MIN_VALID: ProtocolVersion = ProtocolVersion(0x0FDB_00A2_0006_0001);
    /// FDB 7.4's `minInvalidProtocolVersionValue`: the first version `IncludeVersion`
    /// refuses to read.
    pub const MIN_INVALID_7_4: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_8100_0000);
    /// FDB 8.0's `minInvalidProtocolVersionValue`.
    pub const MIN_INVALID_8_0: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_9100_0000);

    /// `ProtocolVersion::objectSerializerFlag`: set in the version a peer sends when its
    /// messages are in the flatbuffers dialect.
    pub const OBJECT_SERIALIZER_FLAG: u64 = 0x1000_0000_0000_0000;
    /// `ProtocolVersion::versionFlagMask`: the bits that are the version, not flags.
    pub const VERSION_MASK: u64 = 0x0FFF_FFFF_FFFF_FFFF;
    /// `ProtocolVersion::compatibleProtocolVersionMask`: the bits two versions share when
    /// they can talk to each other.
    pub const COMPATIBLE_MASK: u64 = 0xFFFF_FFFF_FFFF_0000;

    /// The version without its flags (`version()`). C++ compares versions (`==`, `<`,
    /// `isCompatible`, `isValid`) through this; the derived `==`, `Ord` and `Hash` of this
    /// type compare the whole word, flags included (see the type's docs).
    pub const fn version(self) -> u64 {
        self.0 & Self::VERSION_MASK
    }

    /// `isValid()`: the version (ignoring flags) is at least [`ProtocolVersion::MIN_VALID`].
    pub const fn is_valid(self) -> bool {
        self.version() >= Self::MIN_VALID.0
    }

    /// `hasObjectSerializerFlag()`.
    pub const fn has_object_serializer_flag(self) -> bool {
        self.0 & Self::OBJECT_SERIALIZER_FLAG != 0
    }

    /// The version with the object-serializer flag set (`addObjectSerializerFlag()`).
    pub const fn with_object_serializer_flag(self) -> ProtocolVersion {
        ProtocolVersion(self.0 | Self::OBJECT_SERIALIZER_FLAG)
    }

    /// The version with the object-serializer flag cleared (`removeObjectSerializerFlag()`).
    pub const fn without_object_serializer_flag(self) -> ProtocolVersion {
        ProtocolVersion(self.0 & !Self::OBJECT_SERIALIZER_FLAG)
    }

    /// The version without any flags (`removeAllFlags()`).
    pub const fn without_flags(self) -> ProtocolVersion {
        ProtocolVersion(self.version())
    }

    /// `isCompatible(other)`: the versions agree outside their last two bytes. Flags are
    /// ignored, as in C++ (`version() & compatibleProtocolVersionMask`): a peer's version
    /// with the object-serializer flag is compatible with the same version without it.
    pub const fn is_compatible(self, other: ProtocolVersion) -> bool {
        self.version() & Self::COMPATIBLE_MASK == other.version() & Self::COMPATIBLE_MASK
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::ProtocolVersion;

    const FLAG: u64 = ProtocolVersion::OBJECT_SERIALIZER_FLAG;

    #[test]
    fn compatibility_ignores_flags() {
        let v = ProtocolVersion::V7_4;
        let flagged = v.with_object_serializer_flag();
        assert_ne!(v, flagged, "the derived equality sees the flag");
        assert!(v.is_compatible(v));
        assert!(flagged.is_compatible(v));
        assert!(v.is_compatible(flagged));
        assert!(flagged.is_compatible(flagged));
        assert!(
            !ProtocolVersion::V8_0
                .with_object_serializer_flag()
                .is_compatible(v)
        );
        assert!(!v.is_compatible(ProtocolVersion::V8_0.with_object_serializer_flag()));
    }

    #[test]
    fn compatibility_ignores_the_last_two_bytes() {
        let v = ProtocolVersion::V7_4;
        for low in [1, 0x1234, 0xffff] {
            let other = ProtocolVersion(v.0 + low);
            assert!(v.is_compatible(other), "{low:#x}");
            assert!(
                v.with_object_serializer_flag().is_compatible(other),
                "{low:#x}"
            );
        }
        assert!(!v.is_compatible(ProtocolVersion(v.0 + 0x1_0000)));
        assert!(!v.is_compatible(ProtocolVersion(v.0 - 0x1_0000)));
    }

    #[test]
    fn flags_are_not_part_of_the_version() {
        let v = ProtocolVersion(ProtocolVersion::V7_4.0 | FLAG);
        assert_eq!(v.version(), ProtocolVersion::V7_4.0);
        assert!(v.has_object_serializer_flag());
        assert!(v.is_valid());
        assert_eq!(v.without_flags(), ProtocolVersion::V7_4);
        assert_eq!(v.without_object_serializer_flag(), ProtocolVersion::V7_4);
        assert_eq!(ProtocolVersion::V7_4.with_object_serializer_flag(), v);
        assert!(!ProtocolVersion(ProtocolVersion::MIN_VALID.0 - 1).is_valid());
        assert!(ProtocolVersion(ProtocolVersion::MIN_VALID.0 | FLAG).is_valid());
        // A flag-only difference does not make a version valid.
        assert!(!ProtocolVersion(FLAG).is_valid());
    }
}
