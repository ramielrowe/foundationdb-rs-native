//! Protocol-version flags and the version options of `BinaryWriter` and `BinaryReader`.

use crate::ProtocolVersion;

impl ProtocolVersion {
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

    /// The version without its flags (`version()`). C++ compares versions through this;
    /// the derived `==` and `Ord` of this type compare the flags too.
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

    /// `isCompatible(other)`: the versions agree outside their last two bytes (flags
    /// included, as in C++).
    pub const fn is_compatible(self, other: ProtocolVersion) -> bool {
        self.0 & Self::COMPATIBLE_MASK == other.0 & Self::COMPATIBLE_MASK
    }
}

/// What a [`BinaryWriter`](crate::binary::BinaryWriter) or
/// [`BinaryReader`](crate::binary::BinaryReader) does with the protocol version: FDB's
/// `IncludeVersion`, `AssumeVersion` and `Unversioned` version options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Versioning {
    /// `Unversioned()`: no version is written or read, and the archive's protocol version is
    /// unset. Fine for scalars, UIDs and keys; FDB asserts if a container or a struct is
    /// serialized unversioned, which this crate does not enforce.
    Unversioned,
    /// `IncludeVersion(v)`: the writer sets its version to `v` and writes it as the first
    /// 8 bytes; the reader reads those 8 bytes, checks them, and uses them as its version
    /// (the `v` given to a reader is ignored; it is `IncludeVersion`'s default, which only
    /// the writer uses).
    ///
    /// If the version has the
    /// [object-serializer flag](ProtocolVersion::has_object_serializer_flag), everything
    /// after the prefix is a flatbuffers-dialect message, not binary: see
    /// [`BinaryReader::remaining`](crate::binary::BinaryReader::remaining).
    Include(ProtocolVersion),
    /// `AssumeVersion(v)`: nothing is written or read; the archive's version is `v`.
    Assume(ProtocolVersion),
}
