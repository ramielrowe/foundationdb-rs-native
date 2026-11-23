//! The version options of the binary [`Writer`](super::Writer) and
//! [`Reader`](super::Reader).

use crate::ProtocolVersion;

/// What a [`Writer`](super::Writer) does with the protocol version: FDB's `IncludeVersion`,
/// `AssumeVersion` and `Unversioned` version options. A reader takes a [`ReadVersioning`],
/// which names the limit an `IncludeVersion` prefix is checked against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Versioning {
    /// `Unversioned()`: no version is written, and the archive's protocol version is unset.
    /// Fine for scalars, UIDs and keys; FDB asserts if a container or a struct is serialized
    /// unversioned, which this crate does not enforce.
    Unversioned,
    /// `IncludeVersion(v)`: the writer sets its version to `v` and writes it as the first
    /// 8 bytes.
    ///
    /// If the version has the
    /// [object-serializer flag](ProtocolVersion::has_object_serializer_flag), everything
    /// after the prefix is a flatbuffers-dialect message, not binary: see
    /// [`Reader::remaining`](super::Reader::remaining).
    Include(ProtocolVersion),
    /// `AssumeVersion(v)`: nothing is written; the archive's version is `v`, which must be
    /// [valid](ProtocolVersion::is_valid).
    Assume(ProtocolVersion),
}

/// What a [`Reader`](super::Reader) does with the protocol version: FDB's `IncludeVersion`,
/// `AssumeVersion` and `Unversioned` version options, for reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadVersioning {
    /// `Unversioned()`: nothing is read, and the archive's protocol version is unset.
    Unversioned,
    /// `IncludeVersion()`: the first 8 bytes are a protocol version, which must be
    /// [valid](ProtocolVersion::is_valid) and, flags aside, below `min_invalid` (the
    /// release's `minInvalidProtocolVersion`: [`ProtocolVersion::MIN_INVALID_7_4`] or
    /// [`ProtocolVersion::MIN_INVALID_8_0`]), as `_IncludeVersion::read` requires. The
    /// archive's version is then the one read, flags included.
    ///
    /// If it has the [object-serializer flag](ProtocolVersion::has_object_serializer_flag),
    /// everything after the prefix is a flatbuffers-dialect message, not binary: see
    /// [`Reader::remaining`](super::Reader::remaining).
    Include {
        /// The first protocol version the reader refuses.
        min_invalid: ProtocolVersion,
    },
    /// `AssumeVersion(v)`: nothing is read; the archive's version is `v`, which must be
    /// [valid](ProtocolVersion::is_valid).
    Assume(ProtocolVersion),
}

impl ReadVersioning {
    /// `IncludeVersion()` as FDB 7.4 reads it.
    pub const INCLUDE_7_4: ReadVersioning = ReadVersioning::Include {
        min_invalid: ProtocolVersion::MIN_INVALID_7_4,
    };
    /// `IncludeVersion()` as FDB 8.0 reads it: the newest limit this crate knows, and so
    /// the one for a client that talks to either release.
    pub const INCLUDE_8_0: ReadVersioning = ReadVersioning::Include {
        min_invalid: ProtocolVersion::MIN_INVALID_8_0,
    };
}
