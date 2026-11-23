//! Choosing the schema set.

use core::fmt;

use fdb_native_wire::ProtocolVersion;

use crate::streams::CommitProxyStreams;

/// The schema set a connection uses: one per supported FDB release.
///
/// Requests of the two releases share file identifiers but not slot layouts (8.0 removed
/// fields from the middle of several), so nothing in a message says which schema it has: a
/// connection picks the set from the peer's protocol version
/// ([`SchemaVersion::for_protocol_version`]) and uses it for every message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaVersion {
    /// FDB 7.4: [`crate::v7_4`].
    V7_4,
    /// FDB 8.0: [`crate::v8_0`].
    V8_0,
}

impl SchemaVersion {
    /// Every supported version, oldest first.
    pub const ALL: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];

    /// The schema set for a peer of protocol version `version` (as the connect packet carries
    /// it, flags allowed): the release it is
    /// [compatible](ProtocolVersion::is_compatible) with, or `None` if it is neither 7.4 nor
    /// 8.0.
    ///
    /// ```
    /// use fdb_native_schema::SchemaVersion;
    /// use fdb_native_wire::ProtocolVersion;
    ///
    /// let peer = ProtocolVersion(0x0FDB_00B0_7400_0001).with_object_serializer_flag();
    /// assert_eq!(SchemaVersion::for_protocol_version(peer), Some(SchemaVersion::V7_4));
    /// assert_eq!(SchemaVersion::for_protocol_version(ProtocolVersion::V8_0), Some(SchemaVersion::V8_0));
    /// assert_eq!(SchemaVersion::for_protocol_version(ProtocolVersion(0x0FDB_00B0_7300_0000)), None);
    /// ```
    pub const fn for_protocol_version(version: ProtocolVersion) -> Option<SchemaVersion> {
        if version.is_compatible(ProtocolVersion::V7_4) {
            Some(SchemaVersion::V7_4)
        } else if version.is_compatible(ProtocolVersion::V8_0) {
            Some(SchemaVersion::V8_0)
        } else {
            None
        }
    }

    /// The release's protocol version.
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            SchemaVersion::V7_4 => ProtocolVersion::V7_4,
            SchemaVersion::V8_0 => ProtocolVersion::V8_0,
        }
    }

    /// `CommitProxyInterface`'s adjusted-endpoint indices on this version.
    pub const fn commit_proxy_streams(self) -> CommitProxyStreams<u32> {
        match self {
            SchemaVersion::V7_4 => crate::v7_4::COMMIT_PROXY_STREAMS,
            SchemaVersion::V8_0 => crate::v8_0::COMMIT_PROXY_STREAMS,
        }
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SchemaVersion::V7_4 => "7.4",
            SchemaVersion::V8_0 => "8.0",
        })
    }
}
