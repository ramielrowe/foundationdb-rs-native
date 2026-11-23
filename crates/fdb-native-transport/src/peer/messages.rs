//! A message that needs no schema crate: the protocol-info request every coordinator answers.

use fdb_native_wire::{ProtocolVersion, ReplyPromise, Wire};

/// The reply to a [`ProtocolInfoRequest`]: the protocol version the server speaks, flags
/// included (`ProtocolInfoReply`, `fdbclient/include/fdbclient/CoordinationInterface.h`
/// 7.4:305-319 and 8.0:305-319, file identifier 7784298; one `uint64_t`, the
/// `ProtocolVersion::versionWithFlags()`).
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 7784298)]
pub struct ProtocolInfoReply {
    /// The server's protocol version with its flag bits.
    pub version: u64,
}

impl ProtocolInfoReply {
    /// The server's [`ProtocolVersion`] (flags included).
    pub fn protocol_version(&self) -> ProtocolVersion {
        ProtocolVersion(self.version)
    }
}

/// Asks a coordinator (or any process that serves it) for its protocol version
/// (`ProtocolInfoRequest`, `CoordinationInterface.h` 7.4:321-331 and 8.0:321-331, file
/// identifier 13261233; `serializer(ar, reply)`). Send it to
/// [`well_known::PROTOCOL_INFO`](crate::connection::well_known::PROTOCOL_INFO) (token 10, "stable
/// and not to change"); the answer is a [`ProtocolInfoReply`]. The messages are the same on
/// 7.4 and 8.0.
///
/// ```
/// use fdb_native_transport::peer::ProtocolInfoRequest;
/// use fdb_native_wire::{ReplyPromise, Uid};
///
/// let request = ProtocolInfoRequest { reply: ReplyPromise::new(Uid::new(1, 2)) };
/// let bytes = fdb_native_wire::encode(&request)?;
/// assert_eq!(fdb_native_wire::decode::<ProtocolInfoRequest>(&bytes)?, request);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 13261233)]
pub struct ProtocolInfoRequest {
    /// Where the reply goes: the reply token of the request.
    pub reply: ReplyPromise<ProtocolInfoReply>,
}
