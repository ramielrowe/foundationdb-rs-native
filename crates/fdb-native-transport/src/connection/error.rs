//! Why a connection ended, and why a send was refused.

use std::time::Duration;

use fdb_native_wire::{ProtocolVersion, Uid, binary};

use crate::{ConnectPacketError, FrameError};

/// Why a [`Connection`](super::Connection) ended. Every variant is terminal: the connection
/// accepts no more input and produces no more output.
///
/// The names of the FDB errors each variant corresponds to are given for reference
/// ([`ConnectionError::fdb_error`]).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionError {
    /// The peer's ConnectPacket carries a valid version that is not compatible with the one
    /// announced (`incompatible_protocol_version`). Reconnect announcing `peer` if that
    /// version is supported: see [`incompatible_version`](ConnectionError::incompatible_version)
    /// and the [module documentation](super#version-detection).
    #[error("peer protocol version {peer} is not compatible with the announced {local}")]
    Incompatible {
        /// The version this side announced.
        local: ProtocolVersion,
        /// The version the peer sent, flags included.
        peer: ProtocolVersion,
    },
    /// The peer's ConnectPacket was refused for another reason: an invalid version, a version
    /// without the object-serializer flag, or a port other than the one dialed.
    #[error("peer's connect packet refused: {0}")]
    HandshakeRejected(ConnectPacketError),
    /// The first bytes from the peer are not a ConnectPacket (its length field is not 40).
    #[error("malformed connect packet: {0}")]
    MalformedConnectPacket(binary::DecodeError),
    /// The peer did not send its whole ConnectPacket in time (`connection_failed`).
    #[error("no connect packet from the peer within {0:?}")]
    HandshakeTimeout(Duration),
    /// A frame was refused (`checksum_failed` or `platform_error`).
    #[error("framing error: {0}")]
    Framing(#[from] FrameError),
    /// A FlowTransport message (a ping, or an endpoint-not-found notice) could not be
    /// decoded.
    #[error("malformed message for token {token:?}: {error}")]
    MalformedMessage {
        /// The token it was sent to.
        token: Uid,
        /// What was wrong with it.
        error: fdb_native_wire::DecodeError,
    },
    /// The peer closed the connection ([`Connection::recv_eof`](super::Connection::recv_eof)).
    #[error("peer closed the connection{}", if *during_handshake { " during the handshake" } else { "" })]
    PeerClosed {
        /// Whether it happened before the peer's ConnectPacket was read.
        during_handshake: bool,
    },
    /// The socket failed ([`Connection::io_error`](super::Connection::io_error)).
    #[error("i/o error: {0}")]
    Io(std::io::ErrorKind),
    /// Nothing at all arrived from the peer within the monitor's timeout after a ping
    /// (`connection_failed`, logged as `ConnectionTimeout`).
    #[error("no bytes from the peer within {0:?} of a ping")]
    PingTimeout(Duration),
    /// The connection had nothing outstanding and sent no data for the idle timeout
    /// (`connection_idle`).
    #[error("connection idle")]
    Idle,
    /// The connection was unreferenced, had nothing outstanding and sent no data for the
    /// unreferenced close delay (`connection_unreferenced`).
    #[error("connection unreferenced")]
    Unreferenced,
    /// [`Connection::close`](super::Connection::close) was called.
    #[error("connection closed locally")]
    LocalClose,
    /// A FlowTransport message this side sends (the ConnectPacket, a ping, a ping reply, an
    /// endpoint-not-found notice) could not be encoded. Not expected to happen; the string is
    /// the encoder's error.
    #[error("encoding a transport message failed: {0}")]
    InternalEncode(String),
}

impl ConnectionError {
    /// The peer's version, without flags, if the connection ended because the peer is
    /// [`Incompatible`](ConnectionError::Incompatible): the version to announce on the next
    /// attempt.
    pub fn incompatible_version(&self) -> Option<ProtocolVersion> {
        match self {
            ConnectionError::Incompatible { peer, .. } => Some(peer.without_flags()),
            _ => None,
        }
    }

    /// Whether this is an orderly close rather than a failure: FDB's connection keeper treats
    /// `connection_idle` and `connection_unreferenced` (and a cancellation) as closes that say
    /// nothing about the peer's health.
    pub fn is_clean(&self) -> bool {
        matches!(
            self,
            ConnectionError::Idle | ConnectionError::Unreferenced | ConnectionError::LocalClose
        )
    }

    /// The name of the FDB error this corresponds to (`flow/include/flow/error_definitions.h`).
    pub fn fdb_error(&self) -> &'static str {
        match self {
            ConnectionError::Incompatible { .. } => "incompatible_protocol_version",
            ConnectionError::HandshakeRejected(ConnectPacketError::PortMismatch { .. }) => {
                "internal_error"
            }
            ConnectionError::HandshakeRejected(_) => "incompatible_protocol_version",
            ConnectionError::MalformedConnectPacket(_) => "serialization_failed",
            ConnectionError::Framing(FrameError::ChecksumMismatch { .. }) => "checksum_failed",
            ConnectionError::Framing(_) => "platform_error",
            ConnectionError::MalformedMessage { .. } => "serialization_failed",
            ConnectionError::Idle => "connection_idle",
            ConnectionError::Unreferenced => "connection_unreferenced",
            ConnectionError::LocalClose => "operation_cancelled",
            ConnectionError::InternalEncode(_) => "internal_error",
            ConnectionError::HandshakeTimeout(_)
            | ConnectionError::PeerClosed { .. }
            | ConnectionError::Io(_)
            | ConnectionError::PingTimeout(_) => "connection_failed",
        }
    }
}

/// Why [`Connection::send`](super::Connection::send) refused a frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SendError<E = core::convert::Infallible> {
    /// The connection has ended.
    #[error("connection is closed")]
    Closed,
    /// The frame could not be built (the payload is too large).
    #[error(transparent)]
    Frame(FrameError),
    /// The payload writer failed.
    #[error("writing the payload failed: {0}")]
    Payload(E),
}
