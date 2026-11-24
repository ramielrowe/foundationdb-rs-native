//! Why a connection ended, and why a send was refused.

use std::fmt;
use std::io;
use std::sync::Arc;
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
    /// A FlowTransport message (a ping, or an endpoint-not-found notice) could not be decoded,
    /// has another file identifier than its type's (FDB's `ObjectReader` asserts on that), or a
    /// frame has an empty payload (FDB asserts `!reader.empty()`, `FlowTransport.actor.cpp`
    /// 7.4:1443, `FlowTransport.cpp` 8.0:1563).
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
    /// The socket failed ([`Connection::io_error`](super::Connection::io_error)). The
    /// [`IoError`] keeps the underlying [`io::Error`], so a cause such as a TLS alert from the
    /// peer (`received fatal alert: UnknownCA`) is in the message and the
    /// [`source`](std::error::Error::source) chain, and not only its kind.
    #[error("i/o error: {0}")]
    Io(#[source] IoError),
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
    /// The peer kept asking for replies (pings) while not reading them: the FlowTransport
    /// traffic it caused reached [`max_pending_output`](super::ConnectionConfig::max_pending_output)
    /// bytes unwritten.
    #[error("{0} bytes of transport replies wait unwritten; the peer is not reading")]
    OutputOverflow(usize),
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
            | ConnectionError::OutputOverflow(_)
            | ConnectionError::PingTimeout(_) => "connection_failed",
        }
    }
}

/// An I/O failure of the socket (or of the TLS layer over it), with its cause.
///
/// An [`ErrorKind`](io::ErrorKind) alone says little: a rustls error that surfaces from a
/// read is an [`io::Error`] of kind `InvalidData` whatever the alert was. This keeps the whole
/// error behind an [`Arc`], so it is cheap to clone and stays an [`Error`](std::error::Error)
/// with a [`source`](std::error::Error::source).
///
/// * `Display` is the error's message, e.g. `received fatal alert: UnknownCA` for a server
///   that refused our client certificate, or `Connection reset by peer (os error 104)`.
/// * [`source`](std::error::Error::source) is the error that [`io::Error::new`] wrapped, if
///   there was one: a `rustls::Error` for a TLS failure (with the `tls` feature,
///   `fdb_native_transport::tls::rustls_error` and `tls::alert_received` read it).
/// * Equality compares the kind and the message.
#[derive(Clone)]
pub struct IoError {
    kind: io::ErrorKind,
    error: Option<Arc<io::Error>>,
}

impl IoError {
    /// Wraps `error`, keeping it for [`io_error`](Self::io_error) and the source chain.
    pub fn new(error: io::Error) -> Self {
        IoError {
            kind: error.kind(),
            error: Some(Arc::new(error)),
        }
    }

    /// The error's kind.
    pub fn kind(&self) -> io::ErrorKind {
        self.kind
    }

    /// The [`io::Error`] this was made from; `None` if only a kind is known.
    pub fn io_error(&self) -> Option<&io::Error> {
        self.error.as_deref()
    }

    /// The error that [`io::Error::new`] wrapped (the TLS library's error, for a TLS
    /// failure), if the I/O error had one.
    pub fn cause(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.error.as_deref()?.get_ref()
    }
}

impl From<io::ErrorKind> for IoError {
    fn from(kind: io::ErrorKind) -> Self {
        IoError { kind, error: None }
    }
}

impl From<io::Error> for IoError {
    fn from(error: io::Error) -> Self {
        IoError::new(error)
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.error {
            Some(error) => fmt::Display::fmt(error, f),
            None => fmt::Display::fmt(&self.kind, f),
        }
    }
}

impl fmt::Debug for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("IoError");
        s.field("kind", &self.kind);
        if let Some(error) = &self.error {
            s.field("message", &error.to_string());
        }
        s.finish()
    }
}

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.to_string() == other.to_string()
    }
}

impl Eq for IoError {}

impl std::error::Error for IoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// Why [`Connection::send`](super::Connection::send) refused a frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SendError<E = core::convert::Infallible> {
    /// The connection has ended.
    #[error("connection is closed")]
    Closed,
    /// Output waiting to be written has reached
    /// [`max_pending_output`](super::ConnectionConfig::max_pending_output): write some, then
    /// retry. Nothing was sent.
    #[error("too much output waits to be written")]
    Backpressure,
    /// The frame could not be built (the payload is too large).
    #[error(transparent)]
    Frame(FrameError),
    /// The payload writer failed.
    #[error("writing the payload failed: {0}")]
    Payload(E),
}
