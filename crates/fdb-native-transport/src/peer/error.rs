//! Errors of the peer manager.

use std::io;
use std::time::Duration;

use fdb_native_wire::{ProtocolVersion, Uid};

use crate::{ConnectionError, FrameError};

/// Why a connection attempt, or a connection, to a peer failed or ended.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PeerError {
    /// The TCP connection was not established within the connect timeout.
    #[error("connecting timed out after {after:?}")]
    ConnectTimeout {
        /// The timeout.
        after: Duration,
    },
    /// The TCP connection failed.
    #[error("connecting failed: {0}")]
    Connect(io::ErrorKind),
    /// The address is a TLS address but the manager has no [stream
    /// wrapper](super::StreamWrapper) for TLS.
    #[error("the address needs TLS, which is not configured")]
    TlsNotConfigured,
    /// The TLS handshake did not complete within the connect timeout.
    #[error("the TLS handshake timed out after {after:?}")]
    TlsTimeout {
        /// The timeout.
        after: Duration,
    },
    /// The [stream wrapper](super::StreamWrapper) failed.
    #[error("TLS setup failed: {message} ({kind})")]
    Tls {
        /// The kind of the I/O error.
        kind: io::ErrorKind,
        /// Its message.
        message: String,
    },
    /// The connection ended: see [`ConnectionError`], which also says whether it was clean.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// The peer speaks a protocol version this manager does not support: the cluster is newer
    /// (or older) than the schemas this client has.
    #[error("the peer speaks protocol version {peer}, which is not supported (supported: {})", versions(.supported))]
    UnsupportedVersion {
        /// The peer's version.
        peer: ProtocolVersion,
        /// The versions this manager supports.
        supported: Vec<ProtocolVersion>,
    },
    /// The peer manager was shut down.
    #[error("the transport was shut down")]
    Shutdown,
}

fn versions(vs: &[ProtocolVersion]) -> String {
    vs.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

impl PeerError {
    /// Whether this is an orderly end that says nothing about the peer's health: an idle,
    /// unreferenced or locally closed connection ([`ConnectionError::is_clean`]).
    pub fn is_clean(&self) -> bool {
        match self {
            PeerError::Connection(e) => e.is_clean(),
            _ => false,
        }
    }
}

/// Why a command to a peer ([`send`](super::PeerHandle::send),
/// [`register`](super::PeerHandle::register)) was refused. In every case **nothing was sent**
/// and nothing was registered: a command is either applied, and acknowledged with the
/// [`Generation`](super::Generation) of the connection it went to, or refused with one of
/// these.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// The command was not applied because no connection could be established (or the one it
    /// waited for ended before it could be applied). The frame was **not** sent.
    #[error("not sent: {0}")]
    NotSent(PeerError),
    /// The peer manager was shut down (or the peer's task is gone). The frame was not sent.
    #[error("the transport was shut down")]
    Shutdown,
    /// The frame could not be built: its payload is too large.
    #[error("frame refused: {0}")]
    Frame(FrameError),
    /// The token cannot be registered: it is already registered, well-known, or a ping's.
    #[error("token {0:?} cannot be registered")]
    TokenUnavailable(Uid),
    /// [`Outgoing::for_version`](super::Outgoing::for_version) asked for a protocol version
    /// other than the one the connection announces, so the payload was encoded for the wrong
    /// schema set.
    #[error("payload is for protocol version {expected} but the connection uses {actual}")]
    VersionMismatch {
        /// The version the payload was encoded for.
        expected: ProtocolVersion,
        /// The version the connection announced.
        actual: ProtocolVersion,
    },
}
