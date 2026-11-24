//! Errors of the peer manager.

use std::io;
use std::time::Duration;

use fdb_native_wire::{EncodeError, ProtocolVersion, Uid};

use crate::{ConnectionError, FrameError, IoError};

/// Why a connection attempt, or a connection, to a peer failed or ended.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PeerError {
    /// TCP connect did not finish within the connect timeout (one bound, shared with the TLS
    /// handshake: see [`PeerManagerConfig::connect_timeout`](super::PeerManagerConfig::connect_timeout)).
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
    /// TCP connect plus TLS handshake did not finish within the single connect timeout.
    #[error("the TLS handshake timed out after {after:?}")]
    TlsTimeout {
        /// The timeout.
        after: Duration,
    },
    /// The [stream wrapper](super::StreamWrapper) failed: the TLS handshake did, for
    /// example because the server's certificate was refused. (A server's refusal of *our*
    /// certificate usually shows up later, as [`ConnectionError::Io`] on the first read:
    /// with TLS 1.3 the client's handshake is done before the server has checked it.)
    #[error("TLS setup failed: {message} ({kind})")]
    Tls {
        /// The kind of the I/O error.
        kind: io::ErrorKind,
        /// Its message.
        message: String,
        /// The I/O error itself, for the cause (with the `tls` feature, a `rustls::Error`:
        /// see [`IoError::cause`]).
        #[source]
        source: IoError,
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

/// Why a one-way command to a peer ([`send`](super::PeerHandle::send),
/// [`send_with`](super::PeerHandle::send_with), [`register`](super::PeerHandle::register)) was
/// refused. In every case **nothing was sent** and nothing was registered: a command is either
/// applied, and acknowledged with the [`Generation`](super::Generation) of the connection it
/// went to, or refused with one of these. (Requests report their own errors: see
/// [`RequestError`](crate::rpc::RequestError).)
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
    /// The payload could not be encoded; nothing was sent.
    #[error("the payload could not be encoded: {0}")]
    Encode(EncodeError),
    /// The token cannot be registered: it is already registered, well-known, or a ping's.
    #[error("token {0:?} cannot be registered")]
    TokenUnavailable(Uid),
}

impl PeerError {
    /// The error of a request that was not sent because of this (connect) failure. A failed
    /// TLS handshake is reported as the I/O error it was, so its cause (an untrusted server
    /// certificate, ...) reaches the request's error.
    pub(crate) fn not_sent(&self) -> crate::rpc::RequestError {
        crate::rpc::RequestError::NotSent(match self {
            PeerError::Connection(e) => Some(e.clone()),
            PeerError::Tls { source, .. } => Some(ConnectionError::Io(source.clone())),
            _ => None,
        })
    }
}
