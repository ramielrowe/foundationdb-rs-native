//! Why a request failed, with the FDB error each case corresponds to.

use fdb_native_wire::{DecodeError, EncodeError, FdbError, Uid};

use crate::{ConnectionError, FrameError};

/// The FDB error codes this layer produces or interprets (`flow/include/flow/error_definitions.h`,
/// the same codes on both releases; lines are 7.4 / 8.0).
pub mod error_code {
    /// `commit_unknown_result` (1021; 50 / 50): what a commit that ended in a
    /// maybe-delivered error becomes (`tryCommit`).
    pub const COMMIT_UNKNOWN_RESULT: u16 = 1021;
    /// `connection_failed` (1026; 53 / 53).
    pub const CONNECTION_FAILED: u16 = 1026;
    /// `request_maybe_delivered` (1030; 57 / 57).
    pub const REQUEST_MAYBE_DELIVERED: u16 = 1030;
    /// `serialization_failed` (1044; 71 / 71).
    pub const SERIALIZATION_FAILED: u16 = 1044;
    /// `broken_promise` (1100; 113 / 112).
    pub const BROKEN_PROMISE: u16 = 1100;
    /// `operation_cancelled` (1101; 114 / 113).
    pub const OPERATION_CANCELLED: u16 = 1101;
    /// `platform_error` (1500; 172 / 176).
    pub const PLATFORM_ERROR: u16 = 1500;
    /// `internal_error` (4100; 411 / 386).
    pub const INTERNAL_ERROR: u16 = 4100;
    /// `unauthorized_attempt` (6001; 416 / 391).
    pub const UNAUTHORIZED_ATTEMPT: u16 = 6001;
}

use error_code::*;

/// Why a request sent through a [`ReplyRegistry`](super::ReplyRegistry) did not produce a
/// reply. See the [module documentation](super) for the C++ behaviour each case follows.
///
/// Three questions classify every case:
///
/// * [`fdb_code`](Self::fdb_code): the FDB error a C++ client would see (or the closest one).
/// * [`is_retryable`](Self::is_retryable): the request was **certainly not delivered**, so it
///   can be sent again (on a new connection, after the output drains, or to another
///   alternative) without risking a second execution.
/// * [`maybe_delivered`](Self::maybe_delivered) (alias
///   [`maybe_committed`](Self::maybe_committed)): the request may have reached the server and
///   been acted on, and nothing says what became of it. A commit that ends this way is
///   `commit_unknown_result`; a non-idempotent request must not be resent blindly.
///
/// An error that is neither (a server error reply, `Unauthorized`, a decode error) is a
/// definite answer: the request was handled (or refused) and resending it changes nothing.
/// An idempotent request (a read) may still be retried after a `maybe_delivered` error, as
/// `loadBalance` does when it is not `AtMostOnce`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RequestError {
    /// The server replied with an FDB error (the `Error` arm of the reply's `ErrorOr`), other
    /// than `broken_promise`.
    #[error("the server replied with FDB error {}", .0.code)]
    Fdb(FdbError),
    /// The server replied `broken_promise` (1100): the request was delivered, and the server
    /// dropped its reply promise without answering (its `networkSender` sends the error,
    /// `fdbrpc/include/fdbrpc/networksender.actor.h` 7.4:34-47, `networksender.h` 8.0:42-66).
    /// The request may have been acted on.
    #[error("the server dropped the request's reply promise (broken_promise)")]
    BrokenPromise,
    /// The connection ended after the request was put on it (`request_maybe_delivered`,
    /// 1030). The request may or may not have reached the server.
    #[error("request may have been delivered; the connection ended: {0}")]
    MaybeDelivered(ConnectionError),
    /// The request was never written: the connection ended before its handshake completed
    /// (requests sent then are held, see the [connection documentation](crate::connection#sending)),
    /// or it had already ended (`None` when only the peer's side has closed). Safe to resend.
    #[error("request not sent; the connection {}", match .0 {
        Some(e) => format!("ended: {e}"),
        None => "is closing".to_owned(),
    })]
    NotSent(Option<ConnectionError>),
    /// The peer does not have the endpoint (`IFailureMonitor::endpointNotFound`). If
    /// `maybe_delivered`, the request was pending when the endpoint was reported missing (or
    /// when another request to it got `broken_promise`); the report does not say which request
    /// caused it, so an earlier one may have been delivered before the endpoint went away. If
    /// not, the endpoint was already known to be gone and the request was not sent.
    #[error("endpoint {token:?} not found on the peer{}", if *maybe_delivered { " (request may have been delivered)" } else { "; request not sent" })]
    EndpointNotFound {
        /// The remote endpoint's token.
        token: Uid,
        /// Whether the request had been sent.
        maybe_delivered: bool,
    },
    /// The peer refused to deliver to the endpoint: it is private and this connection is not
    /// trusted (`unauthorized_attempt`, 6001). Never delivered; resending will not help.
    #[error("the peer refused access to endpoint {token:?} (unauthorized_attempt)")]
    Unauthorized {
        /// The remote endpoint's token.
        token: Uid,
    },
    /// The reply could not be decoded, or has another file identifier than the expected
    /// reply's (`serialization_failed`, 1044). The request was handled.
    #[error("the reply could not be decoded: {0}")]
    Decode(DecodeError),
    /// The request could not be encoded. Nothing was sent.
    #[error("the request could not be encoded: {0}")]
    Encode(EncodeError),
    /// The request does not fit in a frame (`PACKET_LIMIT`). Nothing was sent.
    #[error("the request is too large: {0}")]
    TooLarge(FrameError),
    /// The request was abandoned locally
    /// ([`ReplyRegistry::cancel_all`](super::ReplyRegistry::cancel_all)) before its reply came.
    /// It may have been delivered.
    #[error("the request was cancelled")]
    Cancelled,
    /// The connection's output is full
    /// ([`ConnectionConfig::max_pending_output`](crate::ConnectionConfig::max_pending_output)).
    /// Nothing was sent; retry once output has been written.
    #[error("too much output waits to be written; request not sent")]
    Backpressure,
    /// The registry already tracks its maximum number of requests
    /// ([`RegistryConfig::max_pending`](super::RegistryConfig::max_pending)). Nothing was sent.
    #[error("{limit} requests are already pending; request not sent")]
    TooManyPending {
        /// The limit.
        limit: usize,
    },
}

impl RequestError {
    /// The error a server reply carried: `broken_promise` becomes
    /// [`BrokenPromise`](Self::BrokenPromise), anything else [`Fdb`](Self::Fdb).
    pub fn from_reply_error(error: FdbError) -> RequestError {
        if error.code == BROKEN_PROMISE {
            RequestError::BrokenPromise
        } else {
            RequestError::Fdb(error)
        }
    }

    /// The FDB error code a C++ client would get in the same situation, or the closest one.
    ///
    /// | variant | code | C++ |
    /// | --- | --- | --- |
    /// | `Fdb(e)` | `e.code` | the error the server sent |
    /// | `BrokenPromise` | 1100 `broken_promise` | `getReply` throws it; `tryGetReply` turns it into 1030 |
    /// | `MaybeDelivered` | 1030 `request_maybe_delivered` | `tryGetReply` on a disconnect |
    /// | `NotSent` | 1026 `connection_failed` | (C++ cannot tell; it reports 1030) |
    /// | `EndpointNotFound` | 1030 `request_maybe_delivered` | `tryGetReply` once the endpoint is failed |
    /// | `Unauthorized` | 6001 `unauthorized_attempt` | `tryGetReply` / `getReply` |
    /// | `Decode` | 1044 `serialization_failed` | |
    /// | `Encode` | 4100 `internal_error` | |
    /// | `TooLarge` | 1500 `platform_error` | (C++ logs `PacketLimitExceeded` and sends anyway) |
    /// | `Cancelled` | 1101 `operation_cancelled` | |
    /// | `Backpressure`, `TooManyPending` | 1026 `connection_failed` | (no C++ limit: not sent, like `NotSent`) |
    pub fn fdb_code(&self) -> u16 {
        match self {
            RequestError::Fdb(e) => e.code,
            RequestError::BrokenPromise => BROKEN_PROMISE,
            RequestError::MaybeDelivered(_) | RequestError::EndpointNotFound { .. } => {
                REQUEST_MAYBE_DELIVERED
            }
            RequestError::NotSent(_)
            | RequestError::Backpressure
            | RequestError::TooManyPending { .. } => CONNECTION_FAILED,
            RequestError::Unauthorized { .. } => UNAUTHORIZED_ATTEMPT,
            RequestError::Decode(_) => SERIALIZATION_FAILED,
            RequestError::Encode(_) => INTERNAL_ERROR,
            RequestError::TooLarge(_) => PLATFORM_ERROR,
            RequestError::Cancelled => OPERATION_CANCELLED,
        }
    }

    /// [`fdb_code`](Self::fdb_code) as an [`FdbError`].
    pub fn fdb_error(&self) -> FdbError {
        FdbError::new(self.fdb_code())
    }

    /// Whether the request was certainly not delivered, so that sending it again cannot
    /// execute it twice: `NotSent`, `Backpressure`, `TooManyPending`, `Encode`, `TooLarge`
    /// (though those two will fail again), and `EndpointNotFound` for a request refused
    /// because the endpoint was already known to be gone (send it to the endpoint's
    /// replacement, not to the same one).
    pub fn is_retryable(&self) -> bool {
        match self {
            RequestError::NotSent(_)
            | RequestError::Backpressure
            | RequestError::TooManyPending { .. }
            | RequestError::Encode(_)
            | RequestError::TooLarge(_) => true,
            RequestError::EndpointNotFound {
                maybe_delivered, ..
            } => !maybe_delivered,
            RequestError::Fdb(_)
            | RequestError::BrokenPromise
            | RequestError::MaybeDelivered(_)
            | RequestError::Unauthorized { .. }
            | RequestError::Decode(_)
            | RequestError::Cancelled => false,
        }
    }

    /// Whether the request may have been delivered and acted on without a reply saying so:
    /// `MaybeDelivered`, `BrokenPromise`, `Cancelled`, `EndpointNotFound` for a request that
    /// was pending, and a server error reply of `commit_unknown_result`,
    /// `request_maybe_delivered` or `broken_promise`. These are the errors `loadBalance` calls
    /// `maybeDelivered` (`fdbrpc/include/fdbrpc/LoadBalance.actor.h` 7.4:591,
    /// `LoadBalance.h` 8.0:206) and, with `AtMostOnce`, throws as `request_maybe_delivered`
    /// instead of retrying (7.4:616-618 and 1108-1115, 8.0:232 and 735-741).
    pub fn maybe_delivered(&self) -> bool {
        match self {
            RequestError::Fdb(e) => matches!(
                e.code,
                COMMIT_UNKNOWN_RESULT | REQUEST_MAYBE_DELIVERED | BROKEN_PROMISE
            ),
            RequestError::BrokenPromise
            | RequestError::MaybeDelivered(_)
            | RequestError::Cancelled => true,
            RequestError::EndpointNotFound {
                maybe_delivered, ..
            } => *maybe_delivered,
            RequestError::NotSent(_)
            | RequestError::Unauthorized { .. }
            | RequestError::Decode(_)
            | RequestError::Encode(_)
            | RequestError::TooLarge(_)
            | RequestError::Backpressure
            | RequestError::TooManyPending { .. } => false,
        }
    }

    /// [`maybe_delivered`](Self::maybe_delivered), named for a commit: if it is true for a
    /// commit request, the transaction may have committed (`tryCommit` turns
    /// `request_maybe_delivered` into `commit_unknown_result`, after making sure the commit is
    /// no longer in flight; `fdbclient/NativeAPI.actor.cpp` 7.4:7060-7061,
    /// `NativeAPI.cpp` 8.0:4755-4756).
    pub fn maybe_committed(&self) -> bool {
        self.maybe_delivered()
    }
}

impl From<DecodeError> for RequestError {
    fn from(e: DecodeError) -> RequestError {
        RequestError::Decode(e)
    }
}
