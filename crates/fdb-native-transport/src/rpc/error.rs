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
    /// `default_error_or` (1035; 62 / 62): what a reply whose `ErrorOr` has no arm set decodes
    /// to.
    pub const DEFAULT_ERROR_OR: u16 = 1035;
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
/// Four questions classify every case:
///
/// * [`fdb_code`](Self::fdb_code): the FDB error a C++ client would see (or the closest one).
/// * [`certainly_not_delivered`](Self::certainly_not_delivered): the **safety predicate**. The
///   request never reached the endpoint, so sending it again (on a new connection, after the
///   output drains, or to another alternative) cannot execute it twice.
/// * [`should_retry`](Self::should_retry): certainly not delivered **and** a resend can
///   succeed (the failure was transient: not an encoding error, an oversized request, or an
///   unauthorized endpoint). **A load balancer retries automatically on `should_retry` only.**
/// * [`maybe_delivered`](Self::maybe_delivered) (alias
///   [`maybe_committed`](Self::maybe_committed)): the request may have reached the server and
///   been acted on, and nothing says what became of it. A commit that ends this way is
///   `commit_unknown_result`; a non-idempotent request must not be resent blindly.
///
/// `certainly_not_delivered` and `maybe_delivered` are never both true. An error that is
/// neither (a server error reply with a definite meaning) is an answer: the request was
/// handled and resending it changes nothing. An idempotent request (a read) may still be
/// retried after a `maybe_delivered` error, as `loadBalance` does when it is not `AtMostOnce`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RequestError {
    /// The server replied with an FDB error (the `Error` arm of the reply's `ErrorOr`), other
    /// than `broken_promise`. A reply whose `ErrorOr` has neither arm set decodes as
    /// `default_error_or` (1035), as in C++: the request was delivered and its outcome is
    /// unknown.
    #[error("the server replied with FDB error {}", .0.code)]
    Fdb(FdbError),
    /// The server replied `broken_promise` (1100): the request was delivered, and the server
    /// dropped its reply promise without answering (its `networkSender` sends the error,
    /// `fdbrpc/include/fdbrpc/networksender.actor.h` 7.4:34-47, `networksender.h` 8.0:42-66).
    /// The request may have been acted on.
    #[error("the server dropped the request's reply promise (broken_promise)")]
    BrokenPromise,
    /// The connection ended after at least one byte of the request was handed to the
    /// transport (`request_maybe_delivered`, 1030). The request may or may not have reached
    /// the server. The error is `None` when the connection's end was not seen by the registry
    /// (it was moved to another connection, see [`ReplyRegistry`](super::ReplyRegistry)).
    #[error("request may have been delivered; the connection ended{}", match .0 {
        Some(e) => format!(": {e}"),
        None => String::new(),
    })]
    MaybeDelivered(#[source] Option<ConnectionError>),
    /// The request was never written: the connection ended before its handshake completed
    /// (requests sent then are held, see the [connection documentation](crate::connection#sending)),
    /// or before one byte of the request's frame was handed to the transport (see
    /// [Written offsets](crate::connection#written-offsets)), or it had already ended (`None`
    /// when only the peer's side has closed). Safe to resend.
    #[error("request not sent; the connection {}", match .0 {
        Some(e) => format!("ended: {e}"),
        None => "is closing".to_owned(),
    })]
    NotSent(#[source] Option<ConnectionError>),
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
    /// A reply came but could not be decoded, or has another file identifier than the
    /// expected reply's (`serialization_failed`, 1044). The request **was delivered** and its
    /// outcome is unknown.
    #[error("the reply could not be decoded: {0}")]
    Decode(DecodeError),
    /// The request could not be encoded. Nothing was sent; it would fail again.
    #[error("the request could not be encoded: {0}")]
    Encode(EncodeError),
    /// The request does not fit in a frame (`PACKET_LIMIT`). Nothing was sent; it would fail
    /// again.
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

    /// The FDB error code a C++ client would get from `tryGetReply` in the same situation, or
    /// the closest one.
    ///
    /// | variant | code | C++ |
    /// | --- | --- | --- |
    /// | `Fdb(e)` | `e.code` | the error the server sent |
    /// | `BrokenPromise` | 1030 `request_maybe_delivered` | see below |
    /// | `MaybeDelivered` | 1030 `request_maybe_delivered` | `tryGetReply` on a disconnect |
    /// | `NotSent` | 1026 `connection_failed` | (C++ cannot tell; it reports 1030) |
    /// | `EndpointNotFound` | 1030 `request_maybe_delivered` | `tryGetReply` once the endpoint is failed |
    /// | `Unauthorized` | 6001 `unauthorized_attempt` | `tryGetReply` / `getReply` |
    /// | `Decode` | 1044 `serialization_failed` | |
    /// | `Encode` | 4100 `internal_error` | |
    /// | `TooLarge` | 1500 `platform_error` | (C++ logs `PacketLimitExceeded` and sends anyway) |
    /// | `Cancelled` | 1101 `operation_cancelled` | |
    /// | `Backpressure`, `TooManyPending` | 1026 `connection_failed` | (no C++ limit: not sent, like `NotSent`) |
    ///
    /// **`BrokenPromise` is 1030, not 1100.** `tryGetReply`'s `waitValueOrSignal` catches the
    /// `broken_promise`, marks the endpoint failed and returns `request_maybe_delivered`
    /// (`fdbrpc/include/fdbrpc/genericactors.actor.h` 7.4:398-403, `genericactors.h`
    /// 8.0:401-406), and `tryCommit` wraps its direct `tryGetReply` in
    /// `brokenPromiseToMaybeDelivered`. Only `getReply` lets 1100 through. Match on the
    /// variant to tell the two apart.
    pub fn fdb_code(&self) -> u16 {
        match self {
            RequestError::Fdb(e) => e.code,
            RequestError::BrokenPromise
            | RequestError::MaybeDelivered(_)
            | RequestError::EndpointNotFound { .. } => REQUEST_MAYBE_DELIVERED,
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

    /// Whether the request certainly never reached the endpoint, so that sending it again
    /// cannot execute it twice: `NotSent`, `Backpressure`, `TooManyPending`, `Encode`,
    /// `TooLarge`, `Unauthorized` (the peer refuses delivery), and `EndpointNotFound` for a
    /// request refused because the endpoint was already known to be gone. This is the
    /// at-most-once safety predicate; whether a resend can succeed is
    /// [`should_retry`](Self::should_retry).
    pub fn certainly_not_delivered(&self) -> bool {
        match self {
            RequestError::NotSent(_)
            | RequestError::Backpressure
            | RequestError::TooManyPending { .. }
            | RequestError::Encode(_)
            | RequestError::TooLarge(_)
            | RequestError::Unauthorized { .. } => true,
            RequestError::EndpointNotFound {
                maybe_delivered, ..
            } => !maybe_delivered,
            RequestError::Fdb(_)
            | RequestError::BrokenPromise
            | RequestError::MaybeDelivered(_)
            | RequestError::Decode(_)
            | RequestError::Cancelled => false,
        }
    }

    /// Whether a load balancer should resend the request automatically: it was
    /// [certainly not delivered](Self::certainly_not_delivered) and the failure is transient
    /// (`NotSent`, `Backpressure`, `TooManyPending`, and `EndpointNotFound` refused at once,
    /// which calls for the endpoint's replacement or another alternative). `Encode`, `TooLarge`
    /// and `Unauthorized` would fail the same way again.
    pub fn should_retry(&self) -> bool {
        self.certainly_not_delivered()
            && !matches!(
                self,
                RequestError::Encode(_)
                    | RequestError::TooLarge(_)
                    | RequestError::Unauthorized { .. }
            )
    }

    /// Whether the request may have been delivered and acted on without a reply saying what
    /// became of it: `MaybeDelivered`, `BrokenPromise`, `Cancelled`, `Decode` (a reply came,
    /// but cannot be read), `EndpointNotFound` for a request that was pending, and a server
    /// error reply of `commit_unknown_result`, `request_maybe_delivered`, `default_error_or`
    /// (a reply with no arm set) or `broken_promise`. `broken_promise` and
    /// `request_maybe_delivered` are the errors `loadBalance` calls `maybeDelivered`
    /// (`fdbrpc/include/fdbrpc/LoadBalance.actor.h` 7.4:591, `LoadBalance.h` 8.0:206) and, with
    /// `AtMostOnce`, throws as `request_maybe_delivered` instead of retrying (7.4:616-618 and
    /// 1108-1115, 8.0:232 and 735-741).
    pub fn maybe_delivered(&self) -> bool {
        match self {
            RequestError::Fdb(e) => matches!(
                e.code,
                COMMIT_UNKNOWN_RESULT | REQUEST_MAYBE_DELIVERED | DEFAULT_ERROR_OR | BROKEN_PROMISE
            ),
            RequestError::BrokenPromise
            | RequestError::MaybeDelivered(_)
            | RequestError::Decode(_)
            | RequestError::Cancelled => true,
            RequestError::EndpointNotFound {
                maybe_delivered, ..
            } => *maybe_delivered,
            RequestError::NotSent(_)
            | RequestError::Unauthorized { .. }
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
