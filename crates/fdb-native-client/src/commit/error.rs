//! Errors of the commit path.

use fdb_native_schema::domain::KeyRange;
use fdb_native_transport::rpc::RequestError;
use fdb_native_wire::FdbError;

/// The FDB error codes the commit path produces or interprets (`flow/include/flow/
/// error_definitions.h`; the same on 7.4 and 8.0 unless noted).
pub mod error_code {
    /// `transaction_too_old`.
    pub const TRANSACTION_TOO_OLD: u16 = 1007;
    /// `future_version`.
    pub const FUTURE_VERSION: u16 = 1009;
    /// `not_committed`: the transaction conflicted.
    pub const NOT_COMMITTED: u16 = 1020;
    /// `commit_unknown_result`: the commit may or may not have happened.
    pub const COMMIT_UNKNOWN_RESULT: u16 = 1021;
    /// `connection_failed`: what a request that was certainly not sent maps to.
    pub const CONNECTION_FAILED: u16 = 1026;
    /// `request_maybe_delivered`.
    pub const REQUEST_MAYBE_DELIVERED: u16 = 1030;
    /// `process_behind`.
    pub const PROCESS_BEHIND: u16 = 1037;
    /// `database_locked`.
    pub const DATABASE_LOCKED: u16 = 1038;
    /// `commit_proxy_memory_limit_exceeded`.
    pub const COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1042;
    /// `batch_transaction_throttled`.
    pub const BATCH_TRANSACTION_THROTTLED: u16 = 1051;
    /// `grv_proxy_memory_limit_exceeded`.
    pub const GRV_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1078;
    /// `broken_promise`.
    pub const BROKEN_PROMISE: u16 = 1100;
    /// `operation_cancelled`.
    pub const OPERATION_CANCELLED: u16 = 1101;
    /// `never_reply` (8.0's `tryCommit` treats it like `request_maybe_delivered`).
    pub const NEVER_REPLY: u16 = 1104;
    /// `tag_throttled`.
    pub const TAG_THROTTLED: u16 = 1213;
    /// `proxy_tag_throttled` (7.4 only).
    pub const PROXY_TAG_THROTTLED: u16 = 1223;
    /// `transaction_throttled_hot_shard`.
    pub const TRANSACTION_THROTTLED_HOT_SHARD: u16 = 1235;
    /// `transaction_rejected_range_locked`.
    pub const TRANSACTION_REJECTED_RANGE_LOCKED: u16 = 1242;
    /// `client_invalid_operation`.
    pub const CLIENT_INVALID_OPERATION: u16 = 2000;
    /// `inverted_range`.
    pub const INVERTED_RANGE: u16 = 2005;
    /// `invalid_option_value`.
    pub const INVALID_OPTION_VALUE: u16 = 2006;
    /// `transaction_read_only`.
    pub const TRANSACTION_READ_ONLY: u16 = 2023;
    /// `transaction_too_large`.
    pub const TRANSACTION_TOO_LARGE: u16 = 2101;
    /// `key_too_large`.
    pub const KEY_TOO_LARGE: u16 = 2102;
    /// `value_too_large`.
    pub const VALUE_TOO_LARGE: u16 = 2103;
    /// `internal_error`.
    pub const INTERNAL_ERROR: u16 = 4100;
}

use error_code::*;

/// Why a commit did not commit (or may not have).
///
/// **This is the commit path's own error type.** It maps to the FDB error codes of
/// [`code`](Self::code); it will be unified with the client's general error type when the
/// transaction API is built (let.6).
///
/// | variant | code | when |
/// | --- | --- | --- |
/// | [`KeyTooLarge`](Self::KeyTooLarge) | 2102 `key_too_large` | a set/atomic key over the limit |
/// | [`ValueTooLarge`](Self::ValueTooLarge) | 2103 `value_too_large` | a value over 100000 bytes |
/// | [`TransactionTooLarge`](Self::TransactionTooLarge) | 2101 `transaction_too_large` | `getSize()` over the size limit |
/// | [`TransactionReadOnly`](Self::TransactionReadOnly) | 2023 `transaction_read_only` | read-only option and writes |
/// | [`InvalidOptionValue`](Self::InvalidOptionValue) | 2006 `invalid_option_value` | the size limit option out of range |
/// | [`InvertedRange`](Self::InvertedRange) | 2005 `inverted_range` | a range with `begin > end` |
/// | [`Unsupported`](Self::Unsupported) | 2000 `client_invalid_operation` | a mutation type this client does not send |
/// | [`NotCommitted`](Self::NotCommitted) | 1020 `not_committed` | the transaction conflicted |
/// | [`CommitUnknownResult`](Self::CommitUnknownResult) | 1021 `commit_unknown_result` | the commit may have happened |
/// | [`TransactionTooOld`](Self::TransactionTooOld) | 1007 `transaction_too_old` | the proxy replied so |
/// | [`Server`](Self::Server) | the proxy's code | any other error the proxy replied (`database_locked` 1038, `commit_proxy_memory_limit_exceeded` 1042, `batch_transaction_throttled` 1051, `process_behind` 1037, `tag_throttled` 1213, ...) |
/// | [`NotSent`](Self::NotSent) | 1026 `connection_failed` | the request was certainly not delivered |
/// | [`Request`](Self::Request) | the transport's | a definite transport failure (unauthorized, unencodable) |
/// | [`Closed`](Self::Closed) | 1101 `operation_cancelled` | the cluster monitor was shut down |
/// | [`Internal`](Self::Internal) | 4100 `internal_error` | a request that cannot be encoded |
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CommitError {
    /// A key is longer than the limit for it (`key_too_large`, 2102).
    #[error("key_too_large: a key of {size} bytes exceeds the limit of {limit}")]
    KeyTooLarge {
        /// The key's size.
        size: usize,
        /// The limit.
        limit: usize,
    },
    /// A value is longer than `VALUE_SIZE_LIMIT` (`value_too_large`, 2103).
    #[error("value_too_large: a value of {size} bytes exceeds the limit of {limit}")]
    ValueTooLarge {
        /// The value's size.
        size: usize,
        /// The limit.
        limit: usize,
    },
    /// The transaction is bigger than its size limit (`transaction_too_large`, 2101).
    #[error("transaction_too_large: {size} bytes exceed the limit of {limit}")]
    TransactionTooLarge {
        /// `getSize()`.
        size: u64,
        /// The limit.
        limit: u64,
    },
    /// The transaction has writes but was set read-only (`transaction_read_only`, 2023).
    #[error("transaction_read_only")]
    TransactionReadOnly,
    /// A [`CommitOptions`](super::CommitOptions) value is out of range
    /// (`invalid_option_value`, 2006).
    #[error("invalid_option_value: {0}")]
    InvalidOptionValue(&'static str),
    /// A range has `begin > end` (`inverted_range`, 2005).
    #[error("inverted_range: begin is larger than end")]
    InvertedRange,
    /// A mutation this client does not send (`client_invalid_operation`, 2000): the
    /// versionstamp operations, which are not supported yet, or a type that is not a client
    /// mutation. The byte is the mutation type.
    #[error("client_invalid_operation: mutation type {0} is not supported")]
    Unsupported(u8),
    /// The transaction conflicted (`not_committed`, 1020). Nothing was committed; retry the
    /// transaction.
    #[error("not_committed: the transaction conflicted")]
    NotCommitted {
        /// With `report_conflicting_keys`: the read conflict ranges (as sent) that
        /// conflicted, deduplicated, in request order. `None` if the proxy did not say.
        conflicting_ranges: Option<Vec<KeyRange>>,
    },
    /// The commit may or may not have happened (`commit_unknown_result`, 1021). Once the
    /// request may have been delivered this is the **only** error returned, even if the
    /// conflicting dummy transaction that settles it could not be committed (the monitor was
    /// shut down, a definite error from the proxy). When the dummy did commit, the original is
    /// no longer in flight: either it committed or it never will; with `causal_write_risky`
    /// (or a dummy that failed) it may still be.
    #[error("commit_unknown_result: the transaction may have committed")]
    CommitUnknownResult,
    /// The proxy said the read version is too old (`transaction_too_old`, 1007).
    #[error("transaction_too_old")]
    TransactionTooOld,
    /// Another error the commit proxy replied with.
    #[error("the commit proxy replied with FDB error {}", .0.code)]
    Server(FdbError),
    /// The request was certainly not delivered to a proxy (the connection failed before the
    /// request was sent, or every proxy refused it), so it is safe to commit again; this is
    /// `connection_failed` (1026). C++ cannot tell this from a maybe-delivered failure and
    /// reports `commit_unknown_result` for it; the transport can. For the read-version
    /// request a commit may make (see [`Committer`](super::Committer)) it also covers a
    /// failure that may have been delivered, which is harmless for a read version.
    #[error("connection_failed: {0}")]
    NotSent(RequestError),
    /// A definite transport failure that retrying will not fix (an unauthorized endpoint, a
    /// request that cannot be encoded or does not fit a frame).
    #[error("request failed: {0}")]
    Request(RequestError),
    /// The cluster monitor was shut down (`operation_cancelled`, 1101).
    #[error("operation_cancelled: the cluster monitor was shut down")]
    Closed,
    /// The commit request could not be built (`internal_error`, 4100).
    #[error("internal_error: {0}")]
    Internal(String),
}

impl CommitError {
    /// The FDB error code a C++ client would report.
    pub fn code(&self) -> u16 {
        match self {
            CommitError::KeyTooLarge { .. } => KEY_TOO_LARGE,
            CommitError::ValueTooLarge { .. } => VALUE_TOO_LARGE,
            CommitError::TransactionTooLarge { .. } => TRANSACTION_TOO_LARGE,
            CommitError::TransactionReadOnly => TRANSACTION_READ_ONLY,
            CommitError::InvalidOptionValue(_) => INVALID_OPTION_VALUE,
            CommitError::InvertedRange => INVERTED_RANGE,
            CommitError::Unsupported(_) => CLIENT_INVALID_OPERATION,
            CommitError::NotCommitted { .. } => NOT_COMMITTED,
            CommitError::CommitUnknownResult => COMMIT_UNKNOWN_RESULT,
            CommitError::TransactionTooOld => TRANSACTION_TOO_OLD,
            CommitError::Server(e) => e.code,
            CommitError::NotSent(_) => CONNECTION_FAILED,
            CommitError::Request(e) => e.fdb_code(),
            CommitError::Closed => OPERATION_CANCELLED,
            CommitError::Internal(_) => INTERNAL_ERROR,
        }
    }

    /// Whether the commit may have been applied: only
    /// [`CommitUnknownResult`](Self::CommitUnknownResult). Every other error means the
    /// transaction certainly did not commit.
    pub fn maybe_committed(&self) -> bool {
        matches!(self, CommitError::CommitUnknownResult)
    }

    /// Whether `Transaction::onError` would retry the transaction after this error: the list
    /// in `Transaction::onError` (`NativeAPI.actor.cpp` 7.4:8119-8156): `not_committed`,
    /// `commit_unknown_result`, `database_locked`, `commit_proxy_memory_limit_exceeded`,
    /// `grv_proxy_memory_limit_exceeded`, `process_behind`, `batch_transaction_throttled`,
    /// `tag_throttled`, `proxy_tag_throttled`, `transaction_throttled_hot_shard`,
    /// `transaction_rejected_range_locked` (the default `TRANSACTION_LOCK_REJECTION_RETRIABLE`
    /// is taken to be true), and (with a short fixed delay) `transaction_too_old` and
    /// `future_version`. A [`NotSent`](Self::NotSent) is not in C++'s list (C++ reports
    /// `commit_unknown_result` for such a failure, which is in it, so a retry loop retries
    /// it); the request was certainly not delivered, so it is retryable here too, see
    /// [`is_safe_to_retry`](Self::is_safe_to_retry).
    pub fn is_retryable(&self) -> bool {
        match self {
            CommitError::NotCommitted { .. }
            | CommitError::CommitUnknownResult
            | CommitError::TransactionTooOld
            | CommitError::NotSent(_) => true,
            CommitError::Server(e) => matches!(
                e.code,
                DATABASE_LOCKED
                    | COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED
                    | GRV_PROXY_MEMORY_LIMIT_EXCEEDED
                    | PROCESS_BEHIND
                    | BATCH_TRANSACTION_THROTTLED
                    | TAG_THROTTLED
                    | PROXY_TAG_THROTTLED
                    | TRANSACTION_THROTTLED_HOT_SHARD
                    | TRANSACTION_REJECTED_RANGE_LOCKED
                    | FUTURE_VERSION
            ),
            _ => false,
        }
    }

    /// Whether the commit request was certainly not applied and sending the same request
    /// again cannot execute it twice: every error but
    /// [`CommitUnknownResult`](Self::CommitUnknownResult). (Whether a resend can *succeed* is
    /// [`is_retryable`](Self::is_retryable).)
    pub fn is_safe_to_retry(&self) -> bool {
        !self.maybe_committed()
    }

    /// The error for an FDB error the commit proxy replied with.
    pub(crate) fn from_reply_code(error: FdbError) -> CommitError {
        match error.code {
            NOT_COMMITTED => CommitError::NotCommitted {
                conflicting_ranges: None,
            },
            TRANSACTION_TOO_OLD => CommitError::TransactionTooOld,
            _ => CommitError::Server(error),
        }
    }
}
