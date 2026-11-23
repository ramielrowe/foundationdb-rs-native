//! [`Error`]: the one error type of the client, an FDB error as the client API reports it,
//! with the retry semantics of C++'s `Transaction::onError` and `fdb_error_predicate`.
//!
//! An FDB error is a 16-bit code (`flow/include/flow/error_definitions.h`). Errors reach a
//! client from these places, and all of them become an [`Error`]:
//!
//! * a **server** replied with one (`transaction_too_old` from a storage server,
//!   `not_committed` from a commit proxy, ...);
//! * the **transport** failed a request ([`RequestError`]; the error keeps it as its
//!   [`source`](std::error::Error::source) and takes the code a C++ client would have seen,
//!   see [`Error::from_request`]);
//! * the client's own **logic** decided on one (`all_alternatives_failed`,
//!   `database_locked`, `client_invalid_operation`, ...);
//! * the **commit path** ([`Committer`](crate::Committer), [`CommitBuffer`](crate::CommitBuffer)):
//!   `not_committed` (with the conflicting ranges, [`Error::conflicting_ranges`]),
//!   `commit_unknown_result`, `key_too_large`, `transaction_too_large`, ...;
//! * **opening a database** (`no_cluster_file_found`, `connection_string_invalid`,
//!   `tls_error`, ...; the cause is the [`source`](std::error::Error::source)).
//!
//! Besides its code an error may carry a message (sizes and limits, what was wrong), the
//! cause, and the conflicting ranges. Errors are not comparable: compare their
//! [`code`](Error::code)s, and ask the predicates (two errors of one code may differ in
//! whether the transaction may have committed).
//!
//! # Retry semantics
//!
//! Citations are `file:line` on `origin/release-7.4` / `origin/release-8.0`; `fdb_c` is
//! `bindings/c/fdb_c.cpp`, `NativeAPI` is `fdbclient/NativeAPI.actor.cpp` (7.4) /
//! `fdbclient/NativeAPI.cpp` (8.0).
//!
//! * [`is_maybe_committed`](Error::is_maybe_committed) is
//!   `fdb_error_predicate(MAYBE_COMMITTED)` (`fdb_c` 7.4:83-85, 8.0:213-215):
//!   `commit_unknown_result` (1021) and `cluster_version_changed` (1039).
//! * [`is_retryable_not_committed`](Error::is_retryable_not_committed) is
//!   `RETRYABLE_NOT_COMMITTED` (7.4:86-94, 8.0:216-224): `not_committed`,
//!   `transaction_too_old`, `future_version`, `database_locked`,
//!   `grv_proxy_memory_limit_exceeded`, `commit_proxy_memory_limit_exceeded`,
//!   `transaction_throttled_hot_shard`, `batch_transaction_throttled`, `process_behind`,
//!   `tag_throttled`, `transaction_rejected_range_locked`, and on 7.4 `proxy_tag_throttled`.
//! * [`is_retryable`](Error::is_retryable) is `RETRYABLE`, the union of the two.
//! * [`transaction_retry`](Error::transaction_retry) (and
//!   [`on_error`](Error::on_error)) is what `Transaction::onError`
//!   does with the error (NativeAPI 7.4:8112-8167, 8.0:5908-5958): reset the transaction and
//!   retry after the transaction's exponential backoff ([`TransactionRetry::Backoff`]) for
//!   the codes listed there, after `FUTURE_VERSION_RETRY_DELAY` (10 ms, capped by the
//!   transaction's max backoff; [`TransactionRetry::FutureVersion`]) for
//!   `transaction_too_old` and `future_version`, and rethrow anything else. It differs from
//!   the predicate in two codes: `cluster_version_changed` is not retried by `onError` (the
//!   multi-version client handles it, and this client has no counterpart), and
//!   `blob_granule_request_failed` (7.4 only) is retried by `onError` but not in the
//!   predicate. `success` (0) is not an error: `onError(success)` fails with
//!   `client_invalid_operation` (7.4:8118-8120, 8.0:5914-5916).
//! * **`commit_unknown_result` is retried, but the transaction may already have
//!   committed.** `onError` retries it like a conflict, so a retry loop re-runs a
//!   transaction that may have been applied: the transaction must be idempotent (or check
//!   whether its first attempt landed) for that to be safe. That is what
//!   [`is_maybe_committed`](Error::is_maybe_committed) is for.
//!
//! # Commits
//!
//! A request that may have reached the server without a reply saying what became of it is
//! `request_maybe_delivered` for a read ([`Error::from_request`]); for a commit, C++'s
//! `tryCommit` turns it into `commit_unknown_result` (NativeAPI 7.4:7060-7061, 8.0:4755-4756):
//! [`Error::from_commit_request`]. The [`Committer`](crate::Committer) reports every commit
//! failure that way.
//!
//! ## Where the predicates know more than the code
//!
//! The predicates are those of the code, with two exceptions, both about commits, where this
//! client knows something a C++ client cannot. Neither changes the code.
//!
//! * **A commit that was certainly not delivered** (every proxy tried refused it before it
//!   was sent: the transport can tell, `tryGetReply` cannot) is `connection_failed` (1026),
//!   as [`from_commit_request`](Error::from_commit_request) maps it. C++ would have reported
//!   `commit_unknown_result` and retried it. So that a retry loop still retries it, such an
//!   error is [`is_retryable_not_committed`](Error::is_retryable_not_committed) and `onError`
//!   retries it with the backoff ([`TransactionRetry::Backoff`]); a plain
//!   `connection_failed` is neither.
//! * **An error that hides a commit that may have happened**: a transaction that times out
//!   (`transaction_timed_out`, 1031) or is cancelled (`transaction_cancelled`, 1025) while
//!   its commit is outstanding, as C++ reports it, may still have committed; a retry loop
//!   that gives up after a `commit_unknown_result` returns the last error, which may be
//!   another. Such an error [`is_maybe_committed`](Error::is_maybe_committed), whatever its
//!   code. (It is not made retryable by that: `onError` decides by the code.)
//!
//! **Both releases.** The two releases agree on every code except a few that exist on one
//! only (7.4's tenant and blob errors, `proxy_tag_throttled` 1223 and
//! `blob_granule_request_failed` 1079; 8.0's `transaction_grv_queue_rejected` 1251 and
//! bulk-load errors), and 1249 and 1250, which name different (server-internal) errors.
//! A code a release does not define cannot come from a server of that release, so the
//! predicates are the union of the two releases' rules. 8.0's
//! `transaction_grv_queue_rejected` is not retryable on 8.0 (`onError` rethrows it), and is
//! not here either.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::KeyRange;
use fdb_native_transport::rpc::RequestError;
use fdb_native_wire::FdbError;

mod table;

/// FDB error codes the client produces or interprets (`error_definitions.h`; the same code
/// on 7.4 and 8.0 unless noted).
pub mod codes {
    /// `success` (0).
    pub const SUCCESS: u16 = 0;
    /// `wrong_shard_server` (1001): the storage server does not (or no longer) holds the key's
    /// shard; the client invalidates its location cache and retries.
    pub const WRONG_SHARD_SERVER: u16 = 1001;
    /// `timed_out` (1004).
    pub const TIMED_OUT: u16 = 1004;
    /// `all_alternatives_failed` (1006): no replica of a shard could be reached for a while.
    pub const ALL_ALTERNATIVES_FAILED: u16 = 1006;
    /// `transaction_too_old` (1007): the read version is older than the storage servers keep.
    pub const TRANSACTION_TOO_OLD: u16 = 1007;
    /// `future_version` (1009): the storage server has not caught up to the read version.
    pub const FUTURE_VERSION: u16 = 1009;
    /// `not_committed` (1020): the transaction conflicted.
    pub const NOT_COMMITTED: u16 = 1020;
    /// `commit_unknown_result` (1021): the commit may or may not have happened.
    pub const COMMIT_UNKNOWN_RESULT: u16 = 1021;
    /// `transaction_cancelled` (1025).
    pub const TRANSACTION_CANCELLED: u16 = 1025;
    /// `connection_failed` (1026).
    pub const CONNECTION_FAILED: u16 = 1026;
    /// `request_maybe_delivered` (1030).
    pub const REQUEST_MAYBE_DELIVERED: u16 = 1030;
    /// `transaction_timed_out` (1031).
    pub const TRANSACTION_TIMED_OUT: u16 = 1031;
    /// `too_many_watches` (1032): the database already has as many outstanding watches as
    /// its cap (`MAX_WATCHES`, default 10 000).
    pub const TOO_MANY_WATCHES: u16 = 1032;
    /// `watch_cancelled` (1029): a storage server holds too many watches, so the client
    /// polls instead.
    pub const WATCH_CANCELLED: u16 = 1029;
    /// `process_behind` (1037): the storage server lags too far behind.
    pub const PROCESS_BEHIND: u16 = 1037;
    /// `database_locked` (1038).
    pub const DATABASE_LOCKED: u16 = 1038;
    /// `cluster_version_changed` (1039).
    pub const CLUSTER_VERSION_CHANGED: u16 = 1039;
    /// `commit_proxy_memory_limit_exceeded` (1042).
    pub const COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1042;
    /// `serialization_failed` (1044).
    pub const SERIALIZATION_FAILED: u16 = 1044;
    /// `batch_transaction_throttled` (1051).
    pub const BATCH_TRANSACTION_THROTTLED: u16 = 1051;
    /// `grv_proxy_memory_limit_exceeded` (1078).
    pub const GRV_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1078;
    /// `blob_granule_request_failed` (1079; 7.4 only).
    pub const BLOB_GRANULE_REQUEST_FAILED: u16 = 1079;
    /// `never_reply` (1104; 8.0's `tryCommit` treats it as an unknown result).
    pub const NEVER_REPLY: u16 = 1104;
    /// `broken_promise` (1100).
    pub const BROKEN_PROMISE: u16 = 1100;
    /// `operation_cancelled` (1101).
    pub const OPERATION_CANCELLED: u16 = 1101;
    /// `server_overloaded` (1211): the load balancer tries another replica.
    pub const SERVER_OVERLOADED: u16 = 1211;
    /// `tag_throttled` (1213).
    pub const TAG_THROTTLED: u16 = 1213;
    /// `proxy_tag_throttled` (1223; 7.4 only).
    pub const PROXY_TAG_THROTTLED: u16 = 1223;
    /// `transaction_throttled_hot_shard` (1235).
    pub const TRANSACTION_THROTTLED_HOT_SHARD: u16 = 1235;
    /// `transaction_rejected_range_locked` (1242).
    pub const TRANSACTION_REJECTED_RANGE_LOCKED: u16 = 1242;
    /// `transaction_grv_queue_rejected` (1251; 8.0 only).
    pub const TRANSACTION_GRV_QUEUE_REJECTED: u16 = 1251;
    /// `io_error` (1510).
    pub const IO_ERROR: u16 = 1510;
    /// `no_cluster_file_found` (1515).
    pub const NO_CLUSTER_FILE_FOUND: u16 = 1515;
    /// `file_too_large` (1516).
    pub const FILE_TOO_LARGE: u16 = 1516;
    /// `client_invalid_operation` (2000).
    pub const CLIENT_INVALID_OPERATION: u16 = 2000;
    /// `key_outside_legal_range` (2004).
    pub const KEY_OUTSIDE_LEGAL_RANGE: u16 = 2004;
    /// `inverted_range` (2005).
    pub const INVERTED_RANGE: u16 = 2005;
    /// `invalid_option_value` (2006).
    pub const INVALID_OPTION_VALUE: u16 = 2006;
    /// `read_version_already_set` (2010).
    pub const READ_VERSION_ALREADY_SET: u16 = 2010;
    /// `version_invalid` (2011).
    pub const VERSION_INVALID: u16 = 2011;
    /// `range_limits_invalid` (2012).
    pub const RANGE_LIMITS_INVALID: u16 = 2012;
    /// `used_during_commit` (2017).
    pub const USED_DURING_COMMIT: u16 = 2017;
    /// `invalid_mutation_type` (2018).
    pub const INVALID_MUTATION_TYPE: u16 = 2018;
    /// `transaction_invalid_version` (2020).
    pub const TRANSACTION_INVALID_VERSION: u16 = 2020;
    /// `no_commit_version` (2021).
    pub const NO_COMMIT_VERSION: u16 = 2021;
    /// `transaction_read_only` (2023).
    pub const TRANSACTION_READ_ONLY: u16 = 2023;
    /// `transaction_too_large` (2101).
    pub const TRANSACTION_TOO_LARGE: u16 = 2101;
    /// `key_too_large` (2102).
    pub const KEY_TOO_LARGE: u16 = 2102;
    /// `value_too_large` (2103).
    pub const VALUE_TOO_LARGE: u16 = 2103;
    /// `connection_string_invalid` (2104).
    pub const CONNECTION_STRING_INVALID: u16 = 2104;
    /// `tls_error` (2107).
    pub const TLS_ERROR: u16 = 2107;
    /// `exact_mode_without_limits` (2210).
    pub const EXACT_MODE_WITHOUT_LIMITS: u16 = 2210;
    /// `unknown_error` (4000).
    pub const UNKNOWN_ERROR: u16 = 4000;
    /// `internal_error` (4100).
    pub const INTERNAL_ERROR: u16 = 4100;
}

use codes::*;

/// Which releases define an error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Release {
    Both,
    V7_4,
    V8_0,
}

impl Release {
    fn has(self, schema: SchemaVersion) -> bool {
        match self {
            Release::Both => true,
            Release::V7_4 => schema == SchemaVersion::V7_4,
            Release::V8_0 => schema == SchemaVersion::V8_0,
        }
    }
}

/// What `Transaction::onError` does with an error it retries. See
/// [`Error::transaction_retry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransactionRetry {
    /// Reset and retry after the transaction's exponential backoff (`getBackoff`: a random
    /// fraction of the current backoff, which then grows by `BACKOFF_GROWTH_RATE` up to the
    /// max backoff).
    Backoff,
    /// Reset and retry after `FUTURE_VERSION_RETRY_DELAY` (10 ms), capped by the
    /// transaction's max backoff: `transaction_too_old` and `future_version`.
    FutureVersion,
}

/// An FDB error, as the client API reports it: the code a C++ client would get, with what the
/// client knows besides (a message, the cause, the conflicting ranges of a conflict). See the
/// [module documentation](self) for the retry predicates.
///
/// Errors do not implement `PartialEq`: compare [`code`](Self::code)s, and use the
/// predicates, which may say more than the code (see the [module
/// documentation](self#where-the-predicates-know-more-than-the-code)).
///
/// ```
/// use fdb_native_client::{Error, TransactionRetry, error::codes};
///
/// let e = Error::new(codes::FUTURE_VERSION);
/// assert_eq!(e.name(), Some("future_version"));
/// assert!(e.is_retryable() && !e.is_maybe_committed());
/// assert_eq!(e.transaction_retry(), Some(TransactionRetry::FutureVersion));
/// assert_eq!(e.to_string(), "Request for future version (future_version, 1009)");
///
/// let unknown = Error::new(codes::COMMIT_UNKNOWN_RESULT);
/// assert!(unknown.is_maybe_committed() && unknown.is_retryable());
/// assert!(!Error::new(codes::KEY_TOO_LARGE).is_retryable());
/// ```
#[derive(Clone)]
pub struct Error {
    code: u16,
    detail: Option<Arc<Detail>>,
}

/// What a commit error says beyond its code (see the module documentation).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CommitState {
    /// The code says it all.
    #[default]
    ByCode,
    /// The commit certainly did not reach a proxy: retryable, not committed.
    NotDelivered,
    /// The commit may have happened, whatever the code says.
    MaybeCommitted,
}

#[derive(Clone, Default)]
struct Detail {
    message: Option<Cow<'static, str>>,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
    conflicting_ranges: Option<Vec<KeyRange>>,
    commit: CommitState,
}

impl Error {
    /// The error with `code`.
    pub const fn new(code: u16) -> Error {
        Error { code, detail: None }
    }

    /// The error with `code` and a message saying more (shown after the description).
    pub fn with_message(code: u16, message: impl Into<Cow<'static, str>>) -> Error {
        Error::new(code).map_detail(|d| d.message = Some(message.into()))
    }

    /// The error with `code`, caused by `source` (its
    /// [`source`](std::error::Error::source)).
    pub fn with_source(code: u16, source: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error::new(code).map_detail(|d| d.source = Some(Arc::new(source)))
    }

    fn map_detail(mut self, f: impl FnOnce(&mut Detail)) -> Error {
        let detail = self.detail.get_or_insert_with(Default::default);
        f(Arc::make_mut(detail));
        self
    }

    fn commit_state(&self) -> CommitState {
        self.detail
            .as_ref()
            .map_or(CommitState::ByCode, |d| d.commit)
    }

    /// The error a C++ client gets when a request fails the way `error` says, as
    /// `tryGetReply` reports it, keeping `error` as the source:
    ///
    /// * a server's error reply ([`RequestError::Fdb`]) is that error;
    /// * a request that may have been delivered (`MaybeDelivered`, `BrokenPromise`, a pending
    ///   request to an endpoint that went away) is `request_maybe_delivered` (1030), as
    ///   `tryGetReply` reports a broken connection or a broken promise;
    /// * anything else is [`RequestError::fdb_code`] (`NotSent` is `connection_failed`,
    ///   `Decode` `serialization_failed`, `Encode` `internal_error`, ...).
    pub fn from_request(error: RequestError) -> Error {
        let code = error.fdb_code();
        Error::with_source(code, error)
    }

    /// The error a commit request gets when it fails the way `error` says (`tryCommit`'s
    /// `catch`, as the [`Committer`](crate::Committer) classifies it):
    ///
    /// * a request that may have been delivered with no definite answer
    ///   ([`RequestError::maybe_delivered`]: a broken connection, a cancellation, an
    ///   undecodable reply, a pending request to an endpoint that went away, and the server
    ///   replies `broken_promise` (1100), `request_maybe_delivered` (1030),
    ///   `commit_unknown_result` (1021) and `default_error_or` (1035)), and a server reply of
    ///   `never_reply` (1104, which 8.0's `tryCommit` treats so), is `commit_unknown_result`
    ///   (1021): the transaction may have committed;
    /// * a server's other error replies keep their code (`not_committed`,
    ///   `transaction_too_old`, ...);
    /// * a request that certainly was not delivered is [`from_request`](Self::from_request)'s
    ///   code (`connection_failed` for `NotSent`, ...); if it was a transient refusal
    ///   ([`RequestError::should_retry`]: the connection failed before the request was sent)
    ///   it is retryable (see the [module
    ///   documentation](self#where-the-predicates-know-more-than-the-code)).
    ///
    /// The [`RequestError`] is kept as the source.
    pub fn from_commit_request(error: RequestError) -> Error {
        let code = if commit_maybe_delivered(&error) {
            COMMIT_UNKNOWN_RESULT
        } else {
            error.fdb_code()
        };
        let not_delivered = error.should_retry();
        let e = Error::with_source(code, error);
        if not_delivered {
            e.map_detail(|d| d.commit = CommitState::NotDelivered)
        } else {
            e
        }
    }

    /// `connection_failed` (1026) for a request that was not sent, or whose failure does
    /// not matter (a read version the committer asked for itself): retryable, not committed.
    pub(crate) fn not_sent(error: RequestError) -> Error {
        Error::with_source(CONNECTION_FAILED, error)
            .map_detail(|d| d.commit = CommitState::NotDelivered)
    }

    /// `not_committed` (1020), with the read conflict ranges the proxy said conflicted, if it
    /// said (`report_conflicting_keys`).
    pub(crate) fn not_committed(conflicting_ranges: Option<Vec<KeyRange>>) -> Error {
        Error::new(NOT_COMMITTED).map_detail(|d| d.conflicting_ranges = conflicting_ranges)
    }

    /// This error, marked as one that may hide a commit that happened
    /// ([`is_maybe_committed`](Self::is_maybe_committed)); the code is kept.
    pub(crate) fn into_maybe_committed(self) -> Error {
        if self.is_maybe_committed() {
            return self;
        }
        self.map_detail(|d| d.commit = CommitState::MaybeCommitted)
    }

    /// The code.
    pub const fn code(&self) -> u16 {
        self.code
    }

    /// The code as an [`FdbError`].
    pub const fn fdb_error(&self) -> FdbError {
        FdbError::new(self.code)
    }

    /// The transport error behind this one, if it came from the transport.
    pub fn request_error(&self) -> Option<&RequestError> {
        self.detail
            .as_ref()?
            .source
            .as_deref()?
            .downcast_ref::<RequestError>()
    }

    /// What the client says about the error beyond its description, if anything (sizes and
    /// limits, the option that was wrong, ...).
    pub fn message(&self) -> Option<&str> {
        self.detail.as_ref()?.message.as_deref()
    }

    /// For `not_committed` with the `report_conflicting_keys` option: the read conflict
    /// ranges (as the commit sent them) that conflicted, without duplicates, in request
    /// order. `None` if the proxy did not say (or the error is another).
    pub fn conflicting_ranges(&self) -> Option<&[KeyRange]> {
        self.detail.as_ref()?.conflicting_ranges.as_deref()
    }

    /// The error's name (`"transaction_too_old"`), if either release defines the code and
    /// both that do agree on its name. See [`name_for`](Self::name_for).
    pub fn name(&self) -> Option<&'static str> {
        agreed(entries(self.code).map(|e| e.name))
    }

    /// The error's name on a server of `schema`'s release.
    pub fn name_for(&self, schema: SchemaVersion) -> Option<&'static str> {
        entries(self.code)
            .find(|e| e.release.has(schema))
            .map(|e| e.name)
    }

    /// The error's description (what `fdb_get_error` returns), if the releases that define
    /// the code agree on it. See [`description_for`](Self::description_for).
    pub fn description(&self) -> Option<&'static str> {
        agreed(entries(self.code).map(|e| e.description))
    }

    /// The error's description on a server of `schema`'s release.
    pub fn description_for(&self, schema: SchemaVersion) -> Option<&'static str> {
        entries(self.code)
            .find(|e| e.release.has(schema))
            .map(|e| e.description)
    }

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED)`: the transaction may have
    /// committed (`commit_unknown_result`, `cluster_version_changed`), and also an error of
    /// another code that hides a commit that may have happened (see the [module
    /// documentation](self#where-the-predicates-know-more-than-the-code)).
    pub fn is_maybe_committed(&self) -> bool {
        matches!(self.code, COMMIT_UNKNOWN_RESULT | CLUSTER_VERSION_CHANGED)
            || self.commit_state() == CommitState::MaybeCommitted
    }

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED)`: the transaction
    /// did not commit, and retrying it may succeed. Also a commit that certainly was not
    /// delivered; never an error marked [maybe committed](Self::is_maybe_committed).
    pub fn is_retryable_not_committed(&self) -> bool {
        match self.commit_state() {
            CommitState::NotDelivered => true,
            CommitState::MaybeCommitted => false,
            CommitState::ByCode => retryable_not_committed(self.code),
        }
    }

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE)`: retrying the transaction may
    /// succeed: the code is `RETRYABLE` (the union of the maybe-committed and the
    /// retryable-not-committed codes), or the commit certainly was not delivered.
    pub fn is_retryable(&self) -> bool {
        matches!(self.code, COMMIT_UNKNOWN_RESULT | CLUSTER_VERSION_CHANGED)
            || retryable_not_committed(self.code)
            || self.commit_state() == CommitState::NotDelivered
    }

    /// What `Transaction::onError` does with this error: `Some` if it resets the transaction
    /// and retries (and how long it waits first), `None` if it rethrows the error (or, for
    /// `success`, fails with `client_invalid_operation`; see [`on_error`](Self::on_error)).
    /// [`TransactionRetry::Backoff`] for `commit_unknown_result` means the transaction may
    /// already have committed. A commit that certainly was not delivered is retried with the
    /// backoff (C++ would have reported it as `commit_unknown_result`).
    pub fn transaction_retry(&self) -> Option<TransactionRetry> {
        if self.commit_state() == CommitState::NotDelivered {
            return Some(TransactionRetry::Backoff);
        }
        match self.code {
            NOT_COMMITTED
            | COMMIT_UNKNOWN_RESULT
            | DATABASE_LOCKED
            | COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED
            | GRV_PROXY_MEMORY_LIMIT_EXCEEDED
            | PROCESS_BEHIND
            | BATCH_TRANSACTION_THROTTLED
            | TAG_THROTTLED
            | BLOB_GRANULE_REQUEST_FAILED
            | PROXY_TAG_THROTTLED
            | TRANSACTION_THROTTLED_HOT_SHARD
            // TRANSACTION_LOCK_REJECTION_RETRIABLE is true on both releases.
            | TRANSACTION_REJECTED_RANGE_LOCKED => Some(TransactionRetry::Backoff),
            TRANSACTION_TOO_OLD | FUTURE_VERSION => Some(TransactionRetry::FutureVersion),
            _ => None,
        }
    }

    /// `Transaction::onError(e)` as a result: `Ok` with how to wait before retrying the
    /// reset transaction, or `Err` with the error the transaction fails with: this one, or
    /// `client_invalid_operation` for `success` (C++: "onError(success)" is a misuse).
    pub fn on_error(&self) -> Result<TransactionRetry, Error> {
        if self.code == SUCCESS {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        self.transaction_retry().ok_or_else(|| self.clone())
    }

    /// Whether `getBackoff` grows the backoff after this error up to
    /// `RESOURCE_CONSTRAINED_MAX_BACKOFF` instead of the transaction's max backoff
    /// (`Transaction::getBackoff`, NativeAPI 7.4:6433-6438).
    pub(crate) fn is_resource_constrained(&self) -> bool {
        matches!(
            self.code,
            COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED
                | GRV_PROXY_MEMORY_LIMIT_EXCEEDED
                | TRANSACTION_THROTTLED_HOT_SHARD
                | TRANSACTION_REJECTED_RANGE_LOCKED
        )
    }
}

/// `RETRYABLE_NOT_COMMITTED` by code.
const fn retryable_not_committed(code: u16) -> bool {
    matches!(
        code,
        NOT_COMMITTED
            | TRANSACTION_TOO_OLD
            | FUTURE_VERSION
            | DATABASE_LOCKED
            | GRV_PROXY_MEMORY_LIMIT_EXCEEDED
            | COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED
            | TRANSACTION_THROTTLED_HOT_SHARD
            | BATCH_TRANSACTION_THROTTLED
            | PROCESS_BEHIND
            | TAG_THROTTLED
            | PROXY_TAG_THROTTLED
            | TRANSACTION_REJECTED_RANGE_LOCKED
    )
}

/// The value every item has, if they all have the same one.
fn agreed(mut items: impl Iterator<Item = &'static str>) -> Option<&'static str> {
    let first = items.next()?;
    items.all(|i| i == first).then_some(first)
}

fn entries(code: u16) -> impl Iterator<Item = &'static table::Entry> {
    let start = table::ERRORS.partition_point(|e| e.code < code);
    table::ERRORS[start..]
        .iter()
        .take_while(move |e| e.code == code)
}

/// Whether a failed commit request may have been delivered without a definite answer, so
/// that the transaction may have committed: `tryCommit`'s `catch` (NativeAPI 7.4:7060-7061,
/// 8.0:4755-4757) for `request_maybe_delivered` and `commit_unknown_result` (and on 8.0
/// `never_reply`), with `broken_promise` already turned into `request_maybe_delivered` by
/// `tryGetReply` / `AtMostOnce`.
pub(crate) fn commit_maybe_delivered(error: &RequestError) -> bool {
    error.maybe_delivered() || matches!(error, RequestError::Fdb(e) if e.code == NEVER_REPLY)
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("Error");
        s.field("code", &self.code);
        if let Some(name) = self.name() {
            s.field("name", &name);
        }
        if let Some(detail) = &self.detail {
            if let Some(message) = &detail.message {
                s.field("message", message);
            }
            if let Some(source) = &detail.source {
                s.field("source", source);
            }
            if let Some(ranges) = &detail.conflicting_ranges {
                s.field("conflicting_ranges", ranges);
            }
            if detail.commit != CommitState::ByCode {
                s.field("commit", &detail.commit);
            }
        }
        s.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.description(), self.name()) {
            (Some(description), Some(name)) => write!(f, "{description} ({name}, {})", self.code)?,
            (None, Some(name)) => write!(f, "{name} ({})", self.code)?,
            _ => write!(f, "FDB error {}", self.code)?,
        }
        if let Some(detail) = &self.detail {
            if let Some(message) = &detail.message {
                write!(f, ": {message}")?;
            }
            if let Some(source) = &detail.source {
                write!(f, ": {source}")?;
            }
            match detail.commit {
                CommitState::MaybeCommitted => write!(f, " (the transaction may have committed)")?,
                CommitState::NotDelivered => write!(f, " (the commit was not delivered)")?,
                CommitState::ByCode => {}
            }
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.detail
            .as_ref()?
            .source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<FdbError> for Error {
    fn from(error: FdbError) -> Error {
        Error::new(error.code)
    }
}

impl From<RequestError> for Error {
    fn from(error: RequestError) -> Error {
        Error::from_request(error)
    }
}

impl From<Error> for FdbError {
    fn from(error: Error) -> FdbError {
        error.fdb_error()
    }
}

impl From<crate::cluster_file::ClusterFileError> for Error {
    /// `no_cluster_file_found` (1515), `file_too_large` (1516), `io_error` (1510) or
    /// `connection_string_invalid` (2104), with the cause as the source.
    fn from(error: crate::cluster_file::ClusterFileError) -> Error {
        use crate::cluster_file::ClusterFileError as E;
        let code = match &error {
            E::NotFound { .. } => NO_CLUSTER_FILE_FOUND,
            E::TooLarge { .. } => FILE_TOO_LARGE,
            E::Io { .. } => IO_ERROR,
            E::Invalid { .. } => CONNECTION_STRING_INVALID,
        };
        Error::with_source(code, error)
    }
}

impl From<crate::cluster_file::ConnectionStringError> for Error {
    /// `connection_string_invalid` (2104), with the cause as the source.
    fn from(error: crate::cluster_file::ConnectionStringError) -> Error {
        Error::with_source(CONNECTION_STRING_INVALID, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_sorted_and_names_resolve() {
        assert!(table::ERRORS.windows(2).all(|w| w[0].code <= w[1].code));
        assert_eq!(Error::new(1007).name(), Some("transaction_too_old"));
        assert_eq!(Error::new(1223).name(), Some("proxy_tag_throttled"));
        assert_eq!(
            Error::new(1251).name_for(SchemaVersion::V8_0),
            Some("transaction_grv_queue_rejected")
        );
        assert_eq!(Error::new(1251).name_for(SchemaVersion::V7_4), None);
        // Defined differently on the two releases.
        assert_eq!(Error::new(1249).name(), None);
        assert_eq!(
            Error::new(1249).name_for(SchemaVersion::V7_4),
            Some("finish_move_keys_too_many_retries")
        );
        // The same name, different descriptions.
        let e = Error::new(2117);
        assert_eq!(e.name(), Some("special_keys_api_failure"));
        assert_eq!(e.description(), None);
        assert!(
            e.description_for(SchemaVersion::V8_0)
                .unwrap()
                .contains("within the same transaction")
        );
        assert_eq!(e.to_string(), "special_keys_api_failure (2117)");
        assert_eq!(Error::new(9999).name(), None);
        assert_eq!(Error::new(9999).to_string(), "FDB error 9999");
    }

    #[test]
    fn predicates_match_fdb_c() {
        let retryable_not_committed = [
            1020, 1007, 1009, 1038, 1078, 1042, 1235, 1051, 1037, 1213, 1223, 1242,
        ];
        for code in retryable_not_committed {
            let e = Error::new(code);
            assert!(e.is_retryable_not_committed(), "{code}");
            assert!(e.is_retryable() && !e.is_maybe_committed(), "{code}");
        }
        for code in [1021, 1039] {
            let e = Error::new(code);
            assert!(e.is_maybe_committed() && e.is_retryable(), "{code}");
            assert!(!e.is_retryable_not_committed(), "{code}");
        }
        for code in [0, 1001, 1006, 1030, 1101, 1251, 2000, 2102, 4100] {
            assert!(!Error::new(code).is_retryable(), "{code}");
        }
    }

    #[test]
    fn on_error_semantics() {
        use TransactionRetry::*;
        assert_eq!(Error::new(1020).transaction_retry(), Some(Backoff));
        assert_eq!(Error::new(1021).transaction_retry(), Some(Backoff));
        assert_eq!(Error::new(1079).transaction_retry(), Some(Backoff));
        assert_eq!(Error::new(1007).transaction_retry(), Some(FutureVersion));
        assert_eq!(Error::new(1009).transaction_retry(), Some(FutureVersion));
        assert_eq!(Error::new(1039).transaction_retry(), None);
        assert_eq!(Error::new(1251).transaction_retry(), None);
        assert_eq!(Error::new(2102).transaction_retry(), None);
        assert_eq!(Error::new(0).transaction_retry(), None);
        assert_eq!(
            Error::new(0).on_error().unwrap_err().code(),
            CLIENT_INVALID_OPERATION
        );
        assert_eq!(Error::new(1021).on_error().unwrap(), Backoff);
        assert_eq!(Error::new(2102).on_error().unwrap_err().code(), 2102);
    }

    #[test]
    fn commit_requests_that_may_have_been_delivered_are_unknown_results() {
        let unknown = [
            RequestError::MaybeDelivered(None),
            RequestError::BrokenPromise,
            RequestError::Cancelled,
            RequestError::Decode(fdb_native_wire::DecodeError::TooShort { len: 1 }),
            RequestError::EndpointNotFound {
                token: fdb_native_wire::Uid::new(1, 2),
                maybe_delivered: true,
            },
            RequestError::Fdb(FdbError::new(REQUEST_MAYBE_DELIVERED)),
            // What the committer also treats as an unknown result: a server's
            // broken_promise, commit_unknown_result, default_error_or, never_reply.
            RequestError::Fdb(FdbError::new(BROKEN_PROMISE)),
            RequestError::Fdb(FdbError::new(COMMIT_UNKNOWN_RESULT)),
            RequestError::Fdb(FdbError::new(1035)),
            RequestError::Fdb(FdbError::new(NEVER_REPLY)),
        ];
        for error in unknown {
            let e = Error::from_commit_request(error.clone());
            assert_eq!(e.code(), COMMIT_UNKNOWN_RESULT, "{error:?}");
            assert!(e.is_maybe_committed());
            assert!(e.request_error().is_some());
        }
        // Certainly not delivered: what `from_request` says.
        for error in [
            RequestError::NotSent(None),
            RequestError::Backpressure,
            RequestError::EndpointNotFound {
                token: fdb_native_wire::Uid::new(1, 2),
                maybe_delivered: false,
            },
        ] {
            let e = Error::from_commit_request(error.clone());
            assert_eq!(e.code(), Error::from_request(error.clone()).code());
            assert!(!e.is_maybe_committed(), "{error:?}");
        }
        // A server's definite answer.
        assert_eq!(
            Error::from_commit_request(RequestError::Fdb(FdbError::new(NOT_COMMITTED))).code(),
            NOT_COMMITTED
        );
    }

    #[test]
    fn from_request_keeps_the_source() {
        let e = Error::from_request(RequestError::MaybeDelivered(None));
        assert_eq!(e.code(), REQUEST_MAYBE_DELIVERED);
        assert!(e.request_error().is_some());
        assert!(std::error::Error::source(&e).is_some());
        assert_eq!(e.code(), REQUEST_MAYBE_DELIVERED);
        let e = Error::from_request(RequestError::Fdb(FdbError::new(1009)));
        assert_eq!(e.code(), FUTURE_VERSION);
    }
}
