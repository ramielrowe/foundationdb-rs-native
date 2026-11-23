//! [`ClientError`]: an FDB error as the client API reports it, with the retry semantics of
//! C++'s `Transaction::onError` and `fdb_error_predicate`.
//!
//! An FDB error is a 16-bit code (`flow/include/flow/error_definitions.h`). Errors reach a
//! client from three places, and all of them become a [`ClientError`]:
//!
//! * a **server** replied with one (`transaction_too_old` from a storage server,
//!   `not_committed` from a commit proxy, ...);
//! * the **transport** failed a request ([`RequestError`]; the error keeps it as its
//!   [`source`](std::error::Error::source) and takes the code a C++ client would have seen,
//!   see [`ClientError::from_request`]);
//! * the client's own **logic** decided on one (`all_alternatives_failed`,
//!   `database_locked`, `client_invalid_operation`, ...).
//!
//! # Retry semantics
//!
//! Citations are `file:line` on `origin/release-7.4` / `origin/release-8.0`; `fdb_c` is
//! `bindings/c/fdb_c.cpp`, `NativeAPI` is `fdbclient/NativeAPI.actor.cpp` (7.4) /
//! `fdbclient/NativeAPI.cpp` (8.0).
//!
//! * [`is_maybe_committed`](ClientError::is_maybe_committed) is
//!   `fdb_error_predicate(MAYBE_COMMITTED)` (`fdb_c` 7.4:83-85, 8.0:213-215):
//!   `commit_unknown_result` (1021) and `cluster_version_changed` (1039).
//! * [`is_retryable_not_committed`](ClientError::is_retryable_not_committed) is
//!   `RETRYABLE_NOT_COMMITTED` (7.4:86-94, 8.0:216-224): `not_committed`,
//!   `transaction_too_old`, `future_version`, `database_locked`,
//!   `grv_proxy_memory_limit_exceeded`, `commit_proxy_memory_limit_exceeded`,
//!   `transaction_throttled_hot_shard`, `batch_transaction_throttled`, `process_behind`,
//!   `tag_throttled`, `transaction_rejected_range_locked`, and on 7.4 `proxy_tag_throttled`.
//! * [`is_retryable`](ClientError::is_retryable) is `RETRYABLE`, the union of the two.
//! * [`transaction_retry`](ClientError::transaction_retry) is what `Transaction::onError`
//!   does with the error (NativeAPI 7.4:8112-8167, 8.0:5908-5958): reset the transaction and
//!   retry after the transaction's exponential backoff ([`TransactionRetry::Backoff`]) for
//!   the codes listed there, after `FUTURE_VERSION_RETRY_DELAY` (10 ms, capped by the
//!   transaction's max backoff; [`TransactionRetry::FutureVersion`]) for
//!   `transaction_too_old` and `future_version`, and rethrow anything else. It differs from
//!   the predicate in two codes: `cluster_version_changed` is not retried by `onError` (the
//!   multi-version client handles it, and this client has no counterpart), and
//!   `blob_granule_request_failed` (7.4 only) is retried by `onError` but not in the
//!   predicate.
//!
//! **Both releases.** The two releases agree on every code except a few that exist on one
//! only (7.4's tenant and blob errors, `proxy_tag_throttled` 1223 and
//! `blob_granule_request_failed` 1079; 8.0's `transaction_grv_queue_rejected` 1251 and
//! bulk-load errors), and 1249 and 1250, which name different (server-internal) errors.
//! A code a release does not define cannot come from a server of that release, so the
//! predicates are the union of the two releases' rules. 8.0's
//! `transaction_grv_queue_rejected` is not retryable on 8.0 (`onError` rethrows it), and is
//! not here either.

use std::fmt;
use std::sync::Arc;

use fdb_native_schema::SchemaVersion;
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
    /// `connection_failed` (1026).
    pub const CONNECTION_FAILED: u16 = 1026;
    /// `request_maybe_delivered` (1030).
    pub const REQUEST_MAYBE_DELIVERED: u16 = 1030;
    /// `transaction_timed_out` (1031).
    pub const TRANSACTION_TIMED_OUT: u16 = 1031;
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
    /// `client_invalid_operation` (2000).
    pub const CLIENT_INVALID_OPERATION: u16 = 2000;
    /// `key_outside_legal_range` (2004).
    pub const KEY_OUTSIDE_LEGAL_RANGE: u16 = 2004;
    /// `inverted_range` (2005).
    pub const INVERTED_RANGE: u16 = 2005;
    /// `version_invalid` (2011).
    pub const VERSION_INVALID: u16 = 2011;
    /// `key_too_large` (2102).
    pub const KEY_TOO_LARGE: u16 = 2102;
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
/// [`ClientError::transaction_retry`].
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

/// An FDB error, as the client API reports it: the code a C++ client would get, and, when it
/// came from the transport, the [`RequestError`] behind it (its
/// [`source`](std::error::Error::source)). See the [module documentation](self) for the
/// retry predicates.
///
/// Two errors are equal when their codes are; the source is diagnostic only.
///
/// ```
/// use fdb_native_client::{ClientError, TransactionRetry, error::codes};
///
/// let e = ClientError::new(codes::FUTURE_VERSION);
/// assert_eq!(e.name(), Some("future_version"));
/// assert!(e.is_retryable() && !e.is_maybe_committed());
/// assert_eq!(e.transaction_retry(), Some(TransactionRetry::FutureVersion));
/// assert_eq!(e.to_string(), "Request for future version (future_version, 1009)");
///
/// let unknown = ClientError::new(codes::COMMIT_UNKNOWN_RESULT);
/// assert!(unknown.is_maybe_committed() && unknown.is_retryable());
/// assert!(!ClientError::new(codes::KEY_TOO_LARGE).is_retryable());
/// ```
#[derive(Clone)]
pub struct ClientError {
    code: u16,
    source: Option<Arc<RequestError>>,
}

impl ClientError {
    /// The error with `code`.
    pub const fn new(code: u16) -> ClientError {
        ClientError { code, source: None }
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
    pub fn from_request(error: RequestError) -> ClientError {
        let code = error.fdb_code();
        ClientError {
            code,
            source: Some(Arc::new(error)),
        }
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
        self.source.as_deref()
    }

    /// The error's name (`"transaction_too_old"`), if either release defines the code and
    /// both agree on it. See [`name_for`](Self::name_for).
    pub fn name(&self) -> Option<&'static str> {
        let mut entries = entries(self.code);
        let first = entries.next()?;
        match entries.next() {
            None => Some(first.name),
            Some(_) => None,
        }
    }

    /// The error's name on a server of `schema`'s release.
    pub fn name_for(&self, schema: SchemaVersion) -> Option<&'static str> {
        entries(self.code)
            .find(|e| e.release.has(schema))
            .map(|e| e.name)
    }

    /// The error's description (what `fdb_get_error` returns), if [`name`](Self::name) is
    /// known.
    pub fn description(&self) -> Option<&'static str> {
        let mut entries = entries(self.code);
        let first = entries.next()?;
        match entries.next() {
            None => Some(first.description),
            Some(_) => None,
        }
    }

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED)`: the transaction may have
    /// committed (`commit_unknown_result`, `cluster_version_changed`).
    pub const fn is_maybe_committed(&self) -> bool {
        matches!(self.code, COMMIT_UNKNOWN_RESULT | CLUSTER_VERSION_CHANGED)
    }

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED)`: the transaction
    /// did not commit, and retrying it may succeed.
    pub const fn is_retryable_not_committed(&self) -> bool {
        matches!(
            self.code,
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

    /// `fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE)`: retrying the transaction may
    /// succeed ([`is_maybe_committed`](Self::is_maybe_committed) or
    /// [`is_retryable_not_committed`](Self::is_retryable_not_committed)).
    pub const fn is_retryable(&self) -> bool {
        self.is_maybe_committed() || self.is_retryable_not_committed()
    }

    /// What `Transaction::onError` does with this error: `Some` if it resets the transaction
    /// and retries (and how long it waits first), `None` if it rethrows the error.
    pub const fn transaction_retry(&self) -> Option<TransactionRetry> {
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
}

fn entries(code: u16) -> impl Iterator<Item = &'static table::Entry> {
    let start = table::ERRORS.partition_point(|e| e.code < code);
    table::ERRORS[start..]
        .iter()
        .take_while(move |e| e.code == code)
}

impl PartialEq for ClientError {
    fn eq(&self, other: &ClientError) -> bool {
        self.code == other.code
    }
}

impl Eq for ClientError {}

impl fmt::Debug for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("ClientError");
        s.field("code", &self.code);
        if let Some(name) = self.name() {
            s.field("name", &name);
        }
        if let Some(source) = &self.source {
            s.field("source", source);
        }
        s.finish()
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.description(), self.name()) {
            (Some(description), Some(name)) => write!(f, "{description} ({name}, {})", self.code)?,
            _ => write!(f, "FDB error {}", self.code)?,
        }
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<FdbError> for ClientError {
    fn from(error: FdbError) -> ClientError {
        ClientError::new(error.code)
    }
}

impl From<RequestError> for ClientError {
    fn from(error: RequestError) -> ClientError {
        ClientError::from_request(error)
    }
}

impl From<ClientError> for FdbError {
    fn from(error: ClientError) -> FdbError {
        error.fdb_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_sorted_and_names_resolve() {
        assert!(table::ERRORS.windows(2).all(|w| w[0].code <= w[1].code));
        assert_eq!(ClientError::new(1007).name(), Some("transaction_too_old"));
        assert_eq!(ClientError::new(1223).name(), Some("proxy_tag_throttled"));
        assert_eq!(
            ClientError::new(1251).name_for(SchemaVersion::V8_0),
            Some("transaction_grv_queue_rejected")
        );
        assert_eq!(ClientError::new(1251).name_for(SchemaVersion::V7_4), None);
        // Defined differently on the two releases.
        assert_eq!(ClientError::new(1249).name(), None);
        assert_eq!(
            ClientError::new(1249).name_for(SchemaVersion::V7_4),
            Some("finish_move_keys_too_many_retries")
        );
        assert_eq!(ClientError::new(9999).name(), None);
        assert_eq!(ClientError::new(9999).to_string(), "FDB error 9999");
    }

    #[test]
    fn predicates_match_fdb_c() {
        let retryable_not_committed = [
            1020, 1007, 1009, 1038, 1078, 1042, 1235, 1051, 1037, 1213, 1223, 1242,
        ];
        for code in retryable_not_committed {
            let e = ClientError::new(code);
            assert!(e.is_retryable_not_committed(), "{code}");
            assert!(e.is_retryable() && !e.is_maybe_committed(), "{code}");
        }
        for code in [1021, 1039] {
            let e = ClientError::new(code);
            assert!(e.is_maybe_committed() && e.is_retryable(), "{code}");
            assert!(!e.is_retryable_not_committed(), "{code}");
        }
        for code in [0, 1001, 1006, 1030, 1101, 1251, 2000, 2102, 4100] {
            assert!(!ClientError::new(code).is_retryable(), "{code}");
        }
    }

    #[test]
    fn on_error_semantics() {
        use TransactionRetry::*;
        assert_eq!(ClientError::new(1020).transaction_retry(), Some(Backoff));
        assert_eq!(ClientError::new(1021).transaction_retry(), Some(Backoff));
        assert_eq!(ClientError::new(1079).transaction_retry(), Some(Backoff));
        assert_eq!(
            ClientError::new(1007).transaction_retry(),
            Some(FutureVersion)
        );
        assert_eq!(
            ClientError::new(1009).transaction_retry(),
            Some(FutureVersion)
        );
        assert_eq!(ClientError::new(1039).transaction_retry(), None);
        assert_eq!(ClientError::new(1251).transaction_retry(), None);
        assert_eq!(ClientError::new(2102).transaction_retry(), None);
    }

    #[test]
    fn from_request_keeps_the_source() {
        let e = ClientError::from_request(RequestError::MaybeDelivered(None));
        assert_eq!(e.code(), REQUEST_MAYBE_DELIVERED);
        assert!(e.request_error().is_some());
        assert!(std::error::Error::source(&e).is_some());
        assert_eq!(e, ClientError::new(REQUEST_MAYBE_DELIVERED));
        let e = ClientError::from_request(RequestError::Fdb(FdbError::new(1009)));
        assert_eq!(e.code(), FUTURE_VERSION);
    }
}
