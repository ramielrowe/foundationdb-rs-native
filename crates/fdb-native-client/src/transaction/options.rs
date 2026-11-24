//! Transaction options (`FDBTransactionOptions`, `fdbclient/vexillographer/fdb.options`):
//! the typed [`TransactionOption`], and what a transaction does with each.
//!
//! # What each option does here
//!
//! The option list is that of `fdb.options` on 7.4 and 8.0 (identical but for
//! `MAX_GRV_QUEUE_DELAY`, which 8.0 added). `setOption` is `ReadYourWritesTransaction::setOptionImpl`
//! followed by `Transaction::setOption` (`ReadYourWrites.actor.cpp` 7.4:2534-2620,
//! 8.0:2450-2540; `NativeAPI` 7.4:7297-7590, 8.0:4990-5200); the values are range checked as
//! `extractIntOption` does (`invalid_option_value`, 2006).
//!
//! | Option | Code | Persistent | Status |
//! |---|---|---|---|
//! | [`Timeout`](TransactionOption::Timeout) | 500 | yes | supported |
//! | [`RetryLimit`](TransactionOption::RetryLimit) | 501 | yes | supported |
//! | [`MaxRetryDelay`](TransactionOption::MaxRetryDelay) | 502 | yes | supported |
//! | [`SizeLimit`](TransactionOption::SizeLimit) | 503 | no | supported |
//! | [`CausalWriteRisky`](TransactionOption::CausalWriteRisky) | 10 | no | supported |
//! | [`CausalReadRisky`](TransactionOption::CausalReadRisky) | 20 | no | supported |
//! | [`CausalReadDisable`](TransactionOption::CausalReadDisable) | 21 | no | no-op (C++ ignores it too) |
//! | [`IncludePortInAddress`](TransactionOption::IncludePortInAddress) | 23 | no | no-op (no `getAddressesForKey` yet; the port is always included) |
//! | [`NextWriteNoWriteConflictRange`](TransactionOption::NextWriteNoWriteConflictRange) | 30 | no | supported |
//! | [`CommitOnFirstProxy`](TransactionOption::CommitOnFirstProxy) | 40 | no | supported (hidden test option) |
//! | [`ReadYourWritesDisable`](TransactionOption::ReadYourWritesDisable) | 51 | no | supported (only before any read or write) |
//! | [`ReadServerSideCacheEnable`](TransactionOption::ReadServerSideCacheEnable) / [`Disable`](TransactionOption::ReadServerSideCacheDisable) | 507 / 508 | no | accepted; hint, not sent to storage servers yet |
//! | [`ReadPriorityNormal`](TransactionOption::ReadPriorityNormal) / [`Low`](TransactionOption::ReadPriorityLow) / [`High`](TransactionOption::ReadPriorityHigh) | 509 / 510 / 511 | no | accepted; hint, not sent to storage servers yet |
//! | [`PrioritySystemImmediate`](TransactionOption::PrioritySystemImmediate) | 200 | no | supported (read version priority) |
//! | [`PriorityBatch`](TransactionOption::PriorityBatch) | 201 | no | supported (read version priority) |
//! | [`InitializeNewDatabase`](TransactionOption::InitializeNewDatabase) | 300 | no | supported |
//! | [`AccessSystemKeys`](TransactionOption::AccessSystemKeys) | 301 | no | supported |
//! | [`ReadSystemKeys`](TransactionOption::ReadSystemKeys) | 302 | no | supported |
//! | [`RawAccess`](TransactionOption::RawAccess) | 303 | no | no-op (no tenants: access is always raw) |
//! | [`BypassStorageQuota`](TransactionOption::BypassStorageQuota) | 304 | no | supported |
//! | [`DebugRetryLogging`](TransactionOption::DebugRetryLogging) | 401 | no | accepted; no trace log, no-op |
//! | [`DebugTransactionIdentifier`](TransactionOption::DebugTransactionIdentifier) | 403 | no | validated as C++ does; no trace log, no-op |
//! | [`LogTransaction`](TransactionOption::LogTransaction) | 404 | no | validated as C++ does (needs the identifier); no trace log, no-op |
//! | [`TransactionLoggingMaxFieldLength`](TransactionOption::TransactionLoggingMaxFieldLength) | 405 | no | validated; no-op |
//! | [`ServerRequestTracing`](TransactionOption::ServerRequestTracing) | 406 | no | accepted; no server-side debug id is sent, no-op |
//! | [`SnapshotRywEnable`](TransactionOption::SnapshotRywEnable) / [`Disable`](TransactionOption::SnapshotRywDisable) | 600 / 601 | no | supported |
//! | [`LockAware`](TransactionOption::LockAware) | 700 | no | supported |
//! | [`UsedDuringCommitProtectionDisable`](TransactionOption::UsedDuringCommitProtectionDisable) | 701 | no | supported |
//! | [`ReadLockAware`](TransactionOption::ReadLockAware) | 702 | no | supported |
//! | [`FirstInBatch`](TransactionOption::FirstInBatch) | 710 | no | supported (hidden test option) |
//! | [`UseProvisionalProxies`](TransactionOption::UseProvisionalProxies) | 711 | no | supported |
//! | [`ReportConflictingKeys`](TransactionOption::ReportConflictingKeys) | 712 | no | supported |
//! | [`Tag`](TransactionOption::Tag) / [`AutoThrottleTag`](TransactionOption::AutoThrottleTag) | 800 / 801 | no | **unsupported**: `invalid_option` (2007) |
//! | [`SpanParent`](TransactionOption::SpanParent) | 900 | no | validated (33 bytes); not propagated, no-op |
//! | [`ExpensiveClearCostEstimationEnable`](TransactionOption::ExpensiveClearCostEstimationEnable) | 1000 | no | no-op (it only refines tag throttling costs) |
//! | [`BypassUnreadable`](TransactionOption::BypassUnreadable) | 1100 | no | supported (`get`, as C++) |
//! | [`UseGrvCache`](TransactionOption::UseGrvCache) | 1101 | no | no-op (there is no GRV cache: the read version is always fresh) |
//! | [`MaxGrvQueueDelay`](TransactionOption::MaxGrvQueueDelay) | 1103 | no | supported on 8.0 clusters; ignored by 7.4 ones |
//!
//! Not offered (a typed API has no variant for them): the deprecated
//! (`READ_AHEAD_DISABLE`, `DURABILITY_*`, `TRANSACTION_LOGGING_ENABLE`) and the debugging
//! (`CHECK_WRITES_ENABLE`, `DEBUG_DUMP`, `SKIP_GRV_CACHE`) options, the special key space
//! options (`SPECIAL_KEY_SPACE_RELAXED`, `..._ENABLE_WRITES`: there is no special key space
//! yet), `IDEMPOTENCY_ID` and `AUTOMATIC_IDEMPOTENCY` (no idempotency ids yet),
//! `AUTHORIZATION_TOKEN` (no tenants or token authorization yet) and the replica consistency
//! check options.
//!
//! # Persistence
//!
//! Only the options `fdb.options` marks `persistent="true"` ([`Timeout`], [`RetryLimit`],
//! [`MaxRetryDelay`]; `AUTHORIZATION_TOKEN` is not offered) survive [`Transaction::on_error`]:
//! C++ keeps them in `persistentOptions` and replays them after every reset
//! (`applyPersistentOptions`, 7.4:2681). **So does the list of database defaults** (see
//! [`DatabaseOption`](crate::DatabaseOption)): C++ copies them into `persistentOptions` too, so
//! `TRANSACTION_SIZE_LIMIT` (the default for [`SizeLimit`]) is back after `on_error` although
//! `SIZE_LIMIT` itself is forgotten. A full [`Transaction::reset`] forgets the persistent
//! options too, and copies the database's defaults again (`ReadYourWritesTransaction::reset`,
//! 7.4:2738).
//!
//! [`Timeout`]: TransactionOption::Timeout
//! [`RetryLimit`]: TransactionOption::RetryLimit
//! [`MaxRetryDelay`]: TransactionOption::MaxRetryDelay
//! [`SizeLimit`]: TransactionOption::SizeLimit
//! [`Transaction::on_error`]: crate::Transaction::on_error
//! [`Transaction::reset`]: crate::Transaction::reset

use std::time::Duration;

use super::{ReadVersion, SYSTEM_KEYS_BEGIN, SYSTEM_KEYS_END, State};
use crate::error::Error;
use crate::error::codes::*;
use crate::read::Priority;

/// `extractIntOption`'s upper bound for the millisecond options.
const MAX_MILLIS: u128 = i32::MAX as u128;
/// `d` in whole milliseconds, rounded up: C++ takes an integer number of milliseconds, where
/// zero disables the timeout, so a duration that is not zero must not become zero.
fn millis(d: Duration) -> u128 {
    d.as_millis() + u128::from(d.subsec_nanos() % 1_000_000 != 0)
}

/// The longest `DEBUG_TRANSACTION_IDENTIFIER` (`value.get().size() > 100`).
const MAX_IDENTIFIER_LEN: usize = 100;
/// The length of a serialized `SPAN_PARENT`.
const SPAN_PARENT_LEN: usize = 33;
/// The smallest `SIZE_LIMIT`.
const MIN_SIZE_LIMIT: u64 = 32;

/// A transaction option (`FDBTransactionOption`), for [`Transaction::set_option`] (and, as
/// the `Transaction*` variants of [`DatabaseOption`](crate::DatabaseOption), for the
/// database's defaults).
///
/// See the [module documentation](self) for what each one does here (a table with its code,
/// whether it is persistent and whether it is supported or accepted as a no-op).
///
/// [`Timeout`](Self::Timeout), [`RetryLimit`](Self::RetryLimit) and
/// [`MaxRetryDelay`](Self::MaxRetryDelay) persist across
/// [`on_error`](crate::Transaction::on_error) (`fdb.options`' "persistent" options); the others
/// apply until the transaction is reset, by `on_error` or [`reset`](crate::Transaction::reset),
/// except that the database's defaults are applied again. A full reset clears them all
/// (and applies the defaults).
///
/// ```no_run
/// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
/// use fdb_native_client::TransactionOption;
/// use std::time::Duration;
///
/// db.run(|tx, _| async move {
///     tx.set_option(TransactionOption::Timeout(Duration::from_secs(5)))?;
///     tx.set_option(TransactionOption::RetryLimit(10))?;
///     tx.set_option(TransactionOption::ReadSystemKeys)?;
///     tx.get(b"\xff/conf/storage_engine").await
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Transaction::set_option`]: crate::Transaction::set_option
/// [`Transaction::on_error`]: crate::Transaction::on_error
/// [`Transaction::reset`]: crate::Transaction::reset
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransactionOption {
    /// `TIMEOUT` (500): the transaction fails with `transaction_timed_out` once this long has
    /// passed since it was created or last [reset](crate::Transaction::reset). Zero disables
    /// it. At most `i32::MAX` milliseconds (a fraction of a millisecond counts as a whole
    /// one). Persistent.
    Timeout(Duration),
    /// `RETRY_LIMIT` (501): how many times [`on_error`](crate::Transaction::on_error) may
    /// retry; `-1` (the default) is unlimited. Persistent.
    RetryLimit(i32),
    /// `MAX_RETRY_DELAY` (502): the largest backoff [`on_error`](crate::Transaction::on_error)
    /// waits (default 1 s). At most `i32::MAX` milliseconds. Persistent.
    MaxRetryDelay(Duration),
    /// `SIZE_LIMIT` (503): the largest size (`getSize()`) the transaction may commit, 32
    /// bytes to `TRANSACTION_SIZE_LIMIT` (10 000 000, the default); a bigger one fails with
    /// `transaction_too_large` (2101). Not persistent: `on_error` forgets it (the database's
    /// default, if any, comes back).
    SizeLimit(u32),
    /// `CAUSAL_WRITE_RISKY` (10): the commit adds no self-conflicting key to a blind write,
    /// and does not settle an unknown result with a dummy transaction.
    CausalWriteRisky,
    /// `CAUSAL_READ_RISKY` (20): the read version may not reflect every earlier commit.
    CausalReadRisky,
    /// `CAUSAL_READ_DISABLE` (21): C++ ignores it; so does this.
    CausalReadDisable,
    /// `INCLUDE_PORT_IN_ADDRESS` (23): no-op (the addresses of `getAddressesForKey` always
    /// include the port as of API version 630).
    IncludePortInAddress,
    /// `NEXT_WRITE_NO_WRITE_CONFLICT_RANGE` (30): the next [`set`](crate::Transaction::set),
    /// [`clear`](crate::Transaction::clear), [`clear_range`](crate::Transaction::clear_range) or
    /// [`atomic_op`](crate::Transaction::atomic_op) adds no write conflict range (other
    /// transactions that read what it writes do not conflict with this one). It is used up by
    /// that write, whether it succeeds or fails (C++'s `getAndResetWriteConflictDisabled`).
    NextWriteNoWriteConflictRange,
    /// `COMMIT_ON_FIRST_PROXY` (40, hidden): the commit goes to the first commit proxy
    /// instead of being load balanced.
    CommitOnFirstProxy,
    /// `READ_YOUR_WRITES_DISABLE` (51): reads see the database at the read version, not the
    /// transaction's own writes, and add the read conflict ranges C++'s `Transaction` adds
    /// (`readThrough`); see [`Transaction`](crate::Transaction). `client_invalid_operation`
    /// (2000) if the transaction has a read in progress, has cached a read, or has written
    /// (C++: `reading.getFutureCount() > 0 || !cache.empty() || !writes.empty()`).
    ReadYourWritesDisable,
    /// `READ_SERVER_SIDE_CACHE_ENABLE` (507): a hint to the storage servers (the default).
    /// Accepted; not sent yet.
    ReadServerSideCacheEnable,
    /// `READ_SERVER_SIDE_CACHE_DISABLE` (508): a hint to the storage servers. Accepted; not
    /// sent yet.
    ReadServerSideCacheDisable,
    /// `READ_PRIORITY_NORMAL` (509): a hint to the storage servers (the default). Accepted;
    /// not sent yet.
    ReadPriorityNormal,
    /// `READ_PRIORITY_LOW` (510): a hint to the storage servers. Accepted; not sent yet.
    ReadPriorityLow,
    /// `READ_PRIORITY_HIGH` (511): a hint to the storage servers. Accepted; not sent yet.
    ReadPriorityHigh,
    /// `PRIORITY_SYSTEM_IMMEDIATE` (200): the transaction's read versions are requested with
    /// the highest priority (never throttled; lower priority transactions wait behind it).
    /// The last of this and [`PriorityBatch`](Self::PriorityBatch) set wins.
    PrioritySystemImmediate,
    /// `PRIORITY_BATCH` (201): the transaction's read versions are requested with low
    /// priority: throttled first.
    PriorityBatch,
    /// `INITIALIZE_NEW_DATABASE` (300): a write-only transaction that sets a new database's
    /// initial configuration (read version 0, and [`CausalWriteRisky`](Self::CausalWriteRisky)).
    /// For database tools; `read_version_already_set` (2010) if the transaction has a read
    /// version.
    InitializeNewDatabase,
    /// `ACCESS_SYSTEM_KEYS` (301): the transaction may read and write keys from `\xff` to
    /// `\xff\xff`.
    AccessSystemKeys,
    /// `READ_SYSTEM_KEYS` (302): the transaction may read keys from `\xff` to `\xff\xff`.
    ReadSystemKeys,
    /// `RAW_ACCESS` (303): no-op (without tenants, access is always raw).
    RawAccess,
    /// `BYPASS_STORAGE_QUOTA` (304): the commit bypasses storage quota enforcement.
    BypassStorageQuota,
    /// `DEBUG_RETRY_LOGGING` (401): log the retries under this name (the name is optional).
    /// Accepted; there is no trace log, so it does nothing.
    DebugRetryLogging(Option<String>),
    /// `DEBUG_TRANSACTION_IDENTIFIER` (403): a client identifier of the transaction for
    /// tracing and profiling: 1 to 100 bytes (`invalid_option_value` otherwise), and it
    /// cannot change once set (`client_invalid_operation`; the same one again is fine). There
    /// is no trace log or profiling yet: validated, nothing more.
    DebugTransactionIdentifier(String),
    /// `LOG_TRANSACTION` (404): enable tracing of the transaction. Needs the identifier set
    /// first ([`DebugTransactionIdentifier`](Self::DebugTransactionIdentifier)), else
    /// `client_invalid_operation` (2000). Validated, nothing more (no trace log).
    LogTransaction,
    /// `TRANSACTION_LOGGING_MAX_FIELD_LENGTH` (405): the escaped length of the keys and
    /// values `LOG_TRANSACTION` logs; `-1` for no limit. `invalid_option_value` for 0 or less
    /// than -1. Validated, nothing more.
    TransactionLoggingMaxFieldLength(i32),
    /// `SERVER_REQUEST_TRACING` (406): ask the servers to trace this transaction. Accepted;
    /// no debug id is sent, so it does nothing.
    ServerRequestTracing,
    /// `SNAPSHOT_RYW_ENABLE` (600): snapshot reads see the transaction's writes (the
    /// default). A count, as C++'s `snapshotRywEnabled`: it starts at the database's
    /// ([`DatabaseOption::SnapshotRywEnable`](crate::DatabaseOption::SnapshotRywEnable), 1 by
    /// default) and this option adds one; snapshot reads see the writes while it is above 0.
    SnapshotRywEnable,
    /// `SNAPSHOT_RYW_DISABLE` (601): takes one from the count of
    /// [`SnapshotRywEnable`](Self::SnapshotRywEnable): once it is 0 or less, snapshot reads
    /// do not see the transaction's writes (they read the database at the read version, and
    /// what earlier reads cached of it).
    SnapshotRywDisable,
    /// `LOCK_AWARE` (700): the transaction may read and commit while the database is locked
    /// (it is responsible for having taken the lock). Cancels
    /// [`ReadLockAware`](Self::ReadLockAware)'s read-only effect.
    LockAware,
    /// `USED_DURING_COMMIT_PROTECTION_DISABLE` (701): an operation on the transaction after
    /// the commit started fails with `used_during_commit` alone, without also failing the
    /// operations in progress (the commit among them).
    UsedDuringCommitProtectionDisable,
    /// `READ_LOCK_AWARE` (702): the transaction may read while the database is locked; unless
    /// [`LockAware`](Self::LockAware) is also set, it is read-only (a commit with writes
    /// fails with `transaction_read_only`, 2023).
    ReadLockAware,
    /// `FIRST_IN_BATCH` (710, hidden): no other transaction is applied before this one within
    /// its commit version.
    FirstInBatch,
    /// `USE_PROVISIONAL_PROXIES` (711): use the proxies of a recovery that has not finished
    /// (for tools that change the database configuration).
    UseProvisionalProxies,
    /// `REPORT_CONFLICTING_KEYS` (712): a conflict says which read conflict ranges conflicted
    /// ([`Error::conflicting_ranges`]).
    ReportConflictingKeys,
    /// `TAG` (800): a throttling tag. **Unsupported**: tags are not sent to the cluster yet,
    /// so the option fails with `invalid_option` (2007) rather than be silently ignored
    /// (manual throttling would not apply).
    Tag(String),
    /// `AUTO_THROTTLE_TAG` (801): as [`Tag`](Self::Tag), and **unsupported** the same way.
    AutoThrottleTag(String),
    /// `SPAN_PARENT` (900): the 33-byte serialized parent span of the transaction's
    /// trace span (`invalid_option_value` for another length). Validated; spans are not
    /// sent to the cluster, so it does nothing.
    SpanParent(Vec<u8>),
    /// `EXPENSIVE_CLEAR_COST_ESTIMATION_ENABLE` (1000): ask the storage servers how big a
    /// cleared range is, for the tag throttling costs. No-op (there are no tags).
    ExpensiveClearCostEstimationEnable,
    /// `BYPASS_UNREADABLE` (1100): a [`get`](crate::Transaction::get) of a key the
    /// transaction's versionstamped operations made unreadable does not fail with
    /// `accessed_unreadable` (1036): it returns what the transaction knows (a
    /// `SetVersionstampedValue`'s operand as given, or the database's value of a key a
    /// `SetVersionstampedKey` may write). As in C++, only `get` bypasses: range reads and
    /// `get_key` still fail.
    BypassUnreadable,
    /// `USE_GRV_CACHE` (1101): allow a cached (stale) read version. No-op: there is no
    /// cache, the read version is always requested.
    UseGrvCache,
    /// `MAX_GRV_QUEUE_DELAY` (1103, 8.0): the read version request is rejected with
    /// `transaction_grv_queue_rejected` (1251) if the GRV proxy estimates that ratekeeper
    /// throttling will keep it queued longer than this (at most `i32::MAX` milliseconds).
    /// Ignored by 7.4 clusters, whose GRV proxies have no such option (a 7.4 C++ client
    /// would not know the option either). The delay is sent in whole milliseconds, a
    /// fraction of a millisecond rounded up (as the other durations here).
    MaxGrvQueueDelay(Duration),
}

impl TransactionOption {
    /// The option's code in `fdb.options` (`FDBTransactionOption`).
    pub const fn code(&self) -> i32 {
        use TransactionOption::*;
        match self {
            CausalWriteRisky => 10,
            CausalReadRisky => 20,
            CausalReadDisable => 21,
            IncludePortInAddress => 23,
            NextWriteNoWriteConflictRange => 30,
            CommitOnFirstProxy => 40,
            ReadYourWritesDisable => 51,
            ReadServerSideCacheEnable => 507,
            ReadServerSideCacheDisable => 508,
            ReadPriorityNormal => 509,
            ReadPriorityLow => 510,
            ReadPriorityHigh => 511,
            PrioritySystemImmediate => 200,
            PriorityBatch => 201,
            InitializeNewDatabase => 300,
            AccessSystemKeys => 301,
            ReadSystemKeys => 302,
            RawAccess => 303,
            BypassStorageQuota => 304,
            DebugRetryLogging(_) => 401,
            DebugTransactionIdentifier(_) => 403,
            LogTransaction => 404,
            TransactionLoggingMaxFieldLength(_) => 405,
            ServerRequestTracing => 406,
            Timeout(_) => 500,
            RetryLimit(_) => 501,
            MaxRetryDelay(_) => 502,
            SizeLimit(_) => 503,
            SnapshotRywEnable => 600,
            SnapshotRywDisable => 601,
            LockAware => 700,
            UsedDuringCommitProtectionDisable => 701,
            ReadLockAware => 702,
            FirstInBatch => 710,
            UseProvisionalProxies => 711,
            ReportConflictingKeys => 712,
            Tag(_) => 800,
            AutoThrottleTag(_) => 801,
            SpanParent(_) => 900,
            ExpensiveClearCostEstimationEnable => 1000,
            BypassUnreadable => 1100,
            UseGrvCache => 1101,
            MaxGrvQueueDelay(_) => 1103,
        }
    }

    /// Whether the option survives [`on_error`](crate::Transaction::on_error): `fdb.options`'
    /// `persistent="true"`. Only [`Timeout`](Self::Timeout), [`RetryLimit`](Self::RetryLimit) and
    /// [`MaxRetryDelay`](Self::MaxRetryDelay) are (and the offered options do not include
    /// `AUTHORIZATION_TOKEN`).
    pub const fn is_persistent(&self) -> bool {
        matches!(
            self,
            TransactionOption::Timeout(_)
                | TransactionOption::RetryLimit(_)
                | TransactionOption::MaxRetryDelay(_)
        )
    }

    /// Checks the value as `extractIntOption` and the option's own checks do, with `size_limit_max`
    /// the largest `SIZE_LIMIT` (`TRANSACTION_SIZE_LIMIT`).
    pub(crate) fn validate(&self, size_limit_max: u64) -> Result<(), Error> {
        let invalid = |what: &'static str| Err(Error::with_message(INVALID_OPTION_VALUE, what));
        match self {
            TransactionOption::Timeout(d) if millis(*d) > MAX_MILLIS => {
                invalid("the timeout is at most i32::MAX milliseconds")
            }
            TransactionOption::MaxRetryDelay(d) if millis(*d) > MAX_MILLIS => {
                invalid("the max retry delay is at most i32::MAX milliseconds")
            }
            TransactionOption::MaxGrvQueueDelay(d) if millis(*d) > MAX_MILLIS => {
                invalid("the max GRV queue delay is at most i32::MAX milliseconds")
            }
            TransactionOption::RetryLimit(n) if *n < -1 => invalid("the retry limit is -1 or more"),
            TransactionOption::SizeLimit(n)
                if u64::from(*n) < MIN_SIZE_LIMIT || u64::from(*n) > size_limit_max =>
            {
                invalid("the size limit must be from 32 to TRANSACTION_SIZE_LIMIT")
            }
            TransactionOption::DebugTransactionIdentifier(id)
                if id.is_empty() || id.len() > MAX_IDENTIFIER_LEN =>
            {
                invalid("the transaction identifier is 1 to 100 bytes")
            }
            TransactionOption::TransactionLoggingMaxFieldLength(n) if *n < -1 || *n == 0 => {
                invalid("the maximum field length is -1 or positive")
            }
            TransactionOption::SpanParent(parent) if parent.len() != SPAN_PARENT_LEN => {
                invalid("the span parent is 33 bytes")
            }
            TransactionOption::Tag(_) | TransactionOption::AutoThrottleTag(_) => {
                Err(Error::with_message(
                    INVALID_OPTION,
                    "transaction tags are not supported by fdb-native-client yet",
                ))
            }
            _ => Ok(()),
        }
    }
}

/// The options that persist across `on_error` (`persistent="true"` in `fdb.options`).
#[derive(Clone, Copy, Debug)]
pub(super) struct Persistent {
    pub(super) timeout: Option<Duration>,
    pub(super) retry_limit: Option<u32>,
    pub(super) max_retry_delay: Duration,
}

/// The options a reset clears.
#[derive(Clone, Debug)]
pub(super) struct Flags {
    pub(super) causal_write_risky: bool,
    pub(super) causal_read_risky: bool,
    /// `LOCK_AWARE` or `READ_LOCK_AWARE` (`TransactionOptions::lockAware`).
    pub(super) lock_aware: bool,
    /// `READ_LOCK_AWARE` without `LOCK_AWARE` (`TransactionOptions::readOnly`).
    pub(super) read_only: bool,
    pub(super) use_provisional_proxies: bool,
    pub(super) report_conflicting_keys: bool,
    pub(super) access_system_keys: bool,
    pub(super) read_system_keys: bool,
    pub(super) size_limit: u64,
    pub(super) priority: Priority,
    pub(super) max_grv_queue_delay_ms: Option<i64>,
    pub(super) first_in_batch: bool,
    pub(super) commit_on_first_proxy: bool,
    pub(super) bypass_storage_quota: bool,
    /// `NEXT_WRITE_NO_WRITE_CONFLICT_RANGE`, until a write uses it up.
    pub(super) next_write_no_write_conflict_range: bool,
    pub(super) used_during_commit_protection_disable: bool,
    /// `DEBUG_TRANSACTION_IDENTIFIER`.
    pub(super) debug_identifier: Option<String>,
    /// `READ_YOUR_WRITES_DISABLE` (`options.readYourWritesDisabled`).
    pub(super) read_your_writes_disabled: bool,
    /// `options.snapshotRywEnabled`: snapshot reads see the writes while it is above 0.
    pub(super) snapshot_ryw: i32,
    /// `BYPASS_UNREADABLE`.
    pub(super) bypass_unreadable: bool,
}

impl Flags {
    pub(super) fn new(size_limit: u64, snapshot_ryw: i32) -> Flags {
        Flags {
            causal_write_risky: false,
            causal_read_risky: false,
            lock_aware: false,
            read_only: false,
            use_provisional_proxies: false,
            report_conflicting_keys: false,
            access_system_keys: false,
            read_system_keys: false,
            size_limit,
            priority: Priority::Default,
            max_grv_queue_delay_ms: None,
            first_in_batch: false,
            commit_on_first_proxy: false,
            bypass_storage_quota: false,
            next_write_no_write_conflict_range: false,
            used_during_commit_protection_disable: false,
            debug_identifier: None,
            read_your_writes_disabled: false,
            snapshot_ryw,
            bypass_unreadable: false,
        }
    }

    /// `getMaxReadKey()`.
    pub(super) fn max_read_key(&self) -> &'static [u8] {
        if self.access_system_keys || self.read_system_keys {
            SYSTEM_KEYS_END
        } else {
            SYSTEM_KEYS_BEGIN
        }
    }

    /// `getMaxWriteKey()`.
    pub(super) fn max_write_key(&self) -> &'static [u8] {
        if self.access_system_keys {
            SYSTEM_KEYS_END
        } else {
            SYSTEM_KEYS_BEGIN
        }
    }
}

impl State {
    /// `setOptionImpl` of the read-your-writes layer and `Transaction::setOption`: validates
    /// `option` and applies it. A clock-dependent effect (the timeout's) is the caller's.
    pub(super) fn apply_option(&mut self, option: &TransactionOption) -> Result<(), Error> {
        use TransactionOption::*;
        option.validate(self.buffer.limits().transaction_size_limit)?;
        match option {
            Timeout(d) => {
                let d = Duration::from_millis(millis(*d) as u64);
                self.persistent.timeout = (!d.is_zero()).then_some(d);
            }
            RetryLimit(n) => self.persistent.retry_limit = u32::try_from(*n).ok(),
            MaxRetryDelay(d) => {
                self.persistent.max_retry_delay = Duration::from_millis(millis(*d) as u64);
            }
            SizeLimit(n) => self.flags.size_limit = u64::from(*n),
            CausalWriteRisky => self.flags.causal_write_risky = true,
            CausalReadRisky => self.flags.causal_read_risky = true,
            NextWriteNoWriteConflictRange => self.flags.next_write_no_write_conflict_range = true,
            CommitOnFirstProxy => self.flags.commit_on_first_proxy = true,
            ReadYourWritesDisable => {
                // `reading.getFutureCount() > 0 || !cache.empty() || !writes.empty()` (with
                // read-your-writes disabled, the writes are in the buffer only).
                if self.reading.count > 0
                    || !self.ryw.cache.is_empty()
                    || !self.ryw.writes.is_empty()
                    || !self.buffer.is_empty()
                {
                    return Err(Error::with_message(
                        CLIENT_INVALID_OPERATION,
                        "read-your-writes cannot be disabled after a read or a write",
                    ));
                }
                self.flags.read_your_writes_disabled = true;
            }
            PrioritySystemImmediate => self.flags.priority = Priority::Immediate,
            PriorityBatch => self.flags.priority = Priority::Batch,
            InitializeNewDatabase => {
                if !matches!(self.read_version, ReadVersion::Unset) {
                    return Err(Error::new(READ_VERSION_ALREADY_SET));
                }
                self.read_version = ReadVersion::Set(0);
                self.flags.causal_write_risky = true;
            }
            AccessSystemKeys => self.flags.access_system_keys = true,
            ReadSystemKeys => self.flags.read_system_keys = true,
            BypassStorageQuota => self.flags.bypass_storage_quota = true,
            DebugTransactionIdentifier(id) => match &self.flags.debug_identifier {
                Some(old) if old != id => {
                    return Err(Error::with_message(
                        CLIENT_INVALID_OPERATION,
                        "the transaction identifier cannot change",
                    ));
                }
                _ => self.flags.debug_identifier = Some(id.clone()),
            },
            LogTransaction => {
                if self.flags.debug_identifier.is_none() {
                    return Err(Error::with_message(
                        CLIENT_INVALID_OPERATION,
                        "the debug transaction identifier must be set before logging the \
                         transaction",
                    ));
                }
            }
            LockAware => {
                self.flags.lock_aware = true;
                self.flags.read_only = false;
            }
            ReadLockAware => {
                if !self.flags.lock_aware {
                    self.flags.lock_aware = true;
                    self.flags.read_only = true;
                }
            }
            UsedDuringCommitProtectionDisable => {
                self.flags.used_during_commit_protection_disable = true;
            }
            FirstInBatch => self.flags.first_in_batch = true,
            UseProvisionalProxies => self.flags.use_provisional_proxies = true,
            ReportConflictingKeys => self.flags.report_conflicting_keys = true,
            MaxGrvQueueDelay(d) => self.flags.max_grv_queue_delay_ms = Some(millis(*d) as i64),
            // Validated, nothing to do.
            CausalReadDisable
            | IncludePortInAddress
            | ReadServerSideCacheEnable
            | ReadServerSideCacheDisable
            | ReadPriorityNormal
            | ReadPriorityLow
            | ReadPriorityHigh
            | RawAccess
            | DebugRetryLogging(_)
            | TransactionLoggingMaxFieldLength(_)
            | ServerRequestTracing
            | SpanParent(_)
            | ExpensiveClearCostEstimationEnable
            | UseGrvCache => {}
            SnapshotRywEnable => {
                self.flags.snapshot_ryw = self.flags.snapshot_ryw.saturating_add(1)
            }
            SnapshotRywDisable => {
                self.flags.snapshot_ryw = self.flags.snapshot_ryw.saturating_sub(1)
            }
            BypassUnreadable => self.flags.bypass_unreadable = true,
            // `validate` refuses them.
            Tag(_) | AutoThrottleTag(_) => unreachable!("refused by validate"),
        }
        Ok(())
    }
}
