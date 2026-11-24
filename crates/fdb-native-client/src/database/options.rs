//! Database options (`FDBDatabaseOption`, `fdb.options`): [`DatabaseOption`], for
//! [`Database::set_option`](crate::Database::set_option).
//!
//! | Option | Code | Status |
//! |---|---|---|
//! | [`LocationCacheSize`](DatabaseOption::LocationCacheSize) | 10 | supported (the bound of the location cache, from the next insert on) |
//! | [`MaxWatches`](DatabaseOption::MaxWatches) | 20 | supported |
//! | [`MachineId`](DatabaseOption::MachineId) | 21 | stored (the client's zone id; C++ uses it only with a knob that is off by default); forgets the cached locations |
//! | [`DatacenterId`](DatabaseOption::DatacenterId) | 22 | supported (replicas in this data center are preferred); forgets the cached locations |
//! | [`SnapshotRywEnable`](DatabaseOption::SnapshotRywEnable) / [`Disable`](DatabaseOption::SnapshotRywDisable) | 26 / 27 | supported (a counter, as C++'s `snapshotRywEnabled`) |
//! | [`TransactionLoggingMaxFieldLength`](DatabaseOption::TransactionLoggingMaxFieldLength) | 405 | default for `TRANSACTION_LOGGING_MAX_FIELD_LENGTH` (validated; no-op) |
//! | [`TransactionTimeout`](DatabaseOption::TransactionTimeout) | 500 | default for `TIMEOUT` |
//! | [`TransactionRetryLimit`](DatabaseOption::TransactionRetryLimit) | 501 | default for `RETRY_LIMIT` |
//! | [`TransactionMaxRetryDelay`](DatabaseOption::TransactionMaxRetryDelay) | 502 | default for `MAX_RETRY_DELAY` |
//! | [`TransactionSizeLimit`](DatabaseOption::TransactionSizeLimit) | 503 | default for `SIZE_LIMIT` |
//! | [`TransactionCausalReadRisky`](DatabaseOption::TransactionCausalReadRisky) | 504 | default for `CAUSAL_READ_RISKY` |
//! | [`TransactionIncludePortInAddress`](DatabaseOption::TransactionIncludePortInAddress) | 505 | default for `INCLUDE_PORT_IN_ADDRESS` (no-op) |
//! | [`TransactionBypassUnreadable`](DatabaseOption::TransactionBypassUnreadable) | 700 | default for `BYPASS_UNREADABLE` |
//! | [`TransactionUsedDuringCommitProtectionDisable`](DatabaseOption::TransactionUsedDuringCommitProtectionDisable) | 701 | default for `USED_DURING_COMMIT_PROTECTION_DISABLE` |
//! | [`TransactionReportConflictingKeys`](DatabaseOption::TransactionReportConflictingKeys) | 702 | default for `REPORT_CONFLICTING_KEYS` |
//!
//! Not offered: `USE_CONFIG_DATABASE` (800, 7.4 only: the configuration database is a server-side tool's), `TEST_CAUSAL_READ_RISKY` (a
//! simulation aid) and `TRANSACTION_AUTOMATIC_IDEMPOTENCY` (no idempotency ids yet).
//!
//! # Transaction defaults
//!
//! The `Transaction*` options are **defaults for new transactions**
//! (`DatabaseContext::setOption`, `defaultFor` in `fdb.options`, NativeAPI 7.4:2345-2352):
//! they are the transaction option of the same name set at the start of every transaction
//! created from then on (and of a transaction after a full [`reset`](crate::Transaction::reset),
//! which copies the database's defaults again), as if the transaction had called
//! [`set_option`](crate::Transaction::set_option) first thing. A transaction keeps what it
//! was created with: later changes of the defaults do not reach it before its reset. They
//! also stay across [`on_error`](crate::Transaction::on_error) whether or not the option is
//! persistent (C++ keeps them in the transaction's `persistentOptions`): a database
//! `TransactionSizeLimit` is back after every retry. The transaction's own `set_option`
//! overrides the default.
//!
//! Setting one option again replaces its earlier default. The values are checked when
//! they are set (C++ checks them when a transaction is created, and fails there).

use std::time::Duration;

use crate::transaction::TransactionOption;

/// A database option (`FDBDatabaseOption`), for
/// [`Database::set_option`](crate::Database::set_option). See the [module
/// documentation](self).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DatabaseOption {
    /// `LOCATION_CACHE_SIZE` (10): the most shards whose location is cached (C++'s default
    /// is 100 000 in `fdb.options`, but `LOCATION_CACHE_EVICTION_SIZE`, 600 000, in
    /// `DatabaseContext`; here [`ReadConfig::location_cache_size`], 600 000). 0 to
    /// `i32::MAX` (`invalid_option_value` otherwise; 0 is taken as 1). As in C++, a smaller
    /// bound evicts when the next locations are cached, not at once.
    ///
    /// [`ReadConfig::location_cache_size`]: crate::read::ReadConfig::location_cache_size
    LocationCacheSize(usize),
    /// `MAX_WATCHES` (20): the most watches that may be outstanding at once (default 10 000);
    /// 0 to 1 000 000 (`invalid_option_value` otherwise). Watches already outstanding stay,
    /// whatever the new cap.
    MaxWatches(usize),
    /// `MACHINE_ID` (21): the machine id given to the `fdbserver` processes on this client's
    /// machine (C++ stores it as the client locality's `zoneid`); `None` clears it. C++
    /// prefers replicas on the same machine only with the knob
    /// `LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED`, off by default and not offered here, so the
    /// id is stored ([`Reader::client_zone_id`]) and changes no choice. As in C++, the
    /// cached key locations are forgotten.
    ///
    /// [`Reader::client_zone_id`]: crate::read::Reader::client_zone_id
    MachineId(Option<Vec<u8>>),
    /// `DATACENTER_ID` (22): the data center id given to the `fdbserver` processes of this
    /// client's data center (the client locality's `dcid`); `None` clears it. Reads prefer
    /// the replicas whose `dcid` is this one (`loadBalanceDistance`'s `SAME_DC`; the
    /// initial value is [`ReadConfig::client_dc_id`]). As in C++, the cached key locations
    /// are forgotten, so that every replica's distance is computed again.
    ///
    /// [`ReadConfig::client_dc_id`]: crate::read::ReadConfig::client_dc_id
    DatacenterId(Option<Vec<u8>>),
    /// `SNAPSHOT_RYW_ENABLE` (26): snapshot reads see the transactions' writes (the default).
    /// As in C++ (`DatabaseContext::snapshotRywEnabled`), this and
    /// [`SnapshotRywDisable`](Self::SnapshotRywDisable) count up and down from 1: snapshot
    /// reads see the writes while the count is above 0. A transaction starts (and restarts,
    /// after a reset or `on_error`) with the count the database has then, and its own
    /// [`TransactionOption::SnapshotRywEnable`] / `Disable` count from there.
    SnapshotRywEnable,
    /// `SNAPSHOT_RYW_DISABLE` (27): snapshot reads do not see the transactions' writes (see
    /// [`SnapshotRywEnable`](Self::SnapshotRywEnable)).
    SnapshotRywDisable,
    /// `TRANSACTION_LOGGING_MAX_FIELD_LENGTH` (405): the default
    /// [`TransactionLoggingMaxFieldLength`](TransactionOption::TransactionLoggingMaxFieldLength)
    /// (validated as it is; there is no transaction logging, so it does nothing).
    TransactionLoggingMaxFieldLength(i32),
    /// `TRANSACTION_TIMEOUT` (500): the default [`Timeout`](TransactionOption::Timeout).
    TransactionTimeout(Duration),
    /// `TRANSACTION_RETRY_LIMIT` (501): the default
    /// [`RetryLimit`](TransactionOption::RetryLimit).
    TransactionRetryLimit(i32),
    /// `TRANSACTION_MAX_RETRY_DELAY` (502): the default
    /// [`MaxRetryDelay`](TransactionOption::MaxRetryDelay).
    TransactionMaxRetryDelay(Duration),
    /// `TRANSACTION_SIZE_LIMIT` (503): the default [`SizeLimit`](TransactionOption::SizeLimit).
    TransactionSizeLimit(u32),
    /// `TRANSACTION_CAUSAL_READ_RISKY` (504): every transaction is
    /// [`CausalReadRisky`](TransactionOption::CausalReadRisky).
    TransactionCausalReadRisky,
    /// `TRANSACTION_INCLUDE_PORT_IN_ADDRESS` (505): every transaction is
    /// [`IncludePortInAddress`](TransactionOption::IncludePortInAddress) (a no-op).
    TransactionIncludePortInAddress,
    /// `TRANSACTION_BYPASS_UNREADABLE` (700): every transaction is
    /// [`BypassUnreadable`](TransactionOption::BypassUnreadable).
    TransactionBypassUnreadable,
    /// `TRANSACTION_USED_DURING_COMMIT_PROTECTION_DISABLE` (701): every transaction is
    /// [`UsedDuringCommitProtectionDisable`](TransactionOption::UsedDuringCommitProtectionDisable).
    TransactionUsedDuringCommitProtectionDisable,
    /// `TRANSACTION_REPORT_CONFLICTING_KEYS` (702): every transaction is
    /// [`ReportConflictingKeys`](TransactionOption::ReportConflictingKeys).
    TransactionReportConflictingKeys,
}

impl DatabaseOption {
    /// The option's code in `fdb.options` (`FDBDatabaseOption`).
    pub const fn code(&self) -> i32 {
        match self {
            DatabaseOption::LocationCacheSize(_) => 10,
            DatabaseOption::MaxWatches(_) => 20,
            DatabaseOption::MachineId(_) => 21,
            DatabaseOption::DatacenterId(_) => 22,
            DatabaseOption::SnapshotRywEnable => 26,
            DatabaseOption::SnapshotRywDisable => 27,
            DatabaseOption::TransactionLoggingMaxFieldLength(_) => 405,
            DatabaseOption::TransactionTimeout(_) => 500,
            DatabaseOption::TransactionRetryLimit(_) => 501,
            DatabaseOption::TransactionMaxRetryDelay(_) => 502,
            DatabaseOption::TransactionSizeLimit(_) => 503,
            DatabaseOption::TransactionCausalReadRisky => 504,
            DatabaseOption::TransactionIncludePortInAddress => 505,
            DatabaseOption::TransactionBypassUnreadable => 700,
            DatabaseOption::TransactionUsedDuringCommitProtectionDisable => 701,
            DatabaseOption::TransactionReportConflictingKeys => 702,
        }
    }

    /// The transaction option this one is the default for (`defaultFor` in `fdb.options`);
    /// `None` for the options that are not transaction defaults.
    pub fn default_for(&self) -> Option<TransactionOption> {
        Some(match self {
            DatabaseOption::LocationCacheSize(_)
            | DatabaseOption::MaxWatches(_)
            | DatabaseOption::MachineId(_)
            | DatabaseOption::DatacenterId(_)
            | DatabaseOption::SnapshotRywEnable
            | DatabaseOption::SnapshotRywDisable => return None,
            DatabaseOption::TransactionLoggingMaxFieldLength(n) => {
                TransactionOption::TransactionLoggingMaxFieldLength(*n)
            }
            DatabaseOption::TransactionTimeout(d) => TransactionOption::Timeout(*d),
            DatabaseOption::TransactionRetryLimit(n) => TransactionOption::RetryLimit(*n),
            DatabaseOption::TransactionMaxRetryDelay(d) => TransactionOption::MaxRetryDelay(*d),
            DatabaseOption::TransactionSizeLimit(n) => TransactionOption::SizeLimit(*n),
            DatabaseOption::TransactionCausalReadRisky => TransactionOption::CausalReadRisky,
            DatabaseOption::TransactionIncludePortInAddress => {
                TransactionOption::IncludePortInAddress
            }
            DatabaseOption::TransactionBypassUnreadable => TransactionOption::BypassUnreadable,
            DatabaseOption::TransactionUsedDuringCommitProtectionDisable => {
                TransactionOption::UsedDuringCommitProtectionDisable
            }
            DatabaseOption::TransactionReportConflictingKeys => {
                TransactionOption::ReportConflictingKeys
            }
        })
    }
}
