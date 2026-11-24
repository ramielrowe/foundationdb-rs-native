//! Database options (`FDBDatabaseOption`, `fdb.options`): [`DatabaseOption`], for
//! [`Database::set_option`](crate::Database::set_option).
//!
//! | Option | Code | Status |
//! |---|---|---|
//! | [`MaxWatches`](DatabaseOption::MaxWatches) | 20 | supported |
//! | [`SnapshotRywEnable`](DatabaseOption::SnapshotRywEnable) / [`Disable`](DatabaseOption::SnapshotRywDisable) | 26 / 27 | no-op until read-your-writes exists |
//! | [`TransactionTimeout`](DatabaseOption::TransactionTimeout) | 500 | default for `TIMEOUT` |
//! | [`TransactionRetryLimit`](DatabaseOption::TransactionRetryLimit) | 501 | default for `RETRY_LIMIT` |
//! | [`TransactionMaxRetryDelay`](DatabaseOption::TransactionMaxRetryDelay) | 502 | default for `MAX_RETRY_DELAY` |
//! | [`TransactionSizeLimit`](DatabaseOption::TransactionSizeLimit) | 503 | default for `SIZE_LIMIT` |
//! | [`TransactionCausalReadRisky`](DatabaseOption::TransactionCausalReadRisky) | 504 | default for `CAUSAL_READ_RISKY` |
//! | [`TransactionIncludePortInAddress`](DatabaseOption::TransactionIncludePortInAddress) | 505 | default for `INCLUDE_PORT_IN_ADDRESS` (no-op) |
//! | [`TransactionBypassUnreadable`](DatabaseOption::TransactionBypassUnreadable) | 700 | default for `BYPASS_UNREADABLE` (no-op) |
//! | [`TransactionUsedDuringCommitProtectionDisable`](DatabaseOption::TransactionUsedDuringCommitProtectionDisable) | 701 | default for `USED_DURING_COMMIT_PROTECTION_DISABLE` |
//! | [`TransactionReportConflictingKeys`](DatabaseOption::TransactionReportConflictingKeys) | 702 | default for `REPORT_CONFLICTING_KEYS` |
//!
//! Not offered: `LOCATION_CACHE_SIZE` (the cache is [`ReadConfig`](crate::read::ReadConfig)'s,
//! fixed when the database is opened), `MACHINE_ID` and `DATACENTER_ID` (the locality that
//! prefers a same-datacenter replica; there is none yet), `TEST_CAUSAL_READ_RISKY` (a
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
    /// `MAX_WATCHES` (20): the most watches that may be outstanding at once (default 10 000);
    /// 0 to 1 000 000 (`invalid_option_value` otherwise). Watches already outstanding stay,
    /// whatever the new cap.
    MaxWatches(usize),
    /// `SNAPSHOT_RYW_ENABLE` (26): snapshot reads see the transactions' writes (the default).
    /// No-op until read-your-writes exists.
    SnapshotRywEnable,
    /// `SNAPSHOT_RYW_DISABLE` (27): snapshot reads do not see the transactions' writes. No-op
    /// until read-your-writes exists.
    SnapshotRywDisable,
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
    /// [`BypassUnreadable`](TransactionOption::BypassUnreadable) (a no-op for now).
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
            DatabaseOption::MaxWatches(_) => 20,
            DatabaseOption::SnapshotRywEnable => 26,
            DatabaseOption::SnapshotRywDisable => 27,
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
            DatabaseOption::MaxWatches(_)
            | DatabaseOption::SnapshotRywEnable
            | DatabaseOption::SnapshotRywDisable => return None,
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
