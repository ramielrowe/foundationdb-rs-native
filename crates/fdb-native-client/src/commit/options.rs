//! Per-commit options and the committer's configuration.

use std::time::Duration;

use super::limits::CommitLimits;

/// The transaction options that shape a commit (`TransactionOptions`, `NativeAPI.actor.cpp`
/// 7.4:6440-6470). Build one with [`CommitOptions::default`] and set the public fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitOptions {
    /// `CAUSAL_WRITE_RISKY`: do not add a self-conflicting key when no read and write
    /// conflict range intersect, and do not run the "make sure the commit is no longer in
    /// flight" dummy transaction after an unknown result. Default `false`.
    pub causal_write_risky: bool,
    /// `LOCK_AWARE`: sets `FLAG_IS_LOCK_AWARE`. Default `false`.
    pub lock_aware: bool,
    /// `FIRST_IN_BATCH`: sets `FLAG_FIRST_IN_BATCH`. Default `false`.
    pub first_in_batch: bool,
    /// `BYPASS_STORAGE_QUOTA`: sets `FLAG_BYPASS_STORAGE_QUOTA`. Default `false`.
    pub bypass_storage_quota: bool,
    /// `REPORT_CONFLICTING_KEYS`: the proxy says which read conflict ranges conflicted.
    pub report_conflicting_keys: bool,
    /// `READ_ONLY` (`readOnly`): a commit that has writes fails with `transaction_read_only`.
    pub read_only: bool,
    /// `COMMIT_ON_FIRST_PROXY`: send the commit to the first commit proxy of the
    /// `ClientDBInfo` instead of load balancing.
    pub commit_on_first_proxy: bool,
    /// `USE_PROVISIONAL_PROXIES`: also use the provisional proxies of a recovery that has not
    /// finished.
    pub use_provisional_proxies: bool,
    /// `SIZE_LIMIT`: the largest `getSize()` the commit may have (default
    /// `TRANSACTION_SIZE_LIMIT`, 10 000 000). C++ accepts 32 up to that value
    /// (`extractIntOption(value, 32, TRANSACTION_SIZE_LIMIT)`, `NativeAPI.actor.cpp`
    /// 7.4:7418); another value is `invalid_option_value`.
    pub size_limit: u64,
}

impl Default for CommitOptions {
    fn default() -> Self {
        CommitOptions {
            causal_write_risky: false,
            lock_aware: false,
            first_in_batch: false,
            bypass_storage_quota: false,
            report_conflicting_keys: false,
            read_only: false,
            commit_on_first_proxy: false,
            use_provisional_proxies: false,
            size_limit: 10_000_000,
        }
    }
}

/// How a [`Committer`](super::Committer) behaves besides the per-commit options.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CommitterConfig {
    /// The size limits to enforce. `None` (the default) picks them from the cluster's
    /// release ([`CommitLimits::for_schema`]).
    pub limits: Option<CommitLimits>,
    /// `DEFAULT_BACKOFF` (0.01 s): the first retry delay of the dummy transaction that
    /// settles an unknown result.
    pub initial_backoff: Duration,
    /// `DEFAULT_MAX_BACKOFF` (1 s).
    pub max_backoff: Duration,
    /// `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s): the cap after
    /// `commit_proxy_memory_limit_exceeded`, `grv_proxy_memory_limit_exceeded`,
    /// `transaction_throttled_hot_shard` and `transaction_rejected_range_locked`
    /// (`Transaction::getBackoff`, 7.4:6433-6438).
    pub resource_constrained_max_backoff: Duration,
    /// `BACKOFF_GROWTH_RATE` (2).
    pub backoff_growth: f64,
    /// `FUTURE_VERSION_RETRY_DELAY` (0.01 s): the delay after `transaction_too_old` and
    /// `future_version`.
    pub future_version_retry_delay: Duration,
    /// Seed of the random numbers (the self-conflict key, the proxy chosen, the backoff
    /// jitter). `None` draws one.
    pub seed: Option<u64>,
}

impl Default for CommitterConfig {
    fn default() -> Self {
        CommitterConfig {
            limits: None,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            resource_constrained_max_backoff: Duration::from_secs(30),
            backoff_growth: 2.0,
            future_version_retry_delay: Duration::from_millis(10),
            seed: None,
        }
    }
}
