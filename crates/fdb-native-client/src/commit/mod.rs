//! The commit path: a transaction's mutations and conflict ranges to a `CommitTransactionRequest`,
//! sent to a commit proxy, and its `CommitID` (or error) back.
//!
//! * [`CommitBuffer`]: what the C++ `Transaction` buffers (`set`, `clear`, `clear_range`,
//!   atomic operations, conflict ranges).
//! * [`Committer::commit`]: `Transaction::commitMutations` and `tryCommit`.
//! * [`CommitError`]: the errors, with their FDB codes. This is the commit path's own error
//!   type; it will be unified with the client's general error type in let.6.
//!
//! ```no_run
//! use std::sync::Arc;
//! use fdb_native_client::commit::{CommitBuffer, CommitOptions, Committer, CommitterConfig};
//! use fdb_native_client::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
//! use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
//! use fdb_native_transport::runtime::TokioRuntime;
//!
//! # async fn example(read_version: i64) -> Result<(), Box<dyn std::error::Error>> {
//! let rt = TokioRuntime::new();
//! let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
//! let record = ConnectionRecord::open("/etc/foundationdb/fdb.cluster")?;
//! let monitor = Arc::new(ClusterMonitor::start(
//!     rt.clone(), peers.clone(), record, ClusterMonitorConfig::default()));
//! let committer = Committer::new(rt, peers, monitor, CommitterConfig::default());
//!
//! let mut buffer = CommitBuffer::new(committer.limits());
//! buffer.set(b"hello", b"world")?;
//! buffer.clear_range(b"a", b"b")?;
//! let outcome = committer.commit_buffer(buffer, read_version, &CommitOptions::default()).await?;
//! println!("committed at {}", outcome.version());
//! # Ok(())
//! # }
//! ```
//!
//! # What C++ does, and what is ported
//!
//! Citations are `file:line` on `origin/release-7.4` (7.4) and `origin/release-8.0` (8.0).
//! `NativeAPI` is `fdbclient/NativeAPI.actor.cpp` (7.4) / `NativeAPI.cpp` (8.0); `LB` is
//! `fdbrpc/include/fdbrpc/LoadBalance.actor.h` (7.4) / `LoadBalance.h` (8.0). The logic is the
//! same on both releases, except for what is noted (7.4 has tenants; 8.0's `tryCommit` also
//! treats `never_reply` as an unknown result and its `getMaxWriteKeySize` has no tenant
//! slack).
//!
//! ## Building the request
//!
//! * **Mutations and write conflict ranges** (`Transaction::set` 7.4:6277, 8.0:4020;
//!   `atomicOp` 7.4:6296, 8.0:4039; `clear(range)` 7.4:6334, 8.0:4077; `clear(key)`
//!   7.4:6362, 8.0:4105): `set` is a `SetValue` of `r.begin` for `r = singleKeyRange(key)`
//!   and adds `r` to the write conflict ranges; `clear(key)` is the `ClearRange`
//!   `[key, key\x00)` with the same conflict range (a key over the limit is skipped);
//!   `clear(range)` truncates keys over the limit to limit + 1 byte, drops an empty range,
//!   and adds the range as write conflict range; an atomic operation adds the key's
//!   conflict range unless it is `SetVersionstampedKey`, and `Min`/`And` become
//!   `MinV2`/`AndV2` (`apiVersionAtLeast(510)`). **Atomic operations are supported except the
//!   two versionstamp operations**, which need the offset validation of the read-your-writes
//!   layer and are rejected with [`Unsupported`](CommitError::Unsupported).
//! * **Key and value limits**: `set` and `atomicOp` throw `key_too_large` above
//!   `getMaxWriteKeySize` (`KEY_SIZE_LIMIT` 10000, or `SYSTEM_KEY_SIZE_LIMIT` 30000 for keys
//!   starting `\xff`; 7.4:11995, 8.0:7378) and `value_too_large` above `VALUE_SIZE_LIMIT`
//!   (100000). The knobs are the same on both releases (`ClientKnobs.cpp` 7.4:78-81,
//!   8.0:183-186). Conflict ranges and clears use `getMaxKeySize`, which on 7.4 allows 8 more
//!   bytes to non-system keys (the tenant prefix size). [`CommitLimits::for_schema`].
//! * **Read conflict ranges** are the reads' (`addReadConflictRange` 7.4:6242, 8.0:3985:
//!   truncated as above, empty dropped); the caller passes them in.
//! * **`commitMutations`** (7.4:7129, 8.0:4820): no write conflict range and no mutation:
//!   nothing is sent ([`CommitOutcome::NothingToCommit`]). The `readOnly` option with writes:
//!   `transaction_read_only`. `getSize()` (7.4:8106: the `expectedSize()` of the mutations
//!   and of both conflict range lists: 44 bytes per mutation and 24 per range plus their key
//!   and value bytes, [`expected_size`]) over `SIZE_LIMIT` (default 10 000 000):
//!   `transaction_too_large`, checked **before** the self-conflicting key is added.
//! * **Self-conflicting** (`makeSelfConflicting`, 7.4:6268, 8.0:4011; the condition at
//!   7.4:7192, 8.0:4883): unless `causalWriteRisky`, if no write conflict range intersects a
//!   read conflict range (`intersects`, 7.4:6536, which sorts both lists in place by
//!   `begin`), add the single-key range `\xff/SC/<random uid>` (21 bytes, [`self_conflict_key`
//!   ](fdb_native_schema::self_conflict_key)) to **both** lists. A blind write therefore
//!   conflicts with itself, so that an unknown result can be settled.
//! * **Flags**: `FLAG_IS_LOCK_AWARE` (the `LOCK_AWARE` option; off by default),
//!   `FLAG_FIRST_IN_BATCH`, `FLAG_BYPASS_STORAGE_QUOTA`, and `report_conflicting_keys`. The
//!   request's `transaction.lock_aware` stays false, as in C++. The read snapshot is the
//!   transaction's read version. On 7.4 the request carries the tenant-less `TenantInfo` (the
//!   schema set does that).
//!
//! ## Sending (`tryCommit`, 7.4:6894, 8.0:4572)
//!
//! * A proxy is chosen as `basicLoadBalance(..., AtMostOnce::True)` does (LB 7.4:1037, 8.0:665):
//!   from a random start, the first whose endpoint the failure monitor does not mark failed;
//!   if all are failed, wait until one is not. The request goes out with `tryGetReply` (at
//!   most once). With `commitOnFirstProxy` it goes to the first proxy of the `ClientDBInfo`
//!   (`tryGetReply` wrapped in `brokenPromiseToMaybeDelivered`).
//! * It is raced against `onProxiesChanged()` (7.4:6977, 8.0:4656): if the commit or GRV
//!   proxies of the `ClientDBInfo` change while it is outstanding, the request is cancelled
//!   and the result is `request_maybe_delivered`, never a resend.
//! * `CommitID` with a valid version: committed. With `invalidVersion`: `not_committed`, with
//!   the conflicting read ranges if `conflictingKRIndices` is present.
//! * **Errors**, as `tryCommit`'s `catch` (7.4:7060-7104, 8.0:4755-4799): `request_maybe_delivered` and
//!   `commit_unknown_result` (and on 8.0 `never_reply`) are an unknown result; so are a
//!   `broken_promise` reply and a lost connection after the send, which `tryGetReply` and
//!   `AtMostOnce` turn into `request_maybe_delivered`. Unless `causalWriteRisky`, the
//!   committer then runs `commitDummyTransaction` (7.4:6631, 8.0:4373) on the first key of
//!   the intersection of the request's read and write conflict ranges (a read and a write
//!   conflict range on that key, `CAUSAL_WRITE_RISKY`, `LOCK_AWARE`, retried with `onError`'s
//!   backoff until it commits): after that the original request is either committed or can
//!   never commit. Then [`CommitUnknownResult`](CommitError::CommitUnknownResult). Any other
//!   error is returned as is ([`CommitError`] table).
//!
//! ## Differences from C++
//!
//! * **Certainly-not-delivered is distinguished.** `tryGetReply` cannot tell a request that
//!   was never sent from one that was; the transport can
//!   ([`RequestError::certainly_not_delivered`](fdb_native_transport::rpc::RequestError::certainly_not_delivered)).
//!   When a connection fails before the request is sent, the next proxy is tried (each once);
//!   if none got it the result is [`NotSent`](CommitError::NotSent) (code 1026), which is safe
//!   and retryable (`is_retryable`) and needs no dummy transaction. C++ reports `commit_unknown_result`.
//! * **An unknown result stays unknown.** In C++ an error of the dummy transaction that
//!   `onError` does not retry escapes `tryCommit` in place of `commit_unknown_result`, so the
//!   caller would believe the original did not commit although it may have been delivered.
//!   Here every way out of the settling is [`CommitUnknownResult`](CommitError::CommitUnknownResult):
//!   the dummy's own failure only means that the original was not settled. The dummy's read
//!   version is requested again if the proxies change while it waits (as
//!   `getConsistentReadVersion` does) and its backoff is capped at
//!   `RESOURCE_CONSTRAINED_MAX_BACKOFF` for the resource-constrained errors.
//! * **Shutdown of the cluster monitor**: a commit that was never offered to a proxy is
//!   [`Closed`](CommitError::Closed); once the request may have been delivered it is an
//!   unknown result. Nothing is sent after the shutdown.
//! * **The dummy's key**: when the first key of the intersection was truncated to its limit
//!   plus one byte, the one-key range would be cut to nothing (C++ then commits an empty
//!   transaction that settles nothing), so the intersection's range is used instead.
//! * **No proxies yet** (a recovery): C++ would wait and then report an unknown result without
//!   having sent anything; the committer waits for proxies and then sends.
//! * **Read version**: supplied by the caller. A transaction that did not read has none; C++
//!   fetches one (`FLAG_CAUSAL_READ_RISKY`) in `tryCommit`. The committer fetches one itself
//!   only for the dummy transaction.
//! * **Not ported yet**: idempotency ids (they need the idempotency-record read path),
//!   versionstamped mutations, transaction tags and commit cost estimation, tenants,
//!   `checkWrites`, debug ids and span contexts, the metadata-version cache and
//!   `expireIdempotencyId`. The read-your-writes layer's `extraConflictRanges` and its key
//!   legality checks (`key_outside_legal_range`, access to `\xff` keys) belong to let.6.

mod buffer;
mod committer;
mod error;
mod limits;
mod options;
mod prepare;
mod rng;

pub use buffer::CommitBuffer;
pub use committer::{CommitOutcome, Committer, versionstamp};
pub use error::{CommitError, error_code};
pub use limits::{CommitLimits, KEY_RANGE_REF_SIZE, MUTATION_REF_SIZE, expected_size};
pub use options::{CommitOptions, CommitterConfig};

#[cfg(test)]
mod tests;
