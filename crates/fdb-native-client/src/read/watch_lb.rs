//! `watchValue` (NativeAPI 7.4:4215-4310, 8.0:2024-2133): one watch's conversation with the
//! storage servers of a key's shard, until a server reports that the value changed. The
//! database-wide bookkeeping of watches (the watch map, deduplication, the cap) is
//! [`crate::watch`]; this is the read path's part.
//!
//! * **The request.** `getKeyLocation(key, ..., watchValue, version)` for the shard's
//!   replicas (a cached location is refreshed when the `watchValue` endpoint of one of them
//!   failed on a server that is up, `checkOnlyEndpointFailed`), then `loadBalance` of a `WatchValueRequest(key, value, version)` over them.
//!   The storage server answers when the key's value (at its latest version, at or after
//!   `version`) differs from `value`, with the version it saw the difference at; a set of
//!   the same value does not answer. It may also answer with an error (below).
//! * **The load balancer** is the one of `getValue` ([`super::balance`]) without the queue
//!   model and without second requests: the first choice is random among the nearest
//!   replicas, the others follow in order; replicas the failure monitor marks failed are
//!   skipped (`all_alternatives_failed` after `allAlternativesFailedDelay` if all are);
//!   `server_overloaded`, `broken_promise`, `request_maybe_delivered` and (until every
//!   replica was tried) `process_behind` try the next replica, with the backoff of
//!   `basicLoadBalance` once all were tried; any other error is returned. A watch is a
//!   request that may stay outstanding for 30 s, so it is not counted in the queue model
//!   (C++ counts it, which makes the replica look busy to `getValue`).
//! * **A reply is checked against the committed version.** `waitForCommittedVersion`
//!   (NativeAPI 7.4:4142-4190): the storage server may have seen the change in a version that
//!   is later rolled back by a recovery, so the client asks the GRV proxies (priority
//!   `IMMEDIATE`) for read versions until one is at least the reply's. C++ sends each request
//!   on its own (`basicLoadBalance`); here they go through the read version batcher of the
//!   `IMMEDIATE` priority: a request waits up to the batcher's current batch time (half the
//!   recent reply latency, at most `GRV_BATCH_TIMEOUT`) and may share a request with others.
//!   If
//!   that version is `MAX_VERSIONS_IN_FLIGHT / 2` (50 000 000) or more ahead of the reply's
//!   (a recovery may have happened in between), the watch is sent again at that version.
//! * **Errors** (the `catch` of `watchValue`):
//!   - `wrong_shard_server`, `all_alternatives_failed`: invalidate the key's cached
//!     location, wait `WRONG_SHARD_SERVER_DELAY`, send again;
//!   - `watch_cancelled` (the storage server holds too many watches), `process_behind`:
//!     wait `WATCH_POLLING_TIME` (1 s), send again, which makes the watch a poll;
//!   - `timed_out` (a storage server times a watch out after 30 s, 20 s on an idle
//!     server): wait `FUTURE_VERSION_RETRY_DELAY` (10 ms), send again;
//!   - anything else (`future_version`, `transaction_too_old`, `operation_cancelled`, ...):
//!     wait `FUTURE_VERSION_RETRY_DELAY` and return it (no wait for `operation_cancelled`).
//! * **Not ported:** the `connectionRecord->onChange()` race (a watch of a database whose
//!   cluster file changed waits for the application to restart it), debug ids and spans.

use std::time::Duration;

use fdb_native_schema::domain::{WatchValue, WatchValueReply};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::{NetworkAddress, Uid};

use super::balance::client_error;
use super::grv::{Priority, ReadVersionOptions};
use super::location::Location;
use super::util::{secs, try_get_reply, try_get_reply_code};
use super::{KEY_SPACE_END, Reader, Shared, lock};
use crate::error::Error;
use crate::error::codes::*;

/// `GRV_ERROR_RETRY_DELAY` (5 s; ClientKnobs 7.4:67), jittered: `waitForCommittedVersion`'s wait
/// after a throttled or overloaded GRV proxy.
const GRV_ERROR_RETRY_DELAY: f64 = 5.0;
/// The version gap past which a watch is sent again (`v - resp.version < 50'000'000`).
const WATCH_VERSION_GAP: i64 = 50_000_000;
/// `LOAD_BALANCE_START_BACKOFF`, `LOAD_BALANCE_BACKOFF_RATE` and `LOAD_BALANCE_MAX_BACKOFF`
/// (`flow/Knobs.cpp` 7.4:285-287): the backoff once every replica was tried.
const BACKOFF_START: f64 = 0.01;
const BACKOFF_RATE: f64 = 2.0;
const BACKOFF_MAX: f64 = 5.0;

/// The waits of `watchValue`'s retry loop (ClientKnobs).
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct WatchTimings {
    /// `WATCH_POLLING_TIME` (1 s; ClientKnobs 7.4:129, 8.0:237): the wait before a watch is
    /// sent again after `watch_cancelled` or `process_behind`.
    pub polling_time: Duration,
    /// `FUTURE_VERSION_RETRY_DELAY` (10 ms; ClientKnobs 7.4:66, 8.0:172): the wait before a
    /// watch is sent again after `timed_out`, and before another error is returned.
    pub future_version_retry_delay: Duration,
}

impl Default for WatchTimings {
    fn default() -> Self {
        WatchTimings {
            polling_time: Duration::from_secs(1),
            future_version_retry_delay: Duration::from_millis(10),
        }
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Reader<R, W> {
    /// `watchValue`: resolves with the version at which a storage server saw `key`'s value
    /// differ from `value` (`None`: the key not being set), as of `version` or later, once
    /// that version is known to be committed. Sends the watch again as C++ does (see
    /// [`crate::watch`] for the C++ semantics); an error it returns is not retried here (`future_version`
    /// and `transaction_too_old` are the caller's to retry, `operation_cancelled` is a shut
    /// down database).
    ///
    /// `version` is the version `value` was read at (or the commit version of the
    /// transaction that set it): a positive version, else `client_invalid_operation`. A
    /// storage server answers `future_version` if it does not reach `version` for a while.
    pub async fn watch_value(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        version: i64,
        use_provisional_proxies: bool,
        timings: WatchTimings,
    ) -> Result<i64, Error> {
        if version <= 0 {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        if key >= KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        let shared = &self.inner.shared;
        let mut ver = version;
        // Kept until the next location is known, as C++'s `state locationInfo`.
        let mut _previous: Option<std::sync::Arc<Location>> = None;
        loop {
            // `getKeyLocation(..., &StorageServerInterface::watchValue, ...)`: a cached
            // location is refreshed if a replica's `watchValue` endpoint failed.
            let (_, location) = shared
                .key_location_for(
                    key,
                    version,
                    use_provisional_proxies,
                    false,
                    STORAGE_SERVER_STREAMS.watch_value,
                )
                .await?;
            _previous = Some(location.clone());
            let request = WatchValue {
                key: key.to_vec(),
                value: value.map(<[u8]>::to_vec),
                version: ver,
                tags: None,
                debug_id: None,
                span_context: Default::default(),
            };
            let attempt = async {
                let reply = shared.load_balance_watch(&location, request).await?;
                let committed = self
                    .wait_for_committed_version(reply.version, timings)
                    .await?;
                Ok::<_, Error>((reply.version, committed))
            };
            let error = match attempt.await {
                Ok((changed_at, committed)) => {
                    if committed - changed_at < WATCH_VERSION_GAP {
                        return Ok(changed_at);
                    }
                    // A recovery may have rolled the change back: ask again from there.
                    ver = committed;
                    continue;
                }
                Err(e) => e,
            };
            match error.code() {
                WRONG_SHARD_SERVER | ALL_ALTERNATIVES_FAILED => {
                    lock(&shared.locations).invalidate_key(key);
                    shared
                        .rt
                        .sleep(shared.config.wrong_shard_server_delay)
                        .await;
                }
                WATCH_CANCELLED | PROCESS_BEHIND => shared.rt.sleep(timings.polling_time).await,
                TIMED_OUT => shared.rt.sleep(timings.future_version_retry_delay).await,
                OPERATION_CANCELLED => return Err(error),
                _ => {
                    shared.rt.sleep(timings.future_version_retry_delay).await;
                    return Err(error);
                }
            }
        }
    }

    /// `waitForCommittedVersion`: a read version of at least `version`.
    async fn wait_for_committed_version(
        &self,
        version: i64,
        timings: WatchTimings,
    ) -> Result<i64, Error> {
        let shared = &self.inner.shared;
        let options = ReadVersionOptions {
            priority: Priority::Immediate,
            lock_aware: true,
            ..ReadVersionOptions::default()
        };
        loop {
            match self.get_read_version_with(options).await {
                Ok(v) if v >= version => return Ok(v),
                Ok(_) => shared.rt.sleep(timings.future_version_retry_delay).await,
                Err(e)
                    if matches!(
                        e.code(),
                        BATCH_TRANSACTION_THROTTLED | GRV_PROXY_MEMORY_LIMIT_EXCEEDED
                    ) =>
                {
                    let delay = lock(&shared.rng).jitter(GRV_ERROR_RETRY_DELAY);
                    shared.rt.sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// `loadBalance` of a `WatchValueRequest` over a shard's replicas (see the [module
    /// documentation](self)).
    async fn load_balance_watch(
        &self,
        location: &Location,
        request: WatchValue,
    ) -> Result<WatchValueReply, Error> {
        let n = location.servers.len();
        if n == 0 {
            // An empty team (C++ asserts): nothing to ask until the location is refreshed.
            return Err(Error::new(ALL_ALTERNATIVES_FAILED));
        }
        // The `watchValue` endpoint of each replica.
        let targets: Vec<(NetworkAddress, Uid)> = location
            .servers
            .iter()
            .map(|s| {
                let endpoint = s.server().endpoints().watch_value;
                (
                    self.peers.primary_address(&endpoint.addresses),
                    endpoint.token,
                )
            })
            .collect();
        let start = lock(&self.rng).below(location.best_count.clamp(1, n));
        let failed = |target: &(NetworkAddress, Uid)| {
            self.peers
                .failure_monitor()
                .endpoint_status(&target.0, target.1)
                .is_failed()
        };
        // How many replicas the walk has passed, and how many requests failed over.
        let mut cursor = 0usize;
        let mut num_attempts = 0;
        let mut backoff = 0.0;
        loop {
            self.check_open()?;
            let mut chosen = None;
            for _ in 0..n {
                let alt = (start + cursor) % n;
                if !failed(&targets[alt]) {
                    chosen = Some(alt);
                    break;
                }
                cursor += 1;
            }
            let Some(alt) = chosen else {
                // Everything is down: wait for a replica to come back.
                self.all_alternatives_failed_delay(&targets).await?;
                num_attempts = 0;
                continue;
            };
            if backoff > 0.0 {
                self.rt.sleep(secs(backoff)).await;
            }
            let (address, token) = targets[alt];
            let request = request.clone();
            let result = try_get_reply(&self.peers, &address, token, move |s, r, out| {
                request.encode_request_into(s, r, out)
            })
            .await;
            let error = match result {
                Ok((schema, payload)) => match decode(schema, &payload) {
                    Ok(reply) => return Ok(reply),
                    Err(e) => e,
                },
                Err(e) => e,
            };
            let code = try_get_reply_code(&error);
            let tried_all = cursor + 1 >= n;
            match code {
                SERVER_OVERLOADED | BROKEN_PROMISE | REQUEST_MAYBE_DELIVERED => {}
                PROCESS_BEHIND if !tried_all => {}
                _ => return Err(client_error(error)),
            }
            num_attempts += 1;
            if num_attempts >= n {
                backoff = BACKOFF_MAX.min(BACKOFF_START.max(backoff * BACKOFF_RATE));
            }
            cursor += 1;
        }
    }
}

/// A `WatchValueReply`, or why the request failed.
fn decode(schema: SchemaVersion, payload: &[u8]) -> Result<WatchValueReply, RequestError> {
    match WatchValue::decode_reply(schema, payload) {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(e)) => Err(RequestError::from_reply_error(e)),
        Err(e) => Err(RequestError::Decode(e)),
    }
}
