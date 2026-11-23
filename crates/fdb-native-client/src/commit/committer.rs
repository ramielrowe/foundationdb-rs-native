//! [`Committer`]: `tryCommit` and its unknown-result handling.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;

use fdb_native_schema::domain::{
    Commit, CommitId, GetReadVersion, INVALID_VERSION, KeyRange, Mutation,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::{NoTls, PeerHandle, PeerManager, PeerRef, Reply, StreamWrapper};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::{DecodeError, EncodeError, Uid};

use super::buffer::CommitBuffer;
use super::error;
use super::limits::{CommitLimits, normalize_range};
use super::options::{CommitOptions, CommitterConfig};
use super::prepare::{intersects, prepare};
use super::rng::Rng;
use crate::cluster::{ClusterInfo, ClusterMonitor, CommitProxyEndpoints, MonitorClosed};
use crate::error::Error;
use crate::error::codes::{
    COMMIT_UNKNOWN_RESULT, FUTURE_VERSION, NEVER_REPLY, TRANSACTION_TOO_OLD,
};

/// What a successful [`Committer::commit`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitOutcome {
    /// There was nothing to write (no mutation and no write conflict range): like C++, no
    /// request was sent. The committed version is `-1` and there is no versionstamp
    /// (`no_commit_version`).
    NothingToCommit,
    /// The transaction committed.
    Committed(CommitId),
}

impl CommitOutcome {
    /// The proxy's reply, if a commit was sent.
    pub fn commit_id(&self) -> Option<&CommitId> {
        match self {
            CommitOutcome::NothingToCommit => None,
            CommitOutcome::Committed(id) => Some(id),
        }
    }

    /// `getCommittedVersion()`: the commit version, or `-1` for a transaction with nothing to
    /// write.
    pub fn version(&self) -> i64 {
        self.commit_id().map_or(INVALID_VERSION, |id| id.version)
    }

    /// `getVersionstamp()`: the 10 bytes (commit version, then the transaction's index in its
    /// batch, both big-endian) that `SetVersionstamped*` operations of this transaction wrote;
    /// `None` if nothing was committed.
    pub fn versionstamp(&self) -> Option<[u8; 10]> {
        self.commit_id().map(versionstamp)
    }
}

/// `placeVersionstamp`: the commit version and the transaction's batch index, big-endian.
pub fn versionstamp(id: &CommitId) -> [u8; 10] {
    let mut out = [0u8; 10];
    out[..8].copy_from_slice(&id.version.to_be_bytes());
    out[8..].copy_from_slice(&id.txn_batch_id.to_be_bytes());
    out
}

/// How one send of a commit request ended, short of success.
#[derive(Debug)]
enum Attempt {
    /// `request_maybe_delivered` / `commit_unknown_result`: the commit may have happened and
    /// may still be in flight.
    Unknown,
    /// A definite error.
    Error(Error),
}

/// Sends commits to the commit proxies, as `tryCommit` does. See the [module
/// documentation](super) for the rules.
///
/// It holds no per-transaction state and may be shared between tasks (`&self` methods);
/// build it once over the [`PeerManager`] and [`ClusterMonitor`] of a database.
pub struct Committer<R: Runtime, W: StreamWrapper<R::Stream> = NoTls> {
    rt: R,
    peers: PeerManager<R, W>,
    monitor: Arc<ClusterMonitor<R>>,
    config: CommitterConfig,
    rng: Mutex<Rng>,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> fmt::Debug for Committer<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Committer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Committer<R, W> {
    /// A committer that sends over `peers` to the commit proxies `monitor` publishes.
    pub fn new(
        rt: R,
        peers: PeerManager<R, W>,
        monitor: Arc<ClusterMonitor<R>>,
        config: CommitterConfig,
    ) -> Committer<R, W> {
        Committer {
            rt,
            peers,
            monitor,
            rng: Mutex::new(Rng::new(config.seed)),
            config,
        }
    }

    fn rng<T>(&self, f: impl FnOnce(&mut Rng) -> T) -> T {
        f(&mut self.rng.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The limits to enforce: the configured ones, else those of the cluster's release (8.0's,
    /// the stricter, before the monitor has published anything).
    pub fn limits(&self) -> CommitLimits {
        self.config.limits.unwrap_or_else(|| {
            CommitLimits::for_schema(self.monitor.schema_version().unwrap_or(SchemaVersion::V8_0))
        })
    }

    /// Commits a [`CommitBuffer`]: [`commit`](Self::commit) with its parts.
    pub async fn commit_buffer(
        &self,
        buffer: CommitBuffer,
        read_version: i64,
        options: &CommitOptions,
    ) -> Result<CommitOutcome, Error> {
        let (mutations, reads, writes) = buffer.into_parts();
        self.commit(read_version, mutations, reads, writes, options)
            .await
    }

    /// Commits a transaction: `Transaction::commitMutations` and `tryCommit`.
    ///
    /// `read_version` is the transaction's read snapshot. A transaction that read nothing
    /// has none; C++ then fetches one with `FLAG_CAUSAL_READ_RISKY` first, which this layer
    /// leaves to the caller (the read path). `mutations` and the conflict ranges are as a
    /// [`CommitBuffer`] produces them (the write conflict ranges of the mutations included);
    /// they are checked and normalized as the `Transaction` methods would
    /// ([`prepare`](super) rules in the module documentation).
    ///
    /// * A transaction with no mutation and no write conflict range sends nothing and
    ///   returns [`CommitOutcome::NothingToCommit`].
    /// * `Ok(Committed(id))` carries the proxy's [`CommitId`].
    /// * `not_committed` (1020): the transaction conflicted (reply `not_committed`, or a
    ///   `CommitID` with an invalid version, which carries the conflicting ranges with
    ///   `report_conflicting_keys`: [`Error::conflicting_ranges`]).
    /// * `commit_unknown_result` (1021): the request may have been
    ///   delivered (the connection ended after it was sent, or the proxy replied
    ///   `broken_promise`, `request_maybe_delivered` or `commit_unknown_result`) **or the commit
    ///   proxies changed while it was outstanding**. The request is never sent again. Unless
    ///   `causal_write_risky` is set this returns once a conflicting dummy transaction on
    ///   the self-conflicting key has committed, which guarantees that the original commit
    ///   either happened or never will; if the dummy cannot be committed (the monitor was shut
    ///   down, a definite error) the result is still this error, unsettled.
    /// * `operation_cancelled` (1101): the monitor was shut down before the request was
    ///   offered to a proxy.
    /// * `connection_failed` (1026), retryable: every proxy that was tried certainly did not
    ///   get the request, so committing again is safe.
    /// * Any other error a proxy replied, as [`Error::from_commit_request`] maps it, and the
    ///   limit errors of the request's checks (`key_too_large`, `transaction_too_large`, ...).
    ///
    /// Dropping the future cancels the commit locally: the request may still reach the
    /// proxy, as in C++.
    pub async fn commit(
        &self,
        read_version: i64,
        mutations: Vec<Mutation>,
        read_conflicts: Vec<KeyRange>,
        write_conflicts: Vec<KeyRange>,
        options: &CommitOptions,
    ) -> Result<CommitOutcome, Error> {
        let limits = self.limits();
        let uid = self.rng(|r| Uid::new(r.next_u64(), r.next_u64()));
        let Some(commit) = prepare(
            read_version,
            mutations,
            read_conflicts,
            write_conflicts,
            options,
            &limits,
            || uid,
        )?
        else {
            return Ok(CommitOutcome::NothingToCommit);
        };
        let request = Arc::new(commit);
        match self.send_commit(&request, options).await {
            Ok(id) => Ok(CommitOutcome::Committed(id)),
            Err(Attempt::Error(e)) => Err(e),
            Err(Attempt::Unknown) => Err(self.settle_unknown(&request, options, &limits).await),
        }
    }

    /// `tryCommit`'s catch for `request_maybe_delivered` / `commit_unknown_result`
    /// (`NativeAPI.actor.cpp` 7.4:7060-7104, 8.0:4755-4799): unless `causal_write_risky`,
    /// commit a dummy transaction on a single key in the intersection of the request's read
    /// and write conflict ranges, so that the original, if still in flight, conflicts with it;
    /// then report `commit_unknown_result`. (With an idempotency id C++ would then look the id
    /// up; idempotency ids are not supported.)
    ///
    /// **Whatever happens to the dummy transaction, the result is
    /// `commit_unknown_result`.** C++ lets a non-retryable
    /// dummy error escape, which would tell the caller the commit certainly did not happen
    /// although the original request may have been delivered (and may still be in flight if
    /// the dummy could not run): a caller that retries on such an error could commit twice.
    /// Here the dummy's own failure (monitor shut down, a definite server error) only means
    /// that the original was not settled; it is still unknown.
    async fn settle_unknown(
        &self,
        request: &Commit,
        options: &CommitOptions,
        limits: &CommitLimits,
    ) -> Error {
        if !options.causal_write_risky {
            let mut writes = request.write_conflict_ranges.clone();
            let mut reads = request.read_conflict_ranges.clone();
            if let Some(overlap) = intersects(&mut writes, &mut reads) {
                let range = dummy_range(&overlap, limits);
                // An error here: the commit stays unknown (see above).
                let _ = self.commit_dummy(range, options, limits).await;
            }
        }
        Error::new(COMMIT_UNKNOWN_RESULT)
    }

    /// `commitDummyTransaction` (`NativeAPI.actor.cpp` 7.4:6631-6665, 8.0:4373-4402): a
    /// transaction with `CAUSAL_WRITE_RISKY` and `LOCK_AWARE` whose only content is a read
    /// and a write conflict range on `range`, committed with `onError`'s retries until it
    /// succeeds or fails with an error that is not retried (which includes the monitor being
    /// shut down).
    async fn commit_dummy(
        &self,
        range: KeyRange,
        base: &CommitOptions,
        limits: &CommitLimits,
    ) -> Result<(), Error> {
        let mut options = base.clone();
        options.causal_write_risky = true;
        options.lock_aware = true;
        options.read_only = false;
        options.report_conflicting_keys = false;
        let mut backoff = self.config.initial_backoff;
        loop {
            let error = match self.dummy_attempt(&range, &options, limits).await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            let delay = if error.code() == TRANSACTION_TOO_OLD || error.code() == FUTURE_VERSION {
                // onError: min(FUTURE_VERSION_RETRY_DELAY, maxBackoff)
                self.config
                    .future_version_retry_delay
                    .min(self.config.max_backoff)
            } else if error.transaction_retry().is_some() {
                // getBackoff: backoff * random01, then the backoff grows, to a higher cap for
                // the resource-constrained errors (7.4:6433-6438).
                let cap = if error.is_resource_constrained() {
                    self.config.resource_constrained_max_backoff
                } else {
                    self.config.max_backoff
                };
                let delay = backoff.mul_f64(self.rng(Rng::random01));
                backoff = backoff.mul_f64(self.config.backoff_growth).min(cap);
                delay
            } else {
                return Err(error);
            };
            self.rt.sleep(delay).await;
        }
    }

    async fn dummy_attempt(
        &self,
        range: &KeyRange,
        options: &CommitOptions,
        limits: &CommitLimits,
    ) -> Result<(), Error> {
        let read_version = self.read_version(options).await?;
        let commit = prepare(
            read_version,
            Vec::new(),
            vec![range.clone()],
            vec![range.clone()],
            options,
            limits,
            Uid::default,
        )?
        .ok_or_else(|| error::internal("the dummy transaction is empty".into()))?;
        match self.send_commit(&Arc::new(commit), options).await {
            Ok(_) => Ok(()),
            Err(Attempt::Error(e)) => Err(e),
            // CAUSAL_WRITE_RISKY: no further dummy, `commit_unknown_result`.
            Err(Attempt::Unknown) => Err(Error::new(COMMIT_UNKNOWN_RESULT)),
        }
    }

    /// One `getConsistentReadVersion` with `FLAG_CAUSAL_READ_RISKY`, which is what
    /// `tryCommit`'s `startTransaction(GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)` asks
    /// for a transaction that has no read version. Only the dummy transaction needs it here:
    /// the read path (let.4) owns the general one. Like `getConsistentReadVersion`
    /// (`NativeAPI.actor.cpp` 7.4:7592-7655) it asks again when the proxies change while it
    /// waits. A read version is idempotent, so a request that may have been delivered is
    /// sent to the next proxy, as `loadBalance` does.
    async fn read_version(&self, options: &CommitOptions) -> Result<i64, Error> {
        loop {
            if self.monitor.is_closed() {
                return Err(error::closed());
            }
            let info = self
                .monitor
                .proxies(options.use_provisional_proxies)
                .await
                .map_err(|MonitorClosed| error::closed())?;
            let mut changed = pin!(self.monitor.proxies_changed(&info));
            let mut ask = pin!(self.read_version_once(&info, options));
            let result = poll_fn(|cx| {
                if changed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                ask.as_mut().poll(cx).map(Some)
            })
            .await;
            if let Some(result) = result {
                return result;
            }
        }
    }

    async fn read_version_once(
        &self,
        info: &ClusterInfo,
        options: &CommitOptions,
    ) -> Result<i64, Error> {
        let alternatives = info
            .available_grv_proxies(options.use_provisional_proxies)
            .unwrap_or_default();
        if alternatives.is_empty() {
            return Err(Error::not_sent(RequestError::NotSent(None)));
        }
        let mut flags = GetReadVersion::PRIORITY_DEFAULT | GetReadVersion::FLAG_CAUSAL_READ_RISKY;
        if options.use_provisional_proxies {
            flags |= GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES;
        }
        let request = Arc::new(GetReadVersion {
            transaction_count: 1,
            flags,
            tags: BTreeMap::new(),
            debug_id: None,
            span_context: Default::default(),
            max_version: INVALID_VERSION,
            max_grv_queue_delay_ms: None,
        });
        let _refs: Vec<PeerRef> = alternatives
            .iter()
            .map(|a| {
                self.peers
                    .peer_for_endpoint(&a.streams.get_consistent_read_version)
                    .reference()
            })
            .collect();
        let start = self.rng(|r| r.below(alternatives.len()));
        let mut last = RequestError::NotSent(None);
        for k in 0..alternatives.len() {
            let alt = &alternatives[(start + k) % alternatives.len()];
            let endpoint = alt.streams.get_consistent_read_version;
            let peer = self.peers.peer_for_endpoint(&endpoint);
            if self.is_failed(&peer, endpoint.token) {
                continue;
            }
            let result = match send_request(&peer, endpoint.token, request.clone()).await {
                Ok(reply) => decode_grv(&reply),
                Err(e) => Err(e),
            };
            match result {
                // `ASSERT(v.version > 0)`
                Ok(version) if version <= 0 => {
                    return Err(error::internal(format!(
                        "the GRV proxy replied with the invalid read version {version}"
                    )));
                }
                Ok(version) => return Ok(version),
                Err(e) if e.should_retry() || e.maybe_delivered() => last = e,
                Err(e) => return Err(Error::from_request(e)),
            }
        }
        Err(Error::not_sent(last))
    }

    fn is_failed(&self, peer: &PeerHandle, token: Uid) -> bool {
        self.peers
            .failure_monitor()
            .endpoint_status(peer.address(), token)
            .is_failed()
    }

    /// The inside of `tryCommit`'s `try`: send the request and race it against the proxies
    /// changing.
    async fn send_commit(
        &self,
        request: &Arc<Commit>,
        options: &CommitOptions,
    ) -> Result<CommitId, Attempt> {
        // C++ would take the proxies of the current `ClientDBInfo` and, if there are none
        // (a recovery), wait for the proxies to change and then report an unknown result
        // although nothing was sent. Here nothing is sent until there are proxies, which is
        // safe, and avoids the dummy transaction.
        //
        // A monitor that was shut down keeps its last info: nothing is sent after that.
        if self.monitor.is_closed() {
            return Err(Attempt::Error(error::closed()));
        }
        let info = self
            .monitor
            .proxies(options.use_provisional_proxies)
            .await
            .map_err(|MonitorClosed| Attempt::Error(error::closed()))?;
        let alternatives: Vec<CommitProxyEndpoints> = if options.commit_on_first_proxy {
            // `firstCommitProxy`, or `proxies[0]` of the list.
            info.first_commit_proxy().into_iter().collect()
        } else {
            info.available_commit_proxies(options.use_provisional_proxies)
                .map(<[CommitProxyEndpoints]>::to_vec)
                .unwrap_or_default()
        };
        if alternatives.is_empty() {
            return Err(Attempt::Error(Error::not_sent(RequestError::NotSent(None))));
        }

        // choose { when(onProxiesChanged) {...} when(reply) {...} }, the first listed first.
        let attempted = AtomicBool::new(false);
        let mut changed = pin!(self.monitor.proxies_changed(&info));
        let mut send = pin!(self.balance(
            &alternatives,
            request,
            options.commit_on_first_proxy,
            &attempted
        ));
        let outcome = poll_fn(|cx| {
            if let Poll::Ready(changed) = changed.as_mut().poll(cx) {
                return Poll::Ready(Err(changed));
            }
            send.as_mut().poll(cx).map(Ok)
        })
        .await;
        // Dropping `send` cancels the request: the transport forgets its reply token.
        let sent = match outcome {
            Ok(sent) => sent,
            // The proxies changed: `request_maybe_delivered`.
            Err(Ok(_)) => return Err(Attempt::Unknown),
            // The monitor was shut down. Not a change of proxies: if the request may have
            // reached a proxy it is unknown (and cannot be settled, there are no proxies to
            // ask); if it was never offered to one, it certainly did not happen.
            Err(Err(MonitorClosed)) => {
                return Err(if attempted.load(Ordering::SeqCst) {
                    Attempt::Unknown
                } else {
                    Attempt::Error(error::closed())
                });
            }
        };

        match sent {
            Ok(id) if id.version != INVALID_VERSION => Ok(id),
            Ok(id) => Err(Attempt::Error(Error::not_committed(conflicting_ranges(
                request, &id,
            )))),
            Err(e) => Err(classify(e)),
        }
    }

    /// `basicLoadBalance(..., AtMostOnce::True)` (`LoadBalance.actor.h` 7.4:1037-1130,
    /// `LoadBalance.h` 8.0:665-790): a proxy, from a random starting point, that the failure
    /// monitor does not say is failed; when every one is failed, wait until one is not. A
    /// request that was certainly not delivered (`should_retry`) is tried on the next proxy,
    /// each at most once; C++ cannot tell and reports `request_maybe_delivered` instead. Any
    /// other error, and a request that may have been delivered, ends it: never resent.
    async fn balance(
        &self,
        alternatives: &[CommitProxyEndpoints],
        request: &Arc<Commit>,
        first_only: bool,
        attempted_flag: &AtomicBool,
    ) -> Result<CommitId, RequestError> {
        // Referenced peers are kept connected and probed while they are failed.
        let _refs: Vec<PeerRef> = alternatives
            .iter()
            .map(|a| self.peers.peer_for_endpoint(&a.streams.commit).reference())
            .collect();
        let n = alternatives.len();
        let start = self.rng(|r| r.below(n));
        loop {
            let mut attempted = false;
            let mut last = None;
            for k in 0..n {
                let endpoint = alternatives[(start + k) % n].streams.commit;
                let peer = self.peers.peer_for_endpoint(&endpoint);
                // `commitOnFirstProxy` goes straight to `tryGetReply`, which fails at once
                // for a failed endpoint.
                if !first_only && self.is_failed(&peer, endpoint.token) {
                    continue;
                }
                attempted = true;
                // From here the request may reach the proxy.
                attempted_flag.store(true, Ordering::SeqCst);
                match send_commit_request(&peer, endpoint.token, request).await {
                    Ok(id) => return Ok(id),
                    Err(e) if e.should_retry() => last = Some(e),
                    Err(e) => return Err(e),
                }
            }
            if let Some(last) = last {
                return Err(last);
            }
            if attempted {
                return Err(RequestError::NotSent(None));
            }
            // Everything is down: wait for someone to be up.
            self.wait_any_available(alternatives).await;
        }
    }

    /// `quorum(onStateEqual(endpoint, available), 1)`.
    async fn wait_any_available(&self, alternatives: &[CommitProxyEndpoints]) {
        let targets: Vec<_> = alternatives
            .iter()
            .map(|a| {
                let endpoint = a.streams.commit;
                (
                    *self.peers.peer_for_endpoint(&endpoint).address(),
                    endpoint.token,
                )
            })
            .collect();
        let monitor = self.peers.failure_monitor().clone();
        let mut waits: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = targets
            .iter()
            .map(|&(address, token)| {
                let monitor = monitor.clone();
                Box::pin(async move {
                    monitor.on_state_changed(&address, token).await;
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })
            .collect();
        poll_fn(|cx| {
            for wait in &mut waits {
                if wait.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
            }
            // The waits note the state at their first poll: look again now that they have.
            if targets
                .iter()
                .any(|(address, token)| !monitor.endpoint_status(address, *token).is_failed())
            {
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
    }
}

/// The range the dummy transaction reads and writes: `singleKeyRange(begin)` of the overlap of
/// the request's ranges. When `begin` is a key cut to its limit + 1 bytes, the single-key
/// range is cut to nothing (C++ then commits an empty transaction, which sends nothing and
/// settles nothing), so the overlap itself is used instead: it is non-empty and it
/// intersects both of the request's ranges.
pub(super) fn dummy_range(overlap: &KeyRange, limits: &CommitLimits) -> KeyRange {
    match normalize_range(&KeyRange::single_key(&overlap.begin), limits) {
        Ok(Some(range)) => range,
        _ => overlap.clone(),
    }
}

fn schema_for(version: fdb_native_wire::ProtocolVersion) -> Result<SchemaVersion, EncodeError> {
    SchemaVersion::for_protocol_version(version).ok_or(EncodeError::Invalid(
        "no schema set for the proxy's protocol version",
    ))
}

/// One `tryGetReply`: the request, encoded for the schema set of the connection it goes out
/// on.
async fn send_request<Q>(
    peer: &PeerHandle,
    token: Uid,
    request: Arc<Q>,
) -> Result<Reply, RequestError>
where
    Q: Request + Send + Sync + 'static,
{
    peer.request(token, move |reply, version, out| {
        request.encode_request_into(schema_for(version)?, reply, out)
    })
    .await
}

async fn send_commit_request(
    peer: &PeerHandle,
    token: Uid,
    request: &Arc<Commit>,
) -> Result<CommitId, RequestError> {
    let reply = send_request(peer, token, request.clone()).await?;
    let schema = SchemaVersion::for_protocol_version(reply.version).ok_or(RequestError::Decode(
        DecodeError::Invalid("unsupported protocol version"),
    ))?;
    match Commit::decode_reply(schema, &reply.payload) {
        Ok(Ok(id)) => Ok(id),
        Ok(Err(error)) => Err(RequestError::from_reply_error(error)),
        Err(error) => Err(RequestError::Decode(error)),
    }
}

fn decode_grv(reply: &Reply) -> Result<i64, RequestError> {
    let schema = SchemaVersion::for_protocol_version(reply.version).ok_or(RequestError::Decode(
        DecodeError::Invalid("unsupported protocol version"),
    ))?;
    match GetReadVersion::decode_reply(schema, &reply.payload) {
        Ok(Ok(rv)) => Ok(rv.version),
        Ok(Err(error)) => Err(RequestError::from_reply_error(error)),
        Err(error) => Err(RequestError::Decode(error)),
    }
}

/// What `tryCommit`'s `catch` does with an error: `request_maybe_delivered` and
/// `commit_unknown_result` (and 8.0's `never_reply`) are an unknown result, any other error
/// is rethrown. `broken_promise` is `request_maybe_delivered` by the time it gets there
/// (`brokenPromiseToMaybeDelivered`, `basicLoadBalance`'s `AtMostOnce`).
fn classify(error: RequestError) -> Attempt {
    if error.maybe_delivered() || matches!(&error, RequestError::Fdb(e) if e.code == NEVER_REPLY) {
        return Attempt::Unknown;
    }
    // A definite answer, or certainly not delivered: `from_commit_request` keeps the code
    // (and marks a transient refusal as retryable).
    Attempt::Error(Error::from_commit_request(error))
}

/// The read conflict ranges `conflictingKRIndices` names, without duplicates, in order
/// (`tryCommit` collects them in a set); `None` if the reply has none. An index outside the
/// request is ignored (C++ would read out of bounds).
fn conflicting_ranges(request: &Commit, id: &CommitId) -> Option<Vec<KeyRange>> {
    let indices = id.conflicting_kr_indices.as_ref()?;
    let unique: BTreeSet<usize> = indices
        .iter()
        .filter_map(|&i| usize::try_from(i).ok())
        .collect();
    Some(
        unique
            .into_iter()
            .filter_map(|i| request.read_conflict_ranges.get(i).cloned())
            .collect(),
    )
}
