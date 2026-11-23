//! The [`ReplyRegistry`]: pending requests of one peer and what becomes of them.

use std::collections::hash_map::{Entry, RandomState};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};

use fdb_native_wire::{Encode, EncodeError, FileIdentified, Uid};

use super::RequestError;
use super::error::error_code::BROKEN_PROMISE;
use super::reply::peek_reply_error;
use crate::connection::{TOKEN_STREAM_FLAG, well_known};
use crate::{
    ConnectPacket, Connection, ConnectionError, ConnectionState, Event, OwnedFrame, SendError,
};

/// The default [`RegistryConfig::max_pending`].
pub const DEFAULT_MAX_PENDING: usize = 1 << 16;

/// The default [`RegistryConfig::max_failed_endpoints`]: C++ clears its failed-endpoint map
/// once it holds more than 100000 (`SimpleFailureMonitor::endpointNotFound`,
/// `fdbrpc/FailureMonitor.actor.cpp` 7.4:132-135, `FailureMonitor.cpp` 8.0:130-133).
pub const DEFAULT_MAX_FAILED_ENDPOINTS: usize = 100_000;

/// How a [`ReplyRegistry`] behaves. Build one with [`RegistryConfig::new`] and adjust the
/// public fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RegistryConfig {
    /// The most requests tracked at once; beyond it a send fails with
    /// [`RequestError::TooManyPending`]. FDB has no such limit. Default
    /// [`DEFAULT_MAX_PENDING`].
    pub max_pending: usize,
    /// The most failed endpoints remembered: an endpoint-not-found that finds more than this
    /// many remembered forgets them all first, as C++'s `endpointNotFound` does (`size() >
    /// 100000`; an unauthorized notice never clears). Default [`DEFAULT_MAX_FAILED_ENDPOINTS`].
    pub max_failed_endpoints: usize,
    /// Seed of the reply-token generator. [`RegistryConfig::new`] draws one; set it for a
    /// reproducible run.
    pub seed: u64,
}

impl RegistryConfig {
    /// The defaults, with a random seed.
    pub fn new() -> RegistryConfig {
        RegistryConfig {
            max_pending: DEFAULT_MAX_PENDING,
            max_failed_endpoints: DEFAULT_MAX_FAILED_ENDPOINTS,
            seed: RandomState::new().build_hasher().finish(),
        }
    }
}

impl Default for RegistryConfig {
    fn default() -> RegistryConfig {
        RegistryConfig::new()
    }
}

/// Where a pending request is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequestState {
    /// Sent before the connection's handshake completed: held by the connection, not written.
    /// If the connection ends before [`Event::Connected`], the request was not sent
    /// ([`RequestError::NotSent`]).
    Queued,
    /// Put on an established connection: it may have been delivered. If the connection ends
    /// before the reply, the request fails with [`RequestError::MaybeDelivered`].
    Sent,
}

/// Why an endpoint is permanently failed (C++'s `FailedReason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EndpointFailure {
    /// The peer does not have it (`NOT_FOUND`): it sent an endpoint-not-found notice, or a
    /// request to it got `broken_promise`.
    NotFound,
    /// The peer refuses to deliver to it from this connection (`UNAUTHORIZED`).
    Unauthorized,
}

impl EndpointFailure {
    /// The error of a request to an endpoint failed for this reason.
    fn error(self, token: Uid, maybe_delivered: bool) -> RequestError {
        match self {
            EndpointFailure::NotFound => RequestError::EndpointNotFound {
                token,
                maybe_delivered,
            },
            EndpointFailure::Unauthorized => RequestError::Unauthorized { token },
        }
    }
}

/// A request that has finished, from [`ReplyRegistry::poll_event`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion<K> {
    /// The key the request was sent with.
    pub key: K,
    /// The request's reply token (no longer registered).
    pub reply_token: Uid,
    /// The token of the remote endpoint it was sent to.
    pub endpoint: Uid,
    /// The reply payload, an `ErrorOr<EnsureTable<Reply>>` message to decode with
    /// [`decode_reply`](super::decode_reply) (which also maps an error reply), or why there
    /// is none.
    pub result: Result<Vec<u8>, RequestError>,
}

/// What [`ReplyRegistry::poll_event`] gives: a connection event after the registry has applied
/// it, or something the registry concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RpcEvent<K> {
    /// The handshake completed ([`Event::Connected`]); requests that were
    /// [`Queued`](RequestState::Queued) are now [`Sent`](RequestState::Sent).
    Connected {
        /// The peer's ConnectPacket.
        peer: ConnectPacket,
    },
    /// A request finished: its reply came, or it failed.
    Completion(Completion<K>),
    /// An endpoint of the peer became permanently failed (an endpoint-not-found or
    /// unauthorized notice, or a `broken_promise` reply). Requests pending to it have failed
    /// (their completions follow) and new ones are refused. Reported once per endpoint.
    EndpointFailed {
        /// The remote endpoint's token.
        token: Uid,
        /// Why.
        failure: EndpointFailure,
    },
    /// A frame for a token registered on the connection by someone other than this registry
    /// ([`Event::Frame`], copied).
    Frame(OwnedFrame),
    /// [`Event::UnknownToken`]: typically a late reply to a cancelled request.
    UnknownToken {
        /// The token.
        token: Uid,
        /// Whether an endpoint-not-found packet was sent back.
        reported: bool,
    },
    /// The connection ended ([`Event::Closed`]). Every pending request has completed before
    /// this, which is the last event of the connection.
    Closed(ConnectionError),
}

/// A pending request.
#[derive(Debug)]
struct Pending<K> {
    key: K,
    endpoint: Uid,
    state: RequestState,
    /// Order of sending, to complete requests that fail together in that order.
    seq: u64,
}

/// A connection event without the borrow.
enum Owned {
    Connected(ConnectPacket),
    Reply(Uid, Vec<u8>),
    Frame(OwnedFrame),
    Unknown(Uid, bool),
    NotFound(Uid),
    Unauthorized(Uid),
    Closed(ConnectionError),
}

/// A small deterministic generator (SplitMix64) for reply tokens.
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The pending requests of one peer, as a sans-IO companion of its [`Connection`]: it mints
/// reply tokens, sends requests, and turns the connection's events into completions with
/// FDB's at-most-once semantics. See the [module documentation](super).
///
/// `K` is whatever the caller wants back with a request's [`Completion`]: a request id, a
/// waker slot, a oneshot sender.
///
/// # Binding to a connection
///
/// One registry serves one connection at a time and may outlive it: once a connection's
/// [`Closed`](RpcEvent::Closed) has been drained, nothing is pending and the next connection to
/// the same peer can use the registry, keeping its failed endpoints (endpoint tokens are
/// random per server process, so a failed one never comes back).
///
/// The registry is bound to the connection ([`Connection::id`]) it was last called with.
/// Calling it with another connection while requests are pending is a driver bug: it
/// `debug_assert`s, and (in any build) fails every pending request, `Queued` ones included,
/// with [`MaybeDelivered(None)`](RequestError::MaybeDelivered), since what became of them on
/// the old connection is unknown. Their tokens stay registered on the old connection until
/// the registry is next called with it, which unregisters them. If the connection has ended
/// and someone else consumed its [`Event::Closed`], [`poll_event`](Self::poll_event) still
/// fails the pending requests (without giving `Closed` again), so no waiter is left hanging.
///
/// # Dropping
///
/// Dropping a registry with requests pending drops their keys without completions. Whoever
/// holds the waiters must then fail them as **maybe delivered** (like
/// [`RequestError::MaybeDelivered`]), never as retryable.
#[derive(Debug)]
pub struct ReplyRegistry<K> {
    config: RegistryConfig,
    rng: Rng,
    next_seq: u64,
    pending: HashMap<Uid, Pending<K>>,
    /// Reply tokens of the pending requests, by remote endpoint.
    by_endpoint: HashMap<Uid, HashSet<Uid>>,
    failed: HashMap<Uid, EndpointFailure>,
    /// Events to give before polling the connection again.
    queue: VecDeque<RpcEvent<K>>,
    /// The [`Connection::id`] the registry is bound to.
    bound: Option<u64>,
    /// How many pending requests are `Queued`.
    queued: usize,
    /// Reply tokens left registered on a connection the registry was moved away from with
    /// requests pending, unregistered if the registry sees that connection again.
    orphaned: Option<(u64, Vec<Uid>)>,
}

impl<K> ReplyRegistry<K> {
    /// An empty registry.
    pub fn new(config: RegistryConfig) -> ReplyRegistry<K> {
        ReplyRegistry {
            rng: Rng(config.seed),
            config,
            next_seq: 0,
            pending: HashMap::new(),
            by_endpoint: HashMap::new(),
            failed: HashMap::new(),
            queue: VecDeque::new(),
            bound: None,
            queued: 0,
            orphaned: None,
        }
    }

    /// The configuration.
    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    // ---- state ---------------------------------------------------------------------------

    /// How many requests are pending.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether no request is pending.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Where the request with this reply token is, if it is pending (as of the last call that
    /// was given the connection).
    pub fn state(&self, reply_token: Uid) -> Option<RequestState> {
        self.pending.get(&reply_token).map(|p| p.state)
    }

    /// The remote endpoint the pending request with this reply token was sent to.
    pub fn endpoint_of(&self, reply_token: Uid) -> Option<Uid> {
        self.pending.get(&reply_token).map(|p| p.endpoint)
    }

    /// How many requests to the remote endpoint `endpoint` are pending.
    pub fn pending_to(&self, endpoint: Uid) -> usize {
        self.by_endpoint.get(&endpoint).map_or(0, HashSet::len)
    }

    /// Why the remote endpoint `endpoint` is failed, if it is (`permanentlyFailed`,
    /// `knownUnauthorized`).
    pub fn endpoint_failure(&self, endpoint: Uid) -> Option<EndpointFailure> {
        self.failed.get(&endpoint).copied()
    }

    /// The failed endpoints, in no particular order.
    pub fn failed_endpoints(&self) -> impl Iterator<Item = (Uid, EndpointFailure)> + '_ {
        self.failed.iter().map(|(t, f)| (*t, *f))
    }

    /// Forgets that `endpoint` failed, so requests to it are sent again. Returns whether it was
    /// failed. (C++ never does this; it only forgets all of them at the limit.)
    pub fn forget_failed_endpoint(&mut self, endpoint: Uid) -> bool {
        self.failed.remove(&endpoint).is_some()
    }

    /// Whether events wait to be given by [`poll_event`](Self::poll_event) without polling the
    /// connection (completions of requests that failed together, after an endpoint failure or
    /// the end of a connection).
    pub fn has_queued_events(&self) -> bool {
        !self.queue.is_empty()
    }

    // ---- sending -------------------------------------------------------------------------

    /// A fresh reply token: random, with [`TOKEN_STREAM_FLAG`] clear (a single reply, as
    /// `FlowTransport::addEndpoint` makes for a `ReplyPromise`), so neither well-known nor zero,
    /// and not the token of a pending request. [`send_with`](Self::send_with) mints its own;
    /// this is for callers that want to see one.
    pub fn mint_token(&mut self) -> Uid {
        loop {
            let token = Uid::new(
                self.rng.next_u64() & !TOKEN_STREAM_FLAG,
                self.rng.next_u64(),
            );
            if token != Uid::default() && !self.pending.contains_key(&token) {
                return token;
            }
        }
    }

    /// Sends a request to the remote endpoint `endpoint` on `conn`: mints a reply token,
    /// registers it on the connection, and calls `write(reply_token, out)` to append the
    /// request message to `out` (for instance
    /// `|reply, out| request.encode_request_into(version, reply, out)` with
    /// `fdb_native_schema::Request`). The message must carry `reply_token` as its
    /// `ReplyPromise`. Returns the reply token, which identifies the request for
    /// [`cancel`](Self::cancel); `key` comes back in its [`Completion`].
    ///
    /// The request is [`Queued`](RequestState::Queued) if the connection is still
    /// handshaking and [`Sent`](RequestState::Sent) otherwise.
    ///
    /// Fails, sending nothing, with [`TooManyPending`](RequestError::TooManyPending),
    /// [`EndpointNotFound`](RequestError::EndpointNotFound) (not delivered) or
    /// [`Unauthorized`](RequestError::Unauthorized) if the endpoint is known to be failed
    /// (`tryGetReply` returns at once when `onDisconnectOrFailure` is ready), and with the
    /// connection's refusals: [`NotSent`](RequestError::NotSent) once it has ended,
    /// [`Backpressure`](RequestError::Backpressure), [`TooLarge`](RequestError::TooLarge),
    /// or [`Encode`](RequestError::Encode) if `write` fails.
    pub fn send_with<F>(
        &mut self,
        conn: &mut Connection,
        endpoint: Uid,
        key: K,
        write: F,
    ) -> Result<Uid, RequestError>
    where
        F: FnOnce(Uid, &mut Vec<u8>) -> Result<(), EncodeError>,
    {
        let token = self.prepare(conn, endpoint)?;
        let sent = conn.send_with(endpoint, |out| write(token, out));
        self.finish_send(conn, endpoint, key, token, sent)
    }

    /// Sends the request `build(reply_token)` (a message whose `ReplyPromise` field is
    /// `ReplyPromise::new(reply_token)`) with its type's file identifier. Otherwise the same as
    /// [`send_with`](Self::send_with).
    pub fn send_message<R, F>(
        &mut self,
        conn: &mut Connection,
        endpoint: Uid,
        key: K,
        build: F,
    ) -> Result<Uid, RequestError>
    where
        R: Encode + FileIdentified,
        F: FnOnce(Uid) -> R,
    {
        let token = self.prepare(conn, endpoint)?;
        let sent = conn.send_message(endpoint, &build(token));
        self.finish_send(conn, endpoint, key, token, sent)
    }

    /// Binds the registry to `conn` (see [Binding](Self#binding-to-a-connection)), and marks
    /// queued requests sent if the connection's handshake has completed.
    fn bind(&mut self, conn: &mut Connection) {
        let id = conn.id();
        if self.bound != Some(id) {
            if let Some((old, tokens)) = self.orphaned.take() {
                if old == id {
                    for token in tokens {
                        conn.unregister(token);
                    }
                } else {
                    self.orphaned = Some((old, tokens));
                }
            }
            let moved = !self.pending.is_empty();
            if moved {
                let tokens: Vec<Uid> = self.pending.keys().copied().collect();
                if let Some(old) = self.bound {
                    self.orphaned = Some((old, tokens.clone()));
                }
                self.fail(None, tokens, |_| RequestError::MaybeDelivered(None));
            }
            self.bound = Some(id);
            debug_assert!(
                !moved,
                "a ReplyRegistry with pending requests was used with another connection \
                 (drain the old connection's Closed through the registry first)"
            );
        }
        self.sync_established(conn);
    }

    /// Queued requests are sent once the connection's handshake has completed (its held frames
    /// were then released to the output).
    fn sync_established(&mut self, conn: &Connection) {
        if self.queued > 0 && conn.was_established() {
            for pending in self.pending.values_mut() {
                pending.state = RequestState::Sent;
            }
            self.queued = 0;
        }
    }

    /// Checks that a request to `endpoint` may be sent, and mints and registers its token.
    fn prepare(&mut self, conn: &mut Connection, endpoint: Uid) -> Result<Uid, RequestError> {
        self.bind(conn);
        if self.pending.len() >= self.config.max_pending {
            return Err(RequestError::TooManyPending {
                limit: self.config.max_pending,
            });
        }
        if let Some(failure) = self.failed.get(&endpoint) {
            return Err(failure.error(endpoint, false));
        }
        if conn.state() == ConnectionState::Closed {
            return Err(RequestError::NotSent(conn.error().cloned()));
        }
        loop {
            let token = self.mint_token();
            // Refused only for a token already registered on the connection (by someone else,
            // or the connection's own ping): draw another.
            if conn.register(token) {
                return Ok(token);
            }
        }
    }

    fn finish_send(
        &mut self,
        conn: &mut Connection,
        endpoint: Uid,
        key: K,
        token: Uid,
        sent: Result<(), SendError<EncodeError>>,
    ) -> Result<Uid, RequestError> {
        if let Err(e) = sent {
            conn.unregister(token);
            return Err(match e {
                SendError::Closed => RequestError::NotSent(conn.error().cloned()),
                SendError::Backpressure => RequestError::Backpressure,
                SendError::Frame(f) => RequestError::TooLarge(f),
                SendError::Payload(p) => RequestError::Encode(p),
            });
        }
        let state = match conn.state() {
            ConnectionState::Established => RequestState::Sent,
            _ => RequestState::Queued,
        };
        if state == RequestState::Queued {
            self.queued += 1;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending.insert(
            token,
            Pending {
                key,
                endpoint,
                state,
                seq,
            },
        );
        self.by_endpoint.entry(endpoint).or_default().insert(token);
        Ok(token)
    }

    // ---- cancelling ----------------------------------------------------------------------

    /// Cancels the pending request with this reply token: forgets it and unregisters its token,
    /// so that a late reply is dropped (as an [`UnknownToken`](RpcEvent::UnknownToken)).
    /// Returns its key, or `None` if it is not pending. Nothing is sent to the peer, and a
    /// request still held by the connection is not withdrawn: as in C++, cancelling says
    /// nothing about whether the request is delivered.
    ///
    /// The key comes back **without a completion**. If a waiter is still attached to it, the
    /// caller must complete it as maybe delivered (a `Queued` request is still written once the
    /// handshake completes), as C++'s `tryCommit` throws `request_maybe_delivered` when it
    /// abandons a commit on `onProxiesChanged` (`fdbclient/NativeAPI.actor.cpp` 7.4:6976-6979,
    /// `NativeAPI.cpp` 8.0:4656-4659).
    pub fn cancel(&mut self, conn: &mut Connection, reply_token: Uid) -> Option<K> {
        self.bind(conn);
        let pending = self.remove(reply_token)?;
        conn.unregister(reply_token);
        Some(pending.key)
    }

    /// Fails every pending request with [`RequestError::Cancelled`] (for instance when the
    /// peer is being shut down) and unregisters their tokens. The completions come from
    /// [`poll_event`](Self::poll_event).
    pub fn cancel_all(&mut self, conn: &mut Connection) {
        self.bind(conn);
        let tokens: Vec<Uid> = self.pending.keys().copied().collect();
        self.fail(Some(conn), tokens, |_| RequestError::Cancelled);
    }

    // ---- events --------------------------------------------------------------------------

    /// The next event: use it **instead of** [`Connection::poll_event`], until it returns
    /// `None`, after every input to the connection. It gives the events the registry has
    /// queued, then polls the connection and applies what it gives:
    ///
    /// * [`Event::Connected`]: queued requests are now sent; passed on as
    ///   [`RpcEvent::Connected`].
    /// * [`Event::Frame`] for a pending request's reply token: the token is unregistered and
    ///   the request completes with the payload. A `broken_promise` reply also fails the
    ///   endpoint (unless it is well-known), as C++ does.
    /// * [`Event::EndpointNotFound`]: the endpoint is failed and every request pending to it
    ///   fails with [`RequestError::EndpointNotFound`]; a well-known endpoint is ignored, as
    ///   C++ ignores it. [`Event::UnauthorizedEndpoint`]: the same, with
    ///   [`RequestError::Unauthorized`]. Each gives [`RpcEvent::EndpointFailed`] the first time.
    /// * [`Event::Closed`]: every pending request fails, [`NotSent`](RequestError::NotSent) if
    ///   the connection's handshake never completed
    ///   ([`Connection::was_established`]) and [`MaybeDelivered`](RequestError::MaybeDelivered)
    ///   otherwise; then [`RpcEvent::Closed`].
    /// * Other frames and unknown tokens are passed on.
    ///
    /// If the connection has ended, its events are exhausted and requests are still pending
    /// (someone else consumed `Closed`), they fail as on `Closed`, without a second `Closed`.
    pub fn poll_event(&mut self, conn: &mut Connection) -> Option<RpcEvent<K>> {
        self.bind(conn);
        loop {
            if let Some(event) = self.queue.pop_front() {
                return Some(event);
            }
            let Some(event) = conn.poll_event() else {
                if conn.state() == ConnectionState::Closed && !self.pending.is_empty() {
                    let error = conn.error().cloned();
                    self.close_all(conn, error, false);
                    continue;
                }
                return None;
            };
            let event = match event {
                Event::Connected { peer } => Owned::Connected(peer),
                Event::Frame(frame) if self.pending.contains_key(&frame.token) => {
                    Owned::Reply(frame.token, frame.payload.to_vec())
                }
                Event::Frame(frame) => Owned::Frame(frame.into_owned()),
                Event::UnknownToken { token, reported } => Owned::Unknown(token, reported),
                Event::EndpointNotFound { token } => Owned::NotFound(token),
                Event::UnauthorizedEndpoint { token } => Owned::Unauthorized(token),
                Event::Closed(error) => Owned::Closed(error),
            };
            match event {
                Owned::Connected(peer) => {
                    self.sync_established(conn);
                    return Some(RpcEvent::Connected { peer });
                }
                Owned::Reply(token, payload) => {
                    if let Some(event) = self.reply(conn, token, payload) {
                        return Some(event);
                    }
                }
                Owned::Frame(frame) => return Some(RpcEvent::Frame(frame)),
                Owned::Unknown(token, reported) => {
                    return Some(RpcEvent::UnknownToken { token, reported });
                }
                Owned::NotFound(token) => {
                    // `endpointNotFound` ignores well-known endpoints: it neither records nor
                    // triggers them.
                    if !well_known::is_well_known(token) {
                        self.endpoint_failed(conn, token, EndpointFailure::NotFound);
                    }
                }
                Owned::Unauthorized(token) => {
                    self.endpoint_failed(conn, token, EndpointFailure::Unauthorized);
                }
                Owned::Closed(error) => {
                    self.close_all(conn, Some(error), true);
                }
            }
        }
    }

    /// A reply frame for a pending request.
    fn reply(
        &mut self,
        conn: &mut Connection,
        token: Uid,
        payload: Vec<u8>,
    ) -> Option<RpcEvent<K>> {
        let pending = self.remove(token)?;
        conn.unregister(token);
        let endpoint = pending.endpoint;
        let broken = peek_reply_error(&payload).is_some_and(|e| e.code == BROKEN_PROMISE);
        let completion = RpcEvent::Completion(Completion {
            key: pending.key,
            reply_token: token,
            endpoint,
            result: Ok(payload),
        });
        // `getReply` (`sendCanceler`) and `tryGetReply` (`waitValueOrSignal`) both report a
        // broken promise as `endpointNotFound(endpoint)`.
        if broken && !well_known::is_well_known(endpoint) {
            self.endpoint_failed(conn, endpoint, EndpointFailure::NotFound);
        }
        Some(completion)
    }

    /// Marks `endpoint` failed (unless it is already) and fails the requests pending to it.
    fn endpoint_failed(&mut self, conn: &mut Connection, endpoint: Uid, failure: EndpointFailure) {
        let known = self.failed.contains_key(&endpoint);
        // `endpointNotFound` clears the whole map when it holds more than the limit
        // (`size() > 100000`); `unauthorizedEndpoint` does not.
        if failure == EndpointFailure::NotFound
            && self.failed.len() > self.config.max_failed_endpoints
        {
            self.failed.clear();
        }
        // C++ `emplace`s: a recorded reason is kept.
        let failure = *self.failed.entry(endpoint).or_insert(failure);
        if !known {
            self.queue.push_back(RpcEvent::EndpointFailed {
                token: endpoint,
                failure,
            });
        }
        let tokens: Vec<Uid> = self
            .by_endpoint
            .get(&endpoint)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        self.fail(Some(conn), tokens, |_| failure.error(endpoint, true));
    }

    /// The connection ended: every pending request fails, then `Closed` is given (if
    /// `give_closed`). A request was sent if the handshake ever completed: frames held before
    /// it were then released to the output.
    fn close_all(
        &mut self,
        conn: &mut Connection,
        error: Option<ConnectionError>,
        give_closed: bool,
    ) {
        let established = conn.was_established();
        let mut all: Vec<(Uid, Pending<K>)> = self.pending.drain().collect();
        self.by_endpoint.clear();
        self.queued = 0;
        all.sort_by_key(|(_, p)| p.seq);
        for (token, pending) in all {
            conn.unregister(token);
            let result = Err(if established || pending.state == RequestState::Sent {
                RequestError::MaybeDelivered(error.clone())
            } else {
                RequestError::NotSent(error.clone())
            });
            self.queue.push_back(RpcEvent::Completion(Completion {
                key: pending.key,
                reply_token: token,
                endpoint: pending.endpoint,
                result,
            }));
        }
        if let (true, Some(error)) = (give_closed, error) {
            self.queue.push_back(RpcEvent::Closed(error));
        }
    }

    /// Fails the pending requests with these tokens, in the order they were sent, and
    /// unregisters them from `conn`.
    fn fail(
        &mut self,
        mut conn: Option<&mut Connection>,
        tokens: Vec<Uid>,
        error: impl Fn(&Pending<K>) -> RequestError,
    ) {
        let mut failed: Vec<(Uid, Pending<K>)> = tokens
            .into_iter()
            .filter_map(|t| self.remove(t).map(|p| (t, p)))
            .collect();
        failed.sort_by_key(|(_, p)| p.seq);
        for (token, pending) in failed {
            if let Some(conn) = conn.as_deref_mut() {
                conn.unregister(token);
            }
            let result = Err(error(&pending));
            self.queue.push_back(RpcEvent::Completion(Completion {
                key: pending.key,
                reply_token: token,
                endpoint: pending.endpoint,
                result,
            }));
        }
    }

    fn remove(&mut self, token: Uid) -> Option<Pending<K>> {
        let pending = self.pending.remove(&token)?;
        if pending.state == RequestState::Queued {
            self.queued -= 1;
        }
        if let Entry::Occupied(mut set) = self.by_endpoint.entry(pending.endpoint) {
            set.get_mut().remove(&token);
            if set.get().is_empty() {
                set.remove();
            }
        }
        Some(pending)
    }
}
