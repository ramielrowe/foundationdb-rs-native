//! Requests and replies over a [`Connection`], sans-IO: reply tokens, pending requests, and
//! FDB's at-most-once failure semantics.
//!
//! A [`ReplyRegistry`] sits next to a peer's [`Connection`]. It
//! [sends](ReplyRegistry::send_with) a request to a remote endpoint token with a freshly
//! minted reply token, tracks the request as [`Queued`](RequestState::Queued) or
//! [`Sent`](RequestState::Sent), and turns the connection's events into
//! [`Completion`]s: the reply payload, or a [`RequestError`] that says whether the request
//! may have been delivered. [`decode_reply`] then decodes the payload as
//! `ErrorOr<EnsureTable<T>>`. The registry does no I/O and has no timers; the peer manager
//! owns both and drives them (see [Driving](#driving)).
//!
//! The layer is bytes- and wire-type-level: a request is any message that carries its reply
//! token as a `ReplyPromise` ([`send_message`](ReplyRegistry::send_message) builds one from the
//! token; [`send_with`](ReplyRegistry::send_with) lets an encoder such as
//! `fdb_native_schema::Request::encode_request_into` write it), and a reply is decoded by the
//! caller, who knows its type.
//!
//! # What C++ does
//!
//! Citations: `fdbrpc.h` is `fdbrpc/include/fdbrpc/fdbrpc.h`; `genericactors` is
//! `fdbrpc/include/fdbrpc/genericactors.actor.h` (7.4) / `genericactors.h` (8.0);
//! `FailureMonitor` is `fdbrpc/FailureMonitor.actor.cpp` / `FailureMonitor.cpp`; `FlowTransport`
//! is `fdbrpc/FlowTransport.actor.cpp` / `FlowTransport.cpp`; `LoadBalance` is
//! `fdbrpc/include/fdbrpc/LoadBalance.actor.h` / `LoadBalance.h`; `NativeAPI` is
//! `fdbclient/NativeAPI.actor.cpp` / `NativeAPI.cpp`. `7.4:N` is a line on `release-7.4`, `8.0:N`
//! on `release-8.0`. The behaviour is the same on both unless noted.
//!
//! **Reply tokens.** A request struct holds a `ReplyPromise<Reply>`. Serializing it calls
//! `getEndpoint()`, which on first use registers a local receiver with
//! `FlowTransport::addEndpoint` (`fdbrpc.h` 7.4:88-95, 8.0:88-95; serializer 7.4:237-251,
//! 8.0:249-262): a random UID with `TOKEN_STREAM_FLAG` **cleared** for a single reply, and an
//! empty address list (`FlowTransport` 7.4:2233-2243, 8.0:2372-2382). Only the token travels;
//! the server sends the reply to the connection the request came in on. A token is used once:
//! `getReply` asserts the promise is not yet set (`fdbrpc.h` 7.4:769, 8.0:788).
//! [`ReplyRegistry::mint_token`] does the same: random, stream flag clear, unique among the
//! pending requests, registered on the connection.
//!
//! **Replies.** The server's `networkSender` sends `ErrorOr<EnsureTable<T>>` with the value, or
//! with the error the server's promise was broken with (`broken_promise` when it was dropped;
//! nothing for `never_reply`), to the reply token (`networksender.actor.h` 7.4:34-47,
//! `networksender.h` 8.0:42-66). `NetSAV::receive` decodes it and completes the future with the
//! value or the error (`fdbrpc.h` 7.4:129-140, 8.0:130-141). [`decode_reply`] does the same
//! and checks the file identifier.
//!
//! **`getReply` vs `tryGetReply`.** `getReply` sends *reliably*: the packet is kept and
//! resent on every reconnection until the reply comes (at least once; `fdbrpc.h` 7.4:760-777,
//! 8.0:779-796; `sendCanceler`, `genericactors` 7.4:408-440, 8.0:411-443). It never fails for a
//! disconnect; on a permanently failed endpoint it cancels the resend and waits forever unless
//! the endpoint is unauthorized (`unauthorized_attempt`); callers race it with other signals.
//! `tryGetReply` sends *unreliably* (at most once; `fdbrpc.h` 7.4:794-841, 8.0:813-860): if
//! `onDisconnectOrFailure(endpoint)` is already ready (the address is failed or the endpoint
//! permanently failed) it returns `request_maybe_delivered` (or `unauthorized_attempt`) without
//! sending; otherwise it sends and `waitValueOrSignal` (`genericactors` 7.4:362-406, 8.0:362-409)
//! returns the reply, or `request_maybe_delivered` when the endpoint fails or the peer
//! disconnects (8.0 also returns `unauthorized_attempt` on a disconnect if the endpoint is known
//! unauthorized). On a connection failure the peer discards its unsent unreliable packets and
//! keeps the reliable ones (`discardUnreliablePackets`, `FlowTransport` 7.4:1005 and 1160-1171,
//! 8.0:1098 and 1252), then `notifyDisconnect` wakes every `onDisconnectOrFailure` waiter of
//! the address (7.4:1073, 8.0:1165; `FailureMonitor` 7.4:150-154, 8.0:148-152).
//!
//! **This layer implements `tryGetReply`**: at most once, no resend. Any retry is the
//! caller's (a load balancer's) decision, made from [`RequestError::is_retryable`] and
//! [`RequestError::maybe_delivered`]. It refines C++ in one place: C++ cannot tell an unsent
//! packet from a sent one and reports `request_maybe_delivered` for both; here a request sent
//! before the handshake completed is held by the connection and never written if the
//! handshake fails (see the [connection documentation](crate::connection#sending)), so it
//! fails with [`RequestError::NotSent`], which is safe to retry. A request put on an
//! established connection fails with [`RequestError::MaybeDelivered`] when the connection
//! ends, even if its bytes were still in the output buffer: conservative, as C++ is.
//!
//! **Endpoint not found.** A server that gets a packet for a stream token it does not have
//! answers on `WLTOKEN_ENDPOINT_NOT_FOUND` with the token (`deliver`, `FlowTransport`
//! 7.4:1288-1316, 8.0:1361-1389). The receiver calls `IFailureMonitor::endpointNotFound`
//! (7.4:222-236, 8.0:325-339), which ignores well-known tokens (`first == -1`), records the
//! endpoint as permanently failed (`NOT_FOUND`, if its address is public; the whole map is
//! cleared past 100000 entries) and triggers it (`FailureMonitor` 7.4:117-139, 8.0:115-137).
//! Every `tryGetReply` waiting on that endpoint then returns `request_maybe_delivered`, and
//! later ones return it without sending. The notice does not say which packet caused it, and
//! a request delivered before the endpoint went away may still be processed, so pending
//! requests fail with [`RequestError::EndpointNotFound`] `{ maybe_delivered: true }` and later
//! ones with `{ maybe_delivered: false }` (not sent). A `broken_promise` reply has the same
//! effect on the endpoint: `sendCanceler`, `waitValueOrSignal` and `reportEndpointFailure`
//! all call `endpointNotFound(endpoint)` on it (`genericactors` 7.4:402, 436 and
//! 449; 8.0:405, 439 and 451); the request itself completes with the reply,
//! which [`decode_reply`] turns into [`RequestError::BrokenPromise`].
//!
//! **Unauthorized endpoint.** A server that has the endpoint but will not deliver to an
//! untrusted peer answers on `WLTOKEN_UNAUTHORIZED_ENDPOINT` (`deliver`, 7.4:1290-1300,
//! 8.0:1363-1373); `unauthorizedEndpoint` records `UNAUTHORIZED` (for well-known tokens too)
//! and triggers it (`FailureMonitor` 7.4:141-148, 8.0:139-146), and requests to it fail with
//! `unauthorized_attempt` ([`RequestError::Unauthorized`]): never delivered, and resending
//! will not help.
//!
//! **Cancellation.** Dropping a `tryGetReply` future cancels `waitValueOrSignal`; the last
//! reference to the `ReplyPromise` goes, and the `FlowReceiver` destructor removes the reply
//! endpoint (`fdbrpc.h` 7.4:57-60, 8.0:57-60). Nothing is sent to the server, and a packet not
//! yet written is still written. A late reply finds no receiver and is dropped: its token has
//! no stream flag, so no endpoint-not-found notice goes back (`deliver`, above). Dropping a
//! `getReply` future also cancels the resend (`cancelReliable`, `FlowTransport` 7.4:2427-2431,
//! 8.0:2653). [`ReplyRegistry::cancel`] forgets the request and unregisters its token.
//!
//! **At most once, upstream.** `loadBalance` treats `broken_promise` and
//! `request_maybe_delivered` as "maybe delivered" (`LoadBalance` 7.4:591, 8.0:206): without
//! `AtMostOnce` it retries another alternative; with `AtMostOnce` it throws
//! `request_maybe_delivered` (7.4:616-618 and 1108-1115, 8.0:232 and 735-741). Commits are sent
//! with `AtMostOnce::True` (or `tryGetReply` wrapped in `brokenPromiseToMaybeDelivered`;
//! `NativeAPI` 7.4:6959-6973, 8.0:4637-4651), and `tryCommit` turns `request_maybe_delivered`
//! into `commit_unknown_result` after making sure the commit is no longer in flight
//! (7.4:7060-7061, 8.0:4755-4756). [`RequestError::maybe_committed`] is that test.
//!
//! # Errors
//!
//! | [`RequestError`] | when | code | retryable | maybe delivered |
//! | --- | --- | --- | --- | --- |
//! | `Fdb(e)` | the server replied with an error | `e.code` | no | for 1021, 1030, 1100 |
//! | `BrokenPromise` | the server replied `broken_promise` | 1100 | no | yes |
//! | `MaybeDelivered` | the connection ended after the request was sent | 1030 | no | yes |
//! | `NotSent` | the connection ended before the handshake, or had ended | 1026 | yes | no |
//! | `EndpointNotFound` | the endpoint is gone (pending / refused) | 1030 | no / yes | yes / no |
//! | `Unauthorized` | the peer refuses the endpoint | 6001 | no | no |
//! | `Decode` | malformed reply, or wrong file identifier | 1044 | no | no |
//! | `Encode`, `TooLarge` | the request could not be framed; nothing sent | 4100, 1500 | yes | no |
//! | `Cancelled` | [`ReplyRegistry::cancel_all`] | 1101 | no | yes |
//! | `Backpressure` | the connection's output is full; nothing sent | 1026 | yes | no |
//! | `TooManyPending` | [`RegistryConfig::max_pending`] reached; nothing sent | 1026 | yes | no |
//!
//! # Driving
//!
//! One registry per peer address, created with the peer and kept across its connections. In
//! the [connection's driver loop](crate::connection), the registry takes the place of
//! [`Connection::poll_event`]:
//!
//! ```text
//! registry = ReplyRegistry::new(RegistryConfig::new())        // once per peer
//! for each connection to the peer:
//!     conn = Connection::new(config, now)
//!     loop:
//!         1. input     socket bytes / deadline, as for the connection, or a command:
//!                        send   → match registry.send_with(&mut conn, endpoint.token, key, encode)
//!                                   Ok(reply_token) → remember key ↔ reply_token (to cancel)
//!                                   Err(e)          → complete key with e (nothing pending)
//!                        cancel → registry.cancel(&mut conn, reply_token)
//!         2. drain     while let Some(ev) = registry.poll_event(&mut conn):
//!                        Completion(c)              → complete c.key with c.result
//!                        EndpointFailed { token, .. } → per-endpoint failure state
//!                        Connected { .. }           → the peer is available
//!                        Closed(e)                  → every request has completed; end the loop
//!                        Frame / UnknownToken       → ignore (a late reply) or log
//!         3. write     conn.pending_output(); conn.consume_output(n)
//!         4. wait      socket readable | conn.poll_timeout() | a command
//! ```
//!
//! * Drain through the registry after every input, including after a send (a send produces no
//!   event, but a pending input might) and after [`Connection::close`]: the
//!   [`Closed`](RpcEvent::Closed) event is what fails the pending requests, and comes after
//!   their completions.
//! * Never drop a connection without draining its `Closed` through the registry: its pending
//!   requests would never complete. To shut a peer down, call
//!   [`cancel_all`](ReplyRegistry::cancel_all) (or `conn.close()`) and drain.
//! * While the peer has no connection (connecting, or backing off), hold requests and send
//!   them once a connection exists (they are then [`Queued`](RequestState::Queued) until the
//!   handshake), or fail them with [`RequestError::NotSent`]: C++'s `tryGetReply` fails at
//!   once when the address is marked failed, and otherwise queues on the peer.
//! * Sends to an endpoint in [`endpoint_failure`](ReplyRegistry::endpoint_failure) are
//!   refused at once, as C++'s are.

mod error;
mod registry;
mod reply;

pub use error::{RequestError, error_code};
pub use registry::{
    Completion, DEFAULT_MAX_FAILED_ENDPOINTS, DEFAULT_MAX_PENDING, EndpointFailure, RegistryConfig,
    ReplyRegistry, RequestState, RpcEvent,
};
pub use reply::{decode_reply, reply_result};

#[cfg(doc)]
use crate::Connection;
