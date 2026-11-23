//! What a peer reports to observers: connection life and traffic that is not a reply.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use fdb_native_wire::{NetworkAddress, ProtocolVersion, Uid};
use futures_channel::mpsc;
use futures_core::Stream;

use super::PeerError;
use crate::{ConnectPacket, OwnedFrame};

/// Identifies one peer of a [`PeerManager`](super::PeerManager) for as long as it exists.
///
/// A peer that was removed (see [removal](super#removal)) and is created again for the same
/// address is a new peer with a new id, so an observer keyed by id never mixes two lives of an
/// address. Ids are not reused within a manager.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub u64);

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer{}", self.0)
    }
}

/// Identifies one connection of a peer: the first connection is generation 1, and every new
/// [`Connection`](crate::Connection) (a reconnect, or the retry after an incompatible
/// version) gets the next. [`PeerEvent::Connected`] and [`PeerEvent::Closed`] carry it, and
/// the acknowledgement of a one-way [`send`](super::PeerHandle::send) /
/// [`register`](super::PeerHandle::register) says which connection took the command.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Something a peer's connection reported, in order. The events of one peer arrive in the
/// order the [`Connection`](crate::Connection) produced them. Replies to requests are **not**
/// events: they complete the [`request`](super::PeerHandle::request) futures.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerEvent {
    /// The handshake with the peer completed: queued commands are now applied to this
    /// [generation](Generation), and the peer's protocol version is known.
    Connected {
        /// The connection.
        generation: Generation,
        /// The peer's protocol version, without flags: the schema set to use.
        version: ProtocolVersion,
        /// The peer's ConnectPacket.
        peer: ConnectPacket,
    },
    /// A frame for a token registered with [`register`](super::PeerHandle::register).
    Frame(OwnedFrame),
    /// A frame for a token that is not registered; it was dropped (see
    /// [`Event::UnknownToken`](crate::Event::UnknownToken)). Typically a late reply to a
    /// request that was cancelled.
    UnknownToken {
        /// The token.
        token: Uid,
        /// Whether an endpoint-not-found notice was sent back to the peer.
        reported: bool,
    },
    /// The peer does not have the endpoint with this token (reported once per endpoint). The
    /// [failure monitor](super::FailureMonitor) has already been told, and the requests
    /// pending to it have failed with
    /// [`RequestError::EndpointNotFound`](crate::rpc::RequestError::EndpointNotFound).
    EndpointNotFound {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// The peer refused to deliver to the endpoint with this token (reported once). The
    /// failure monitor has already been told.
    UnauthorizedEndpoint {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// A connection ended. Every request that was on it has already completed (with the reply
    /// or an error), and nothing more arrives for its tokens, which are no longer registered.
    /// If `established` is false the handshake never completed and no command was applied to
    /// it.
    Closed {
        /// The connection.
        generation: Generation,
        /// Why it ended; [`is_clean`](PeerError::is_clean) tells an idle close from a failure.
        error: PeerError,
        /// Whether its handshake had completed ([`PeerEvent::Connected`] was reported).
        established: bool,
    },
    /// An attempt to connect failed before there was a connection (TCP or TLS). Commands
    /// waiting for the connection were refused.
    ConnectFailed {
        /// Why.
        error: PeerError,
    },
    /// The peer was removed from its manager: it was idle, unreferenced and had nothing
    /// outstanding. Nothing more will be reported for this [`PeerId`].
    Removed,
}

/// Receives everything the peers of a [`PeerManager`](super::PeerManager) report: the hook for
/// observers (connection life, unsolicited frames). Replies do not come through here.
///
/// `on_event` is called from the peer's own task, once per event, in order, with the peer's
/// stable [`PeerId`] and address. It must not block or wait: do the minimum (push to a queue)
/// and return, since the connection is not read, written or timed while it runs. Frames
/// arrive as owned copies. A panic in `on_event` takes the peer's task down.
///
/// `on_event` is shared by all peers and called from many tasks concurrently, hence
/// `Send + Sync`; use interior mutability.
pub trait PeerEventSink: Send + Sync + 'static {
    /// One event of the peer `peer` at `address`.
    fn on_event(&self, peer: PeerId, address: &NetworkAddress, event: PeerEvent);
}

impl<F> PeerEventSink for F
where
    F: Fn(PeerId, &NetworkAddress, PeerEvent) + Send + Sync + 'static,
{
    fn on_event(&self, peer: PeerId, address: &NetworkAddress, event: PeerEvent) {
        self(peer, address, event)
    }
}

/// A sink that drops every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscardEvents;

impl PeerEventSink for DiscardEvents {
    fn on_event(&self, _: PeerId, _: &NetworkAddress, _: PeerEvent) {}
}

/// One event of one peer, as [`event_channel`] delivers it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEventRecord {
    /// The peer.
    pub peer: PeerId,
    /// Its address.
    pub address: NetworkAddress,
    /// What happened.
    pub event: PeerEvent,
}

/// A [`PeerEventSink`] that queues every event on an unbounded channel: see
/// [`event_channel`].
#[derive(Clone, Debug)]
pub struct EventSender {
    tx: mpsc::UnboundedSender<PeerEventRecord>,
}

impl PeerEventSink for EventSender {
    fn on_event(&self, peer: PeerId, address: &NetworkAddress, event: PeerEvent) {
        // A closed receiver means nobody is listening any more.
        let _ = self.tx.unbounded_send(PeerEventRecord {
            peer,
            address: *address,
            event,
        });
    }
}

/// The receiving end of [`event_channel`]: a [`Stream`] of [`PeerEventRecord`]s.
#[derive(Debug)]
pub struct EventReceiver {
    rx: mpsc::UnboundedReceiver<PeerEventRecord>,
}

impl EventReceiver {
    /// The next event, or `None` once every sender is gone.
    pub async fn next(&mut self) -> Option<PeerEventRecord> {
        std::future::poll_fn(|cx| Pin::new(&mut self.rx).poll_next(cx)).await
    }

    /// An event that is already waiting, if any.
    pub fn try_next(&mut self) -> Option<PeerEventRecord> {
        self.rx.try_recv().ok()
    }
}

impl Stream for EventReceiver {
    type Item = PeerEventRecord;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// A sink that forwards every event to a channel, and the receiver of that channel. The
/// channel is unbounded: a consumer that falls behind makes it grow (frames are only
/// delivered for tokens it registered, so this follows its own use).
pub fn event_channel() -> (EventSender, EventReceiver) {
    let (tx, rx) = mpsc::unbounded();
    (EventSender { tx }, EventReceiver { rx })
}
