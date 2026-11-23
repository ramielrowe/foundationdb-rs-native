//! What a peer reports: the dispatch hook of the request layer.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use fdb_native_wire::{NetworkAddress, ProtocolVersion, Uid};
use futures_channel::mpsc;
use futures_core::Stream;

use super::PeerError;
use crate::{ConnectPacket, OwnedFrame};

/// Identifies one connection of a peer: the first connection is generation 1, and every new
/// [`Connection`](crate::Connection) (a reconnect, or the retry after an incompatible
/// version) gets the next. Commands are acknowledged with the generation of the established
/// connection they were applied to, and [`PeerEvent::Connected`] / [`PeerEvent::Closed`]
/// carry it, so a request layer can tell which requests were on a connection that ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Something a peer's connection reported, in order. The events of one peer arrive in the
/// order the [`Connection`](crate::Connection) produced them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerEvent {
    /// The handshake with the peer completed: commands waiting for a connection are now
    /// applied to this [generation](Generation), and the peer's protocol version is known.
    Connected {
        /// The connection.
        generation: Generation,
        /// The peer's protocol version, without flags: the schema set to use.
        version: ProtocolVersion,
        /// The peer's ConnectPacket.
        peer: ConnectPacket,
    },
    /// A frame for a token registered with
    /// [`register`](super::PeerHandle::register) (or as the reply token of
    /// [`Outgoing::reply_token`](super::Outgoing::reply_token)).
    Frame(OwnedFrame),
    /// A frame for a token that is not registered; it was dropped (see
    /// [`Event::UnknownToken`](crate::Event::UnknownToken)).
    UnknownToken {
        /// The token.
        token: Uid,
        /// Whether an endpoint-not-found notice was sent back to the peer.
        reported: bool,
    },
    /// The peer does not have the endpoint with this token. The
    /// [failure monitor](super::FailureMonitor) has already been told.
    EndpointNotFound {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// The peer refused to deliver to the endpoint with this token. The failure monitor has
    /// already been told.
    UnauthorizedEndpoint {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// A connection ended. Frames acknowledged on it but not yet written were discarded;
    /// nothing more arrives for its tokens, which are no longer registered. If
    /// `established` is false the handshake never completed and no command was applied to
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
    /// waiting for the connection were refused with
    /// [`SendError::NotSent`](super::SendError::NotSent).
    ConnectFailed {
        /// Why.
        error: PeerError,
    },
}

/// Receives everything the peers of a [`PeerManager`](super::PeerManager) report: the hook a
/// request layer plugs into.
///
/// `on_event` is called from the peer's own task, once per event, in order, with the peer's
/// address. It must not block or wait: do the minimum (complete a oneshot, push to a queue)
/// and return, since the connection is not read, written or timed while it runs. Frames
/// arrive as owned copies. A panic in `on_event` takes the peer's task down.
///
/// `on_event` is shared by all peers and called from many tasks concurrently, hence
/// `Send + Sync`; use interior mutability.
pub trait PeerEventSink: Send + Sync + 'static {
    /// One event of the peer at `peer`.
    fn on_event(&self, peer: &NetworkAddress, event: PeerEvent);
}

impl<F> PeerEventSink for F
where
    F: Fn(&NetworkAddress, PeerEvent) + Send + Sync + 'static,
{
    fn on_event(&self, peer: &NetworkAddress, event: PeerEvent) {
        self(peer, event)
    }
}

/// A sink that drops every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscardEvents;

impl PeerEventSink for DiscardEvents {
    fn on_event(&self, _: &NetworkAddress, _: PeerEvent) {}
}

/// A [`PeerEventSink`] that queues every event on an unbounded channel: see
/// [`event_channel`].
#[derive(Clone, Debug)]
pub struct EventSender {
    tx: mpsc::UnboundedSender<(NetworkAddress, PeerEvent)>,
}

impl PeerEventSink for EventSender {
    fn on_event(&self, peer: &NetworkAddress, event: PeerEvent) {
        // A closed receiver means nobody is listening any more.
        let _ = self.tx.unbounded_send((*peer, event));
    }
}

/// The receiving end of [`event_channel`]: a [`Stream`] of `(peer, event)` pairs.
#[derive(Debug)]
pub struct EventReceiver {
    rx: mpsc::UnboundedReceiver<(NetworkAddress, PeerEvent)>,
}

impl EventReceiver {
    /// The next event, or `None` once every sender is gone.
    pub async fn next(&mut self) -> Option<(NetworkAddress, PeerEvent)> {
        std::future::poll_fn(|cx| Pin::new(&mut self.rx).poll_next(cx)).await
    }

    /// An event that is already waiting, if any.
    pub fn try_next(&mut self) -> Option<(NetworkAddress, PeerEvent)> {
        self.rx.try_recv().ok()
    }
}

impl Stream for EventReceiver {
    type Item = (NetworkAddress, PeerEvent);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// A sink that forwards every event to a channel, and the receiver of that channel. The
/// channel is unbounded: a consumer that falls behind makes it grow (frames are only
/// delivered for replies it asked for, so this follows its own request rate).
pub fn event_channel() -> (EventSender, EventReceiver) {
    let (tx, rx) = mpsc::unbounded();
    (EventSender { tx }, EventReceiver { rx })
}
