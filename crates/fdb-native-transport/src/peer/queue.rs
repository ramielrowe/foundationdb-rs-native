//! The commands waiting for a peer's task: a bounded queue with its own count.
//!
//! A `futures-channel` bounded channel admits one extra message per `Sender` clone, so cloning
//! the sender per call (as a handle that must be `Clone` does) bounds nothing. This queue
//! counts its items under one lock instead, and keeps the wakers of the senders waiting for
//! room in a slab, so a sender that gives up (a timeout) takes its slot *and* its item with it
//! (see [`CommandQueue::remove`]).

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use fdb_native_wire::{EncodeError, ProtocolVersion, Uid};
use futures_channel::oneshot;

use super::waiters::Waiters;
use super::{Generation, PeerError, Reply, SendError};
use crate::rpc::RequestError;

/// Writes a request message (reply token, negotiated version, output).
pub(crate) type RequestEncoder =
    Box<dyn FnOnce(Uid, ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send>;

/// Writes the payload of a one-way message (negotiated version, output).
pub(crate) type FrameEncoder =
    Box<dyn FnOnce(ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send>;

/// A command from a handle to its peer's task.
pub(crate) enum Cmd {
    /// A request, completed through `reply`.
    Request {
        id: u64,
        /// The id of the user of the handle that sent it (see
        /// [`PeerManager::share`](super::PeerManager::share)). An id, not the user: a
        /// command touches no state shared between threads beyond the queue.
        user: u64,
        endpoint: Uid,
        encode: RequestEncoder,
        reply: oneshot::Sender<Result<Reply, RequestError>>,
    },
    /// A one-way frame, acknowledged through `ack`.
    Send {
        id: u64,
        user: u64,
        token: Uid,
        encode: FrameEncoder,
        ack: oneshot::Sender<Result<Generation, SendError>>,
    },
    /// A token to register for incoming frames.
    Register {
        id: u64,
        user: u64,
        token: Uid,
        ack: oneshot::Sender<Result<Generation, SendError>>,
    },
}

/// Why a command was not applied.
pub(crate) enum Refusal {
    /// No connection could be made (or the one that was ended first).
    NotSent(PeerError),
    /// The peer is shut down.
    Shutdown,
}

impl Cmd {
    pub(crate) fn id(&self) -> u64 {
        match self {
            Cmd::Request { id, .. } | Cmd::Send { id, .. } | Cmd::Register { id, .. } => *id,
        }
    }

    /// The id of the user of the handle that sent it.
    pub(crate) fn user(&self) -> u64 {
        match self {
            Cmd::Request { user, .. } | Cmd::Send { user, .. } | Cmd::Register { user, .. } => {
                *user
            }
        }
    }

    /// Whether the caller gave up waiting.
    pub(crate) fn is_cancelled(&self) -> bool {
        match self {
            Cmd::Request { reply, .. } => reply.is_canceled(),
            Cmd::Send { ack, .. } | Cmd::Register { ack, .. } => ack.is_canceled(),
        }
    }

    /// Answers the command's caller: nothing was sent.
    pub(crate) fn refuse(self, why: Refusal) {
        match self {
            Cmd::Request { reply, .. } => {
                let _ = reply.send(Err(match why {
                    Refusal::NotSent(e) => e.not_sent(),
                    Refusal::Shutdown => RequestError::NotSent(None),
                }));
            }
            Cmd::Send { ack, .. } | Cmd::Register { ack, .. } => {
                let _ = ack.send(Err(match why {
                    Refusal::NotSent(e) => SendError::NotSent(e),
                    Refusal::Shutdown => SendError::Shutdown,
                }));
            }
        }
    }
}

impl fmt::Debug for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::Request { id, endpoint, .. } => f
                .debug_struct("Request")
                .field("id", id)
                .field("endpoint", endpoint)
                .finish_non_exhaustive(),
            Cmd::Send { id, token, .. } => f
                .debug_struct("Send")
                .field("id", id)
                .field("token", token)
                .finish_non_exhaustive(),
            Cmd::Register { id, token, .. } => f
                .debug_struct("Register")
                .field("id", id)
                .field("token", token)
                .finish_non_exhaustive(),
        }
    }
}

/// Why a command was not queued.
#[derive(Debug)]
pub(crate) enum Rejected {
    /// The queue was closed (the peer shut down).
    Closed,
    /// The attempt the sender was waiting for failed: see [`CommandQueue::refuse_waiting`].
    Refused(PeerError),
}

#[derive(Debug)]
struct Inner {
    items: VecDeque<Cmd>,
    closed: bool,
    /// The next ticket a waiting sender takes (they are numbered in the order they first had
    /// to wait).
    next_ticket: u64,
    /// Senders holding a ticket below this are refused with the error (see
    /// [`CommandQueue::refuse_waiting`]).
    refuse_below: u64,
    refusal: Option<PeerError>,
    /// The task that takes commands, when it waits for one.
    consumer: Option<Waker>,
    /// The senders waiting for room.
    senders: Waiters,
}

/// A bounded multi-producer queue with one consumer, the peer's task.
#[derive(Debug)]
pub(crate) struct CommandQueue {
    inner: Mutex<Inner>,
    limit: usize,
}

impl CommandQueue {
    pub(crate) fn new(limit: usize) -> CommandQueue {
        CommandQueue {
            inner: Mutex::new(Inner {
                items: VecDeque::new(),
                closed: false,
                next_ticket: 0,
                refuse_below: 0,
                refusal: None,
                consumer: None,
                senders: Waiters::default(),
            }),
            limit: limit.max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves plain data behind: keep going.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits for room and appends `cmd`. Fails if the queue is closed.
    pub(crate) fn push(&self, cmd: Cmd) -> Push<'_> {
        Push {
            queue: self,
            cmd: Some(cmd),
            slot: None,
            ticket: None,
        }
    }

    /// Takes the command with this id back, if it is still queued (its caller gave up before
    /// the task took it), and frees its place for a waiting sender.
    pub(crate) fn remove(&self, id: u64) -> Option<Cmd> {
        let (cmd, waker) = {
            let mut inner = self.lock();
            let at = inner.items.iter().position(|c| c.id() == id)?;
            let cmd = inner.items.remove(at);
            (cmd, inner.senders.take_one())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        cmd
    }

    /// Takes the oldest command, or registers the task's waker.
    pub(crate) fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<Option<Cmd>> {
        let (cmd, waker) = {
            let mut inner = self.lock();
            match inner.items.pop_front() {
                Some(cmd) => (cmd, inner.senders.take_one()),
                None if inner.closed => return Poll::Ready(None),
                None => {
                    inner.consumer = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Poll::Ready(Some(cmd))
    }

    /// Ready once a command is waiting (it is left in place), or the queue is closed (`false`).
    pub(crate) fn poll_nonempty(&self, cx: &mut Context<'_>) -> Poll<bool> {
        let mut inner = self.lock();
        if !inner.items.is_empty() {
            Poll::Ready(true)
        } else if inner.closed {
            Poll::Ready(false)
        } else {
            inner.consumer = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Takes back every queued command of the user `user` (it was shut down), and frees
    /// their places for the senders waiting for room. Decided under the queue's lock: what it
    /// returns was never taken by the task.
    pub(crate) fn take_user(&self, user: u64) -> Vec<Cmd> {
        let (taken, wakers) = {
            let mut inner = self.lock();
            if !inner.items.iter().any(|c| c.user() == user) {
                return Vec::new();
            }
            let (taken, kept): (Vec<Cmd>, Vec<Cmd>) =
                inner.items.drain(..).partition(|c| c.user() == user);
            inner.items = kept.into();
            let wakers = (0..taken.len())
                .filter_map(|_| inner.senders.take_one())
                .collect::<Vec<_>>();
            (taken, wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        taken
    }

    /// Drops the commands whose callers gave up, and frees their places.
    pub(crate) fn purge_cancelled(&self) {
        let wakers = {
            let mut inner = self.lock();
            let before = inner.items.len();
            inner.items.retain(|c| !c.is_cancelled());
            let freed = before - inner.items.len();
            (0..freed)
                .filter_map(|_| inner.senders.take_one())
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lock().items.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().items.len()
    }

    /// Refuses every sender that is waiting for room right now, with `error`, and returns the
    /// commands that are queued (to refuse them too). Senders that arrive afterwards are
    /// not affected. Called when an attempt to connect fails: everything that was waiting for
    /// it fails with it, whether or not it had found room in the queue.
    pub(crate) fn refuse_waiting(&self, error: &PeerError) -> Vec<Cmd> {
        let (items, wakers) = {
            let mut inner = self.lock();
            inner.refuse_below = inner.next_ticket;
            inner.refusal = Some(error.clone());
            let items: Vec<Cmd> = inner.items.drain(..).collect();
            (items, inner.senders.take_all())
        };
        for waker in wakers {
            waker.wake();
        }
        items
    }

    /// Closes the queue: pushes fail from now on, and the waiting senders and the task are
    /// woken. Returns what was queued.
    pub(crate) fn close(&self) -> Vec<Cmd> {
        let (items, wakers) = {
            let mut inner = self.lock();
            inner.closed = true;
            let items: Vec<Cmd> = inner.items.drain(..).collect();
            let mut wakers = inner.senders.take_all();
            wakers.extend(inner.consumer.take());
            (items, wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        items
    }

    /// How many senders wait for room (for tests).
    #[cfg(test)]
    pub(crate) fn waiting_senders(&self) -> usize {
        self.lock().senders.len()
    }
}

/// The future of [`CommandQueue::push`].
#[derive(Debug)]
pub(crate) struct Push<'a> {
    queue: &'a CommandQueue,
    cmd: Option<Cmd>,
    slot: Option<usize>,
    /// Taken when the sender first has to wait.
    ticket: Option<u64>,
}

impl Unpin for Push<'_> {}

impl Future for Push<'_> {
    type Output = Result<(), Rejected>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let waker = {
            let mut inner = this.queue.lock();
            if inner.closed {
                inner.senders.release(&mut this.slot);
                return Poll::Ready(Err(Rejected::Closed));
            }
            if let (Some(ticket), Some(error)) = (this.ticket, &inner.refusal) {
                if ticket < inner.refuse_below {
                    let error = error.clone();
                    inner.senders.release(&mut this.slot);
                    return Poll::Ready(Err(Rejected::Refused(error)));
                }
            }
            if inner.items.len() >= this.queue.limit {
                if this.ticket.is_none() {
                    this.ticket = Some(inner.next_ticket);
                    inner.next_ticket += 1;
                }
                inner.senders.register(&mut this.slot, cx.waker());
                return Poll::Pending;
            }
            inner.senders.release(&mut this.slot);
            if let Some(cmd) = this.cmd.take() {
                inner.items.push_back(cmd);
            }
            inner.consumer.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for Push<'_> {
    fn drop(&mut self) {
        if self.slot.is_none() {
            return;
        }
        let waker = {
            let mut inner = self.queue.lock();
            inner.senders.release(&mut self.slot);
            // This sender may have been woken for room it will not use: pass it on.
            if !inner.closed && inner.items.len() < self.queue.limit {
                inner.senders.take_one()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn cmd(id: u64) -> Cmd {
        let (ack, _) = oneshot::channel();
        Cmd::Register {
            id,
            user: 0,
            token: Uid::new(1, id),
            ack,
        }
    }

    fn live_cmd(id: u64) -> (Cmd, oneshot::Receiver<Result<Generation, SendError>>) {
        let (ack, rx) = oneshot::channel();
        (
            Cmd::Register {
                id,
                user: 0,
                token: Uid::new(1, id),
                ack,
            },
            rx,
        )
    }

    #[test]
    fn the_count_bounds_the_queue_whatever_the_senders() {
        let queue = CommandQueue::new(4);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let mut pushes = Vec::new();
        for id in 0..2000 {
            let mut push = Box::pin(queue.push(cmd(id)));
            if push.as_mut().poll(&mut cx).is_pending() {
                pushes.push(push);
            }
        }
        assert_eq!(queue.len(), 4);
        assert_eq!(queue.waiting_senders(), 1996);
        drop(pushes);
        assert_eq!(queue.waiting_senders(), 0, "abandoned pushes keep no slot");
        assert_eq!(queue.len(), 4, "and none of their commands");
    }

    #[test]
    fn a_pop_wakes_a_waiting_sender_and_a_dropped_one_passes_the_room_on() {
        let queue = CommandQueue::new(1);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(
            Box::pin(queue.push(cmd(0)))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        let mut first = Box::pin(queue.push(cmd(1)));
        let mut second = Box::pin(queue.push(cmd(2)));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(queue.poll_pop(&mut cx), Poll::Ready(Some(_))));
        // The first waiter was woken but gives up: the second still gets the room.
        drop(first);
        assert!(second.as_mut().poll(&mut cx).is_ready());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn remove_takes_a_queued_command_back_and_frees_its_place() {
        let queue = CommandQueue::new(2);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        for id in 0..2 {
            assert!(
                Box::pin(queue.push(cmd(id)))
                    .as_mut()
                    .poll(&mut cx)
                    .is_ready()
            );
        }
        assert!(queue.remove(7).is_none());
        assert_eq!(queue.remove(0).map(|c| c.id()), Some(0));
        assert!(
            Box::pin(queue.push(cmd(2)))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
    }

    #[test]
    fn purge_drops_cancelled_commands_only() {
        let queue = CommandQueue::new(8);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let (alive, _keep) = live_cmd(1);
        assert!(
            Box::pin(queue.push(alive))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert!(
            Box::pin(queue.push(cmd(2)))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        queue.purge_cancelled();
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn refusing_the_waiting_senders_spares_the_ones_that_come_later() {
        let queue = CommandQueue::new(1);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(
            Box::pin(queue.push(cmd(0)))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        let mut early: Vec<_> = (1..=5).map(|id| Box::pin(queue.push(cmd(id)))).collect();
        for push in &mut early {
            assert!(push.as_mut().poll(&mut cx).is_pending());
        }
        let error = PeerError::Connect(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(queue.refuse_waiting(&error).len(), 1, "the queued command");
        let mut late = Box::pin(queue.push(cmd(9)));
        assert!(late.as_mut().poll(&mut cx).is_ready(), "room again");
        for push in &mut early {
            assert!(matches!(
                push.as_mut().poll(&mut cx),
                Poll::Ready(Err(Rejected::Refused(e))) if e == error
            ));
        }
        assert_eq!(queue.waiting_senders(), 0);
    }

    #[test]
    fn close_refuses_pushes_and_returns_the_items() {
        let queue = CommandQueue::new(2);
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(
            Box::pin(queue.push(cmd(0)))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert_eq!(queue.close().len(), 1);
        assert!(matches!(
            Box::pin(queue.push(cmd(1))).as_mut().poll(&mut cx),
            Poll::Ready(Err(Rejected::Closed))
        ));
        assert!(matches!(queue.poll_pop(&mut cx), Poll::Ready(None)));
    }
}
