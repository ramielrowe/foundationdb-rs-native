//! The state the monitor's task publishes and its handles read, with executor-neutral waits
//! (a lock and one waker slot per waiter).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use rustc_hash::FxHashMap;

use super::error::MonitorClosed;
use super::info::ClusterInfo;
use super::monitor::MonitorStatus;

pub(crate) struct State {
    pub(crate) info: Option<Arc<ClusterInfo>>,
    /// How many infos were published.
    pub(crate) sequence: u64,
    pub(crate) closed: bool,
    pub(crate) status: MonitorStatus,
    /// FxHash: slots numbered here.
    waiters: FxHashMap<u64, Waker>,
    next_waiter: u64,
    /// How many times [`Shared::modify`] ran: a [`Wait`] whose function said no at this
    /// generation is still registered and need not look again.
    generation: u64,
}

/// What the task and the handles share.
pub(crate) struct Shared {
    state: Mutex<State>,
    /// [`State::generation`], stored under the lock when it changes: a [`Wait`] polled again
    /// with nothing changed and the same waker returns without taking the lock (the GRV
    /// batchers' waits are polled from every worker thread, every time a batcher is).
    generation: AtomicU64,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("Shared")
            .field("sequence", &state.sequence)
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}

impl Shared {
    pub(crate) fn new(status: MonitorStatus) -> Shared {
        Shared {
            state: Mutex::new(State {
                info: None,
                sequence: 0,
                closed: false,
                status,
                waiters: FxHashMap::default(),
                next_waiter: 0,
                generation: 0,
            }),
            generation: AtomicU64::new(0),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Changes the state and wakes every waiter.
    pub(crate) fn modify<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let (result, wakers) = {
            let mut state = self.lock();
            let result = f(&mut state);
            state.generation += 1;
            // Before the waiters are drained (under the lock): a `Wait` that still reads the
            // old generation is woken below.
            self.generation.store(state.generation, Ordering::Release);
            let wakers: Vec<Waker> = state.waiters.drain().map(|(_, w)| w).collect();
            (result, wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        result
    }

    /// Publishes `info` with the next sequence number.
    pub(crate) fn publish(&self, mut info: ClusterInfo) -> Arc<ClusterInfo> {
        self.modify(|state| {
            state.sequence += 1;
            info.sequence = state.sequence;
            let info = Arc::new(info);
            state.info = Some(info.clone());
            info
        })
    }

    pub(crate) fn close(&self) {
        self.modify(|state| state.closed = true);
    }

    /// Resolves with `f`'s first `Some`, evaluated now and after every change; with
    /// [`MonitorClosed`] once the monitor is closed and `f` gave nothing.
    ///
    /// `f` is evaluated again only after a [`modify`](Self::modify) (a [`Wait`] polled with
    /// nothing changed only refreshes its waker), so it must depend only on what `modify`
    /// changes: the info, the sequence and `closed` (not `status`, which is changed without
    /// waking anyone, and nothing outside the state).
    pub(crate) fn wait<T, F>(&self, f: F) -> Wait<'_, F>
    where
        F: FnMut(&State) -> Option<T> + Unpin,
    {
        Wait {
            shared: self,
            slot: None,
            looked_at: None,
            waker: None,
            f,
        }
    }
}

pub(crate) struct Wait<'a, F> {
    shared: &'a Shared,
    slot: Option<u64>,
    /// The [`State::generation`] `f` last said no at (the waiter is registered then).
    looked_at: Option<u64>,
    /// The waker registered in the slot, while `looked_at` is set.
    waker: Option<Waker>,
    f: F,
}

impl<T, F> Future for Wait<'_, F>
where
    F: FnMut(&State) -> Option<T> + Unpin,
{
    type Output = Result<T, MonitorClosed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let (Some(generation), Some(waker)) = (this.looked_at, &this.waker) {
            // Nothing changed since `f` said no, and the registered waker is this one: a
            // change moves the generation on before it drains and wakes the waiters, so if it
            // has not moved, the waker will be woken.
            if this.shared.generation.load(Ordering::Acquire) == generation
                && waker.will_wake(cx.waker())
            {
                return Poll::Pending;
            }
        }
        let mut state = this.shared.lock();
        if let (Some(slot), Some(generation)) = (this.slot, this.looked_at) {
            // Nothing changed since `f` said no (a change drains the waiters and moves the
            // generation on): only the waker may be new.
            if generation == state.generation {
                if let Some(waker) = state.waiters.get_mut(&slot) {
                    if !waker.will_wake(cx.waker()) {
                        waker.clone_from(cx.waker());
                        this.waker = Some(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
            }
        }
        this.looked_at = None;
        this.waker = None;
        if let Some(value) = (this.f)(&state) {
            if let Some(slot) = this.slot.take() {
                state.waiters.remove(&slot);
            }
            return Poll::Ready(Ok(value));
        }
        if state.closed {
            if let Some(slot) = this.slot.take() {
                state.waiters.remove(&slot);
            }
            return Poll::Ready(Err(MonitorClosed));
        }
        let slot = *this.slot.get_or_insert_with(|| {
            state.next_waiter += 1;
            state.next_waiter
        });
        // Registered again after every change: `modify` drains the map.
        state.waiters.insert(slot, cx.waker().clone());
        this.looked_at = Some(state.generation);
        this.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<F> Drop for Wait<'_, F> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            self.shared.lock().waiters.remove(&slot);
        }
    }
}

/// A receiver of the infos a [`ClusterMonitor`](super::ClusterMonitor) publishes, which
/// remembers the last one it has seen, so that a loop over
/// [`changed`](ClusterInfoWatch::changed) misses no update (it may skip intermediate ones:
/// it always gets the latest). Made by [`ClusterMonitor::watch`](super::ClusterMonitor::watch);
/// a new watch has seen nothing, so its first `changed` returns the current info if there is
/// one. Cloning copies what has been seen.
#[derive(Clone)]
pub struct ClusterInfoWatch {
    shared: Arc<Shared>,
    seen: u64,
}

impl fmt::Debug for ClusterInfoWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterInfoWatch")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

impl ClusterInfoWatch {
    pub(crate) fn new(shared: Arc<Shared>, seen: u64) -> ClusterInfoWatch {
        ClusterInfoWatch { shared, seen }
    }

    /// The latest info, if any (without marking it seen).
    pub fn current(&self) -> Option<Arc<ClusterInfo>> {
        self.shared.lock().info.clone()
    }

    /// Marks the latest info seen and returns it.
    pub fn mark_seen(&mut self) -> Option<Arc<ClusterInfo>> {
        let state = self.shared.lock();
        self.seen = state.sequence;
        state.info.clone()
    }

    /// Waits for an info this watch has not seen yet, marks it seen and returns it.
    /// [`MonitorClosed`] once the monitor is shut down (with nothing unseen).
    pub async fn changed(&mut self) -> Result<Arc<ClusterInfo>, MonitorClosed> {
        let seen = self.seen;
        let (sequence, info) = self
            .shared
            .wait(|state| {
                (state.sequence > seen)
                    .then(|| state.info.clone().map(|info| (state.sequence, info)))
                    .flatten()
            })
            .await?;
        self.seen = sequence;
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    use super::*;
    use crate::cluster_file::ClusterConnectionString;

    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count(AtomicUsize::new(0)));
        (count.clone(), Waker::from(count))
    }

    fn shared() -> Shared {
        let cs: ClusterConnectionString = "a:b@127.0.0.1:4500".parse().unwrap();
        Shared::new(MonitorStatus {
            connection_string: cs.clone(),
            record_connection_string: cs,
            coordinator: None,
            generation: 1,
            has_connected: false,
            failures: 0,
            last_error: None,
            persist_error: None,
        })
    }

    #[test]
    fn a_wait_is_woken_by_a_change_whatever_waker_polled_it_last() {
        let shared = shared();
        let evaluated = AtomicUsize::new(0);
        let mut wait = shared.wait(|state| {
            evaluated.fetch_add(1, Ordering::SeqCst);
            state.closed.then_some(())
        });
        let (first, w1) = waker();
        let (second, w2) = waker();
        let mut poll = |w: &Waker| Pin::new(&mut wait).poll(&mut Context::from_waker(w));
        assert!(poll(&w1).is_pending());
        // Polled again with nothing changed: the function is not evaluated again, with the
        // same waker (no lock) or another one (which replaces it).
        assert!(poll(&w1).is_pending());
        assert!(poll(&w2).is_pending());
        assert!(poll(&w2).is_pending());
        assert_eq!(evaluated.load(Ordering::SeqCst), 1);
        shared.modify(|_| ());
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        assert_eq!(second.0.load(Ordering::SeqCst), 1);
        // A change that leaves the function's answer alone: registered again, woken again.
        assert!(poll(&w1).is_pending());
        assert!(poll(&w1).is_pending());
        assert_eq!(evaluated.load(Ordering::SeqCst), 2);
        shared.close();
        assert_eq!(first.0.load(Ordering::SeqCst), 1);
        assert!(poll(&w1).is_ready());
        assert_eq!(evaluated.load(Ordering::SeqCst), 3);
        drop(wait);
        assert!(shared.lock().waiters.is_empty());
    }
}
