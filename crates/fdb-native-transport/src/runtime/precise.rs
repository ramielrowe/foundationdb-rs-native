//! A sub-millisecond timer on a helper thread, for runtimes whose own timer ticks coarsely
//! (tokio's wheel: [`Runtime::sleep_until`](super::Runtime::sleep_until) of a deadline
//! 500 us away completes 1 to 2 ms later).
//!
//! One thread per [`PreciseTimers`] (started on first use, stopped when the last handle to it is
//! dropped) keeps a heap of deadlines and waits on a condition variable to the earliest one.
//! Registering a deadline wakes the thread only if it is not already waiting for an earlier
//! one, and a timer dropped before it fires is marked cancelled and skipped, so the usual use
//! (a hedge that the reply beats) costs a heap insertion, no wake-up, and at most one wake-up
//! per deadline that nobody waits for any more. A timer that fires wakes its task from the
//! helper thread; Linux's default 50 us timer slack makes that 50 to 60 us late.
//!
//! The deadlines are on the real (`std::time::Instant`) clock: the adapter uses this only
//! where tokio's clock is that clock.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

/// One armed timer: shared by the future waiting on it and the heap entry.
#[derive(Debug)]
struct Slot {
    fired: AtomicBool,
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Slot {
    fn fire(&self) {
        self.fired.store(true, atomic::Ordering::Release);
        let waker = lock(&self.waker).take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A deadline in the heap. Ordered earliest first (`BinaryHeap` is a max-heap, so reversed),
/// then by arrival.
#[derive(Debug)]
struct Entry {
    deadline: Instant,
    seq: u64,
    slot: Arc<Slot>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// What the helper thread is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Waiting {
    /// Not waiting (not started, or going through the heap): it looks at the heap again before
    /// it waits, so a new deadline needs no wake-up.
    Not,
    /// Waiting for the earliest deadline.
    Until(Instant),
    /// Waiting for a deadline to be registered.
    Forever,
}

#[derive(Debug)]
struct State {
    heap: BinaryHeap<Entry>,
    next_seq: u64,
    waiting: Waiting,
    started: bool,
    failed: bool,
    shutdown: bool,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Nothing run under these locks panics; a poisoned lock holds consistent data anyway.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A helper-thread timer. See the [module documentation](self).
#[derive(Debug)]
pub(super) struct PreciseTimers {
    shared: Arc<Shared>,
}

impl PreciseTimers {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(PreciseTimers {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    heap: BinaryHeap::new(),
                    next_seq: 0,
                    waiting: Waiting::Not,
                    started: false,
                    failed: false,
                    shutdown: false,
                }),
                wake: Condvar::new(),
            }),
        })
    }

    /// A timer that completes at `deadline`.
    pub(super) fn sleep_until(self: &Arc<Self>, deadline: Instant) -> PreciseSleep {
        PreciseSleep {
            timers: self.clone(),
            deadline,
            state: SleepState::New,
        }
    }

    /// Adds `deadline` to the heap and wakes the thread if it is waiting for a later one.
    /// `false` if there is no thread to run it.
    fn register(&self, deadline: Instant, slot: &Arc<Slot>) -> bool {
        let mut st = lock(&self.shared.state);
        if st.failed || st.shutdown {
            return false;
        }
        if !st.started {
            let shared = self.shared.clone();
            let spawned = std::thread::Builder::new()
                .name("fdb-native-timer".into())
                .spawn(move || run(&shared));
            if spawned.is_err() {
                st.failed = true;
                return false;
            }
            st.started = true;
        }
        let seq = st.next_seq;
        st.next_seq += 1;
        st.heap.push(Entry {
            deadline,
            seq,
            slot: slot.clone(),
        });
        let notify = match st.waiting {
            Waiting::Not => false,
            Waiting::Forever => true,
            Waiting::Until(at) => deadline < at,
        };
        drop(st);
        if notify {
            self.shared.wake.notify_one();
        }
        true
    }

    /// Entries in the heap, cancelled ones included (for tests).
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        lock(&self.shared.state).heap.len()
    }
}

impl Drop for PreciseTimers {
    fn drop(&mut self) {
        lock(&self.shared.state).shutdown = true;
        self.shared.wake.notify_one();
    }
}

/// The helper thread: fires what is due, then waits for the earliest deadline.
fn run(shared: &Shared) {
    let mut due: Vec<Arc<Slot>> = Vec::new();
    let mut st = lock(&shared.state);
    loop {
        if st.shutdown {
            return;
        }
        let now = Instant::now();
        while let Some(top) = st.heap.peek() {
            if top.slot.cancelled.load(atomic::Ordering::Acquire) {
                st.heap.pop();
            } else if top.deadline <= now {
                due.extend(st.heap.pop().map(|e| e.slot));
            } else {
                break;
            }
        }
        if !due.is_empty() {
            st.waiting = Waiting::Not;
            drop(st);
            for slot in due.drain(..) {
                slot.fire();
            }
            st = lock(&shared.state);
            continue;
        }
        st = match st.heap.peek().map(|top| top.deadline) {
            Some(deadline) => {
                st.waiting = Waiting::Until(deadline);
                let wait = deadline.saturating_duration_since(now);
                let (mut st, _) = shared
                    .wake
                    .wait_timeout(st, wait)
                    .unwrap_or_else(|e| e.into_inner());
                st.waiting = Waiting::Not;
                st
            }
            None => {
                st.waiting = Waiting::Forever;
                let mut st = shared.wake.wait(st).unwrap_or_else(|e| e.into_inner());
                st.waiting = Waiting::Not;
                st
            }
        };
    }
}

/// A timer on the helper thread: completes at a deadline. Armed at its first poll; dropping it
/// cancels it.
#[derive(Debug)]
pub(super) struct PreciseSleep {
    timers: Arc<PreciseTimers>,
    deadline: Instant,
    state: SleepState,
}

#[derive(Debug)]
enum SleepState {
    /// Not polled yet.
    New,
    /// In the heap.
    Armed(Arc<Slot>),
    /// The helper thread could not be started: re-poll until the deadline, so that the sleep
    /// is still correct (and busy), which only a system that cannot start a thread would see.
    Spinning,
}

impl Future for PreciseSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        match &this.state {
            SleepState::New => {
                if Instant::now() >= this.deadline {
                    return Poll::Ready(());
                }
                let slot = Arc::new(Slot {
                    fired: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    waker: Mutex::new(Some(cx.waker().clone())),
                });
                if this.timers.register(this.deadline, &slot) {
                    this.state = SleepState::Armed(slot);
                } else {
                    this.state = SleepState::Spinning;
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            SleepState::Armed(slot) => {
                if slot.fired.load(atomic::Ordering::Acquire) {
                    return Poll::Ready(());
                }
                {
                    let mut waker = lock(&slot.waker);
                    match waker.as_ref() {
                        Some(w) if w.will_wake(cx.waker()) => {}
                        _ => *waker = Some(cx.waker().clone()),
                    }
                }
                // It may have fired between the check and the store.
                if slot.fired.load(atomic::Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            SleepState::Spinning => {
                if Instant::now() >= this.deadline {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }
}

impl Drop for PreciseSleep {
    fn drop(&mut self) {
        if let SleepState::Armed(slot) = &self.state {
            slot.cancelled.store(true, atomic::Ordering::Release);
        }
    }
}
