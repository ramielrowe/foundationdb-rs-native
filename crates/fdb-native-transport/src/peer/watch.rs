//! A value behind a lock whose changes can be awaited, without any executor type.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use super::waiters::Waiters;

/// Shared state that tasks can wait on: [`wait_for`](Watch::wait_for) resolves once a predicate
/// over the value holds, and every [`modify`](Watch::modify) that reports a change wakes the
/// waiters to look again. The predicate is evaluated and the waker registered under one lock,
/// so a change cannot slip between the check and the wait.
///
/// Each waiter holds one slot for its waker and returns it when it is dropped (finished or
/// abandoned), so the lock never holds the wakers of waits that are gone.
#[derive(Debug)]
pub(crate) struct Watch<T> {
    inner: Mutex<Inner<T>>,
}

#[derive(Debug)]
struct Inner<T> {
    value: T,
    waiters: Waiters,
}

impl<T> Watch<T> {
    pub(crate) fn new(value: T) -> Watch<T> {
        Watch {
            inner: Mutex::new(Inner {
                value,
                waiters: Waiters::default(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner<T>> {
        // A panic while holding the lock leaves plain data behind: keep going.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads the value.
    pub(crate) fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.lock().value)
    }

    /// Changes the value. `f` returns whether it changed anything (waiters are woken only
    /// then) and a result. `f` runs under the lock: it must not call back into the watch.
    pub(crate) fn modify<R>(&self, f: impl FnOnce(&mut T) -> (bool, R)) -> R {
        let (result, wakers) = {
            let mut inner = self.lock();
            let (changed, result) = f(&mut inner.value);
            let wakers = if changed {
                inner.waiters.take_all()
            } else {
                Vec::new()
            };
            (result, wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        result
    }

    /// Resolves with `f`'s first `Some` result, checked now and after every change. `f` runs
    /// under the lock, on every poll: keep it short, and do not call back into the watch.
    pub(crate) fn wait_for<R, F>(&self, f: F) -> WaitFor<'_, T, F>
    where
        F: FnMut(&T) -> Option<R>,
    {
        WaitFor {
            watch: self,
            f,
            slot: None,
        }
    }

    /// How many waits are registered (for tests).
    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.lock().waiters.len()
    }
}

/// The future of [`Watch::wait_for`].
#[derive(Debug)]
pub(crate) struct WaitFor<'a, T, F> {
    watch: &'a Watch<T>,
    f: F,
    slot: Option<usize>,
}

impl<T, F: Unpin> Unpin for WaitFor<'_, T, F> {}

impl<T, R, F> Future for WaitFor<'_, T, F>
where
    F: FnMut(&T) -> Option<R> + Unpin,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        let this = &mut *self;
        let mut inner = this.watch.lock();
        if let Some(result) = (this.f)(&inner.value) {
            inner.waiters.release(&mut this.slot);
            return Poll::Ready(result);
        }
        inner.waiters.register(&mut this.slot, cx.waker());
        Poll::Pending
    }
}

impl<T, F> Drop for WaitFor<'_, T, F> {
    fn drop(&mut self) {
        if self.slot.is_some() {
            self.watch.lock().waiters.release(&mut self.slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    use super::*;

    struct Noop;
    // Not `Waker::noop()`: the test counts the strong references the waiters retain.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn dropped_waits_keep_nothing() {
        let watch = Watch::new(0u32);
        let tokens: Vec<Arc<Noop>> = (0..1000).map(|_| Arc::new(Noop)).collect();
        for token in &tokens {
            let waker = Waker::from(token.clone());
            let mut cx = Context::from_waker(&waker);
            let mut wait = Box::pin(watch.wait_for(|v| (*v > 0).then_some(*v)));
            assert!(wait.as_mut().poll(&mut cx).is_pending());
            assert_eq!(watch.waiting(), 1);
        }
        assert_eq!(watch.waiting(), 0);
        assert!(tokens.iter().all(|t| Arc::strong_count(t) == 1));
    }

    #[test]
    fn a_change_wakes_the_waiters_and_they_release_their_slots() {
        let watch = Watch::new(0u32);
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let mut a = Box::pin(watch.wait_for(|v| (*v > 0).then_some(*v)));
        let mut b = Box::pin(watch.wait_for(|v| (*v > 1).then_some(*v)));
        assert!(a.as_mut().poll(&mut cx).is_pending());
        assert!(b.as_mut().poll(&mut cx).is_pending());
        assert_eq!(watch.waiting(), 2);
        watch.modify(|v| {
            *v = 1;
            (true, ())
        });
        assert_eq!(a.as_mut().poll(&mut cx), Poll::Ready(1));
        assert!(b.as_mut().poll(&mut cx).is_pending());
        assert_eq!(watch.waiting(), 1);
    }
}
