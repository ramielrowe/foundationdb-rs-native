//! A value behind a lock whose changes can be awaited, without any executor type.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// Shared state that tasks can wait on: [`wait_for`](Watch::wait_for) resolves once a predicate
/// over the value holds, and every [`modify`](Watch::modify) that reports a change wakes the
/// waiters to look again. The predicate is evaluated and the waker registered under one lock,
/// so a change cannot slip between the check and the wait.
#[derive(Debug)]
pub(crate) struct Watch<T> {
    inner: Mutex<Inner<T>>,
}

#[derive(Debug)]
struct Inner<T> {
    value: T,
    wakers: Vec<Waker>,
}

impl<T> Watch<T> {
    pub(crate) fn new(value: T) -> Watch<T> {
        Watch {
            inner: Mutex::new(Inner {
                value,
                wakers: Vec::new(),
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
    /// then) and a result.
    pub(crate) fn modify<R>(&self, f: impl FnOnce(&mut T) -> (bool, R)) -> R {
        let (result, wakers) = {
            let mut inner = self.lock();
            let (changed, result) = f(&mut inner.value);
            let wakers = if changed {
                std::mem::take(&mut inner.wakers)
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

    /// Resolves with `f`'s first `Some` result, checked now and after every change.
    pub(crate) fn wait_for<R, F>(&self, f: F) -> WaitFor<'_, T, F>
    where
        F: FnMut(&T) -> Option<R>,
    {
        WaitFor { watch: self, f }
    }
}

/// The future of [`Watch::wait_for`].
#[derive(Debug)]
pub(crate) struct WaitFor<'a, T, F> {
    watch: &'a Watch<T>,
    f: F,
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
            return Poll::Ready(result);
        }
        if !inner.wakers.iter().any(|w| w.will_wake(cx.waker())) {
            inner.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}
