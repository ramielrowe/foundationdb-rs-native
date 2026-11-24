//! The users of a peer manager's connections ([`PeerManager::share`](super::PeerManager::share)):
//! each has its own shutdown, and the commands of a user that is shut down end at once.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use super::watch::{WaitFor, Watch};

/// One user of the connections: the [`PeerManager`](super::PeerManager) values made by one
/// `new` / `with_stream_wrapper` or `share` call and their clones, and the handles they gave
/// out.
#[derive(Debug)]
pub(crate) struct User {
    /// Set once by [`close`](Self::close); read without the lock on every command.
    closed: AtomicBool,
    /// The same flag, for the commands that wait.
    signal: Watch<bool>,
    /// How many `PeerManager` values of this user exist.
    pub(crate) clones: AtomicUsize,
}

type IsClosed = fn(&bool) -> Option<()>;

fn is_closed(closed: &bool) -> Option<()> {
    closed.then_some(())
}

impl User {
    pub(crate) fn new() -> User {
        User {
            closed: AtomicBool::new(false),
            signal: Watch::new(false),
            clones: AtomicUsize::new(1),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Shuts the user down. Returns whether this call did (false if it already was).
    pub(crate) fn close(&self) -> bool {
        if self.closed.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.signal.modify(|closed| {
            *closed = true;
            (true, ())
        });
        true
    }

    fn wait_closed(&self) -> WaitFor<'_, bool, IsClosed> {
        self.signal.wait_for(is_closed as IsClosed)
    }

    /// Runs `fut` until it completes (`Some`) or the user is shut down (`None`; `fut` is then
    /// dropped). `fut` is polled first: an answer that is ready wins over the shutdown. The
    /// shutdown signal is only looked at (and a waker registered for it) while `fut` is
    /// pending.
    pub(crate) async fn unless_closed<F: Future>(&self, fut: F) -> Option<F::Output> {
        let mut fut = pin!(fut);
        let mut wait: Option<WaitFor<'_, bool, IsClosed>> = None;
        std::future::poll_fn(|cx| {
            if let Poll::Ready(output) = fut.as_mut().poll(cx) {
                return Poll::Ready(Some(output));
            }
            let wait = wait.get_or_insert_with(|| self.wait_closed());
            match Pin::new(wait).poll(cx) {
                Poll::Ready(()) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Waker};

    use super::*;

    #[test]
    fn a_pending_future_ends_when_the_user_closes() {
        let user = User::new();
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(user.unless_closed(std::future::pending::<()>()));
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert_eq!(user.signal.waiting(), 1);
        assert!(user.close());
        assert!(!user.close());
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(None));
        drop(fut);
        assert_eq!(user.signal.waiting(), 0);
    }

    #[test]
    fn a_ready_future_wins_and_registers_nothing() {
        let user = User::new();
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(user.unless_closed(std::future::ready(7)));
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(user.signal.waiting(), 0);
        user.close();
        let mut fut = Box::pin(user.unless_closed(std::future::ready(8)));
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Some(8)));
    }
}
