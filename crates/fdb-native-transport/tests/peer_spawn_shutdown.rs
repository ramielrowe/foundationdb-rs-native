//! A runtime that is shutting down drops a task inside `spawn` (tokio's `OwnedTasks::bind` on
//! a closed task list). `PeerManager::peer` must not hold the manager's lock then: the dropped
//! `PeerTask` removes its own entry, which takes the same lock. Held across `spawn`, that was a
//! self-deadlock on the thread that was ending the runtime.

#![cfg(feature = "tokio")]

mod common;

use std::io;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Duration, Instant as StdInstant};

use common::*;
use fdb_native_transport::peer::{PeerManager, PeerManagerConfig, event_channel};
use fdb_native_transport::runtime::{Instant, Runtime, TokioTask};

/// [`MemRuntime`], except that `spawn` drops the future inside the call, as a runtime does that
/// has begun to shut down. The returned task never ends.
#[derive(Clone)]
struct ClosedRuntime(MemRuntime);

impl Runtime for ClosedRuntime {
    type Stream = MemStream;
    type Task<T: Send + 'static> = TokioTask<T>;

    fn now(&self) -> Instant {
        self.0.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.0.sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        self.0.sleep_until(deadline)
    }

    fn spawn<T, F>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        drop(future);
        self.0.spawn(std::future::pending::<T>())
    }

    fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<MemStream>> + Send {
        self.0.connect(addr)
    }

    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = io::Result<Vec<SocketAddr>>> + Send {
        self.0.resolve(host, port)
    }
}

#[tokio::test]
async fn a_task_dropped_inside_spawn_does_not_deadlock_the_manager() {
    let net = Net::new();
    let (done, finished) = mpsc::channel();
    let runtime = ClosedRuntime(MemRuntime::new(&net));
    // On its own thread, so that a deadlock fails the test instead of hanging it.
    std::thread::spawn(move || {
        let (sink, _events) = event_channel();
        let manager =
            PeerManager::new(runtime, PeerManagerConfig::default().without_jitter(), sink);
        let address = na(sock(4500), false);
        let first = manager.peer(&address);
        // The dropped task removed its entry: the next call creates a peer again (and does not
        // hang either).
        assert_eq!(manager.peer_count(), 0);
        let second = manager.peer(&address);
        assert_eq!(manager.peer_count(), 0);
        let _ = done.send((first.id() != second.id(), StdInstant::now()));
    });
    let (distinct, _) = finished
        .recv_timeout(Duration::from_secs(10))
        .expect("PeerManager::peer deadlocked when the runtime dropped the task inside spawn");
    assert!(distinct, "the second call gave the removed peer back");
}
