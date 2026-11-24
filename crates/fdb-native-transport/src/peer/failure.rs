//! A client-side `SimpleFailureMonitor`: which addresses and endpoints are known to be down.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use fdb_native_wire::{NetworkAddress, Uid};

use super::address::PeerKey;
use super::waiters::Waiters;
use crate::connection::well_known;
use crate::runtime::Instant;

pub use crate::rpc::EndpointFailure;

/// FDB's cap on remembered failed endpoints, after which it forgets them all
/// (`SimpleFailureMonitor::endpointNotFound`, `fdbrpc/FailureMonitor.actor.cpp`).
const MAX_FAILED_ENDPOINTS: usize = 100_000;

/// Whether an address is believed reachable, and since when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddressStatus {
    /// Whether connections to the address have been failing (see
    /// [`PeerManagerConfig::failure_detection_delay`](super::PeerManagerConfig::failure_detection_delay)).
    pub failed: bool,
    /// When the status last changed; `None` for an address nothing has happened to
    /// (available, as in FDB, where an unknown address is not failed).
    pub since: Option<Instant>,
    /// How many times a connection to the address has ended (FDB's `notifyDisconnect`).
    pub disconnects: u64,
}

/// The failure state of one endpoint (an address and a token), `IFailureMonitor::getState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointStatus {
    /// Not known to be failed.
    Available,
    /// The endpoint's address is failed.
    AddressFailed,
    /// The endpoint itself is failed, for good (until [`FailureMonitor::forget_endpoint`]).
    EndpointFailed(EndpointFailure),
}

impl EndpointStatus {
    /// Whether requests to the endpoint are expected to fail.
    pub fn is_failed(self) -> bool {
        self != EndpointStatus::Available
    }
}

#[derive(Debug, Default)]
struct State {
    addresses: HashMap<PeerKey, Entry>,
    endpoints: HashMap<(PeerKey, Uid), EndpointFailure>,
    /// The waits on each address (its status, its disconnects, its endpoints). A change to one
    /// address wakes only that address's waiters.
    topics: HashMap<PeerKey, Waiters>,
    /// FlowTransport's `persistentConnectFailedCount`: failed connects per address.
    connect_failures: HashMap<PeerKey, ConnectFailures>,
    /// `persistentConnectFailedLastPrune`; `None` before the first failure.
    connect_failures_pruned: Option<Instant>,
}

/// `ConnectFailedInfo`: an address's failed connects, and when the last one was.
#[derive(Debug)]
struct ConnectFailures {
    address: NetworkAddress,
    count: u64,
    last_failed: Instant,
}

#[derive(Debug, Default)]
struct Entry {
    failed: bool,
    since: Option<Instant>,
    disconnects: u64,
}

impl State {
    fn address(&self, key: &PeerKey) -> AddressStatus {
        match self.addresses.get(key) {
            Some(e) => AddressStatus {
                failed: e.failed,
                since: e.since,
                disconnects: e.disconnects,
            },
            None => AddressStatus {
                failed: false,
                since: None,
                disconnects: 0,
            },
        }
    }

    fn endpoint(&self, key: &PeerKey, token: Uid) -> EndpointStatus {
        if let Some(why) = self.endpoints.get(&(*key, token)) {
            EndpointStatus::EndpointFailed(*why)
        } else if self.addresses.get(key).is_some_and(|e| e.failed) {
            EndpointStatus::AddressFailed
        } else {
            EndpointStatus::Available
        }
    }

    /// Takes the wakers of the waits on `key`, to wake once the lock is released.
    fn wakers(&mut self, key: &PeerKey) -> Vec<Waker> {
        self.topics
            .get_mut(key)
            .map(Waiters::take_all)
            .unwrap_or_default()
    }
}

/// What this process knows about the health of the processes it talks to: FDB's
/// `IFailureMonitor`, for a client.
///
/// Two kinds of failure are tracked, as `SimpleFailureMonitor` does:
///
/// * **Addresses.** An address starts available (FDB treats an address it knows nothing
///   about as not failed). A [`PeerManager`](super::PeerManager) marks it available when a
///   connection completes its handshake, and failed once connections to it have failed
///   continuously for `failure_detection_delay` (4 s). Every end of a connection or failed
///   attempt is also counted as a disconnect, once.
/// * **Endpoints.** An endpoint (address and token) is failed for good once the peer answers a
///   request to it with endpoint-not-found or unauthorized, or a request to it gets
///   `broken_promise`. It is not cleared by a reconnect (tokens are not reused), only by
///   [`forget_endpoint`](Self::forget_endpoint). Well-known tokens are never marked: FDB
///   ignores a not-found for them (they exist again when the process restarts).
///
/// # Relation to the request layer
///
/// Each peer's [`ReplyRegistry`](crate::rpc::ReplyRegistry) also remembers the endpoints that
/// failed, and reports each new one as an event; the peer task records it here
/// ([`endpoint_not_found`](Self::endpoint_not_found) /
/// [`unauthorized_endpoint`](Self::unauthorized_endpoint)). **This monitor is the source of
/// truth**: it is process-wide, and anything may write to it (a caller that learned of an
/// endpoint's death another way, or [`forget_endpoint`](Self::forget_endpoint) when an endpoint
/// is announced again). Before every request the peer task consults it: a failed endpoint is
/// refused with the matching [`RequestError`](crate::rpc::RequestError) and nothing sent,
/// and an endpoint that is available here is forgotten by the registry too, so the two never
/// disagree for longer than one request.
///
/// The monitor is cheap to clone (a shared handle) and has no executor dependency: the
/// `async` methods wait on its changes with plain wakers, one slot per wait, returned when the
/// wait is dropped. A change to one address wakes only the waits on that address.
#[derive(Clone, Debug, Default)]
pub struct FailureMonitor {
    state: Arc<Mutex<State>>,
}

impl FailureMonitor {
    /// An empty monitor: every address is available.
    pub fn new() -> FailureMonitor {
        FailureMonitor::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while holding the lock leaves plain data behind: keep going.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Changes the state of `key`; `f` returns whether it changed anything (the address's
    /// waiters are woken only then) and a result.
    fn modify<R>(&self, key: &PeerKey, f: impl FnOnce(&mut State) -> (bool, R)) -> R {
        let (result, wakers) = {
            let mut state = self.lock();
            let (changed, result) = f(&mut state);
            let wakers = if changed {
                state.wakers(key)
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

    /// The state of `address`.
    pub fn address_status(&self, address: &NetworkAddress) -> AddressStatus {
        let key = PeerKey::new(address);
        self.lock().address(&key)
    }

    /// Whether `address` is failed.
    pub fn is_failed(&self, address: &NetworkAddress) -> bool {
        self.address_status(address).failed
    }

    /// The state of the endpoint `token` at `address`. A failed endpoint is failed whatever
    /// its address does; an endpoint of a failed address is failed too.
    pub fn endpoint_status(&self, address: &NetworkAddress, token: Uid) -> EndpointStatus {
        let key = PeerKey::new(address);
        self.lock().endpoint(&key, token)
    }

    /// Why the endpoint `token` at `address` is failed for good, if it is (its address being
    /// failed does not count).
    pub fn endpoint_failure(
        &self,
        address: &NetworkAddress,
        token: Uid,
    ) -> Option<EndpointFailure> {
        let key = PeerKey::new(address);
        self.lock().endpoints.get(&(key, token)).copied()
    }

    /// Records that the peer at `address` does not have the endpoint `token`
    /// (`IFailureMonitor::endpointNotFound`). Ignored for well-known tokens. Returns whether
    /// the endpoint's state changed.
    pub fn endpoint_not_found(&self, address: &NetworkAddress, token: Uid) -> bool {
        self.fail_endpoint(address, token, EndpointFailure::NotFound)
    }

    /// Records that the peer at `address` refused the endpoint `token` as unauthorized
    /// (`IFailureMonitor::unauthorizedEndpoint`). Returns whether the state changed.
    pub fn unauthorized_endpoint(&self, address: &NetworkAddress, token: Uid) -> bool {
        self.fail_endpoint(address, token, EndpointFailure::Unauthorized)
    }

    fn fail_endpoint(&self, address: &NetworkAddress, token: Uid, why: EndpointFailure) -> bool {
        if well_known::is_well_known(token) && why == EndpointFailure::NotFound {
            return false;
        }
        let key = PeerKey::new(address);
        self.modify(&key, |s| {
            if why == EndpointFailure::NotFound && s.endpoints.len() > MAX_FAILED_ENDPOINTS {
                s.endpoints.clear();
            }
            let changed = s.endpoints.insert((key, token), why) != Some(why);
            (changed, changed)
        })
    }

    /// Forgets that an endpoint failed (it is announced again, say). Returns whether it was
    /// failed.
    pub fn forget_endpoint(&self, address: &NetworkAddress, token: Uid) -> bool {
        let key = PeerKey::new(address);
        self.modify(&key, |s| {
            let was = s.endpoints.remove(&(key, token)).is_some();
            (was, was)
        })
    }

    /// Marks `address` available or failed at `now`. Returns whether that was a change.
    pub(crate) fn set_address_failed(
        &self,
        address: &NetworkAddress,
        failed: bool,
        now: Instant,
    ) -> bool {
        let key = PeerKey::new(address);
        self.modify(&key, |s| {
            let entry = s.addresses.entry(key).or_default();
            if entry.failed == failed {
                return (false, false);
            }
            entry.failed = failed;
            entry.since = Some(now);
            (true, true)
        })
    }

    /// A connect to `address` failed at `now`: the TCP connect or the TLS handshake, what
    /// `connectionKeeper` counts in `persistentConnectFailedCount` (`FlowTransport.actor.cpp`
    /// 7.4:886-923, `FlowTransport.cpp` 8.0:980-1016). On a failure, once `ttl`
    /// (`PERSISTENT_CONNECT_FAILED_COUNT_TTL`) has passed since the last pruning, the
    /// addresses whose last failure is `ttl` old or more are forgotten; a zero `ttl` never
    /// prunes. Waiters are not woken: this is a counter for periodic readers
    /// ([`connect_failures`](Self::connect_failures)).
    pub(crate) fn connect_failed(&self, address: &NetworkAddress, now: Instant, ttl: Duration) {
        let key = PeerKey::new(address);
        let mut state = self.lock();
        let entry = state
            .connect_failures
            .entry(key)
            .or_insert(ConnectFailures {
                address: *address,
                count: 0,
                last_failed: now,
            });
        entry.count += 1;
        entry.last_failed = now;
        let due = state
            .connect_failures_pruned
            .is_none_or(|at| now.saturating_duration_since(at) >= ttl);
        if !ttl.is_zero() && due {
            state.connect_failures_pruned = Some(now);
            state
                .connect_failures
                .retain(|_, e| now.saturating_duration_since(e.last_failed) < ttl);
        }
    }

    /// The failed connects counted per address
    /// (`FlowTransport::getPersistentConnectFailedCounts`): every address that failed to
    /// connect and was not forgotten since (see
    /// [`PeerManagerConfig::connect_failed_count_ttl`](super::PeerManagerConfig::connect_failed_count_ttl)),
    /// with its count of failures. A count only grows, except for an address that was
    /// forgotten and fails again: it starts over from 1. The location cache's peer evictor
    /// reads it.
    pub fn connect_failures(&self) -> Vec<(NetworkAddress, u64)> {
        self.lock()
            .connect_failures
            .values()
            .map(|e| (e.address, e.count))
            .collect()
    }

    /// A connection to `address` ended (`notifyDisconnect`).
    pub(crate) fn notify_disconnect(&self, address: &NetworkAddress) {
        let key = PeerKey::new(address);
        self.modify(&key, |s| {
            s.addresses.entry(key).or_default().disconnects += 1;
            (true, ())
        });
    }

    /// A wait on `key`'s state that resolves with `f`'s first `Some`.
    fn wait<R, F: FnMut(&State) -> Option<R>>(&self, key: PeerKey, f: F) -> Wait<'_, F> {
        Wait {
            monitor: self,
            key,
            f,
            slot: None,
        }
    }

    /// Resolves once `address` is failed (`failed`) or available (`!failed`): immediately if
    /// it already is. Returns the status then.
    pub async fn wait_address_failed(
        &self,
        address: &NetworkAddress,
        failed: bool,
    ) -> AddressStatus {
        let key = PeerKey::new(address);
        self.wait(key, |s| {
            let status = s.address(&key);
            (status.failed == failed).then_some(status)
        })
        .await
    }

    /// Resolves once the endpoint `token` at `address` is not failed (neither the endpoint
    /// itself nor its address): at once if it is not. This is
    /// `IFailureMonitor::onStateEqual(endpoint, FailureStatus(false))`, which the load
    /// balancers wait on when every alternative is failed: the state is checked and the wait
    /// registered under one lock, so a change between a check and the wait cannot be missed
    /// (as it could be by checking [`endpoint_status`](Self::endpoint_status), then calling
    /// [`on_state_changed`](Self::on_state_changed)). An endpoint that has failed itself
    /// stays failed, so this resolves for one only once it is
    /// [forgotten](Self::forget_endpoint).
    pub async fn wait_endpoint_available(&self, address: &NetworkAddress, token: Uid) {
        let key = PeerKey::new(address);
        self.wait(key, |s| {
            (!s.endpoint(&key, token).is_failed()).then_some(())
        })
        .await
    }

    /// Resolves with the endpoint's new state when it differs from what it is now
    /// (`IFailureMonitor::onStateChanged`). An endpoint that has failed itself never changes
    /// again, so this never resolves for one.
    pub async fn on_state_changed(&self, address: &NetworkAddress, token: Uid) -> EndpointStatus {
        let key = PeerKey::new(address);
        let from = self.lock().endpoint(&key, token);
        self.wait(key, |s| {
            let now = s.endpoint(&key, token);
            (now != from).then_some(now)
        })
        .await
    }

    /// Resolves once a connection to `address` has ended after the call
    /// (`IFailureMonitor::onDisconnect`). Returns the address's disconnect count then.
    pub async fn on_disconnect(&self, address: &NetworkAddress) -> u64 {
        let key = PeerKey::new(address);
        let from = self.lock().address(&key).disconnects;
        self.wait(key, |s| {
            let now = s.address(&key).disconnects;
            (now != from).then_some(now)
        })
        .await
    }

    /// How many waits are registered on `address` (for tests).
    #[cfg(test)]
    fn waiting(&self, address: &NetworkAddress) -> usize {
        let key = PeerKey::new(address);
        self.lock().topics.get(&key).map_or(0, Waiters::len)
    }
}

/// A wait on one address's state.
struct Wait<'a, F> {
    monitor: &'a FailureMonitor,
    key: PeerKey,
    f: F,
    slot: Option<usize>,
}

impl<F> Wait<'_, F> {
    /// Gives the slot back, forgetting the address's waiter list once it is empty.
    fn release(&mut self, state: &mut State) {
        if self.slot.is_none() {
            return;
        }
        if let Some(waiters) = state.topics.get_mut(&self.key) {
            waiters.release(&mut self.slot);
            if waiters.is_empty() {
                state.topics.remove(&self.key);
            }
        }
        self.slot = None;
    }
}

impl<F: Unpin> Unpin for Wait<'_, F> {}

impl<R, F: FnMut(&State) -> Option<R> + Unpin> Future for Wait<'_, F> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        let this = &mut *self;
        let mut state = this.monitor.lock();
        if let Some(result) = (this.f)(&state) {
            this.release(&mut state);
            return Poll::Ready(result);
        }
        state
            .topics
            .entry(this.key)
            .or_default()
            .register(&mut this.slot, cx.waker());
        Poll::Pending
    }
}

impl<F> Drop for Wait<'_, F> {
    fn drop(&mut self) {
        if self.slot.is_some() {
            let mut state = self.monitor.lock();
            self.release(&mut state);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use fdb_native_wire::IpAddress;
    use futures_lite::future::{block_on, poll_once};

    use super::*;

    fn addr(port: u16) -> NetworkAddress {
        NetworkAddress::new(IpAddress::V4(0x7f00_0001), port, true, false)
    }

    fn t(ms: u64) -> Instant {
        Instant::from_origin(Duration::from_millis(ms))
    }

    #[test]
    fn connect_failures_are_counted_and_pruned_after_the_ttl() {
        let fm = FailureMonitor::new();
        let ttl = Duration::from_secs(600);
        let count = |fm: &FailureMonitor, a: &NetworkAddress| {
            fm.connect_failures()
                .into_iter()
                .find(|(x, _)| x == a)
                .map(|(_, n)| n)
        };
        assert!(fm.connect_failures().is_empty());
        fm.connect_failed(&addr(1), t(0), ttl);
        fm.connect_failed(&addr(1), t(1000), ttl);
        fm.connect_failed(&addr(2), t(2000), ttl);
        assert_eq!(count(&fm, &addr(1)), Some(2));
        assert_eq!(count(&fm, &addr(2)), Some(1));
        // A counter only: the status of the addresses is unchanged.
        assert!(!fm.is_failed(&addr(1)));
        assert_eq!(fm.address_status(&addr(1)).disconnects, 0);
        // The pruning runs on a failure once `ttl` has passed since the last one (at 0):
        // address 1 last failed 599 s before and stays; at 601 s it is forgotten.
        fm.connect_failed(&addr(3), t(600_000), ttl);
        assert_eq!(count(&fm, &addr(1)), Some(2));
        fm.connect_failed(&addr(3), t(1_100_000), ttl);
        assert_eq!(count(&fm, &addr(1)), Some(2), "not due before 1200 s");
        fm.connect_failed(&addr(3), t(1_200_000), ttl);
        assert_eq!(count(&fm, &addr(1)), None);
        assert_eq!(count(&fm, &addr(2)), None);
        assert_eq!(count(&fm, &addr(3)), Some(3));
        // A forgotten address that fails again starts over.
        fm.connect_failed(&addr(1), t(1_200_001), ttl);
        assert_eq!(count(&fm, &addr(1)), Some(1));
        // A zero ttl never prunes.
        let fm = FailureMonitor::new();
        fm.connect_failed(&addr(1), t(0), Duration::ZERO);
        fm.connect_failed(&addr(2), t(10_000_000), Duration::ZERO);
        assert_eq!(fm.connect_failures().len(), 2);
    }

    #[test]
    fn unknown_addresses_are_available() {
        let fm = FailureMonitor::new();
        let s = fm.address_status(&addr(1));
        assert!(!s.failed && s.since.is_none() && s.disconnects == 0);
        assert_eq!(
            fm.endpoint_status(&addr(1), Uid::new(2, 3)),
            EndpointStatus::Available
        );
    }

    #[test]
    fn address_transitions_carry_timestamps() {
        let fm = FailureMonitor::new();
        let a = addr(1);
        assert!(fm.set_address_failed(&a, true, t(5)));
        assert!(!fm.set_address_failed(&a, true, t(9)), "no change");
        assert_eq!(
            fm.address_status(&a),
            AddressStatus {
                failed: true,
                since: Some(t(5)),
                disconnects: 0
            }
        );
        assert_eq!(
            fm.endpoint_status(&a, Uid::new(2, 3)),
            EndpointStatus::AddressFailed
        );
        assert!(fm.set_address_failed(&a, false, t(20)));
        assert_eq!(fm.address_status(&a).since, Some(t(20)));
        assert!(!fm.is_failed(&a));
        // Another address is unaffected.
        assert!(!fm.is_failed(&addr(2)));
    }

    #[test]
    fn endpoints_fail_for_good_except_well_known_ones() {
        let fm = FailureMonitor::new();
        let a = addr(1);
        let token = Uid::new(8, 9);
        assert!(fm.endpoint_not_found(&a, token));
        assert!(!fm.endpoint_not_found(&a, token), "already known");
        assert_eq!(
            fm.endpoint_status(&a, token),
            EndpointStatus::EndpointFailed(EndpointFailure::NotFound)
        );
        assert_eq!(
            fm.endpoint_failure(&a, token),
            Some(EndpointFailure::NotFound)
        );
        // A reconnect (an address becoming available) does not clear it.
        fm.set_address_failed(&a, true, t(1));
        fm.set_address_failed(&a, false, t(2));
        assert!(fm.endpoint_status(&a, token).is_failed());
        // Other tokens and addresses are unaffected.
        assert!(!fm.endpoint_status(&a, Uid::new(8, 10)).is_failed());
        assert!(!fm.endpoint_status(&addr(2), token).is_failed());
        // Well-known endpoints are not marked.
        assert!(!fm.endpoint_not_found(&a, well_known::PING_PACKET));
        assert!(!fm.endpoint_status(&a, well_known::PING_PACKET).is_failed());
        assert!(fm.unauthorized_endpoint(&a, token), "reason changed");
        assert!(fm.forget_endpoint(&a, token));
        assert!(!fm.endpoint_status(&a, token).is_failed());
        assert!(!fm.forget_endpoint(&a, token));
    }

    #[test]
    fn waiters_wake_on_changes() {
        let fm = FailureMonitor::new();
        let a = addr(1);
        let token = Uid::new(8, 9);
        let mut failed = Box::pin(fm.wait_address_failed(&a, true));
        let mut changed = Box::pin(fm.on_state_changed(&a, token));
        let mut disconnect = Box::pin(fm.on_disconnect(&a));
        let mut available = Box::pin(fm.wait_address_failed(&a, false));
        block_on(async {
            assert!(poll_once(&mut failed).await.is_none());
            assert!(poll_once(&mut changed).await.is_none());
            assert!(poll_once(&mut disconnect).await.is_none());
            // Already available: resolves at once.
            assert!(poll_once(&mut available).await.is_some());
        });
        fm.notify_disconnect(&a);
        fm.set_address_failed(&a, true, t(3));
        block_on(async {
            assert_eq!(disconnect.await, 1);
            let s = failed.await;
            assert!(s.failed);
            assert_eq!(s.since, Some(t(3)));
            assert_eq!(changed.await, EndpointStatus::AddressFailed);
        });
        assert_eq!(fm.waiting(&a), 0, "finished waits give their slots back");
    }

    #[test]
    fn waiting_for_an_endpoint_to_be_available() {
        let fm = FailureMonitor::new();
        let a = addr(1);
        let token = Uid::new(8, 9);
        block_on(async {
            // Available: at once.
            assert!(
                poll_once(Box::pin(fm.wait_endpoint_available(&a, token)))
                    .await
                    .is_some()
            );
        });

        // The address fails, then comes back.
        fm.set_address_failed(&a, true, t(1));
        let mut wait = Box::pin(fm.wait_endpoint_available(&a, token));
        block_on(async { assert!(poll_once(&mut wait).await.is_none()) });
        // A change that leaves it failed does not end the wait.
        fm.notify_disconnect(&a);
        block_on(async { assert!(poll_once(&mut wait).await.is_none()) });
        fm.set_address_failed(&a, false, t(2));
        block_on(async { assert!(poll_once(&mut wait).await.is_some()) });

        // A transition that happens between creating the wait and polling it (or between two
        // polls) is seen: the predicate is evaluated on the state, not compared with a
        // snapshot taken earlier.
        fm.set_address_failed(&a, true, t(3));
        let mut wait = Box::pin(fm.wait_endpoint_available(&a, token));
        fm.set_address_failed(&a, false, t(4));
        block_on(async { assert!(poll_once(&mut wait).await.is_some()) });

        // An endpoint that failed itself: only when it is forgotten, whatever the address.
        assert!(fm.endpoint_not_found(&a, token));
        let mut wait = Box::pin(fm.wait_endpoint_available(&a, token));
        block_on(async { assert!(poll_once(&mut wait).await.is_none()) });
        fm.set_address_failed(&a, true, t(5));
        fm.set_address_failed(&a, false, t(6));
        block_on(async { assert!(poll_once(&mut wait).await.is_none()) });
        assert!(fm.forget_endpoint(&a, token));
        block_on(async { assert!(poll_once(&mut wait).await.is_some()) });
        drop(wait);
        assert_eq!(fm.waiting(&a), 0);
    }

    struct Counting(AtomicUsize);

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn dropped_waits_keep_no_wakers() {
        let fm = FailureMonitor::new();
        let a = addr(1);
        let tokens: Vec<Arc<Counting>> = (0..1000)
            .map(|_| Arc::new(Counting(AtomicUsize::new(0))))
            .collect();
        for (i, token) in tokens.iter().enumerate() {
            let waker = Waker::from(token.clone());
            let mut cx = Context::from_waker(&waker);
            let mut wait = Box::pin(fm.on_state_changed(&a, Uid::new(1, i as u64)));
            assert!(wait.as_mut().poll(&mut cx).is_pending());
            assert_eq!(fm.waiting(&a), 1);
        }
        assert_eq!(fm.waiting(&a), 0);
        assert!(tokens.iter().all(|t| Arc::strong_count(t) == 1));
        assert!(
            fm.lock().topics.is_empty(),
            "the address's list is gone too"
        );
    }

    #[test]
    fn a_change_wakes_only_the_waits_on_that_address() {
        let fm = FailureMonitor::new();
        let (a, b) = (addr(1), addr(2));
        let on_a = Arc::new(Counting(AtomicUsize::new(0)));
        let on_b = Arc::new(Counting(AtomicUsize::new(0)));
        let (wa, wb) = (Waker::from(on_a.clone()), Waker::from(on_b.clone()));
        let mut wait_a = Box::pin(fm.on_disconnect(&a));
        let mut wait_b = Box::pin(fm.on_disconnect(&b));
        assert!(
            wait_a
                .as_mut()
                .poll(&mut Context::from_waker(&wa))
                .is_pending()
        );
        assert!(
            wait_b
                .as_mut()
                .poll(&mut Context::from_waker(&wb))
                .is_pending()
        );
        fm.notify_disconnect(&a);
        assert_eq!(on_a.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            on_b.0.load(Ordering::SeqCst),
            0,
            "another address is not woken"
        );
        fm.notify_disconnect(&b);
        assert_eq!(on_b.0.load(Ordering::SeqCst), 1);
    }
}
