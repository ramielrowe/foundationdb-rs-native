//! A client-side `SimpleFailureMonitor`: which addresses and endpoints are known to be down.

use std::collections::HashMap;
use std::sync::Arc;

use fdb_native_wire::{NetworkAddress, Uid};

use super::address::PeerKey;
use super::watch::Watch;
use crate::connection::well_known;
use crate::runtime::Instant;

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

/// Why an endpoint is known to be unusable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointFailure {
    /// The peer answered a request with endpoint-not-found: it does not have the endpoint.
    NotFound,
    /// The peer refused the endpoint as unauthorized (`WLTOKEN_UNAUTHORIZED_ENDPOINT`).
    Unauthorized,
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
}

/// What this process knows about the health of the processes it talks to: FDB's
/// `IFailureMonitor`, for a client.
///
/// Two kinds of failure are tracked, as `SimpleFailureMonitor` does:
///
/// * **Addresses.** An address starts available (FDB treats an address it knows nothing
///   about as not failed). A [`PeerManager`](super::PeerManager) marks it available when a
///   connection completes its handshake, and failed once connections to it have failed
///   continuously for `failure_detection_delay` (4 s). Every end of a connection is also
///   counted as a disconnect.
/// * **Endpoints.** An endpoint (address and token) is failed for good once the peer answers a
///   request to it with endpoint-not-found ([`endpoint_not_found`](Self::endpoint_not_found),
///   which the peer manager calls itself for every such notice) or unauthorized. It is not
///   cleared by a reconnect (tokens are not reused), only by
///   [`forget_endpoint`](Self::forget_endpoint). Well-known tokens are never marked: FDB
///   ignores a not-found for them (they exist again when the process restarts).
///
/// The monitor is cheap to clone (a shared handle) and has no executor dependency: the
/// `async` methods wait on its changes with plain wakers.
#[derive(Clone, Debug, Default)]
pub struct FailureMonitor {
    state: Arc<Watch<State>>,
}

impl Default for Watch<State> {
    fn default() -> Self {
        Watch::new(State::default())
    }
}

impl FailureMonitor {
    /// An empty monitor: every address is available.
    pub fn new() -> FailureMonitor {
        FailureMonitor::default()
    }

    /// The state of `address`.
    pub fn address_status(&self, address: &NetworkAddress) -> AddressStatus {
        let key = PeerKey::new(address);
        self.state.read(|s| s.address(&key))
    }

    /// Whether `address` is failed.
    pub fn is_failed(&self, address: &NetworkAddress) -> bool {
        self.address_status(address).failed
    }

    /// The state of the endpoint `token` at `address`. A failed endpoint is failed whatever
    /// its address does; an endpoint of a failed address is failed too.
    pub fn endpoint_status(&self, address: &NetworkAddress, token: Uid) -> EndpointStatus {
        let key = PeerKey::new(address);
        self.state.read(|s| s.endpoint(&key, token))
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
        if well_known::is_well_known(token) {
            return false;
        }
        let key = PeerKey::new(address);
        self.state.modify(|s| {
            if s.endpoints.len() > MAX_FAILED_ENDPOINTS {
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
        self.state.modify(|s| {
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
        self.state.modify(|s| {
            let entry = s.addresses.entry(key).or_default();
            if entry.failed == failed {
                return (false, false);
            }
            entry.failed = failed;
            entry.since = Some(now);
            (true, true)
        })
    }

    /// A connection to `address` ended (`notifyDisconnect`).
    pub(crate) fn notify_disconnect(&self, address: &NetworkAddress) {
        let key = PeerKey::new(address);
        self.state.modify(|s| {
            s.addresses.entry(key).or_default().disconnects += 1;
            (true, ())
        });
    }

    /// Resolves once `address` is failed (`failed`) or available (`!failed`): immediately if
    /// it already is. Returns the status then.
    pub async fn wait_address_failed(
        &self,
        address: &NetworkAddress,
        failed: bool,
    ) -> AddressStatus {
        let key = PeerKey::new(address);
        self.state
            .wait_for(|s| {
                let status = s.address(&key);
                (status.failed == failed).then_some(status)
            })
            .await
    }

    /// Resolves with the endpoint's new state when it differs from what it is now
    /// (`IFailureMonitor::onStateChanged`). An endpoint that has failed itself never changes
    /// again, so this never resolves for one.
    pub async fn on_state_changed(&self, address: &NetworkAddress, token: Uid) -> EndpointStatus {
        let key = PeerKey::new(address);
        let from = self.state.read(|s| s.endpoint(&key, token));
        self.state
            .wait_for(|s| {
                let now = s.endpoint(&key, token);
                (now != from).then_some(now)
            })
            .await
    }

    /// Resolves once a connection to `address` has ended after the call
    /// (`IFailureMonitor::onDisconnect`). Returns the address's disconnect count then.
    pub async fn on_disconnect(&self, address: &NetworkAddress) -> u64 {
        let key = PeerKey::new(address);
        let from = self.state.read(|s| s.address(&key).disconnects);
        self.state
            .wait_for(|s| {
                let now = s.address(&key).disconnects;
                (now != from).then_some(now)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    }
}
