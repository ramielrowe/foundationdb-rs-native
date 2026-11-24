//! The location cache's peer evictor (`locationCachePeerEvictorActor`). See the [module
//! documentation](super#the-peer-evictor) for how it follows C++.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::NetworkAddress;

use super::location::{AddressKey, address_key};
use super::util::{Either, race};
use super::{Shared, lock};
use crate::cluster::ClusterMonitor;

/// `locationCachePeerEvictorActor` (`DatabaseContext.cpp` 8.0:1260-1336, NativeAPI
/// 7.4:1286-1362): every `delay`, the addresses whose connect failures grew by more than
/// `threshold` since the last sweep lose their cached locations. Ends when the reader is
/// gone, its peer manager shut down or its cluster monitor closed.
pub(super) async fn run<R: Runtime, W: StreamWrapper<R::Stream>>(
    rt: R,
    monitor: Arc<ClusterMonitor<R>>,
    shared: Weak<Shared<R, W>>,
    delay: Duration,
    threshold: u64,
) {
    let mut last = HashMap::new();
    loop {
        let closed = async {
            let _ = monitor.wait_for(|_| None::<()>).await;
        };
        if let Either::Right(()) = race(rt.sleep(delay), closed).await {
            return;
        }
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if shared.check_open().is_err() {
            return;
        }
        let counts = shared.peers.failure_monitor().connect_failures();
        let failing = failing_addresses(&mut last, counts, threshold);
        if !failing.is_empty() {
            let evicted = lock(&shared.locations).invalidate_addresses(&failing);
            lock(&shared.state).peer_evictions += evicted as u64;
        }
    }
}

/// One sweep's reading of the transport's connect failure counts (`counts`, `FlowTransport`'s
/// `getPersistentConnectFailedCounts`) against the previous one (`last`, which becomes this
/// one): the addresses whose count grew by more than `threshold`. A count that went down was
/// forgotten and started over, so it counts from zero. Addresses the transport forgot are
/// dropped from `last`.
pub(super) fn failing_addresses(
    last: &mut HashMap<AddressKey, u64>,
    counts: Vec<(NetworkAddress, u64)>,
    threshold: u64,
) -> HashSet<AddressKey> {
    let mut failing = HashSet::new();
    let mut now = HashMap::with_capacity(counts.len());
    for (address, count) in counts {
        let key = address_key(&address);
        let previous = last.get(&key).copied().unwrap_or(0);
        let delta = if count >= previous {
            count - previous
        } else {
            count
        };
        if delta > threshold {
            failing.insert(key);
        }
        now.insert(key, count);
    }
    *last = now;
    failing
}

#[cfg(test)]
mod tests {
    use fdb_native_wire::IpAddress;

    use super::*;

    fn addr(last: u8) -> NetworkAddress {
        NetworkAddress::new(
            IpAddress::V4(0x0a00_0000 + u32::from(last)),
            4500,
            true,
            false,
        )
    }

    #[test]
    fn an_address_fails_when_its_count_grows_past_the_threshold() {
        let key = |a: u8| address_key(&addr(a));
        let mut last = HashMap::new();
        // First sweep: every count is new, so all grew.
        let failing = failing_addresses(&mut last, vec![(addr(1), 3), (addr(2), 1)], 0);
        assert_eq!(failing, HashSet::from([key(1), key(2)]));
        // Unchanged counts: nothing; a grown one: that one.
        let failing = failing_addresses(&mut last, vec![(addr(1), 3), (addr(2), 2)], 0);
        assert_eq!(failing, HashSet::from([key(2)]));
        // A count that went down was forgotten and started over: it counts from zero.
        let failing = failing_addresses(&mut last, vec![(addr(1), 1), (addr(2), 2)], 0);
        assert_eq!(failing, HashSet::from([key(1)]));
        // A forgotten address is dropped from the snapshot.
        let failing = failing_addresses(&mut last, vec![(addr(2), 2)], 0);
        assert!(failing.is_empty());
        assert_eq!(last.len(), 1);
        // The threshold: more than it.
        let failing = failing_addresses(&mut last, vec![(addr(2), 4), (addr(3), 3)], 2);
        assert_eq!(failing, HashSet::from([key(3)]));
        // The TLS flag is part of the address.
        let tls = NetworkAddress::new(IpAddress::V4(0x0a00_0003), 4500, true, true);
        let failing = failing_addresses(&mut last, vec![(addr(3), 3), (tls, 1)], 0);
        assert_eq!(failing, HashSet::from([address_key(&tls)]));
    }
}
