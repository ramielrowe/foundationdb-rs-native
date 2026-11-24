//! The key location cache: C++'s `DatabaseContext::locationCache`, a range map from key
//! ranges (shards) to the storage servers that hold them.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use fdb_native_schema::domain::{KeyRange, StorageServer};
use fdb_native_transport::peer::PeerRef;
use fdb_native_wire::{Endpoint, NetworkAddress, Uid};

use super::util::Rng;

/// `allKeys.end`: the end of the key space (`\xff\xff`). Keys at or after it have no
/// location.
pub const KEY_SPACE_END: &[u8] = b"\xff\xff";

/// `LBDistance::Type`: how far a replica is from this client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Distance {
    /// `SAME_MACHINE` (by zone id; `LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED` is off, so never).
    #[allow(dead_code)]
    SameMachine,
    /// `SAME_DC`: the replica's `dcid` is the client's.
    SameDc,
    /// `DISTANT`.
    Distant,
}

/// `loadBalanceDistance` (`fdbrpc/Locality.cpp` 7.4:327-339; 8.0 `Locality.cpp:32`) with
/// the default knobs: `SAME_DC` when the client has a dc id and the server's locality has
/// the same one, else `DISTANT`.
pub(crate) fn distance(client_dc_id: Option<&[u8]>, server: &StorageServer) -> Distance {
    match (client_dc_id, server.locality.get(b"dcid".as_slice())) {
        (Some(client), Some(Some(dc))) if client == dc.as_slice() => Distance::SameDc,
        _ => Distance::Distant,
    }
}

/// A storage server as the cache holds it (C++'s `StorageServerInfo`, a
/// `ReferencedInterface<StorageServerInterface>`): shared by every cached range it serves,
/// with a reference to its peer, so the connection is kept and a failed address is probed
/// (and found again) while any cached range points at the server.
///
/// The interface can change in place: when a server restarts its storage role with new
/// endpoints but the same id and locality, the next location reply that lists it updates
/// this entry, so every cached range sees the new endpoints (`getInterface` assigns `interf`,
/// NativeAPI 7.4:405-433). A read already in progress keeps the endpoints it started with.
#[derive(Debug)]
pub(crate) struct ServerInfo {
    pub(crate) distance: Distance,
    interface: Mutex<Interface>,
}

#[derive(Debug)]
struct Interface {
    server: StorageServer,
    /// The address the client connects to (`choosePrimaryAddress`).
    address: NetworkAddress,
    _peer: PeerRef,
}

impl ServerInfo {
    fn interface(&self) -> MutexGuard<'_, Interface> {
        self.interface
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Where to send a `GetValueRequest`: the address and the `getValue` (base) token.
    #[cfg(test)]
    pub(crate) fn target(&self) -> (NetworkAddress, Uid) {
        let interface = self.interface();
        (interface.address, interface.server.base.token)
    }

    /// Where to send a request to the stream `index` (`STORAGE_SERVER_STREAMS`): the address
    /// and the stream's (adjusted) token.
    pub(crate) fn target_for(&self, index: u32) -> (NetworkAddress, Uid) {
        let interface = self.interface();
        (
            interface.address,
            interface.server.base.adjusted(index).token,
        )
    }

    /// The interface.
    pub(crate) fn server(&self) -> StorageServer {
        self.interface().server.clone()
    }
}

/// The replicas of one shard (C++'s `LocationInfo`, a
/// `MultiInterface<ReferencedInterface<StorageServerInterface>>`): shuffled, then stably
/// sorted by distance, with `best_count` the number at the nearest distance.
#[derive(Debug)]
pub(crate) struct Location {
    pub(crate) servers: Vec<Arc<ServerInfo>>,
    pub(crate) best_count: usize,
}

impl Location {
    pub(crate) fn new(mut servers: Vec<Arc<ServerInfo>>, rng: &mut Rng) -> Location {
        rng.shuffle(&mut servers);
        servers.sort_by_key(|s| s.distance);
        let best_count = servers
            .iter()
            .position(|s| s.distance > servers[0].distance)
            .unwrap_or(servers.len());
        Location {
            servers,
            best_count,
        }
    }

    /// The servers' interfaces.
    pub(crate) fn storage_servers(&self) -> Vec<StorageServer> {
        self.servers.iter().map(|s| s.server()).collect()
    }
}

#[derive(Clone, Debug)]
struct Cached {
    location: Arc<Location>,
    /// Identifies this entry for the eviction pool (an entry replaced at the same key gets
    /// a new one).
    generation: u64,
}

/// The cache. Every key below [`KEY_SPACE_END`] is in exactly one range; a range either has
/// a location or is unknown. See the [module documentation](super) for how it follows C++.
#[derive(Debug)]
pub(crate) struct LocationCache {
    /// Range begin -> its value; the range ends at the next key (or [`KEY_SPACE_END`]).
    /// Always has `""`. Adjacent unknown ranges are merged.
    map: BTreeMap<Vec<u8>, Option<Cached>>,
    /// How many ranges have a location.
    cached: usize,
    /// Eviction candidates: `(begin, generation)`, possibly stale.
    pool: Vec<(Vec<u8>, u64)>,
    next_generation: u64,
    capacity: usize,
    /// `DatabaseContext::server_interf`: one [`ServerInfo`] per storage server id.
    servers: HashMap<Uid, Weak<ServerInfo>>,
    /// The size of `servers` after its last pruning of dead entries.
    servers_after_prune: usize,
}

impl LocationCache {
    pub(crate) fn new(capacity: usize) -> LocationCache {
        let mut map = BTreeMap::new();
        map.insert(Vec::new(), None);
        LocationCache {
            map,
            cached: 0,
            pool: Vec::new(),
            next_generation: 0,
            capacity: capacity.max(1),
            servers: HashMap::new(),
            servers_after_prune: 0,
        }
    }

    /// How many ranges have a location.
    pub(crate) fn len(&self) -> usize {
        self.cached
    }

    /// How many ranges the map has, known or not.
    #[cfg(test)]
    pub(crate) fn ranges(&self) -> usize {
        self.map.len()
    }

    /// `getCachedLocation(key)`: the cached range containing `key` and its location.
    pub(crate) fn get(&self, key: &[u8]) -> Option<(KeyRange, Arc<Location>)> {
        let (begin, value) = self.containing(key)?;
        let cached = value.as_ref()?;
        let end = self.end_of(begin);
        Some((
            KeyRange {
                begin: begin.clone(),
                end,
            },
            cached.location.clone(),
        ))
    }

    /// `getCachedLocation(key, Reverse::True)`: the cached range containing the keys just
    /// before `key` (`rangeContainingKeyBefore`: `begin < key <= end`) and its location.
    pub(crate) fn get_before(&self, key: &[u8]) -> Option<(KeyRange, Arc<Location>)> {
        let (begin, value) = self.containing_before(key)?;
        let cached = value.as_ref()?;
        let end = self.end_of(begin);
        Some((
            KeyRange {
                begin: begin.clone(),
                end,
            },
            cached.location.clone(),
        ))
    }

    /// `invalidateCache(key, Reverse::True)`: forgets the location of the range containing
    /// the keys just before `key`.
    pub(crate) fn invalidate_key_before(&mut self, key: &[u8]) {
        let Some((begin, Some(_))) = self.containing_before(key) else {
            return;
        };
        let begin = begin.clone();
        let end = self.end_of(&begin);
        self.set(&begin, &end, None);
    }

    fn containing_before(&self, key: &[u8]) -> Option<(&Vec<u8>, &Option<Cached>)> {
        if key.is_empty() || key > KEY_SPACE_END {
            return None;
        }
        self.map
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(key)))
            .next_back()
    }

    fn containing(&self, key: &[u8]) -> Option<(&Vec<u8>, &Option<Cached>)> {
        if key >= KEY_SPACE_END {
            return None;
        }
        self.map
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
    }

    fn end_of(&self, begin: &[u8]) -> Vec<u8> {
        self.map
            .range::<[u8], _>((Bound::Excluded(begin), Bound::Unbounded))
            .next()
            .map_or_else(|| KEY_SPACE_END.to_vec(), |(k, _)| k.clone())
    }

    /// The interned [`ServerInfo`] for `server` (`StorageServerInfo::getInterface`): the
    /// cache's entry for its id if it has the same `getValue` token; the entry updated in
    /// place if only the endpoints changed (same locality); otherwise a new entry. `make`
    /// gives the primary address and a peer reference for an interface.
    pub(crate) fn server_info(
        &mut self,
        server: &StorageServer,
        client_dc_id: Option<&[u8]>,
        make: impl FnOnce(&Endpoint) -> (NetworkAddress, PeerRef),
    ) -> Arc<ServerInfo> {
        if let Some(info) = self.servers.get(&server.id).and_then(Weak::upgrade) {
            let mut interface = info.interface();
            if interface.server.base.token == server.base.token {
                drop(interface);
                return info;
            }
            if interface.server.locality == server.locality {
                let (address, peer) = make(&server.base);
                *interface = Interface {
                    server: server.clone(),
                    address,
                    _peer: peer,
                };
                drop(interface);
                return info;
            }
        }
        let (address, peer) = make(&server.base);
        let info = Arc::new(ServerInfo {
            distance: distance(client_dc_id, server),
            interface: Mutex::new(Interface {
                server: server.clone(),
                address,
                _peer: peer,
            }),
        });
        // Dead entries (servers no cached range uses any more) are dropped once they could
        // outnumber the live ones.
        if self.servers.len() >= 2 * self.servers_after_prune + 64 {
            self.servers.retain(|_, w| w.strong_count() > 0);
            self.servers_after_prune = self.servers.len();
        }
        self.servers.insert(server.id, Arc::downgrade(&info));
        info
    }

    /// The targets of the servers that cached ranges (or reads in progress) still use, on
    /// every stream the read path sends to and records endpoint failures of (`getValue`,
    /// `getKey`, `getKeyValues`, `watchValue`, `waitMetrics`, `getRangeSplitPoints`).
    pub(crate) fn live_targets(&self) -> HashSet<(NetworkAddress, Uid)> {
        let streams = &fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
        let indices = [
            streams.get_value,
            streams.get_key,
            streams.get_key_values,
            streams.watch_value,
            streams.wait_metrics,
            streams.get_range_split_points,
        ];
        self.servers
            .values()
            .filter_map(Weak::upgrade)
            .flat_map(|s| indices.map(|i| s.target_for(i)))
            .collect()
    }

    /// How many server entries the cache has, dead or alive.
    pub(crate) fn server_entries(&self) -> usize {
        self.servers.len()
    }

    /// `setCachedLocation`: caches `location` for `range`, overwriting what overlaps it,
    /// after evicting random ranges while the cache is full.
    pub(crate) fn insert(&mut self, range: &KeyRange, location: Arc<Location>, rng: &mut Rng) {
        let end = clamp_end(&range.end);
        if range.begin.as_slice() >= end {
            return;
        }
        let mut attempts = 0;
        while self.cached >= self.capacity && attempts < 100 && !self.pool.is_empty() {
            attempts += 1;
            let (begin, generation) = self.pool.swap_remove(rng.below(self.pool.len()));
            let live = matches!(self.map.get(&begin), Some(Some(c)) if c.generation == generation);
            if live {
                let end = self.end_of(&begin);
                self.set(&begin, &end, None);
            }
        }
        self.set(&range.begin, end, Some(location));
        if self.pool.len() > 2 * self.cached + 64 {
            let map = &self.map;
            self.pool
                .retain(|(k, g)| matches!(map.get(k), Some(Some(c)) if c.generation == *g));
        }
    }

    /// `invalidateCache(key)`: forgets the location of the range containing `key`.
    pub(crate) fn invalidate_key(&mut self, key: &[u8]) {
        let Some((begin, Some(_))) = self.containing(key) else {
            return;
        };
        let begin = begin.clone();
        let end = self.end_of(&begin);
        self.set(&begin, &end, None);
    }

    /// `invalidateCache(range)`: forgets the location of every range intersecting `range`.
    pub(crate) fn invalidate_range(&mut self, range: &KeyRange) {
        let end = clamp_end(&range.end);
        if range.begin.as_slice() >= end {
            return;
        }
        let begin = match self.containing(&range.begin) {
            Some((begin, _)) => begin.clone(),
            None => return,
        };
        let last = self
            .map
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(end)))
            .next_back()
            .map(|(k, _)| k.clone())
            .unwrap_or_default();
        let end = self.end_of(&last);
        self.set(&begin, &end, None);
    }

    /// Sets `[begin, end)` to `value` (`RangeMap::insert`), then merges unknown neighbours.
    fn set(&mut self, begin: &[u8], end: &[u8], location: Option<Arc<Location>>) {
        if end < KEY_SPACE_END && !self.map.contains_key(end) {
            // The range after `end` keeps the value it had.
            let after = self.containing(end).and_then(|(_, v)| v.clone());
            let after = after.map(|c| self.entry(end, c.location));
            self.map.insert(end.to_vec(), after);
        }
        let inside: Vec<Vec<u8>> = self
            .map
            .range::<[u8], _>((Bound::Included(begin), Bound::Excluded(end)))
            .map(|(k, _)| k.clone())
            .collect();
        for key in inside {
            if let Some(Some(_)) = self.map.remove(&key) {
                self.cached -= 1;
            }
        }
        let value = location.map(|l| self.entry(begin, l));
        let unknown = value.is_none();
        self.map.insert(begin.to_vec(), value);
        if unknown {
            self.merge_unknown(begin);
            if end < KEY_SPACE_END {
                self.merge_unknown(end);
            }
        }
    }

    /// A new entry at `begin`: counted, and put in the eviction pool.
    fn entry(&mut self, begin: &[u8], location: Arc<Location>) -> Cached {
        self.next_generation += 1;
        self.cached += 1;
        self.pool.push((begin.to_vec(), self.next_generation));
        Cached {
            location,
            generation: self.next_generation,
        }
    }

    /// Removes the boundary at `key` if both ranges it separates are unknown.
    fn merge_unknown(&mut self, key: &[u8]) {
        if key.is_empty() || !matches!(self.map.get(key), Some(None)) {
            return;
        }
        let before = self
            .map
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(key)))
            .next_back();
        if matches!(before, Some((_, None))) {
            self.map.remove(key);
        }
    }
}

fn clamp_end(end: &[u8]) -> &[u8] {
    if end > KEY_SPACE_END {
        KEY_SPACE_END
    } else {
        end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(tag: u64) -> Arc<Location> {
        // Locations with no servers are enough for the map's logic; `best_count` tags them.
        Arc::new(Location {
            servers: Vec::new(),
            best_count: tag as usize,
        })
    }

    fn range(a: &[u8], b: &[u8]) -> KeyRange {
        KeyRange {
            begin: a.to_vec(),
            end: b.to_vec(),
        }
    }

    fn tag(cache: &LocationCache, key: &[u8]) -> Option<(KeyRange, usize)> {
        cache.get(key).map(|(r, l)| (r, l.best_count))
    }

    #[test]
    fn insert_get_and_split() {
        let mut rng = Rng::new(Some(1));
        let mut cache = LocationCache::new(100);
        assert!(cache.get(b"a").is_none());
        cache.insert(&range(b"b", b"m"), location(1), &mut rng);
        assert_eq!(tag(&cache, b"b"), Some((range(b"b", b"m"), 1)));
        assert_eq!(tag(&cache, b"l\xff"), Some((range(b"b", b"m"), 1)));
        assert_eq!(tag(&cache, b"a"), None);
        assert_eq!(tag(&cache, b"m"), None);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.ranges(), 3);

        // A new shard inside the old one splits it; both outer pieces keep the old location.
        cache.insert(&range(b"d", b"f"), location(2), &mut rng);
        assert_eq!(tag(&cache, b"c"), Some((range(b"b", b"d"), 1)));
        assert_eq!(tag(&cache, b"e"), Some((range(b"d", b"f"), 2)));
        assert_eq!(tag(&cache, b"g"), Some((range(b"f", b"m"), 1)));
        assert_eq!(cache.len(), 3);

        // Up to the end of the key space.
        cache.insert(&range(b"x", KEY_SPACE_END), location(3), &mut rng);
        assert_eq!(
            tag(&cache, b"\xff\x01"),
            Some((range(b"x", KEY_SPACE_END), 3))
        );
        assert_eq!(tag(&cache, b"\xff\xff"), None);
    }

    #[test]
    fn lookups_and_invalidation_before_a_key() {
        let mut rng = Rng::new(Some(1));
        let mut cache = LocationCache::new(100);
        cache.insert(&range(b"a", b"c"), location(1), &mut rng);
        cache.insert(&range(b"c", KEY_SPACE_END), location(2), &mut rng);
        let before = |cache: &LocationCache, key: &[u8]| {
            cache.get_before(key).map(|(r, l)| (r, l.best_count))
        };
        // The range whose keys come just before `key`: at a boundary, the earlier one.
        assert_eq!(before(&cache, b"c"), Some((range(b"a", b"c"), 1)));
        assert_eq!(
            before(&cache, b"c\x00"),
            Some((range(b"c", KEY_SPACE_END), 2))
        );
        assert_eq!(
            before(&cache, KEY_SPACE_END),
            Some((range(b"c", KEY_SPACE_END), 2))
        );
        assert_eq!(before(&cache, b"a"), None, "[\"\", a) is unknown");
        assert_eq!(before(&cache, b""), None);
        assert_eq!(before(&cache, b"\xff\xff\x00"), None);
        cache.invalidate_key_before(b"c");
        assert_eq!(before(&cache, b"c"), None);
        assert_eq!(tag(&cache, b"c"), Some((range(b"c", KEY_SPACE_END), 2)));
        cache.invalidate_key_before(KEY_SPACE_END);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn invalidation_merges_unknown_ranges() {
        let mut rng = Rng::new(Some(1));
        let mut cache = LocationCache::new(100);
        cache.insert(&range(b"a", b"c"), location(1), &mut rng);
        cache.insert(&range(b"c", b"e"), location(2), &mut rng);
        cache.insert(&range(b"e", b"g"), location(3), &mut rng);
        cache.invalidate_key(b"d");
        assert_eq!(tag(&cache, b"d"), None);
        assert_eq!(tag(&cache, b"b"), Some((range(b"a", b"c"), 1)));
        assert_eq!(cache.len(), 2);
        cache.invalidate_range(&range(b"b", b"f"));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.ranges(), 1, "everything unknown is one range");
        cache.invalidate_key(b"zz");
        assert_eq!(cache.ranges(), 1);
    }

    fn storage_server(id: u64, token: u64, dc: &[u8]) -> StorageServer {
        use fdb_native_wire::{IpAddress, NetworkAddressList};
        let mut locality = std::collections::BTreeMap::new();
        locality.insert(b"dcid".to_vec(), Some(dc.to_vec()));
        StorageServer {
            id: Uid::new(0x55, id),
            locality,
            base: Endpoint::new(
                NetworkAddressList {
                    address: NetworkAddress::new(
                        IpAddress::V4(0x0a00_0000 + id as u32),
                        4500,
                        true,
                        false,
                    ),
                    secondary_address: None,
                },
                Uid::new(token, 1),
            ),
            tss_pair_id: None,
            accepting_requests: true,
        }
    }

    #[tokio::test]
    async fn server_entries_are_interned_updated_and_pruned() {
        use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
        use fdb_native_transport::runtime::TokioRuntime;

        let peers = PeerManager::new(
            TokioRuntime::new(),
            PeerManagerConfig::default(),
            DiscardEvents,
        );
        let make = |e: &Endpoint| {
            (
                e.addresses.address,
                peers.peer(&e.addresses.address).reference(),
            )
        };
        let mut rng = Rng::new(Some(5));
        let mut cache = LocationCache::new(10);

        // The same interface: the same entry.
        let a = cache.server_info(&storage_server(1, 100, b"dc1"), Some(b"dc1"), make);
        let again = cache.server_info(&storage_server(1, 100, b"dc1"), Some(b"dc1"), make);
        assert!(Arc::ptr_eq(&a, &again));
        assert_eq!(a.distance, Distance::SameDc);
        // New endpoints, same locality: updated in place.
        let moved = cache.server_info(&storage_server(1, 200, b"dc1"), Some(b"dc1"), make);
        assert!(Arc::ptr_eq(&a, &moved));
        assert_eq!(a.target().1, Uid::new(200, 1));
        // Another locality: a new entry.
        let other = cache.server_info(&storage_server(1, 300, b"dc2"), Some(b"dc1"), make);
        assert!(!Arc::ptr_eq(&a, &other));
        assert_eq!(other.distance, Distance::Distant);
        drop((a, again, moved, other));

        // Many servers come and go through a small cache: dead entries are pruned.
        for i in 0..1000u64 {
            let info = cache.server_info(&storage_server(10 + i, 1000 + i, b"dc1"), None, make);
            let location = Arc::new(Location::new(vec![info], &mut rng));
            let begin = i.to_be_bytes();
            let end = (i + 1).to_be_bytes();
            cache.insert(&range(&begin, &end), location, &mut rng);
            // Six streams per live server.
            let live = cache.live_targets().len();
            assert!(live <= 6 * 10, "{live}");
            assert!(
                cache.server_entries() <= 2 * 10 + 64 + 1,
                "{}",
                cache.server_entries()
            );
        }
        peers.shutdown().await;
    }

    #[test]
    fn bounded_by_capacity() {
        let mut rng = Rng::new(Some(9));
        let mut cache = LocationCache::new(10);
        for i in 0..200u32 {
            let b = i.to_be_bytes();
            let e = (i + 1).to_be_bytes();
            cache.insert(&range(&b, &e), location(i as u64), &mut rng);
            assert!(cache.len() <= 10, "{}", cache.len());
            // The range just inserted is never the one evicted.
            assert!(cache.get(&b).is_some());
        }
        assert_eq!(cache.len(), 10);
        assert!(cache.ranges() <= 2 * 10 + 1);
        assert!(cache.pool.len() <= 2 * cache.len() + 64);
    }
}
