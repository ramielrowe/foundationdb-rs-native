//! Unit tests of the parts that need no network: proxy sampling, the connection record, the
//! request defaults. The monitor itself is tested against fake coordinators in
//! `tests/cluster_monitor.rs`.

use std::collections::HashSet;

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{ClientDbInfo, CommitProxy, GrvProxy};
use fdb_native_wire::{Endpoint, NetworkAddress, NetworkAddressList, Uid};

use super::info::ProxySampler;
use super::monitor::network_address;
use super::rng::Rng;
use super::*;
use crate::cluster_file::{ClusterConnectionString, Coordinator};

fn endpoint(token: Uid) -> Endpoint {
    Endpoint::new(
        NetworkAddressList {
            address: NetworkAddress::new(
                std::net::IpAddr::from([10, 0, 0, 1]).into(),
                4500,
                true,
                false,
            ),
            secondary_address: None,
        },
        token,
    )
}

fn proxies(generation: u64, grv: usize, commit: usize) -> ClientDbInfo {
    ClientDbInfo {
        id: Uid::new(generation, 0),
        grv_proxies: (0..grv as u64)
            .map(|i| GrvProxy {
                process_id: None,
                provisional: false,
                base: endpoint(Uid::new(generation, i)),
            })
            .collect(),
        commit_proxies: (0..commit as u64)
            .map(|i| CommitProxy {
                process_id: None,
                provisional: false,
                base: endpoint(Uid::new(generation, 100 + i)),
                schema: SchemaVersion::V8_0,
            })
            .collect(),
        forward: None,
        history: Vec::new(),
        cluster_id: Uid::default(),
        cluster_type: 0,
        tenant_mode: None,
    }
}

fn ids<T>(v: &[T], id: impl Fn(&T) -> Uid) -> Vec<Uid> {
    v.iter().map(id).collect()
}

#[test]
fn sampling_keeps_short_lists_whole() {
    let mut sampler = ProxySampler::default();
    let mut rng = Rng::new(Some(1));
    let info = proxies(1, 3, 5);
    let (grv, commit) = sampler.sample(&info, 3, 5, &mut rng);
    assert_eq!(grv, info.grv_proxies);
    assert_eq!(commit, info.commit_proxies);
}

#[test]
fn sampling_shrinks_long_lists_and_keeps_the_sample() {
    let mut sampler = ProxySampler::default();
    let mut rng = Rng::new(Some(1));
    let info = proxies(1, 7, 9);
    let (grv, commit) = sampler.sample(&info, 3, 5, &mut rng);
    assert_eq!(grv.len(), 3);
    assert_eq!(commit.len(), 5);
    // A sample of distinct proxies from the list.
    let all: HashSet<Uid> = info.commit_proxies.iter().map(CommitProxy::id).collect();
    let picked: HashSet<Uid> = commit.iter().map(CommitProxy::id).collect();
    assert_eq!(picked.len(), 5);
    assert!(picked.is_subset(&all));

    // A new info with the same proxies (another id): the same sample, no reshuffle.
    let mut again = info.clone();
    again.id = Uid::new(99, 99);
    let (grv2, commit2) = sampler.sample(&again, 3, 5, &mut rng);
    assert_eq!(ids(&grv2, GrvProxy::id), ids(&grv, GrvProxy::id));
    assert_eq!(
        ids(&commit2, CommitProxy::id),
        ids(&commit, CommitProxy::id)
    );

    // Other proxies: a new sample from them.
    let other = proxies(2, 7, 9);
    let (grv3, commit3) = sampler.sample(&other, 3, 5, &mut rng);
    assert!(grv3.iter().all(|p| p.id().first == 2));
    assert!(commit3.iter().all(|p| p.id().first == 2));

    assert!(!sampler.is_empty());

    // Back below the limit: everything, and the cache is cleared
    // (SHRINK_PROXY_LIST_CLEAR_CACHE_BELOW_THRESHOLD), for both kinds.
    let small = proxies(3, 2, 2);
    let (grv4, commit4) = sampler.sample(&small, 3, 5, &mut rng);
    assert_eq!(grv4, small.grv_proxies);
    assert_eq!(commit4, small.commit_proxies);
    assert!(sampler.is_empty());

    // Going above it again with the first set samples anew from it.
    let (grv5, commit5) = sampler.sample(&info, 3, 5, &mut rng);
    assert_eq!((grv5.len(), commit5.len()), (3, 5));
    assert!(!sampler.is_empty());

    // Only one kind below the limit: only its cache is cleared, the other's sample stays.
    let mixed = ClientDbInfo {
        grv_proxies: info.grv_proxies[..2].to_vec(),
        ..info.clone()
    };
    let (grv6, commit6) = sampler.sample(&mixed, 3, 5, &mut rng);
    assert_eq!(grv6.len(), 2);
    assert_eq!(
        ids(&commit6, CommitProxy::id),
        ids(&commit5, CommitProxy::id)
    );
}

#[test]
fn shuffle_is_a_permutation_and_seeded() {
    let mut a: Vec<u32> = (0..20).collect();
    let mut b = a.clone();
    Rng::new(Some(5)).shuffle(&mut a);
    Rng::new(Some(5)).shuffle(&mut b);
    assert_eq!(a, b);
    let mut sorted = a.clone();
    sorted.sort();
    assert_eq!(sorted, (0..20).collect::<Vec<_>>());
}

#[test]
fn default_supported_versions_are_one_per_schema_set() {
    let versions = ClusterMonitorConfig::default_supported_versions();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].protocol_version, b"fdb00b074000000");
    assert_eq!(versions[1].protocol_version, b"fdb00b080000000");
    assert!(
        versions[0]
            .client_version
            .starts_with(b"7.4.0 (fdb-native-client ")
    );
    assert!(
        versions[1]
            .client_version
            .starts_with(b"8.0.0 (fdb-native-client ")
    );
    // No commas: C++ splits "client,source,protocol" strings on them.
    for v in &versions {
        for field in [&v.client_version, &v.source_version, &v.protocol_version] {
            assert!(!field.contains(&b','));
        }
    }
    let config = ClusterMonitorConfig::default();
    assert_eq!(config.trace_log_group, b"default");
    assert!(!config.internal);
    assert_eq!(config.max_grv_proxy_connections, 3);
    assert_eq!(config.max_commit_proxy_connections, 5);
    assert_eq!(
        config.coordinator_reconnection_delay,
        std::time::Duration::from_secs(1)
    );
}

#[test]
fn network_address_keeps_tls_and_from_hostname() {
    let host = Coordinator::parse("coord.example:4500:tls").unwrap();
    let resolved = host.with_resolved("10.1.2.3:4500".parse().unwrap());
    let address = network_address(resolved);
    assert!(address.is_tls());
    assert!(address.is_public());
    assert!(address.from_hostname);
    assert_eq!(address.socket_addr(), "10.1.2.3:4500".parse().unwrap());

    let plain = Coordinator::parse("10.1.2.3:4500").unwrap();
    let address = network_address(plain.resolved().unwrap());
    assert!(!address.is_tls());
    assert!(!address.from_hostname);
}

#[test]
fn record_checks_and_persists_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    let a: ClusterConnectionString = "a:b@10.0.0.1:4500".parse().unwrap();
    let b: ClusterConnectionString = "a:b@10.0.0.2:4500".parse().unwrap();

    // Memory records are never checked.
    assert_eq!(ConnectionRecord::memory(a.clone()).check_file(), None);

    // A record to persist is not checked until written, then written on first contact.
    let mut record = ConnectionRecord::file(&path, a.clone());
    assert_eq!(record.check_file(), None);
    record.notify_connected().unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{CLUSTER_FILE_HEADER}{a}\n")
    );
    assert_eq!(record.check_file(), None);
    record.notify_connected().unwrap();

    // Comments and whitespace do not matter, the coordinators do.
    std::fs::write(&path, "# hello\n a:b@10.0.0.1:4500 # there\n").unwrap();
    let mut record = ConnectionRecord::open(&path).unwrap();
    assert_eq!(record.check_file(), None);
    std::fs::write(&path, b.to_string()).unwrap();
    assert_eq!(record.check_file(), Some(Some(b.clone())));
    std::fs::write(&path, "garbage").unwrap();
    assert_eq!(record.check_file(), Some(None));
    std::fs::remove_file(&path).unwrap();
    assert_eq!(record.check_file(), Some(None));

    record.set_and_persist(b.clone()).unwrap();
    assert_eq!(record.connection_string(), &b);
    assert_eq!(record.check_file(), None);
    // No temporary file is left behind.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

    // A file that cannot be written: the string is still taken.
    let mut record = ConnectionRecord::file(dir.path().join("missing/dir/fdb.cluster"), a.clone());
    assert!(record.set_and_persist(b.clone()).is_err());
    assert_eq!(record.connection_string(), &b);
}

#[test]
fn record_from_a_loaded_cluster_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    std::fs::write(&path, "a:b@10.0.0.1:4500\n").unwrap();
    let file = crate::ClusterFile::open_or_default(Some(&path)).unwrap();
    let record = ConnectionRecord::from(file);
    assert_eq!(record.path(), Some(path.as_path()));
    assert_eq!(record.check_file(), None);
    assert_eq!(record, ConnectionRecord::open(&path).unwrap());
}

/// A persist keeps the file's permission bits (and owner and group: chowning to the same
/// owner always succeeds for that owner), as C++'s `atomicReplace` does.
#[cfg(unix)]
#[test]
fn persist_keeps_mode_and_owner() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    std::fs::write(&path, "a:b@10.0.0.1:4500\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let before = std::fs::metadata(&path).unwrap();

    let mut record = ConnectionRecord::open(&path).unwrap();
    let b: ClusterConnectionString = "a:b@10.0.0.2:4500".parse().unwrap();
    record.set_and_persist(b.clone()).unwrap();

    let after = std::fs::metadata(&path).unwrap();
    assert_ne!(
        after.ino(),
        before.ino(),
        "replaced, not rewritten in place"
    );
    assert_eq!(after.permissions().mode() & 0o7777, 0o640);
    assert_eq!((after.uid(), after.gid()), (before.uid(), before.gid()));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{CLUSTER_FILE_HEADER}{b}\n")
    );

    // A read-only file is still replaced, and stays read-only.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    record
        .set_and_persist("a:b@10.0.0.3:4500".parse().unwrap())
        .unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o444);
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "no temporary file left"
    );
}

/// A new file (nothing to copy) gets the process's defaults.
#[test]
fn persist_creates_a_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    super::record::atomic_replace(&path, b"x").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"x");
}

/// The first commit proxy of the full list, even when the sample leaves it out.
#[test]
fn first_commit_proxy_is_from_the_full_list() {
    let info = proxies(1, 1, 9);
    let mut sampler = ProxySampler::default();
    let mut rng = Rng::new(Some(3));
    let (grv, commit) = sampler.sample(&info, 3, 1, &mut rng);
    let cluster = ClusterInfo {
        client_info: info.clone(),
        schema: SchemaVersion::V8_0,
        protocol_version: SchemaVersion::V8_0.protocol_version(),
        coordinator: Coordinator::parse("10.0.0.1:4500")
            .unwrap()
            .resolved()
            .unwrap(),
        connection_string: "a:b@10.0.0.1:4500".parse().unwrap(),
        grv_proxies: grv.into_iter().map(GrvProxyEndpoints::new).collect(),
        commit_proxies: commit.into_iter().map(CommitProxyEndpoints::new).collect(),
        sequence: 1,
    };
    let first = cluster.first_commit_proxy().unwrap();
    assert_eq!(first.proxy, info.commit_proxies[0]);
    assert_eq!(
        first.streams.expire_idempotency_id,
        info.commit_proxies[0].base.adjusted(9),
        "8.0's index"
    );
    assert_eq!(cluster.commit_proxy_ids().len(), 1);
    assert_eq!(cluster.grv_proxy_ids(), vec![info.grv_proxies[0].id()]);
}
