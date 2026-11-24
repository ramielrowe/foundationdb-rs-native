//! Databases opened on one runtime share their connections (b35.7), against a fake
//! coordinator and fake GRV and commit proxies on an in-memory network under tokio's paused
//! clock: one connection per address for two databases (each with its own long poll), a
//! shutdown of one that leaves the other working, the connections closed with the last one,
//! the opt-out, and TLS material that differs keeping managers apart.

#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::CommitId;
use fdb_native_transport::peer::{PeerManagerConfig, PeerPhase};
use fdb_native_wire::{NetworkAddress, ProtocolVersion, Uid};
use support::proxy::FakeProxy;
use support::{Call, FakeCoordinator, MemRuntime, Net, info, sock, until};

/// 7.4, the version a new connection announces first: every connection is dialed once (an
/// 8.0 server makes the first one reconnect, announcing 8.0).
const SCHEMA: SchemaVersion = SchemaVersion::V7_4;

fn options() -> DatabaseOptions {
    let mut options = DatabaseOptions::default();
    options.peers = PeerManagerConfig::default().without_jitter();
    options.peers.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
    options.peers.seed = Some(1);
    options
}

fn record() -> ConnectionRecord {
    ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap())
}

fn address(last: u8) -> NetworkAddress {
    NetworkAddress::new(sock(last, 4500).ip().into(), 4500, true, false)
}

struct Cluster {
    net: std::sync::Arc<Net>,
    coord: FakeCoordinator,
    /// The long polls the coordinator holds.
    held: Vec<Call>,
    grv: FakeProxy,
    commit: FakeProxy,
}

impl Cluster {
    fn start() -> Cluster {
        let net = Net::new();
        let version = SCHEMA.protocol_version();
        Cluster {
            coord: FakeCoordinator::start(&net, sock(1, 4500), version),
            grv: FakeProxy::start(&net, sock(100, 4500), version),
            commit: FakeProxy::start(&net, sock(150, 4500), version),
            held: Vec::new(),
            net,
        }
    }

    /// A database on `rt` with `options`, once it knows the proxies.
    async fn open(&mut self, rt: &MemRuntime, options: DatabaseOptions) -> Database<MemRuntime> {
        let db = Database::from_record(rt.clone(), record(), options).expect("database");
        // The new database's first request knows no info; the others are the long polls of
        // the databases already open, which are kept (held, unanswered).
        loop {
            let call = self.coord.next_call().await;
            if call.request.known_client_info_id == Uid::default() {
                call.reply(&info(1, 1, 1, SCHEMA));
                break;
            }
            self.held.push(call);
        }
        db.cluster_monitor().proxies(false).await.unwrap();
        db
    }

    /// Waits for `n` long polls to be held.
    async fn hold(&mut self, n: usize) {
        while self.held.len() < n {
            let call = self.coord.next_call().await;
            assert_ne!(call.request.known_client_info_id, Uid::default());
            self.held.push(call);
        }
    }

    /// One blind write committed through `db`.
    async fn commit_one(&mut self, db: &Database<MemRuntime>, version: i64) {
        let tx = db.create_transaction();
        tx.set(b"k", b"v");
        let handle = tokio::spawn(async move { tx.commit().await });
        self.grv.next().await.reply_grv(version);
        self.commit.next().await.reply_commit(Ok(CommitId {
            version: version + 1,
            ..CommitId::default()
        }));
        let committed = tokio::time::timeout(Duration::from_secs(60), handle)
            .await
            .expect("in time")
            .expect("no panic")
            .expect("committed");
        assert_eq!(committed.committed_version(), version + 1);
    }

    fn connects(&self, last: u8) -> usize {
        self.net.connects(sock(last, 4500))
    }
}

/// Two databases on one runtime: one connection to the coordinator and to each proxy, while
/// each database keeps its own long poll (the coordinator gets a request from each).
#[tokio::test(start_paused = true)]
async fn two_databases_share_one_connection_per_address() {
    let mut cluster = Cluster::start();
    let rt = MemRuntime::new(&cluster.net);
    let a = cluster.open(&rt, options()).await;
    let b = cluster.open(&rt, options()).await;
    assert!(a.peers().shares_connections_with(b.peers()));
    assert_eq!(a.peers().users(), 2);
    cluster.commit_one(&a, 100).await;
    cluster.commit_one(&b, 200).await;
    cluster.commit_one(&a, 300).await;
    assert_eq!(cluster.connects(1), 1, "one connection to the coordinator");
    assert_eq!(cluster.connects(100), 1, "one to the GRV proxy");
    assert_eq!(cluster.connects(150), 1, "one to the commit proxy");
    // Both long polls are held: a new info reaches both databases.
    cluster.hold(2).await;
    for call in cluster.held.drain(..) {
        assert_eq!(call.request.known_client_info_id, info(1, 1, 1, SCHEMA).id);
        call.reply(&info(2, 1, 1, SCHEMA));
    }
    let id = |db: &Database<MemRuntime>| db.cluster_monitor().current().map(|i| i.client_info.id);
    until("both databases have the new info", || {
        id(&a) == Some(info(2, 1, 1, SCHEMA).id) && id(&b) == Some(info(2, 1, 1, SCHEMA).id)
    })
    .await;
    assert_eq!(cluster.connects(1), 1);
    a.shutdown().await;
    b.shutdown().await;
}

/// Shutting one database down ends its requests in progress (a read version it waits for
/// fails with `operation_cancelled`) and leaves the other one working on the same
/// connections; dropping both then closes them.
#[tokio::test(start_paused = true)]
async fn shutting_one_database_down_leaves_the_other_working() {
    let mut cluster = Cluster::start();
    let rt = MemRuntime::new(&cluster.net);
    let a = cluster.open(&rt, options()).await;
    let b = cluster.open(&rt, options()).await;
    cluster.commit_one(&a, 100).await;
    // A read version of `a` in flight at the GRV proxy when `a` is shut down.
    let pending = tokio::spawn({
        let tx = a.create_transaction();
        async move { tx.get_read_version().await }
    });
    let held = cluster.grv.next().await;
    a.shutdown().await;
    assert!(a.is_shut_down());
    assert!(!b.is_shut_down());
    assert_eq!(
        pending.await.unwrap().unwrap_err().code(),
        codes::OPERATION_CANCELLED
    );
    drop(held);
    assert_eq!(
        a.create_transaction()
            .get_read_version()
            .await
            .unwrap_err()
            .code(),
        codes::OPERATION_CANCELLED
    );
    assert_eq!(b.peers().users(), 1);
    cluster.commit_one(&b, 200).await;
    assert_eq!(cluster.connects(1), 1);
    assert_eq!(cluster.connects(100), 1);
    assert_eq!(cluster.connects(150), 1);
    let coordinator = b.peers().peer(&address(1));
    assert_eq!(coordinator.status().phase, PeerPhase::Connected);

    // Dropping both closes the connections.
    let weak = b.peers().downgrade();
    drop(a);
    drop(b);
    until("the connections are closed", || {
        weak.is_shut_down() && coordinator.status().phase == PeerPhase::Shutdown
    })
    .await;
    // A database opened now makes new ones.
    let c = cluster.open(&rt, options()).await;
    assert_eq!(c.peers().users(), 1);
    assert_eq!(cluster.connects(1), 2);
    c.shutdown().await;
}

/// Databases share only when they can: not with `share_connections` off, not with other
/// connection knobs, not on another runtime (another in-memory network here).
#[tokio::test(start_paused = true)]
async fn databases_that_cannot_share_have_their_own_connections() {
    let mut cluster = Cluster::start();
    let rt = MemRuntime::new(&cluster.net);
    let a = cluster.open(&rt, options()).await;
    let mut own = options();
    own.share_connections = false;
    let b = cluster.open(&rt, own).await;
    assert!(!a.peers().shares_connections_with(b.peers()));
    let mut knobs = options();
    knobs.peers.connect_timeout = Duration::from_secs(3);
    let c = cluster.open(&rt, knobs).await;
    assert!(!a.peers().shares_connections_with(c.peers()));
    assert!(!b.peers().shares_connections_with(c.peers()));
    assert_eq!(cluster.connects(1), 3);
    // The same knobs again share with `a`.
    let d = cluster.open(&rt, options()).await;
    assert!(a.peers().shares_connections_with(d.peers()));
    assert_eq!(cluster.connects(1), 3);

    let other_net = Net::new();
    let elsewhere = MemRuntime::new(&other_net);
    let e = Database::from_record(elsewhere, record(), options()).expect("database");
    assert!(!a.peers().shares_connections_with(e.peers()));
    for db in [a, b, c, d, e] {
        db.shutdown().await;
    }
}

/// Different TLS material never shares (nor TLS with plain); the same material does.
#[cfg(feature = "tls")]
#[tokio::test(start_paused = true)]
async fn different_tls_configurations_have_their_own_connections() {
    use fdb_native_client::TlsConfig;
    use fdb_native_testkit::tls::TestPki;

    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let tls = |pki: &TestPki| {
        let mut options = options();
        options.tls = Some(
            TlsConfig::new()
                .ca_bytes(pki.ca_pem())
                .cert_bytes(pki.client().cert_pem())
                .key_bytes(pki.client().key_pem()),
        );
        options
    };
    let one = TestPki::generate().unwrap();
    let two = TestPki::generate().unwrap();
    let open = |options| Database::from_record(rt.clone(), record(), options).unwrap();
    let plain = open(options());
    let a = open(tls(&one));
    let a2 = open(tls(&one));
    let b = open(tls(&two));
    assert!(a.peers().shares_connections_with(a2.peers()));
    assert!(!a.peers().shares_connections_with(b.peers()));
    assert!(!a.peers().shares_connections_with(plain.peers()));
    assert!(!b.peers().shares_connections_with(plain.peers()));
    for db in [plain, a, a2, b] {
        db.shutdown().await;
    }
}

/// Each part of the TLS material decides alone: a configuration that differs from another in
/// one field only (certificate, key, CA, password, verify-peers rules, plaintext refusal) has
/// connections of its own. The PEM variants are the same certificate and key with a comment
/// line before them (PEM parsers skip it), so only the bytes differ.
#[cfg(feature = "tls")]
#[tokio::test(start_paused = true)]
async fn each_tls_setting_alone_keeps_managers_apart() {
    use fdb_native_client::TlsConfig;
    use fdb_native_testkit::tls::TestPki;

    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let one = TestPki::generate().unwrap();
    let two = TestPki::generate().unwrap();
    let base = || {
        TlsConfig::new()
            .ca_bytes(one.ca_pem())
            .cert_bytes(one.client().cert_pem())
            .key_bytes(one.client().key_pem())
    };
    let with = |config: TlsConfig| {
        let mut options = options();
        options.tls = Some(config);
        options
    };
    let open = |options| Database::from_record(rt.clone(), record(), options).unwrap();
    let reference = open(with(base()));
    let again = open(with(base()));
    assert!(reference.peers().shares_connections_with(again.peers()));
    let variants: Vec<(&str, TlsConfig)> = vec![
        (
            "certificate",
            base().cert_bytes(format!("# other bytes\n{}", one.client().cert_pem())),
        ),
        (
            "key",
            base().key_bytes(format!("# other bytes\n{}", one.client().key_pem())),
        ),
        ("CA", base().ca_bytes(two.ca_pem())),
        ("password", base().password("unused")),
        ("verify peers", base().verify_peers("Check.Valid=0")),
        ("plaintext", base().disable_plaintext_connection(true)),
    ];
    let mut dbs = vec![reference, again];
    for (what, config) in variants {
        let db = open(with(config.clone()));
        for other in &dbs {
            assert!(
                !db.peers().shares_connections_with(other.peers()),
                "a different {what} shared"
            );
        }
        // The same variant again shares with it.
        let twin = open(with(config));
        assert!(db.peers().shares_connections_with(twin.peers()), "{what}");
        dbs.push(db);
        dbs.push(twin);
    }
    for db in dbs {
        db.shutdown().await;
    }
}

/// `A` is shut down while `B`'s commit and its own are both at the commit proxy, on the
/// same connection: `B`'s completes, `A`'s fails as maybe committed (it was sent:
/// `commit_unknown_result`, never a not-sent error), and the late reply to `A`'s is ignored
/// while `B` goes on.
#[tokio::test(start_paused = true)]
async fn shutting_one_database_down_while_both_commit() {
    let mut cluster = Cluster::start();
    let rt = MemRuntime::new(&cluster.net);
    let a = cluster.open(&rt, options()).await;
    let b = cluster.open(&rt, options()).await;
    cluster.commit_one(&a, 100).await;
    cluster.commit_one(&b, 150).await;
    // B's commit reaches the proxy and is held.
    let tb = b.create_transaction();
    tb.set(b"kb", b"vb");
    let hb = tokio::spawn(async move { tb.commit().await });
    cluster.grv.next().await.reply_grv(200);
    let held_b = cluster.commit.next().await;
    // A's too.
    let ta = a.create_transaction();
    ta.set(b"ka", b"va");
    let ha = tokio::spawn(async move { ta.commit().await });
    cluster.grv.next().await.reply_grv(300);
    let held_a = cluster.commit.next().await;
    a.shutdown().await;
    let ea = tokio::time::timeout(Duration::from_secs(60), ha)
        .await
        .expect("a's commit ends")
        .unwrap()
        .unwrap_err();
    assert!(ea.is_maybe_committed(), "{ea:?}");
    assert_eq!(ea.code(), codes::COMMIT_UNKNOWN_RESULT, "{ea:?}");
    held_b.reply_commit(Ok(CommitId {
        version: 201,
        ..CommitId::default()
    }));
    let rb = tokio::time::timeout(Duration::from_secs(60), hb)
        .await
        .expect("b's commit ends")
        .unwrap();
    assert_eq!(rb.expect("b committed").committed_version(), 201);
    // The reply to A's cancelled commit arrives and is dropped; B goes on.
    held_a.reply_commit(Ok(CommitId {
        version: 301,
        ..CommitId::default()
    }));
    cluster.commit_one(&b, 400).await;
    assert_eq!(cluster.connects(150), 1);
    b.shutdown().await;
}
