//! `getKey` against a real, disposable FDB server started by `fdb-native-testkit` (image from
//! `FDB_IMAGE`, default 7.4.10), on data written by `fdbcli`: the key list `fdbcli
//! getrangekeys` prints is the reference every selector is resolved against (by definition),
//! through the reader and through transactions (clamped to `\xff`, or not with
//! `READ_SYSTEM_KEYS`, where the first system key is checked against `fdbcli`); and the read
//! conflict ranges of `get_key`, checked by the resolver: a write inside the range makes the
//! commit fail with `not_committed`, a write just outside it does not, nor does any write after
//! a snapshot read. The multi-shard case is in the `cluster` test binary. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only`.

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::{Database, KeySelector, TransactionOption};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

async fn cli(fdb: &Arc<FdbContainer>, command: &str) -> String {
    let fdb = fdb.clone();
    let command = command.to_owned();
    tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
        .await
        .unwrap()
        .unwrap_or_else(|e| panic!("fdbcli: {e}"))
}

/// The keys of `fdbcli getrangekeys` output (`` `key' `` lines; printable keys only).
fn parse_keys(out: &str) -> Vec<Vec<u8>> {
    out.lines()
        .filter_map(|l| {
            let l = l.trim();
            let rest = l.strip_prefix('`')?;
            let end = rest.find('\'')?;
            Some(rest.as_bytes()[..end].to_vec())
        })
        .collect()
}

/// What a selector resolves to among `keys`, by definition: `""` before the first, `None`
/// past the last (the system keys, or the end of the key space).
fn reference(keys: &[Vec<u8>], sel: &KeySelector) -> Option<Vec<u8>> {
    let below = keys
        .iter()
        .filter(|k| {
            if sel.or_equal {
                **k <= sel.key
            } else {
                **k < sel.key
            }
        })
        .count() as i64;
    let target = below - 1 + sel.offset as i64;
    if target < 0 {
        Some(Vec::new())
    } else if target as usize >= keys.len() {
        None
    } else {
        Some(keys[target as usize].clone())
    }
}

fn sel(key: &str, or_equal: bool, offset: i32) -> KeySelector {
    KeySelector::new(key.as_bytes().to_vec(), or_equal, offset)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_get_key_resolves_selectors_on_fdbcli_data() {
    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    // `gk/00`, `gk/02`, ..., `gk/58`: the odd ones are absent.
    let mut writes = String::from("writemode on;");
    for i in (0..60).step_by(2) {
        writes.push_str(&format!(" set gk/{i:02} v{i};"));
    }
    let out = cli(&fdb, &writes).await;
    assert!(out.contains("Committed"), "{out}");
    let out = cli(&fdb, "getrangekeys \"\" \\xff 1000").await;
    let keys = parse_keys(&out);
    assert_eq!(keys.len(), 30, "{out}");
    assert_eq!(keys[0], b"gk/00");

    let db = Database::open(Some(fdb.cluster_file())).expect("database");
    let reader = db.reader();
    let version = within("GRV", reader.get_read_version()).await.unwrap();

    let mut selectors = vec![
        sel("gk/10", false, 1),
        sel("gk/11", false, 1),
        sel("gk/10", true, 1),
        sel("gk/10", false, 0),
        sel("gk/11", false, 0),
        sel("gk/10", true, 0),
        sel("gk/11", true, 0),
        sel("gk/10", false, 5),
        sel("gk/10", true, -3),
        sel("gk/30", false, -12),
        sel("gk/30", false, 14),
        sel("gk/30", false, 15),
        sel("gk/30", false, 16),
        sel("gk/30", false, -15),
        sel("gk/30", false, -14),
        sel("gk/00", false, 0),
        sel("", false, 1),
        sel("", true, 1),
        sel("", false, 0),
        sel("a", false, -1000),
        sel("a", false, 1000),
        sel("gk/58", true, 1),
        sel("gk/59", false, 0),
        sel("zzz", false, 0),
        sel("zzz", true, -29),
        sel("zzz", true, -30),
    ];
    for i in -35..=35 {
        selectors.push(sel("gk/31", i % 2 == 0, i));
    }

    let tx = db.create_transaction();
    let system = db.create_transaction();
    system
        .set_option(TransactionOption::ReadSystemKeys)
        .unwrap();
    let mut first_system_key = None;
    for s in &selectors {
        let want = reference(&keys, s);
        let got = within("get_key", reader.get_key(s, version))
            .await
            .unwrap_or_else(|e| panic!("{s:?}: {e}"));
        let in_tx = within("get_key", tx.get_key(s)).await.unwrap();
        let in_system = within("get_key", system.get_key(s)).await.unwrap();
        match want {
            Some(want) => {
                assert_eq!(got, want, "{s:?}");
                assert_eq!(in_tx, want, "{s:?}");
                assert_eq!(in_system, want, "{s:?}");
            }
            None => {
                // Past the user keys: the system keys come next (`\xff/...`), then the end
                // of the key space; a transaction without system key access sees `\xff`.
                assert!(got.as_slice() > b"\xff".as_slice(), "{s:?}: {got:?}");
                assert!(got.as_slice() <= KEY_SPACE_END, "{s:?}: {got:?}");
                assert_eq!(in_tx, b"\xff", "{s:?}");
                assert_eq!(in_system, got, "{s:?}");
                if s.key == b"gk/58" && s.or_equal && s.offset == 1 {
                    first_system_key.get_or_insert(got.clone());
                }
            }
        }
    }

    // The first system key, as fdbcli sees it.
    let first = first_system_key.expect("a selector past the user keys");
    println!("first system key {first:?}");
    let out = cli(
        &fdb,
        "option on READ_SYSTEM_KEYS; getrangekeys \\xff \\xff\\xff 1",
    )
    .await;
    let printed: String = first
        .iter()
        .map(|&b| match b {
            b'\\' => "\\\\".to_owned(),
            32..=126 => (b as char).to_string(),
            _ => format!("\\x{b:02x}"),
        })
        .collect();
    assert!(out.contains(&format!("`{printed}'")), "{printed} in {out}");
    assert!(
        within("get", reader.get(&first, version))
            .await
            .unwrap()
            .is_some(),
        "the first system key exists"
    );

    // Keys out of the legal range, and the bounds themselves.
    let e = tx
        .get_key(&KeySelector::first_greater_or_equal(b"\xff\x00".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    assert_eq!(
        tx.get_key(&KeySelector::first_greater_or_equal(b"\xff".to_vec()))
            .await
            .unwrap(),
        b"\xff"
    );
    assert_eq!(
        system
            .get_key(&KeySelector::first_greater_than(KEY_SPACE_END.to_vec()))
            .await
            .unwrap(),
        KEY_SPACE_END
    );
    assert_eq!(
        system
            .get_key(&KeySelector::last_less_than(KEY_SPACE_END.to_vec()))
            .await
            .unwrap()
            .first(),
        Some(&0xff)
    );

    db.shutdown().await;
    drop(db);
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_get_key_read_conflict_ranges_are_checked_by_the_resolver() {
    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    // Each case gets its own prefix with `10` and `12` set.
    let mut writes = String::from("writemode on;");
    for case in 0..10 {
        writes.push_str(&format!(" set c{case}/10 x; set c{case}/12 x;"));
    }
    let out = cli(&fdb, &writes).await;
    assert!(out.contains("Committed"), "{out}");
    let db = Database::open(Some(fdb.cluster_file())).expect("database");

    // (selector, result, a write, whether the reader then conflicts, snapshot read)
    let cases: Vec<(KeySelector, &str, &str, bool, bool)> = vec![
        // firstGreaterThan(10) = 12: the range is [10\x00, 12\x00).
        (sel("c0/10", true, 1), "c0/12", "c0/11", true, false),
        (sel("c1/10", true, 1), "c1/12", "c1/12", true, false),
        (sel("c2/10", true, 1), "c2/12", "c2/10", false, false),
        (sel("c3/10", true, 1), "c3/12", "c3/12\x00", false, false),
        // lastLessThan(12) = 10: the range is [10, 12).
        (sel("c4/12", false, 0), "c4/10", "c4/11", true, false),
        (sel("c5/12", false, 0), "c5/10", "c5/10", true, false),
        (sel("c6/12", false, 0), "c6/10", "c6/12", false, false),
        // lastLessOrEqual(12) = 12: [12, 12\x00).
        (sel("c7/12", true, 0), "c7/12", "c7/12", true, false),
        (sel("c8/12", true, 0), "c8/12", "c8/11", false, false),
        // A snapshot read: no range at all.
        (sel("c9/10", true, 1), "c9/12", "c9/11", false, true),
    ];
    for (s, want, write, conflicts, snapshot) in cases {
        let reader = db.create_transaction();
        let got = if snapshot {
            reader.snapshot().get_key(&s).await
        } else {
            reader.get_key(&s).await
        }
        .unwrap();
        assert_eq!(got, want.as_bytes(), "{s:?}");
        let writer = db.create_transaction();
        writer.set(write.as_bytes(), b"w");
        within("writer commit", writer.commit()).await.unwrap();
        reader.set(b"unrelated", b"1");
        let result = within("reader commit", reader.commit()).await;
        if conflicts {
            let e = result.expect_err("a conflict");
            assert_eq!(e.code(), codes::NOT_COMMITTED, "{s:?} then {write:?}");
        } else {
            result.unwrap_or_else(|e| panic!("{s:?} then {write:?}: {e}"));
        }
    }

    db.shutdown().await;
    drop(db);
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
