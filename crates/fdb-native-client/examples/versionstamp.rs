//! Versionstamped keys with the tuple layer: an append-only log whose keys sort in commit
//! order, without any coordination between the writers.
//!
//! ```text
//! cargo run -p fdb-native-client --example versionstamp -- [CLUSTER_FILE]
//! ```
//!
//! Each entry's key is `(<subspace>, "log", <versionstamp>)`: the versionstamp is the
//! commit version and the transaction's order in its commit batch (10 bytes), filled in by
//! the cluster when the transaction commits, plus a 2-byte user version that tells the
//! entries of one transaction apart. The example writes under the tuple subspace
//! `("examples", "versionstamp")` and clears it again.

use std::path::PathBuf;

use fdb_native_client::{Database, Error, KeySelector, RangeOptions};
use fdb_native_tuple::{Element, Subspace, Versionstamp, tuple};
use futures_lite::StreamExt;

fn subspace() -> Subspace {
    Subspace::from_tuple(&tuple!["examples", "versionstamp"]).expect("a tuple without a stamp")
}

/// Appends `entries` to the log in one transaction; returns the transaction's versionstamp.
///
/// A versionstamped key is new on every attempt, so if `run` retries after a commit that
/// may have happened (`commit_unknown_result`; `MaybeCommitted(true)` in the closure), the
/// entries may be appended twice. A log that must not have duplicates also writes an id of
/// the entry it can check first.
async fn append(db: &Database, entries: &[&str]) -> Result<[u8; 10], Error> {
    let log = subspace();
    db.run(|tx, _| {
        let log = log.clone();
        async move {
            for (i, entry) in entries.iter().enumerate() {
                // An incomplete versionstamp with user version `i`: the key carries a
                // placeholder and its offset, which the commit replaces with the stamp.
                let key = log
                    .pack_with_versionstamp(&tuple!["log", Versionstamp::incomplete(i as u16)])
                    .expect("one incomplete versionstamp");
                tx.set_versionstamped_key(&key, entry.as_bytes());
            }
            // The stamp of this transaction's commit: ask for it before `run` commits.
            Ok(tx.get_versionstamp())
        }
    })
    .await?
    // `run` committed: the future resolves with the stamp.
    .await
}

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    let log = subspace();
    let (begin, end) = log.range_all();
    db.run(|tx, _| {
        let (begin, end) = (begin.clone(), end.clone());
        async move {
            tx.clear_range(&begin, &end);
            Ok(())
        }
    })
    .await?;

    let first = append(db, &["started", "configured"]).await?;
    let second = append(db, &["stopped"]).await?;
    // Later commits have larger stamps.
    assert!(second > first);

    // Read the log back in key order, which is commit order.
    let entries = db
        .run(|tx, _| {
            let (begin, end) = (begin.clone(), end.clone());
            let log = log.clone();
            async move {
                let mut rows = tx.get_ranges_keyvalues(
                    KeySelector::first_greater_or_equal(begin),
                    KeySelector::first_greater_or_equal(end),
                    RangeOptions::default(),
                );
                let mut entries = Vec::new();
                while let Some(row) = rows.next().await {
                    let row = row?;
                    let key = log.unpack(&row.key).expect("a tuple key");
                    let Some(Element::Versionstamp(stamp)) = key.as_slice().get(1) else {
                        panic!("not a log key: {key:?}");
                    };
                    entries.push((*stamp, String::from_utf8_lossy(&row.value).into_owned()));
                }
                Ok(entries)
            }
        })
        .await?;
    for (stamp, entry) in &entries {
        println!("{stamp:?}: {entry}");
    }
    let order: Vec<&str> = entries.iter().map(|(_, e)| e.as_str()).collect();
    assert_eq!(order, ["started", "configured", "stopped"]);
    // The stamps in the keys are the commits' stamps.
    assert_eq!(entries[0].0, Versionstamp::complete(first, 0));
    assert_eq!(entries[1].0, Versionstamp::complete(first, 1));
    assert_eq!(entries[2].0, Versionstamp::complete(second, 0));

    db.run(|tx, _| {
        let (begin, end) = (begin.clone(), end.clone());
        async move {
            tx.clear_range(&begin, &end);
            Ok(())
        }
    })
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_file = std::env::args().nth(1).map(PathBuf::from);
    let db = Database::open(cluster_file.as_deref())?;
    run(&db).await?;
    db.shutdown().await;
    Ok(())
}
