//! Range reads: one batch with `get_range`, and whole ranges as streams of rows
//! (`get_ranges_keyvalues`) and of batches (`get_ranges`).
//!
//! ```text
//! cargo run -p fdb-native-client --example range_scan -- [CLUSTER_FILE]
//! ```
//!
//! The example writes 500 keys under `examples/range/` and clears them again.

use std::path::PathBuf;

use fdb_native_client::{Database, Error, KeySelector, RangeOptions, StreamingMode};
use futures_lite::StreamExt;

const PREFIX: &[u8] = b"examples/range/";
/// The key after every key that starts with `PREFIX` (`/` + 1 is `0`).
const END: &[u8] = b"examples/range0";
const ROWS: usize = 500;

fn key(i: usize) -> Vec<u8> {
    format!("examples/range/{i:05}").into_bytes()
}

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.clear_range(PREFIX, END);
        for i in 0..ROWS {
            tx.set(&key(i), format!("value {i}").as_bytes());
        }
        Ok(())
    })
    .await?;

    let begin = KeySelector::first_greater_or_equal(PREFIX);
    let end = KeySelector::first_greater_or_equal(END);

    // The rows one at a time: the stream reads batches as the bindings' iterators do (the
    // `Iterator` streaming mode: small batches first, growing). Every batch adds its read
    // conflict range. `StreamExt` is any stream extension trait (futures-lite's here).
    let count = db
        .run(|tx, _| {
            let (begin, end) = (begin.clone(), end.clone());
            async move {
                let mut rows = tx.get_ranges_keyvalues(begin, end, RangeOptions::default());
                let mut count = 0;
                while let Some(row) = rows.next().await {
                    let row = row?;
                    if count == 0 {
                        println!(
                            "first row: {} = {}",
                            String::from_utf8_lossy(&row.key),
                            String::from_utf8_lossy(&row.value)
                        );
                    }
                    count += 1;
                }
                Ok(count)
            }
        })
        .await?;
    assert_eq!(count, ROWS);
    println!("{count} rows");

    // One batch: the last 10 keys, in reverse, all at once (`WantAll`). `more` says whether
    // the range goes on past the batch.
    let last = db
        .run(|tx, _| {
            let (begin, end) = (begin.clone(), end.clone());
            async move {
                let options = RangeOptions::with_limit(10)
                    .reverse(true)
                    .mode(StreamingMode::WantAll);
                tx.get_range(&begin, &end, &options).await
            }
        })
        .await?;
    assert_eq!(last.kvs.len(), 10);
    assert!(last.more);
    assert_eq!(last.kvs[0].key, key(ROWS - 1));

    // The batches, with a snapshot read (no read conflict ranges: other transactions writing
    // the range do not make this one conflict).
    let batches = db
        .run(|tx, _| {
            let (begin, end) = (begin.clone(), end.clone());
            async move {
                let mut batches = tx
                    .snapshot()
                    .get_ranges(begin, end, RangeOptions::default());
                let mut sizes = Vec::new();
                while let Some(batch) = batches.next().await {
                    sizes.push(batch?.kvs.len());
                }
                Ok(sizes)
            }
        })
        .await?;
    assert_eq!(batches.iter().sum::<usize>(), ROWS);
    println!("batch sizes: {batches:?}");

    // Key selectors: the last key before the end of the range.
    let last_key = db
        .run(|tx, _| async move { tx.get_key(&KeySelector::last_less_than(END)).await })
        .await?;
    assert_eq!(last_key, key(ROWS - 1));

    db.run(|tx, _| async move {
        tx.clear_range(PREFIX, END);
        Ok(())
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
