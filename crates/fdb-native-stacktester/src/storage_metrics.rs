//! `GET_ESTIMATED_RANGE_SIZE` and `GET_RANGE_SPLIT_POINTS`: the storage servers' metrics.
//!
//! **Shim.** The client does not have `get_estimated_range_size_bytes` and
//! `get_range_split_points` yet (beads `blr.2`). Until it does, these two functions stand in
//! for them: they succeed without asking the cluster, so the instructions push
//! `GOT_ESTIMATED_RANGE_SIZE` / `GOT_RANGE_SPLIT_POINTS` as the reference tester does when
//! its calls succeed, but an error the real calls would report (a cancelled or timed-out
//! transaction, `used_during_commit`, ...) is not pushed. When `blr.2` lands, replace the
//! bodies with the `Transaction` methods; nothing else in the tester changes.

use fdb_native_client::{Error, Transaction};

/// `tr.get_estimated_range_size_bytes(begin, end)`.
pub(crate) async fn estimated_range_size_bytes(
    tx: &Transaction,
    begin: &[u8],
    end: &[u8],
) -> Result<i64, Error> {
    // blr.2: `tx.get_estimated_range_size_bytes(begin, end).await`
    let _ = (tx, begin, end);
    Ok(0)
}

/// `tr.get_range_split_points(begin, end, chunk_size)`.
pub(crate) async fn range_split_points(
    tx: &Transaction,
    begin: &[u8],
    end: &[u8],
    chunk_size: i64,
) -> Result<Vec<Vec<u8>>, Error> {
    // blr.2: `tx.get_range_split_points(begin, end, chunk_size).await`
    let _ = (tx, chunk_size);
    Ok(vec![begin.to_vec(), end.to_vec()])
}
