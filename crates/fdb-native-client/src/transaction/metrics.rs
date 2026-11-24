//! [`Transaction::get_estimated_range_size_bytes`] and [`Transaction::get_range_split_points`]:
//! the read-your-writes layer passes both through to the storage servers' metrics, with no
//! read version, no read conflict range and nothing for a commit to wait for. The read path
//! is [`crate::read`]'s (see its [storage metrics](crate::read#storage-metrics) section).

use std::future::Future;

use fdb_native_schema::domain::LATEST_VERSION;
use fdb_native_transport::runtime::Runtime;

use super::{ReadFuture, ReadVersion, Transaction};
use crate::error::Error;
use crate::error::codes::*;

impl<R: Runtime> Transaction<R> {
    /// `getEstimatedRangeSizeBytes(range)`: the storage servers' estimate of the bytes
    /// stored in `[begin, end)` (`fdb_transaction_get_estimated_range_size_bytes`).
    ///
    /// The estimate comes from the servers' byte samples, summed over the range's shards:
    /// it is approximate (a small range may be 0 although it has data, and a server's
    /// sample lags its writes by a few seconds), not a count. It is the database's, at no
    /// particular version: the transaction's own writes are not in it.
    ///
    /// As in C++ (`ReadYourWritesTransaction::getEstimatedRangeSizeBytes`), this needs no
    /// read version, adds no read conflict range, accepts any keys (system keys too, with
    /// no option; the range is cut at `\xff\xff`), and is not a read a
    /// [`commit`](Self::commit) waits for. It starts when called, as [`get`](Self::get)
    /// does, and is ended by the transaction's reset, cancellation and timeout.
    ///
    /// **Errors** when called: `inverted_range` (2005) if `begin > end` (`fdb_c`), then the
    /// deferred error of a write, `used_during_commit` (2017), and the transaction's
    /// cancellation or timeout, as for [`get`](Self::get). An empty range is 0.
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// let tx = db.create_transaction();
    /// let bytes = tx.get_estimated_range_size_bytes(b"users/", b"users0").await?;
    /// println!("about {bytes} bytes");
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_estimated_range_size_bytes(
        &self,
        begin: &[u8],
        end: &[u8],
    ) -> impl Future<Output = Result<i64, Error>> + Send + use<R> {
        self.estimated_range_size_bytes(begin, end)
    }

    fn estimated_range_size_bytes(&self, begin: &[u8], end: &[u8]) -> ReadFuture<i64> {
        // `fdb_c` builds the `KeyRangeRef` first, which throws `inverted_range`.
        if begin > end {
            return ReadFuture::ready(Err(Error::new(INVERTED_RANGE)));
        }
        let epoch = {
            let mut state = self.state();
            match self.begin_read(&mut state) {
                Ok(start) => start.epoch,
                Err(e) => return ReadFuture::ready(Err(e)),
            }
        };
        let tx = self.clone();
        let (begin, end) = (begin.to_vec(), end.to_vec());
        ReadFuture::Task(self.spawn_read(async move {
            let reader = &tx.inner.db.inner.reader;
            tx.guarded(epoch, reader.get_estimated_range_size_bytes(&begin, &end))
                .await?
        }))
    }

    /// `getRangeSplitPoints(range, chunkSize)`: keys that cut `[begin, end)` into chunks of
    /// about `chunk_size` bytes each (`fdb_transaction_get_range_split_points`), for
    /// splitting the range into pieces of similar size (to read it in parallel, say).
    ///
    /// The result starts with `begin` and ends with `end`, and is strictly increasing:
    /// `begin`, then for each shard of the range its first key (shard boundaries are always
    /// split points) and the points its storage server chose from its byte sample, then
    /// `end`. Like [`get_estimated_range_size_bytes`](Self::get_estimated_range_size_bytes),
    /// the sizes are estimates, of the database at no particular version, without the
    /// transaction's writes; `begin == end` gives `[begin]`.
    ///
    /// As in C++ (`ReadYourWritesTransaction::getRangeSplitPoints`), this needs no read
    /// version, adds no read conflict range and is not a read a [`commit`](Self::commit)
    /// waits for. It starts when called, and is ended by the transaction's reset,
    /// cancellation and timeout.
    ///
    /// **Errors** when called: `inverted_range` (2005) if `begin > end` (`fdb_c`), then the
    /// deferred error of a write, `used_during_commit` (2017), the transaction's cancellation
    /// or timeout; `key_outside_legal_range` (2004) if `begin` or `end` is after `\xff` (or
    /// after `\xff\xff` with the `ReadSystemKeys` or `AccessSystemKeys` option; `\xff`
    /// itself is allowed as the end); and, unlike C++, `client_invalid_operation` (2000) for
    /// a `chunk_size` below 1 (a storage server given a negative chunk size may never answer).
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// let tx = db.create_transaction();
    /// let points = tx.get_range_split_points(b"users/", b"users0", 10_000_000).await?;
    /// for chunk in points.windows(2) {
    ///     println!("{:?}..{:?}", chunk[0], chunk[1]);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_range_split_points(
        &self,
        begin: &[u8],
        end: &[u8],
        chunk_size: i64,
    ) -> impl Future<Output = Result<Vec<Vec<u8>>, Error>> + Send + use<R> {
        self.range_split_points(begin, end, chunk_size)
    }

    fn range_split_points(
        &self,
        begin: &[u8],
        end: &[u8],
        chunk_size: i64,
    ) -> ReadFuture<Vec<Vec<u8>>> {
        if begin > end {
            return ReadFuture::ready(Err(Error::new(INVERTED_RANGE)));
        }
        let (start, version) = {
            let mut state = self.state();
            let start = match self.begin_read(&mut state) {
                Ok(start) => start,
                Err(e) => return ReadFuture::ready(Err(e)),
            };
            // `getMaxReadKey()`: the bound itself may be given.
            if begin > start.max_read_key || end > start.max_read_key {
                return ReadFuture::ready(Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE)));
            }
            // The read version only goes with the key location requests, if the transaction
            // has one (`getKeyRangeLocations(trState, ...)`); none is requested.
            let version = match state.read_version {
                ReadVersion::Set(v) => v,
                _ => LATEST_VERSION,
            };
            (start, version)
        };
        if chunk_size < 1 {
            return ReadFuture::ready(Err(Error::with_message(
                CLIENT_INVALID_OPERATION,
                "the chunk size of get_range_split_points must be positive",
            )));
        }
        let tx = self.clone();
        let (begin, end) = (begin.to_vec(), end.to_vec());
        ReadFuture::Task(self.spawn_read(async move {
            let reader = &tx.inner.db.inner.reader;
            let points = reader.get_range_split_points(
                &begin,
                &end,
                chunk_size,
                version,
                start.use_provisional_proxies,
            );
            tx.guarded(start.epoch, points).await?
        }))
    }
}
