//! Storage metrics: the estimated size of a key range (`getStorageMetrics`) and its split
//! points (`getRangeSplitPoints`). See the [module documentation](super#storage-metrics).

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use fdb_native_schema::domain::{
    KeyRange, LATEST_VERSION, SplitRange, SplitRangeReply, StorageMetrics, WaitMetrics,
};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::Bytes;

use super::balance::{LoadBalanced, LoadBalancedReply};
use super::location::{KEY_SPACE_END, Location};
use super::{Reader, Shared, lock};
use crate::error::Error;
use crate::error::codes::*;

/// `TOO_MANY` (1 000 000; ClientKnobs 7.4:38, 8.0:143): the most shard locations
/// `getRangeSplitPoints` asks for at once.
const TOO_MANY: i32 = 1_000_000;
/// `FUTURE_VERSION_RETRY_DELAY` (10 ms; ClientKnobs 7.4:66, 8.0:172).
const FUTURE_VERSION_RETRY_DELAY: Duration = Duration::from_millis(10);

impl LoadBalanced for WaitMetrics {
    type Output = StorageMetrics;
    const STREAM: u32 = STORAGE_SERVER_STREAMS.wait_metrics;

    /// `StorageMetrics` is not a `LoadBalancedReply`: no penalty (C++'s `-1`, which leaves
    /// the model's alone) and no error but the envelope's.
    fn split(reply: StorageMetrics, _: &Bytes, keep: bool) -> LoadBalancedReply<StorageMetrics> {
        (-1.0, None, keep.then_some(reply))
    }
}

impl LoadBalanced for SplitRange {
    type Output = Vec<Vec<u8>>;
    const STREAM: u32 = STORAGE_SERVER_STREAMS.get_range_split_points;

    /// Not a `LoadBalancedReply` either.
    fn split(reply: SplitRangeReply, _: &Bytes, keep: bool) -> LoadBalancedReply<Vec<Vec<u8>>> {
        (-1.0, None, keep.then_some(reply.split_points))
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// `waitForAll` of futures started together: their results in order, or the first error
/// as soon as one fails (the others are dropped, as C++ drops an actor's `state` futures
/// when it throws).
async fn try_join_all<T, F>(futures: Vec<F>) -> Result<Vec<T>, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut results: Vec<Option<T>> = futures.iter().map(|_| None).collect();
    poll_fn(|cx| {
        let mut pending = false;
        for (future, result) in futures.iter_mut().zip(results.iter_mut()) {
            if result.is_some() {
                continue;
            }
            match future.as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => *result = Some(value),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => pending = true,
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(results.iter_mut().filter_map(Option::take).collect()))
        }
    })
    .await
}

/// `RangeSplitPointsBuilder` (NativeAPI 8.0:6332-6374) without a limit: the begin, then the
/// split points and shard boundaries, then the end.
struct SplitPoints(Vec<Vec<u8>>);

impl SplitPoints {
    fn new(begin: &[u8]) -> SplitPoints {
        SplitPoints(vec![begin.to_vec()])
    }

    /// `appendShardBoundary`: not if it is the last point already.
    fn boundary(&mut self, key: &[u8]) {
        if self.0.last().map(Vec::as_slice) != Some(key) {
            self.0.push(key.to_vec());
        }
    }

    /// `finish`: the end, unless it is the last point already.
    fn finish(mut self, end: &[u8]) -> Vec<Vec<u8>> {
        self.boundary(end);
        self.0
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Reader<R, W> {
    /// `getStorageMetrics(keys, -1).bytes`, what `Transaction::getEstimatedRangeSizeBytes`
    /// returns: the storage servers' estimate of the bytes stored in `[begin, end)`, the
    /// sum over its shards (see the [module documentation](super#storage-metrics)). It
    /// needs no read version.
    ///
    /// `inverted_range` (2005) if `begin > end`. The range is cut at the end of the key
    /// space (`\xff\xff`); an empty range (or one at or after that end) is 0 without asking
    /// the cluster.
    pub async fn get_estimated_range_size_bytes(
        &self,
        begin: &[u8],
        end: &[u8],
    ) -> Result<i64, Error> {
        if begin > end {
            return Err(Error::new(INVERTED_RANGE));
        }
        let end = end.min(KEY_SPACE_END);
        if begin >= end {
            return Ok(0);
        }
        let keys = KeyRange {
            begin: begin.to_vec(),
            end: end.to_vec(),
        };
        let metrics = self
            .inner
            .shared
            .storage_metrics_large_key_range(keys)
            .await?;
        Ok(metrics.bytes)
    }

    /// `Transaction::getRangeSplitPoints(keys, chunkSize)`: keys that cut `[begin, end)`
    /// into chunks of about `chunk_size` bytes by the storage servers' byte samples, with
    /// `begin` first and `end` last (see the [module documentation](super#storage-metrics)).
    ///
    /// `version` is only what the key location requests carry (a transaction's read
    /// version if it has one, else [`LATEST_VERSION`]); locations are asked of provisional
    /// commit proxies too if `use_provisional_proxies`.
    ///
    /// `inverted_range` (2005) if `begin > end`; `key_outside_legal_range` (2004) for a key
    /// after `\xff\xff`; `client_invalid_operation` (2000) for a `chunk_size` below 1 (C++
    /// sends it, and a storage server given a negative one may never answer). `begin == end`
    /// is `[begin]` without asking the cluster (C++ asserts).
    pub async fn get_range_split_points(
        &self,
        begin: &[u8],
        end: &[u8],
        chunk_size: i64,
        version: i64,
        use_provisional_proxies: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if begin > end {
            return Err(Error::new(INVERTED_RANGE));
        }
        if begin > KEY_SPACE_END || end > KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        if chunk_size < 1 {
            return Err(Error::with_message(
                CLIENT_INVALID_OPERATION,
                "the chunk size of get_range_split_points must be positive",
            ));
        }
        if begin == end {
            return Ok(vec![begin.to_vec()]);
        }
        let keys = KeyRange {
            begin: begin.to_vec(),
            end: end.to_vec(),
        };
        self.inner
            .shared
            .range_split_points(&keys, chunk_size, version, use_provisional_proxies)
            .await
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// `getStorageMetricsLargeKeyRange` (NativeAPI 7.4:8202-8239, 8.0:5993-6025) without a
    /// transaction: the shards of `keys` (all of them, `std::numeric_limits<int>::max()`),
    /// located at `latestVersion` with the regular commit proxies, one
    /// [`do_get_storage_metrics`](Self::do_get_storage_metrics) each, all at once, summed.
    /// `keys` is not empty.
    fn storage_metrics_large_key_range(
        &self,
        keys: KeyRange,
    ) -> BoxFuture<'_, Result<StorageMetrics, Error>> {
        Box::pin(async move {
            self.check_open()?;
            let locations = self
                .key_range_locations(
                    &keys,
                    i32::MAX,
                    false,
                    LATEST_VERSION,
                    false,
                    WaitMetrics::STREAM,
                )
                .await?;
            let parts = locations
                .into_iter()
                .map(|(range, location)| self.do_get_storage_metrics(range, location))
                .collect();
            let mut total = StorageMetrics::default();
            for m in try_join_all(parts).await? {
                // `StorageMetrics::operator+=` (saturating here: a server's numbers are not
                // trusted not to overflow).
                total.bytes = total.bytes.saturating_add(m.bytes);
                total.bytes_written_per_ksecond = total
                    .bytes_written_per_ksecond
                    .saturating_add(m.bytes_written_per_ksecond);
                total.ios_per_ksecond = total.ios_per_ksecond.saturating_add(m.ios_per_ksecond);
                total.bytes_read_per_ksecond = total
                    .bytes_read_per_ksecond
                    .saturating_add(m.bytes_read_per_ksecond);
                total.ops_read_per_ksecond = total
                    .ops_read_per_ksecond
                    .saturating_add(m.ops_read_per_ksecond);
            }
            Ok(total)
        })
    }

    /// `doGetStorageMetrics` (NativeAPI 7.4:8172-8200, 8.0:5964-5991): `loadBalance` a
    /// `WaitMetricsRequest` for one shard's part `keys` that the server answers at once
    /// ([`WaitMetrics::immediate`]). `wrong_shard_server` and `all_alternatives_failed`
    /// invalidate the part's cached locations and, after `WRONG_SHARD_SERVER_DELAY`, start
    /// the part over as a range of its own (it may have been split or moved);
    /// `future_version` does the same after `FUTURE_VERSION_RETRY_DELAY`; any other error is
    /// returned.
    async fn do_get_storage_metrics(
        &self,
        keys: KeyRange,
        location: Arc<Location>,
    ) -> Result<StorageMetrics, Error> {
        let request = WaitMetrics::immediate(keys.clone(), LATEST_VERSION);
        match self.load_balance(&location, request).await {
            Ok(metrics) => return Ok(metrics),
            Err(e) if e.code() == WRONG_SHARD_SERVER || e.code() == ALL_ALTERNATIVES_FAILED => {
                lock(&self.locations).invalidate_range(&keys);
                self.rt.sleep(self.config.wrong_shard_server_delay).await;
            }
            Err(e) if e.code() == FUTURE_VERSION => {
                self.rt.sleep(FUTURE_VERSION_RETRY_DELAY).await;
            }
            Err(e) => return Err(e),
        }
        // The location is let go before the part is located again.
        drop(location);
        self.storage_metrics_large_key_range(keys).await
    }

    /// `getRangeSplitPoints` (NativeAPI 7.4:8544-8606, 8.0:6376-6455) with no limit: until it
    /// succeeds, the split points of `keys` ([`try_range_split_points`](Self::try_range_split_points));
    /// on `wrong_shard_server` or `all_alternatives_failed`, the cached locations of `keys`
    /// are invalidated and everything starts over after `WRONG_SHARD_SERVER_DELAY`; any other
    /// error is returned.
    async fn range_split_points(
        &self,
        keys: &KeyRange,
        chunk_size: i64,
        version: i64,
        use_provisional: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        loop {
            self.check_open()?;
            match self
                .try_range_split_points(keys, chunk_size, version, use_provisional)
                .await
            {
                Ok(points) => return Ok(points),
                Err(e) if e.code() == WRONG_SHARD_SERVER || e.code() == ALL_ALTERNATIVES_FAILED => {
                    lock(&self.locations).invalidate_range(keys);
                    self.rt.sleep(self.config.wrong_shard_server_delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// One attempt of [`range_split_points`](Self::range_split_points): the locations of
    /// (at most `TOO_MANY`) shards of what is left of `keys`, a `SplitRangeRequest` to each
    /// (`limit` -1), all at once; the points are `keys.begin`, then for each shard its begin
    /// (but the first's) and its split points, then `keys.end`, a boundary being left out
    /// when it is the last point already. If the shards did not reach `keys.end`, the
    /// same from where they ended.
    async fn try_range_split_points(
        &self,
        keys: &KeyRange,
        chunk_size: i64,
        version: i64,
        use_provisional: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut points = SplitPoints::new(&keys.begin);
        let mut begin = keys.begin.clone();
        loop {
            let rest = KeyRange {
                begin: begin.clone(),
                end: keys.end.clone(),
            };
            let locations = self
                .key_range_locations(
                    &rest,
                    TOO_MANY,
                    false,
                    version,
                    use_provisional,
                    SplitRange::STREAM,
                )
                .await?;
            let requests = locations
                .iter()
                .map(|(range, location)| {
                    let request = SplitRange {
                        keys: range.clone(),
                        chunk_size,
                        limit: -1,
                    };
                    self.load_balance(location, request)
                })
                .collect();
            let replies = try_join_all(requests).await?;
            for (i, ((range, _), split)) in locations.iter().zip(replies).enumerate() {
                if i > 0 || begin != keys.begin {
                    points.boundary(&range.begin);
                }
                points.0.extend(split);
            }
            // `key_range_locations` returned at least one shard, each cut to `rest`.
            let reached = locations
                .last()
                .map_or_else(|| keys.end.clone(), |(range, _)| range.end.clone());
            if reached >= keys.end {
                return Ok(points.finish(&keys.end));
            }
            begin = reached;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_points_leave_out_repeated_boundaries() {
        let mut p = SplitPoints::new(b"a");
        p.boundary(b"a");
        p.0.extend([b"c".to_vec(), b"f".to_vec()]);
        p.boundary(b"f");
        p.boundary(b"g");
        assert_eq!(
            p.finish(b"z"),
            [&b"a"[..], b"c", b"f", b"g", b"z"].map(<[u8]>::to_vec)
        );
        let mut p = SplitPoints::new(b"a");
        p.0.push(b"z".to_vec());
        assert_eq!(p.finish(b"z"), [&b"a"[..], b"z"].map(<[u8]>::to_vec));
    }

    #[tokio::test]
    async fn try_join_all_fails_fast_and_keeps_order() {
        let ok = try_join_all(vec![
            Box::pin(async { Ok::<_, Error>(1) }) as BoxFuture<'_, _>,
            Box::pin(async { Ok(2) }),
        ])
        .await;
        assert_eq!(ok.unwrap(), vec![1, 2]);
        let failed = try_join_all(vec![
            Box::pin(std::future::pending::<Result<i32, Error>>()) as BoxFuture<'_, _>,
            Box::pin(async { Err(Error::new(WRONG_SHARD_SERVER)) }),
        ])
        .await;
        assert_eq!(failed.unwrap_err().code(), WRONG_SHARD_SERVER);
        let none: Vec<BoxFuture<'_, Result<i32, Error>>> = Vec::new();
        assert!(try_join_all(none).await.unwrap().is_empty());
    }
}
