//! `getRange`: the rows between two key selectors, shard by shard, within row and byte
//! limits. See the [module documentation](super#ranges) for how it follows C++.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{GetKeyValues, GetKeyValuesReply, KeyRange, KeySelector};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::{KeyValue, KeyValueRef, KeyValueVector, VersionVector};

use super::balance::{LoadBalanced, LoadBalancedReply};
use super::key::is_backward;
use super::location::{KEY_SPACE_END, Location};
use super::{ReadParams, ReadSnapshot, Reader, Shared, lock};
use crate::error::Error;
use crate::error::codes::*;

/// `REPLY_BYTE_LIMIT` (80 000; ClientKnobs 7.4:69, 8.0:174): the most rows and bytes one
/// `GetKeyValuesRequest` asks for.
pub(crate) const REPLY_BYTE_LIMIT: i64 = 80_000;
/// `GET_RANGE_SHARD_LIMIT` (2; ClientKnobs 7.4:108, 8.0:218): the most shard locations
/// `getExactRange` looks up at once.
const GET_RANGE_SHARD_LIMIT: i32 = 2;
/// What `GetRangeLimits::decrement` charges a row besides its key and value (`8 -
/// sizeof(KeyValueRef)` on top of `VectorRef::expectedSize()`'s `sizeof(KeyValueRef)`).
const ROW_OVERHEAD: i64 = 8;
/// `TaskPriority::DefaultEndpoint` (5000), the `taskID` of a default transaction, which 8.0's
/// `getRange` sends (`req.taskID = trState->taskID`); 7.4 has no such field.
const DEFAULT_ENDPOINT_TASK: i32 = 5000;
/// `iteration_progression` of `validate_and_update_parameters` (`bindings/c/fdb_c.cpp`
/// 7.4:1007, 8.0:1035): the byte targets of [`StreamingMode::Iterator`], 1.5 times the
/// previous each.
const ITERATION_PROGRESSION: [u32; 10] = [
    4096, 6144, 9216, 13824, 20736, 31104, 46656, 69984, 80000, 120000,
];

/// `FDBStreamingMode`: how many bytes a range read asks for at a time, when the caller does
/// not say. The byte targets are `fdb_c`'s (`validate_and_update_parameters`,
/// `bindings/c/fdb_c.cpp` 7.4:977-1033, 8.0:1006-1062).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StreamingMode {
    /// `WANT_ALL` (-2): the whole range is wanted soon; as [`Serial`](Self::Serial).
    WantAll,
    /// `ITERATOR` (-1, the bindings' default): the batch size grows with the
    /// [`iteration`](RangeOptions::iteration): 4096 bytes, then 1.5 times more each
    /// iteration, up to 120 000 bytes from the tenth.
    #[default]
    Iterator,
    /// `EXACT` (0): exactly the row limit, which must be set (`exact_mode_without_limits`
    /// otherwise); no byte target.
    Exact,
    /// `SMALL` (1): 256 bytes.
    Small,
    /// `MEDIUM` (2): 1000 bytes.
    Medium,
    /// `LARGE` (3): 4096 bytes.
    Large,
    /// `SERIAL` (4): 80 000 bytes with a 7.4 client, 120 000 with an 8.0 one (the batch is
    /// still at most what one storage server request returns, `REPLY_BYTE_LIMIT` = 80 000
    /// bytes).
    Serial,
}

impl StreamingMode {
    /// The `FDBStreamingMode` value.
    pub fn code(self) -> i32 {
        match self {
            StreamingMode::WantAll => -2,
            StreamingMode::Iterator => -1,
            StreamingMode::Exact => 0,
            StreamingMode::Small => 1,
            StreamingMode::Medium => 2,
            StreamingMode::Large => 3,
            StreamingMode::Serial => 4,
        }
    }

    /// The mode of an `FDBStreamingMode` value, `None` for an unknown one (which `fdb_c`
    /// refuses with `client_invalid_operation`).
    pub fn from_code(code: i32) -> Option<StreamingMode> {
        Some(match code {
            -2 => StreamingMode::WantAll,
            -1 => StreamingMode::Iterator,
            0 => StreamingMode::Exact,
            1 => StreamingMode::Small,
            2 => StreamingMode::Medium,
            3 => StreamingMode::Large,
            4 => StreamingMode::Serial,
            _ => return None,
        })
    }

    /// The mode's byte target (`mode_bytes`) for `iteration` (1 for the first batch) with
    /// the `fdb_c` of `schema`'s release: `None` is no target ([`Exact`](Self::Exact)).
    /// [`Iterator`](Self::Iterator) with `iteration` 0 is `client_invalid_operation`.
    pub fn byte_target(self, iteration: u32, schema: SchemaVersion) -> Result<Option<u32>, Error> {
        Ok(Some(match self {
            StreamingMode::Iterator => {
                if iteration == 0 {
                    return Err(Error::with_message(
                        CLIENT_INVALID_OPERATION,
                        "the ITERATOR streaming mode needs an iteration of 1 or more",
                    ));
                }
                let i = (iteration as usize).min(ITERATION_PROGRESSION.len());
                ITERATION_PROGRESSION[i - 1]
            }
            StreamingMode::Exact => return Ok(None),
            StreamingMode::Small => 256,
            StreamingMode::Medium => 1000,
            StreamingMode::Large => 4096,
            // `mode_bytes_array[SERIAL]`: 80000 on 7.4 (fdb_c.cpp 7.4:1003), 120000 on 8.0
            // (8.0:1031).
            StreamingMode::WantAll | StreamingMode::Serial => match schema {
                SchemaVersion::V7_4 => 80_000,
                SchemaVersion::V8_0 => 120_000,
            },
        }))
    }
}

/// `GetRangeLimits` (with `minRows` 1, as every public constructor of it): the most rows
/// and about the most bytes a range read returns. `None` is no limit.
///
/// A byte limit is soft, at row granularity: a read stops once the rows it has exceed it,
/// each counted as its key and value plus 8 bytes, and returns at least one row if there is
/// one. A row limit of 0 reads nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct RangeLimits {
    /// The most rows (`rows`).
    pub rows: Option<u32>,
    /// About the most bytes (`bytes`).
    pub bytes: Option<u32>,
}

impl RangeLimits {
    /// No limit.
    pub const UNLIMITED: RangeLimits = RangeLimits {
        rows: None,
        bytes: None,
    };

    /// At most `rows` rows and about `bytes` bytes.
    pub fn new(rows: Option<u32>, bytes: Option<u32>) -> RangeLimits {
        RangeLimits { rows, bytes }
    }
}

/// The arguments of `fdb_transaction_get_range` besides the selectors and `snapshot`: what a
/// range read (or one batch of a range stream) asks for.
///
/// ```
/// use fdb_native_client::{RangeOptions, StreamingMode};
///
/// // The last 10 rows, in one request.
/// let options = RangeOptions::with_limit(10)
///     .reverse(true)
///     .mode(StreamingMode::WantAll);
/// assert_eq!((options.limit, options.reverse), (10, true));
/// // The default: no limit, the `Iterator` mode, from its first iteration.
/// assert_eq!(RangeOptions::default().mode, StreamingMode::Iterator);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct RangeOptions {
    /// The most rows to return; 0 is no limit.
    pub limit: u32,
    /// About the most bytes to return (see [`RangeLimits`]); 0 is no limit. The streaming
    /// mode's target applies too: the smaller wins.
    pub target_bytes: u32,
    /// The streaming mode (default [`StreamingMode::Iterator`]).
    pub mode: StreamingMode,
    /// Which batch of the [`Iterator`](StreamingMode::Iterator) mode this is, from 1 (the
    /// default). A range stream counts it by itself.
    pub iteration: u32,
    /// Read from the end of the range: the rows come in descending key order, and the
    /// limits keep the last rows.
    pub reverse: bool,
}

impl Default for RangeOptions {
    fn default() -> Self {
        RangeOptions {
            limit: 0,
            target_bytes: 0,
            mode: StreamingMode::Iterator,
            iteration: 1,
            reverse: false,
        }
    }
}

impl RangeOptions {
    /// At most `limit` rows (0: no limit), the rest default.
    #[must_use]
    pub fn with_limit(limit: u32) -> RangeOptions {
        RangeOptions {
            limit,
            ..RangeOptions::default()
        }
    }

    /// These options with at most `limit` rows (0: no limit).
    #[must_use]
    pub fn limit(mut self, limit: u32) -> RangeOptions {
        self.limit = limit;
        self
    }

    /// These options with `mode`.
    #[must_use]
    pub fn mode(mut self, mode: StreamingMode) -> RangeOptions {
        self.mode = mode;
        self
    }

    /// These options reading in reverse if `reverse`.
    #[must_use]
    pub fn reverse(mut self, reverse: bool) -> RangeOptions {
        self.reverse = reverse;
        self
    }

    /// These options with `target_bytes`.
    #[must_use]
    pub fn target_bytes(mut self, target_bytes: u32) -> RangeOptions {
        self.target_bytes = target_bytes;
        self
    }

    /// These options at `iteration`.
    #[must_use]
    pub fn iteration(mut self, iteration: u32) -> RangeOptions {
        self.iteration = iteration;
        self
    }

    /// The checks of `validate_and_update_parameters` that do not depend on the release:
    /// `exact_mode_without_limits` for [`StreamingMode::Exact`] without a limit, and
    /// `client_invalid_operation` for [`StreamingMode::Iterator`] at iteration 0.
    pub fn validate(&self) -> Result<(), Error> {
        self.limits(SchemaVersion::V8_0).map(|_| ())
    }

    /// `validate_and_update_parameters` (`bindings/c/fdb_c.cpp` 7.4:977-1033,
    /// 8.0:1006-1062) with the byte targets of `schema`'s release: 0 is no limit, and the
    /// mode's byte target replaces no byte limit or lowers a larger one.
    pub fn limits(&self, schema: SchemaVersion) -> Result<RangeLimits, Error> {
        let rows = (self.limit != 0).then_some(self.limit);
        let bytes = (self.target_bytes != 0).then_some(self.target_bytes);
        if rows.is_none() && bytes.is_none() && self.mode == StreamingMode::Exact {
            return Err(Error::new(EXACT_MODE_WITHOUT_LIMITS));
        }
        let mode_bytes = self.mode.byte_target(self.iteration, schema)?;
        let bytes = match (bytes, mode_bytes) {
            (None, mode) => mode,
            (Some(b), Some(m)) => Some(b.min(m)),
            (Some(b), None) => Some(b),
        };
        Ok(RangeLimits { rows, bytes })
    }
}

/// What a range read returned (`RangeResultRef`, with `readToBegin`, `readThroughEnd` and
/// `readThrough`): the rows, in the order read (descending for a reverse read), and whether
/// the range may have more.
///
/// The rows stay in the blobs the storage servers sent them in (one copy of each reply's
/// rows, no allocation per row); [`iter`](Self::iter) borrows them, and
/// [`into_key_values`](Self::into_key_values) copies them out.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeRead {
    chunks: Vec<KeyValueVector>,
    len: usize,
    more: bool,
    read_to_begin: bool,
    read_through_end: bool,
    read_through: Option<Vec<u8>>,
}

impl RangeRead {
    /// The number of rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The rows, borrowed.
    pub fn iter(&self) -> impl Iterator<Item = KeyValueRef<'_>> + '_ {
        self.chunks.iter().flat_map(KeyValueVector::iter)
    }

    /// The first row.
    pub fn first(&self) -> Option<KeyValueRef<'_>> {
        self.chunks.first().and_then(|c| c.iter().next())
    }

    /// The last row.
    pub fn last(&self) -> Option<KeyValueRef<'_>> {
        self.chunks.last().and_then(|c| c.iter().last())
    }

    /// `more`: the range may have rows beyond these (they were cut by a limit, or the read
    /// stopped at a shard boundary). `false` means the range is exhausted.
    pub fn more(&self) -> bool {
        self.more
    }

    /// `readToBegin`: the read went to the beginning of the database (the begin selector
    /// resolved before the first key).
    pub fn read_to_begin(&self) -> bool {
        self.read_to_begin
    }

    /// `readThroughEnd`: the read went through the end of the database (the end selector
    /// resolved after the last key).
    pub fn read_through_end(&self) -> bool {
        self.read_through_end
    }

    /// `readThrough`: where a read with [`more`](Self::more) stopped at a shard boundary
    /// (the boundary; `None` when it stopped at a limit).
    pub fn read_through(&self) -> Option<&[u8]> {
        self.read_through.as_deref()
    }

    /// `getReadThrough(reverse)`: the end (or for a reverse read, the beginning) of what was
    /// read, where the next read of the rest continues: the shard boundary, else the key
    /// after the last row (the last row for a reverse read). `None` without
    /// [`more`](Self::more) or rows.
    pub fn continuation(&self, reverse: bool) -> Option<Vec<u8>> {
        if !self.more {
            return None;
        }
        if let Some(k) = &self.read_through {
            return Some(k.clone());
        }
        let last = self.last()?.key;
        Some(if reverse {
            last.to_vec()
        } else {
            key_after(last)
        })
    }

    /// The rows, copied out.
    pub fn into_key_values(self) -> Vec<KeyValue> {
        let mut out = Vec::with_capacity(self.len);
        out.extend(self.iter().map(KeyValueRef::into_owned));
        out
    }

    /// A result with these rows and flags, for tests of the code that reads one.
    #[cfg(test)]
    pub(crate) fn for_tests(
        rows: Vec<KeyValue>,
        more: bool,
        read_to_begin: bool,
        read_through_end: bool,
        read_through: Option<Vec<u8>>,
    ) -> RangeRead {
        let mut read = RangeRead {
            more,
            read_to_begin,
            read_through_end,
            read_through,
            ..RangeRead::default()
        };
        read.push(
            KeyValueVector::from_pairs(rows.iter().map(|kv| KeyValueRef::new(&kv.key, &kv.value)))
                .expect("rows"),
        );
        read
    }

    fn push(&mut self, chunk: KeyValueVector) {
        if !chunk.is_empty() {
            self.len += chunk.len();
            self.chunks.push(chunk);
        }
    }
}

/// A decoded `GetKeyValuesReply`'s result: its rows (one copy of the reply's blob) and
/// `more`.
#[derive(Clone, Debug)]
pub(crate) struct KeyValuesReply {
    data: KeyValueVector,
    more: bool,
}

impl LoadBalanced for GetKeyValues {
    type Output = KeyValuesReply;
    const STREAM: u32 = STORAGE_SERVER_STREAMS.get_key_values;

    fn split(reply: GetKeyValuesReply<'_>, keep: bool) -> LoadBalancedReply<KeyValuesReply> {
        (
            reply.penalty,
            reply.error.map(|e| e.code),
            keep.then(|| KeyValuesReply {
                data: reply.data.into_owned(),
                more: reply.more,
            }),
        )
    }
}

/// The bytes of the keys and values of `rows`.
fn kv_bytes(rows: &KeyValueVector) -> i64 {
    // The blob is a 4-byte count, then per row two 4-byte lengths, the key and the value.
    (rows.payload().len() - 4 - 8 * rows.len()) as i64
}

/// `keyAfter` (`FDBTypes.h` 7.4:579-596, 8.0:598-615): `key\x00`, except that the end of
/// the key space (`\xff\xff`) is its own successor.
pub(crate) fn key_after(key: &[u8]) -> Vec<u8> {
    if key == KEY_SPACE_END {
        return key.to_vec();
    }
    let mut k = Vec::with_capacity(key.len() + 1);
    k.extend_from_slice(key);
    k.push(0);
    k
}

// `KeySelectorRef`'s predicates (`FDBTypes.h` 7.4:657-667, 8.0:676-686).

/// `isFirstGreaterOrEqual`.
pub(crate) fn is_first_greater_or_equal(s: &KeySelector) -> bool {
    !s.or_equal && s.offset == 1
}

/// `isFirstGreaterThan`.
pub(crate) fn is_first_greater_than(s: &KeySelector) -> bool {
    s.or_equal && s.offset == 1
}

/// `isLastLessOrEqual`.
fn is_last_less_or_equal(s: &KeySelector) -> bool {
    s.or_equal && s.offset == 0
}

/// `isDefinitelyGreater(k)`: whatever the database holds, `s` resolves to a key after `k`.
fn is_definitely_greater(s: &KeySelector, k: &[u8]) -> bool {
    s.offset >= 1
        && if is_first_greater_or_equal(s) {
            s.key.as_slice() > k
        } else {
            s.key.as_slice() >= k
        }
}

/// `isDefinitelyLess(k)`: whatever the database holds, `s` resolves to a key before `k`.
fn is_definitely_less(s: &KeySelector, k: &[u8]) -> bool {
    s.offset <= 0
        && if is_last_less_or_equal(s) {
            s.key.as_slice() < k
        } else {
            s.key.as_slice() <= k
        }
}

/// `s - n`.
fn minus(s: &KeySelector, n: i32) -> KeySelector {
    KeySelector::new(s.key.clone(), s.or_equal, s.offset.saturating_sub(n))
}

/// `removeOrEqual`: `(k, true, n)` is `(keyAfter(k), false, n)`.
pub(crate) fn remove_or_equal(s: &KeySelector) -> KeySelector {
    if s.or_equal {
        KeySelector::new(key_after(&s.key), false, s.offset)
    } else {
        s.clone()
    }
}

fn fge(key: &[u8]) -> KeySelector {
    KeySelector::first_greater_or_equal(key.to_vec())
}

/// `getRangeFinished`'s read conflict range (NativeAPI 7.4:4849-4895, 8.0:2650-2702) of a
/// non-snapshot read of `[begin, end)` (the selectors as `getRange` got them, `orEqual`
/// removed) that returned `result`: what the result depends on. `None` when it is empty
/// (the commit leaves such a range out).
pub(crate) fn read_conflict_range(
    begin: &KeySelector,
    end: &KeySelector,
    reverse: bool,
    result: &RangeRead,
) -> Option<KeyRange> {
    let more = result.more;
    let empty = result.is_empty();
    let mut range_begin = if result.read_to_begin {
        Vec::new()
    } else if ((!reverse || !more || begin.offset > 1) && begin.offset > 0) || empty {
        begin.key.clone()
    } else {
        let row = if reverse {
            result.last()
        } else {
            result.first()
        };
        row.map(|kv| kv.key.to_vec()).unwrap_or_default()
    };
    if end.offset > begin.offset && end.key < range_begin {
        range_begin = end.key.clone();
    }
    let mut range_end = if result.read_through_end {
        KEY_SPACE_END.to_vec()
    } else if ((reverse || !more || end.offset <= 0) && end.offset <= 1) || empty {
        end.key.clone()
    } else {
        let row = if reverse {
            result.first()
        } else {
            result.last()
        };
        key_after(row.map(|kv| kv.key).unwrap_or_default())
    };
    if begin.offset < end.offset && begin.key > range_end {
        range_end = begin.key.clone();
    }
    (range_begin < range_end).then_some(KeyRange {
        begin: range_begin,
        end: range_end,
    })
}

/// `GetRangeLimits` as `getRange` counts it down: `-1` is no limit. The read-your-writes
/// layer builds its own (with a `minRows` above 1) and reads with
/// [`Reader::get_range_with_raw_limits`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) rows: i64,
    pub(crate) min_rows: i64,
    pub(crate) bytes: i64,
}

impl Limits {
    pub(crate) fn new(limits: RangeLimits) -> Limits {
        // C++'s limits are `int`s: larger ones are as good as none.
        let cap = |n: u32| i64::from(n.min(i32::MAX as u32));
        Limits {
            rows: limits.rows.map_or(-1, cap),
            min_rows: 1,
            bytes: limits.bytes.map_or(-1, cap),
        }
    }

    pub(crate) fn has_row_limit(&self) -> bool {
        self.rows != -1
    }

    pub(crate) fn has_byte_limit(&self) -> bool {
        self.bytes != -1
    }

    /// `isReached`.
    pub(crate) fn is_reached(&self) -> bool {
        self.rows == 0 || (self.bytes == 0 && self.min_rows == 0)
    }

    /// `isValid`.
    pub(crate) fn is_valid(&self) -> bool {
        (self.rows >= 0 || self.rows == -1)
            && (self.bytes >= 0 || self.bytes == -1)
            && self.min_rows >= 0
            && (self.min_rows <= self.rows || self.rows == -1)
    }

    /// `hasSatisfiedMinRows`.
    pub(crate) fn has_satisfied_min_rows(&self) -> bool {
        self.has_byte_limit() && self.min_rows == 0
    }

    /// `decrement(VectorRef<KeyValueRef>)` (NativeAPI 7.4:3071-3081, 8.0:1086-1096): each
    /// row costs its key and value plus 8 bytes.
    fn decrement(&mut self, rows: &KeyValueVector) {
        let n = rows.len() as i64;
        if self.has_row_limit() {
            self.rows -= n;
        }
        self.min_rows = (self.min_rows - n).max(0);
        if self.has_byte_limit() {
            self.bytes = (self.bytes - kv_bytes(rows) - ROW_OVERHEAD * n).max(0);
        }
    }

    /// `transformRangeLimits` (NativeAPI 7.4:4517-4536, 8.0:2330-2349): the request's
    /// `limit` (negative for a reverse read) and `limitBytes`.
    fn request(&self, reverse: bool) -> (i32, i32) {
        let (limit, limit_bytes) = if self.bytes != 0 {
            let limit = if self.has_row_limit() {
                REPLY_BYTE_LIMIT.min(self.rows)
            } else {
                REPLY_BYTE_LIMIT
            };
            let limit_bytes = if self.has_byte_limit() {
                REPLY_BYTE_LIMIT.min(self.bytes)
            } else {
                REPLY_BYTE_LIMIT
            };
            (limit, limit_bytes)
        } else {
            (self.min_rows, REPLY_BYTE_LIMIT)
        };
        let limit = if reverse { -limit } else { limit };
        (limit as i32, limit_bytes as i32)
    }
}

/// Both futures' results, or an error as `wait(fb); wait(fe)` on two futures started
/// together reports it: `a`'s as soon as `a` fails; `b`'s only once `a` succeeded (so `a`'s
/// if both fail, whichever fails first).
async fn try_join<A, B, T, U>(a: A, b: B) -> Result<(T, U), Error>
where
    A: Future<Output = Result<T, Error>>,
    B: Future<Output = Result<U, Error>>,
{
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut ra = None;
    let mut rb: Option<Result<U, Error>> = None;
    poll_fn(|cx| {
        if ra.is_none() {
            if let Poll::Ready(r) = a.as_mut().poll(cx) {
                ra = Some(r?);
            }
        }
        if rb.is_none() {
            if let Poll::Ready(r) = b.as_mut().poll(cx) {
                rb = Some(r);
            }
        }
        match (ra.take(), rb.take()) {
            (Some(x), Some(Ok(y))) => Poll::Ready(Ok((x, y))),
            (Some(_), Some(Err(e))) => Poll::Ready(Err(e)),
            (x, y) => {
                ra = x;
                rb = y;
                Poll::Pending
            }
        }
    })
    .await
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Reader<R, W> {
    /// The cluster's release, for the byte targets of the streaming modes: the latest
    /// cluster info's, or the first one's.
    pub(crate) async fn schema(&self) -> Result<SchemaVersion, Error> {
        let monitor = &self.inner.shared.monitor;
        if let Some(schema) = monitor.schema_version() {
            return Ok(schema);
        }
        monitor
            .ready()
            .await
            .map(|info| info.schema)
            .map_err(|_| Error::new(OPERATION_CANCELLED))
    }

    /// The rows from the key `begin` resolves to (included) up to the key `end` resolves to
    /// (excluded) at `version`, as `fdb_transaction_get_range` returns them with
    /// read-your-writes disabled and `snapshot` set: `options` are checked and turned into
    /// limits as `fdb_c` does ([`RangeOptions::limits`], with the byte targets of the
    /// cluster's release), then [`get_range_with_limits`](Self::get_range_with_limits).
    ///
    /// One call returns one batch: the rows up to the limits, the streaming mode's byte
    /// target, or (with a byte target) the end of the first shard that has rows, with
    /// [`more`](RangeRead::more) set if the range may go on.
    pub async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        options: &RangeOptions,
        version: i64,
    ) -> Result<RangeRead, Error> {
        options.validate()?;
        let limits = options.limits(self.schema().await?)?;
        self.get_range_with_limits(
            begin,
            end,
            limits,
            options.reverse,
            version,
            ReadParams::new(),
        )
        .await
    }

    /// `Transaction::getRange(begin, end, limits, Snapshot::True, reverse)` (NativeAPI's
    /// `getRangeInternal` and `getRange`): the rows between the selectors at `version`,
    /// across as many shards as it takes, within `limits`, read with `params` (as
    /// [`Reader::get_with`]: provisional commit proxies, and the `ReadOptions` of every
    /// `GetKeyValuesRequest` and `GetKeyRequest`).
    ///
    /// * A row limit of 0, or an inverted range (`begin`'s offset at least `end`'s and its
    ///   key at least `end`'s, `orEqual`s removed), returns nothing.
    /// * The result is not clamped to any key bound: a range reaching into the system keys
    ///   (`\xff...`) returns them (the transaction trims them).
    /// * `version` must be a read version (positive), else `client_invalid_operation`; a
    ///   selector key after [`KEY_SPACE_END`] is `key_outside_legal_range`.
    pub async fn get_range_with_limits(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limits: RangeLimits,
        reverse: bool,
        version: i64,
        params: impl Into<ReadParams>,
    ) -> Result<RangeRead, Error> {
        if version <= 0 {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        self.get_range_with_raw_limits(
            begin,
            end,
            Limits::new(limits),
            reverse,
            version,
            params.into(),
        )
        .await
    }

    /// [`get_range_with_limits`](Self::get_range_with_limits) with C++'s `GetRangeLimits`
    /// as it is (`minRows` included), as the read-your-writes layer asks for its reads
    /// (`RYWImpl::setRequestLimits`). `range_limits_invalid` (2012) for limits that are not
    /// `isValid()`.
    pub(crate) async fn get_range_with_raw_limits(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limits: Limits,
        reverse: bool,
        version: i64,
        params: ReadParams,
    ) -> Result<RangeRead, Error> {
        if version <= 0 {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        if begin.key.as_slice() > KEY_SPACE_END || end.key.as_slice() > KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        if limits.is_reached() {
            return Ok(RangeRead::default());
        }
        if !limits.is_valid() {
            return Err(Error::new(RANGE_LIMITS_INVALID));
        }
        let begin = remove_or_equal(begin);
        let end = remove_or_equal(end);
        if begin.offset >= end.offset && begin.key >= end.key {
            return Ok(RangeRead::default());
        }
        Box::pin(
            self.inner
                .shared
                .get_range(begin, end, limits, reverse, version, params),
        )
        .await
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> ReadSnapshot<R, W> {
    /// The rows between two selectors at the snapshot's version ([`Reader::get_range`]).
    pub async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        options: &RangeOptions,
    ) -> Result<RangeRead, Error> {
        options.validate()?;
        let limits = options.limits(self.reader.schema().await?)?;
        self.reader
            .get_range_with_limits(
                begin,
                end,
                limits,
                options.reverse,
                self.version,
                self.params,
            )
            .await
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// `getRange` (NativeAPI 7.4:4903-5208, 8.0:2709-3026), from after `getRangeInternal`'s
    /// checks: `begin` and `end` have no `orEqual` and are not inverted, and `limits` is not
    /// reached.
    async fn get_range(
        &self,
        mut begin: KeySelector,
        mut end: KeySelector,
        mut limits: Limits,
        reverse: bool,
        version: i64,
        params: ReadParams,
    ) -> Result<RangeRead, Error> {
        let original_begin = begin.clone();
        let original_end = end.clone();
        let original_limits = limits;
        let mut output = RangeRead::default();
        if begin.key.is_empty() && begin.offset < 1 {
            output.read_to_begin = true;
            begin = fge(b"");
        }
        // As in `get_with`: the previous attempt's location stays alive until the next one
        // is known.
        let mut _previous: Option<Arc<Location>> = None;
        loop {
            self.check_open()?;
            if end.key.is_empty() && (end.offset < 1 || is_first_greater_or_equal(&end)) {
                return Ok(output);
            }
            // Not in C++, which would look up the shard of `\xff\xff` (there is none): a
            // forward read whose begin resolves at or after the end of the key space has
            // nothing more to read, and a reverse read whose end does reads up to it. The
            // forward read went through the end of the database (`readThroughEnd`, which C++
            // reaches through `getRangeFallback`), so its read conflict range ends at
            // `\xff\xff`, not after its last row.
            if !reverse && begin.key.as_slice() >= KEY_SPACE_END && !is_backward(&begin) {
                output.read_through_end = true;
                return Ok(output);
            }
            if reverse && end.key.as_slice() >= KEY_SPACE_END && !is_backward(&minus(&end, 1)) {
                end = fge(KEY_SPACE_END);
            }

            let (location_key, backward) = if reverse {
                (end.key.clone(), is_backward(&minus(&end, 1)))
            } else {
                (begin.key.clone(), is_backward(&begin))
            };
            let (shard, location) = self
                .key_location_for(
                    &location_key,
                    version,
                    params.use_provisional_proxies,
                    backward,
                    GetKeyValues::STREAM,
                )
                .await?;
            _previous = Some(location.clone());

            let mut modified = false;
            let request_begin = if reverse
                && is_definitely_less(&minus(&begin, 1), &shard.begin)
                && (!is_first_greater_or_equal(&begin) || begin.key != shard.begin)
            {
                modified = true;
                fge(&shard.begin)
            } else {
                begin.clone()
            };
            let request_end = if !reverse && is_definitely_greater(&end, &shard.end) {
                modified = true;
                fge(&shard.end)
            } else {
                end.clone()
            };
            let (limit, limit_bytes) = limits.request(reverse);
            let request = GetKeyValues {
                begin: request_begin,
                end: request_end,
                version,
                limit,
                limit_bytes,
                tags: None,
                span_context: Default::default(),
                options: params.read_options,
                ss_latest_commit_versions: VersionVector::default(),
                task_id: Some(DEFAULT_ENDPOINT_TASK),
            };

            let reply = match self.load_balance(&location, request).await {
                Ok(reply) => reply,
                Err(e) if e.code() == WRONG_SHARD_SERVER || e.code() == ALL_ALTERNATIVES_FAILED => {
                    if backward {
                        lock(&self.locations).invalidate_key_before(&location_key);
                    } else {
                        lock(&self.locations).invalidate_key(&location_key);
                    }
                    if e.code() == WRONG_SHARD_SERVER {
                        return Box::pin(self.get_range_fallback(
                            &original_begin,
                            &original_end,
                            original_limits,
                            reverse,
                            version,
                            params,
                        ))
                        .await;
                    }
                    self.rt.sleep(self.config.wrong_shard_server_delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            check_reply(&reply, &limits)?;
            limits.decrement(&reply.data);
            let last_key = reply.data.iter().last().map(|kv| kv.key.to_vec());
            if reverse && is_last_less_or_equal(&begin) && last_key.as_ref() == Some(&begin.key) {
                modified = false;
            }
            let finished = limits.is_reached()
                || (!modified && !reply.more)
                || limits.has_satisfied_min_rows();
            let read_through = modified && !reply.more;
            output.push(reply.data);
            if finished {
                output.more = modified || limits.is_reached() || reply.more;
                if read_through {
                    output.read_through = Some(if reverse { shard.begin } else { shard.end });
                }
                return Ok(output);
            }
            match last_key {
                // The shard ended (`modified` is set: an unmodified request that is not
                // `more` is finished) without a row: start again the slow way.
                None if !reply.more => {
                    return Box::pin(self.get_range_fallback(
                        &original_begin,
                        &original_end,
                        original_limits,
                        reverse,
                        version,
                        params,
                    ))
                    .await;
                }
                _ if !reply.more => {
                    if reverse {
                        end = fge(&shard.begin);
                    } else {
                        begin = fge(&shard.end);
                    }
                }
                // `more` with rows (checked above).
                Some(last) => {
                    if reverse {
                        end = KeySelector::first_greater_or_equal(last);
                    } else {
                        begin = KeySelector::first_greater_than(last);
                    }
                }
                None => return Err(Error::new(INTERNAL_ERROR)),
            }
        }
    }

    /// `resolveKey` (NativeAPI 7.4:4751-4759, 8.0:2562-2570): a `firstGreaterOrEqual` or
    /// `firstGreaterThan` selector is its key (or the key after it); any other is resolved
    /// with `getKey`.
    async fn resolve_key(
        &self,
        selector: &KeySelector,
        version: i64,
        params: ReadParams,
    ) -> Result<Vec<u8>, Error> {
        if is_first_greater_or_equal(selector) {
            return Ok(selector.key.clone());
        }
        if is_first_greater_than(selector) {
            return Ok(key_after(&selector.key));
        }
        self.get_key(selector, version, params).await
    }

    /// `getRangeFallback` (NativeAPI 7.4:4761-4837, 8.0:2572-2647): resolve both selectors,
    /// then read the exact range between the keys.
    async fn get_range_fallback(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limits: Limits,
        reverse: bool,
        version: i64,
        params: ReadParams,
    ) -> Result<RangeRead, Error> {
        let (b, e) = try_join(
            self.resolve_key(begin, version, params),
            self.resolve_key(end, version, params),
        )
        .await?;
        if b >= e {
            return Ok(RangeRead::default());
        }
        let keys = KeyRange { begin: b, end: e };
        let mut r =
            Box::pin(self.get_exact_range(keys.clone(), limits, reverse, version, params)).await?;
        // `b` is the beginning of the database: the read went there, or the key `""` exists
        // and is in the conflict range anyway.
        if keys.begin.is_empty() && (!reverse || !r.more) {
            r.read_to_begin = true;
        }
        // C++'s TODO: this makes the conflict range reach the end of the database even when
        // the end selector resolved to just after the last key.
        if keys.end == KEY_SPACE_END && (reverse || !r.more) {
            r.read_through_end = true;
        }
        Ok(r)
    }

    /// `getExactRange` (NativeAPI 7.4:4549-4749, 8.0:2362-2560): the rows of `keys`,
    /// `GET_RANGE_SHARD_LIMIT` shard locations at a time, one request per shard (and more
    /// while a shard's replies have `more`).
    async fn get_exact_range(
        &self,
        mut keys: KeyRange,
        mut limits: Limits,
        reverse: bool,
        version: i64,
        params: ReadParams,
    ) -> Result<RangeRead, Error> {
        let mut output = RangeRead::default();
        loop {
            self.check_open()?;
            let mut locations = self
                .key_range_locations(
                    &keys,
                    GET_RANGE_SHARD_LIMIT,
                    reverse,
                    version,
                    params.use_provisional_proxies,
                    GetKeyValues::STREAM,
                )
                .await?;
            let mut shard = 0;
            loop {
                let (range, location) = locations[shard].clone();
                let (limit, limit_bytes) = limits.request(reverse);
                // 8.0's `getExactRange` leaves `taskID` unset.
                let request = GetKeyValues {
                    begin: fge(&range.begin),
                    end: fge(&range.end),
                    version,
                    limit,
                    limit_bytes,
                    tags: None,
                    span_context: Default::default(),
                    options: params.read_options,
                    ss_latest_commit_versions: VersionVector::default(),
                    task_id: None,
                };
                let reply = match self.load_balance(&location, request).await {
                    Ok(reply) => reply,
                    Err(e)
                        if e.code() == WRONG_SHARD_SERVER
                            || e.code() == ALL_ALTERNATIVES_FAILED =>
                    {
                        keys = if reverse {
                            KeyRange {
                                begin: keys.begin,
                                end: range.end,
                            }
                        } else {
                            KeyRange {
                                begin: range.begin,
                                end: keys.end,
                            }
                        };
                        lock(&self.locations).invalidate_range(&keys);
                        self.rt.sleep(self.config.wrong_shard_server_delay).await;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                check_reply(&reply, &limits)?;
                limits.decrement(&reply.data);
                let last_key = reply.data.iter().last().map(|kv| kv.key.to_vec());
                output.push(reply.data);
                if limits.is_reached() {
                    output.more = true;
                    return Ok(output);
                }
                let mut more = reply.more;
                // The shard is done if a reverse read returned its first key.
                if reverse && more && last_key.as_deref() == Some(range.begin.as_slice()) {
                    more = false;
                }
                if more {
                    // Again on the same shard, after the last row (`check_reply`: there is
                    // one).
                    let last = last_key.unwrap_or_default();
                    locations[shard].0 = if reverse {
                        KeyRange {
                            begin: range.begin.clone(),
                            end: last,
                        }
                    } else {
                        KeyRange {
                            begin: key_after(&last),
                            end: range.end.clone(),
                        }
                    };
                }
                let mut redo_locations = false;
                let current = &locations[shard].0;
                if !more || current.begin >= current.end {
                    if shard == locations.len() - 1 {
                        let (b, e) = if reverse {
                            (keys.begin.clone(), current.begin.clone())
                        } else {
                            (current.end.clone(), keys.end.clone())
                        };
                        if b >= e {
                            output.more = false;
                            return Ok(output);
                        }
                        keys = KeyRange { begin: b, end: e };
                        redo_locations = true;
                    }
                    shard += 1;
                }
                // The soft byte limit: with rows, return early.
                if limits.has_satisfied_min_rows() && !output.is_empty() {
                    output.more = true;
                    return Ok(output);
                }
                if redo_locations {
                    break;
                }
            }
        }
    }

    /// `getKeyRangeLocations` (NativeAPI 7.4:3505-3547, 8.0:1493-1534): the locations of at
    /// most `limit` shards of `keys`, from its end if `reverse`, each intersected with
    /// `keys`: from the cache if it has them all (and none has a replica whose endpoint of
    /// the storage stream `stream`, the request's `member`, failed on a healthy server: that
    /// shard is then forgotten), else from a commit proxy.
    pub(super) async fn key_range_locations(
        &self,
        keys: &KeyRange,
        limit: i32,
        reverse: bool,
        version: i64,
        use_provisional: bool,
        stream: u32,
    ) -> Result<Vec<(KeyRange, Arc<Location>)>, Error> {
        if let Some(cached) = self.cached_locations(keys, limit as usize, reverse) {
            let mut found_failed = false;
            for (range, location) in &cached {
                let mut refresh = false;
                for server in &location.servers {
                    let (address, token) = server.target_for(stream);
                    if self.check_only_endpoint_failed(address, token) {
                        refresh = true;
                    }
                }
                if refresh {
                    lock(&self.locations).invalidate_key(&range.begin);
                    found_failed = true;
                }
            }
            if !found_failed {
                return Ok(cached);
            }
        }
        let fetched = self
            .fetch_key_locations(
                &keys.begin,
                Some(&keys.end),
                limit,
                reverse,
                version,
                use_provisional,
            )
            .await?;
        let results: Vec<_> = fetched
            .into_iter()
            .filter_map(|(range, location)| Some((intersect(&range, keys)?, location)))
            .collect();
        if results.is_empty() {
            // C++ asserts there is at least one.
            return Err(Error::with_message(
                INTERNAL_ERROR,
                "a commit proxy returned no shard of a range",
            ));
        }
        Ok(results)
    }

    /// `getCachedLocations` (NativeAPI 7.4:2191-2224, 8.0:165-192): the cached shards from
    /// the one containing `keys.begin` to the one containing the keys just before
    /// `keys.end` (walked from the latter if `reverse`), at most `limit` of them, each
    /// intersected with `keys`; `None` if one of them is not cached.
    fn cached_locations(
        &self,
        keys: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Option<Vec<(KeyRange, Arc<Location>)>> {
        let cache = lock(&self.locations);
        let mut out = Vec::new();
        let mut key = if reverse {
            keys.end.clone()
        } else {
            keys.begin.clone()
        };
        loop {
            let (range, location) = if reverse {
                cache.get_before(&key)?
            } else {
                cache.get(&key)?
            };
            let done = if reverse {
                range.begin <= keys.begin
            } else {
                range.end >= keys.end
            };
            out.push((intersect(&range, keys)?, location));
            if out.len() >= limit || done {
                return Some(out);
            }
            key = if reverse { range.begin } else { range.end };
        }
    }

    /// `Reader::get_key_with` on the shared state.
    async fn get_key(
        &self,
        selector: &KeySelector,
        version: i64,
        params: ReadParams,
    ) -> Result<Vec<u8>, Error> {
        Box::pin(super::key::get_key(self, selector, version, params)).await
    }
}

/// `KeyRangeRef::operator&`: the intersection, `None` if empty.
fn intersect(a: &KeyRange, b: &KeyRange) -> Option<KeyRange> {
    let begin = a.begin.as_slice().max(b.begin.as_slice());
    let end = a.end.as_slice().min(b.end.as_slice());
    (begin < end).then(|| KeyRange {
        begin: begin.to_vec(),
        end: end.to_vec(),
    })
}

/// What C++ asserts of a reply: no more rows than the row limit, and `more` only with rows.
fn check_reply(reply: &KeyValuesReply, limits: &Limits) -> Result<(), Error> {
    if reply.more && reply.data.is_empty() {
        return Err(Error::with_message(
            INTERNAL_ERROR,
            "a storage server said a range has more rows but sent none",
        ));
    }
    if limits.has_row_limit() && reply.data.len() as i64 > limits.rows {
        return Err(Error::with_message(
            INTERNAL_ERROR,
            "a storage server sent more rows than the limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(key: &[u8], or_equal: bool, offset: i32) -> KeySelector {
        KeySelector::new(key.to_vec(), or_equal, offset)
    }

    #[test]
    fn streaming_mode_byte_targets_follow_fdb_c() {
        use StreamingMode::*;
        for schema in [SchemaVersion::V7_4, SchemaVersion::V8_0] {
            assert_eq!(Exact.byte_target(1, schema).unwrap(), None);
            assert_eq!(Small.byte_target(1, schema).unwrap(), Some(256));
            assert_eq!(Medium.byte_target(7, schema).unwrap(), Some(1000));
            assert_eq!(Large.byte_target(0, schema).unwrap(), Some(4096));
            let progression: Vec<u32> = (1..=12)
                .map(|i| Iterator.byte_target(i, schema).unwrap().unwrap())
                .collect();
            assert_eq!(
                progression,
                [
                    4096, 6144, 9216, 13824, 20736, 31104, 46656, 69984, 80000, 120000, 120000,
                    120000
                ]
            );
            let e = Iterator.byte_target(0, schema).unwrap_err();
            assert_eq!(e.code(), CLIENT_INVALID_OPERATION);
        }
        assert_eq!(
            Serial.byte_target(1, SchemaVersion::V7_4).unwrap(),
            Some(80_000)
        );
        assert_eq!(
            WantAll.byte_target(1, SchemaVersion::V7_4).unwrap(),
            Some(80_000)
        );
        assert_eq!(
            Serial.byte_target(1, SchemaVersion::V8_0).unwrap(),
            Some(120_000)
        );
        for code in -2..=4 {
            assert_eq!(StreamingMode::from_code(code).unwrap().code(), code);
        }
        assert_eq!(StreamingMode::from_code(5), None);
        assert_eq!(StreamingMode::from_code(-3), None);
    }

    #[test]
    fn range_options_map_to_limits_as_fdb_c() {
        let v8 = SchemaVersion::V8_0;
        let o = RangeOptions::default();
        assert_eq!(o.limits(v8).unwrap(), RangeLimits::new(None, Some(4096)));
        let o = RangeOptions::with_limit(10).mode(StreamingMode::Exact);
        assert_eq!(o.limits(v8).unwrap(), RangeLimits::new(Some(10), None));
        let o = RangeOptions::default().mode(StreamingMode::Exact);
        assert_eq!(o.limits(v8).unwrap_err().code(), EXACT_MODE_WITHOUT_LIMITS);
        assert_eq!(o.validate().unwrap_err().code(), EXACT_MODE_WITHOUT_LIMITS);
        // A byte limit and an exact mode: the byte limit alone.
        let o = RangeOptions::default()
            .mode(StreamingMode::Exact)
            .target_bytes(100);
        assert_eq!(o.limits(v8).unwrap(), RangeLimits::new(None, Some(100)));
        // The smaller of the target and the mode's.
        let o = RangeOptions::default()
            .mode(StreamingMode::Small)
            .target_bytes(100);
        assert_eq!(o.limits(v8).unwrap(), RangeLimits::new(None, Some(100)));
        let o = RangeOptions::default()
            .mode(StreamingMode::Small)
            .target_bytes(1000);
        assert_eq!(o.limits(v8).unwrap(), RangeLimits::new(None, Some(256)));
        let o = RangeOptions::default().iteration(0);
        assert_eq!(o.validate().unwrap_err().code(), CLIENT_INVALID_OPERATION);
        let o = RangeOptions::default().mode(StreamingMode::WantAll);
        assert_eq!(
            o.limits(SchemaVersion::V7_4).unwrap(),
            RangeLimits::new(None, Some(80_000))
        );
    }

    #[test]
    fn limits_count_rows_and_bytes_as_cpp() {
        let rows = KeyValueVector::from_pairs([
            KeyValueRef::new(b"ab", b"xyz"),
            KeyValueRef::new(b"c", b""),
        ])
        .unwrap();
        assert_eq!(kv_bytes(&rows), 6);
        let mut l = Limits::new(RangeLimits::new(Some(5), Some(30)));
        assert_eq!(l.request(false), (5, 30));
        assert_eq!(l.request(true), (-5, 30));
        l.decrement(&rows);
        // 6 bytes of keys and values plus 8 per row.
        assert_eq!((l.rows, l.min_rows, l.bytes), (3, 0, 8));
        assert!(!l.is_reached() && l.has_satisfied_min_rows());
        l.decrement(&rows);
        assert_eq!(l.bytes, 0);
        assert!(l.is_reached());

        // No limits: `REPLY_BYTE_LIMIT` of each.
        let l = Limits::new(RangeLimits::UNLIMITED);
        assert_eq!(l.request(false), (80_000, 80_000));
        assert!(!l.has_satisfied_min_rows());
        // A byte limit of 0: `minRows` rows.
        let l = Limits::new(RangeLimits::new(None, Some(0)));
        assert_eq!(l.request(true), (-1, 80_000));
        assert!(!l.is_reached());
        // Huge limits are capped by the reply limit.
        let l = Limits::new(RangeLimits::new(Some(u32::MAX), Some(1_000_000)));
        assert_eq!(l.request(false), (80_000, 80_000));
        assert!(Limits::new(RangeLimits::new(Some(0), None)).is_reached());
    }

    #[test]
    fn selector_predicates() {
        assert!(is_definitely_greater(&sel(b"m", false, 1), b"l"));
        assert!(!is_definitely_greater(&sel(b"m", false, 1), b"m"));
        assert!(is_definitely_greater(&sel(b"m", true, 1), b"m"));
        assert!(is_definitely_greater(&sel(b"m", false, 2), b"m"));
        assert!(!is_definitely_greater(&sel(b"z", false, 0), b"m"));
        assert!(is_definitely_less(&sel(b"m", false, 0), b"m"));
        assert!(!is_definitely_less(&sel(b"m", true, 0), b"m"));
        assert!(is_definitely_less(&sel(b"m", true, 0), b"n"));
        assert!(!is_definitely_less(&sel(b"a", false, 1), b"m"));
        assert_eq!(
            remove_or_equal(&sel(b"a", true, 3)),
            sel(b"a\x00", false, 3)
        );
        assert_eq!(
            remove_or_equal(&sel(KEY_SPACE_END, true, 1)),
            sel(KEY_SPACE_END, false, 1)
        );
        assert_eq!(key_after(b"a"), b"a\x00");
        assert_eq!(key_after(KEY_SPACE_END), KEY_SPACE_END);
    }

    fn read(keys: &[&[u8]], more: bool) -> RangeRead {
        let mut r = RangeRead::default();
        r.push(KeyValueVector::from_pairs(keys.iter().map(|k| KeyValueRef::new(k, b"v"))).unwrap());
        r.more = more;
        r
    }

    fn range(b: &[u8], e: &[u8]) -> Option<KeyRange> {
        Some(KeyRange {
            begin: b.to_vec(),
            end: e.to_vec(),
        })
    }

    #[test]
    fn conflict_ranges_follow_get_range_finished() {
        let fge = |k: &[u8]| sel(k, false, 1);
        // Everything read: the selectors' keys.
        let r = read(&[b"b", b"c"], false);
        assert_eq!(
            read_conflict_range(&fge(b"a"), &fge(b"z"), false, &r),
            range(b"a", b"z")
        );
        // Cut by a limit: up to after the last row (forward), from the last row (reverse).
        let r = read(&[b"b", b"c"], true);
        assert_eq!(
            read_conflict_range(&fge(b"a"), &fge(b"z"), false, &r),
            range(b"a", b"c\x00")
        );
        let r = read(&[b"y", b"x"], true);
        assert_eq!(
            read_conflict_range(&fge(b"a"), &fge(b"z"), true, &r),
            range(b"x", b"z")
        );
        // The ends of the database.
        let mut r = read(&[b"b"], false);
        r.read_to_begin = true;
        r.read_through_end = true;
        assert_eq!(
            read_conflict_range(&fge(b"a"), &fge(b"z"), false, &r),
            range(b"", KEY_SPACE_END)
        );
        // Nothing read, an end key before the begin key but a larger offset: the keys
        // between.
        let r = RangeRead::default();
        assert_eq!(
            read_conflict_range(&fge(b"m"), &sel(b"c", false, 3), false, &r),
            range(b"c", b"m")
        );
        // An end offset above 1 that read past the end key, more: after the last row.
        let r = read(&[b"b", b"q"], true);
        assert_eq!(
            read_conflict_range(&fge(b"a"), &sel(b"c", false, 5), false, &r),
            range(b"a", b"q\x00")
        );
    }

    #[test]
    fn range_read_accessors() {
        let mut r = read(&[b"a", b"b"], true);
        r.push(KeyValueVector::from_pairs([KeyValueRef::new(b"c", b"3")]).unwrap());
        r.push(KeyValueVector::new());
        assert_eq!(r.len(), 3);
        assert_eq!(r.first().unwrap().key, b"a");
        assert_eq!(r.last().unwrap().key, b"c");
        assert_eq!(r.continuation(false).unwrap(), b"c\x00");
        assert_eq!(r.continuation(true).unwrap(), b"c");
        r.read_through = Some(b"m".to_vec());
        assert_eq!(r.continuation(false).unwrap(), b"m");
        let kvs = r.into_key_values();
        assert_eq!(kvs.len(), 3);
        assert_eq!(kvs[2], KeyValue::new(b"c".to_vec(), b"3".to_vec()));
        assert_eq!(RangeRead::default().continuation(false), None);
    }

    #[tokio::test(start_paused = true)]
    async fn try_join_reports_errors_as_wait_fb_then_fe() {
        use std::time::Duration;
        use tokio::time::sleep;
        let fail = |code, ms| async move {
            sleep(Duration::from_millis(ms)).await;
            Err::<(), _>(Error::new(code))
        };
        let ok = |ms| async move {
            sleep(Duration::from_millis(ms)).await;
            Ok::<(), Error>(())
        };
        // Both fail, the end first: the begin's error (C++ waits for `fb` first).
        let e = try_join(fail(1, 20), fail(2, 10)).await.unwrap_err();
        assert_eq!(e.code(), 1);
        let e = try_join(fail(1, 10), fail(2, 20)).await.unwrap_err();
        assert_eq!(e.code(), 1);
        // Only the end fails: its error, once the begin succeeded.
        let started = tokio::time::Instant::now();
        let e = try_join(ok(30), fail(2, 10)).await.unwrap_err();
        assert_eq!(e.code(), 2);
        assert!(started.elapsed() >= Duration::from_millis(30));
        // The begin fails: at once.
        let started = tokio::time::Instant::now();
        let e = try_join(fail(1, 10), ok(30)).await.unwrap_err();
        assert_eq!(e.code(), 1);
        assert!(started.elapsed() < Duration::from_millis(30));
        assert!(try_join(ok(1), ok(2)).await.is_ok());
    }
}
