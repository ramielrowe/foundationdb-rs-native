//! The read-your-writes read paths: a port of `RYWImpl`'s reads (`ReadYourWrites.actor.cpp`
//! on `release-7.4`, `ReadYourWrites.cpp` on `release-8.0`; the logic is the same, 8.0 is
//! written with coroutines): `read(GetValueReq)`, `read(GetKeyReq)`, `getRangeValue`,
//! `getRangeValueBack` and their helpers (`resolveKeySelectorFromCache`, `skipUncached`,
//! `countUncached`, `setRequestLimits`, ...), and the read conflict ranges a serializable
//! read adds (`addConflictRange`, `updateConflictMap`).
//!
//! # How a read runs
//!
//! A read sees the database at the transaction's read version through the snapshot cache
//! (what earlier reads found out), with the transaction's writes on top (a snapshot of the
//! write map taken when the read was called): an [`RywIter`] walks both. Where the cache
//! knows nothing (and no write decides), the read asks the database through
//! [`RywSource`] (a snapshot read: `ryw->tr.get(key, Snapshot::True)`,
//! `ryw->tr.getRange(..., Snapshot::True, ...)`), records the answer in the cache, and goes
//! on. A serializable read then adds its read conflict range, over the keys whose value it
//! depended on: the write map decides which (`updateConflictMap`: unmodified keys and keys
//! with dependent writes; not the keys the transaction set or cleared).
//!
//! C++ keeps its `RYWIterator` over each wait and repositions it after
//! (`resolveKeySelectorFromCache` and `skip` start with `it.skip(key)`); here every
//! synchronous step between two waits runs with the cache locked
//! ([`RywSource::with_cache`]) and builds its iterators from the cache and the write map
//! snapshot, which is all that is kept over a wait. A step does what C++ does between the
//! same two waits.
//!
//! # Line references
//!
//! `ReadYourWrites.actor.cpp` 7.4 (`ReadYourWrites.cpp` 8.0): `read(GetValueReq)` 94-138
//! (103-147), `read(GetKeyReq)` 140-160 (149-169), `addConflictRange` 218-319 (227-329),
//! `updateConflictMap` 321-351 (331-361), `readWithConflictRange*` 353-406 (363-400),
//! `resolveKeySelectorFromCache` 408-485 (402-478), `getKnownKeyRange` 487-509 (480-502),
//! `skipUncached` 513-541 (506-534), `countUncached` 544-567 (537-560), `setRequestLimits`
//! 569-590 (562-583), `getRangeValue` 593-814 (585-810), `getKnownKeyRangeBack` 816-839
//! (812-835), `skipUncachedBack` 843-870 (839-866), `countUncachedBack` 873-894 (869-890),
//! `getRangeValueBack` 896-1127 (892-1130).

use std::future::Future;
use std::pin::Pin;

use fdb_native_schema::domain::KeySelector;
use fdb_native_wire::KeyValue;

use crate::error::Error;
use crate::error::codes::INVERTED_RANGE;
use crate::read::{
    Limits, REPLY_BYTE_LIMIT, RangeRead, is_first_greater_or_equal, key_after, remove_or_equal,
};
use crate::ryw::conflicts::ReadConflicts;
use crate::ryw::iterator::RywIter;
use crate::ryw::{ExtKey, RangeReadOutcome, SelectorBound, SnapshotCache, WriteMapIter};

/// A boxed future, for the reads [`RywSource`] makes and the recursion of `getKey` into
/// `getRangeValue`.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the read paths need from the transaction: its snapshot cache, and snapshot reads of
/// the database at its read version (`ryw->tr` with `Snapshot::True`).
pub(crate) trait RywSource: Send + Sync {
    /// Runs `f` with the snapshot cache (the transaction locked); fails if the transaction
    /// was reset since the read started (its cache is another one now).
    fn with_cache<T>(&self, f: impl FnOnce(&mut SnapshotCache) -> T) -> Result<T, Error>;

    /// `tr.get(key, Snapshot::True)`.
    fn read_value(&self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>>;

    /// `tr.getRange(begin, end, limits, Snapshot::True, reverse)`.
    fn read_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        limits: Limits,
        reverse: bool,
    ) -> BoxFuture<'_, Result<RangeRead, Error>>;

    /// `getMaxReadKey()` now, which C++ reads again after each wait of a read (the
    /// `ReadSystemKeys` and `AccessSystemKeys` options may have changed meanwhile); `None`:
    /// the one of [`RywRead::max_read_key`] holds.
    fn max_read_key(&self) -> Option<&'static [u8]> {
        None
    }
}

/// A read: where it reads from, and the transaction's view when it was called.
pub(crate) struct RywRead<'s, S> {
    /// The cache and the database.
    pub(crate) source: &'s S,
    /// The write map snapshot (an iterator over an empty write map for a snapshot read that
    /// does not see the transaction's writes: C++ reads the cache alone then).
    pub(crate) writes: WriteMapIter,
    /// `getMaxReadKey()` when the read was called (what its synchronous start uses, as
    /// C++'s actor before its first wait); after a wait, [`RywSource::max_read_key`] tells
    /// the current one ([`current_max_read_key`](Self::current_max_read_key)).
    pub(crate) max_read_key: &'static [u8],
    /// `options.bypassUnreadable` (`get` only, as C++, which looks at it when the read is
    /// called: `RYWImpl::read(GetValueReq)` 7.4:98-100 runs before the first wait).
    pub(crate) bypass_unreadable: bool,
}

impl<S: RywSource> RywRead<'_, S> {
    /// `ryw->getMaxReadKey()` after a wait: the transaction's current one.
    fn current_max_read_key(&self) -> &'static [u8] {
        self.source.max_read_key().unwrap_or(self.max_read_key)
    }
}

/// What a range read returns (`RangeResult` with its flags).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RangeOut {
    /// The rows, in the order read.
    pub(crate) rows: Vec<KeyValue>,
    /// `more`.
    pub(crate) more: bool,
    /// `readToBegin`.
    pub(crate) read_to_begin: bool,
    /// `readThroughEnd`.
    pub(crate) read_through_end: bool,
}

impl RangeOut {
    /// `RangeResultRef(readToBegin, readThroughEnd)`: no rows.
    fn empty(read_to_begin: bool, read_through_end: bool) -> RangeOut {
        RangeOut {
            rows: Vec::new(),
            more: false,
            read_to_begin,
            read_through_end,
        }
    }
}

/// `GetRangeLimits(rows)` (`minRows` 1, no byte limit).
pub(crate) fn row_limit(rows: i64) -> Limits {
    Limits {
        rows,
        min_rows: 1,
        bytes: -1,
    }
}

/// `GetRangeLimits::decrement(KeyValueRef)` (NativeAPI 7.4:3083-3089, 8.0:1098-1104).
fn decrement(limits: &mut Limits, kv: &KeyValue) {
    limits.min_rows = (limits.min_rows - 1).max(0);
    if limits.rows != -1 {
        limits.rows -= 1;
    }
    if limits.bytes != -1 {
        limits.bytes = (limits.bytes - 8 - (kv.key.len() + kv.value.len()) as i64).max(0);
    }
}

fn fge(key: Vec<u8>) -> KeySelector {
    KeySelector::first_greater_or_equal(key)
}

/// `KeySelectorRef::isBackward`.
fn is_backward(s: &KeySelector) -> bool {
    !s.or_equal && s.offset <= 0
}

fn is_key_after(e: &[u8], b: &[u8]) -> bool {
    ExtKey::from(e).is_key_after(&ExtKey::from(b))
}

/// `resolveKeySelectorFromCache`: moves `key` toward a `firstGreaterOrEqual` selector
/// through what the iterator knows; `it` ends at the segment the key is in or next to (an
/// unknown one if the cache does not know enough), or the selector becomes
/// `firstGreaterOrEqual("")` (`read_to_begin`) or of `max_key` (`read_through_end`).
fn resolve_key_selector(
    key: &mut KeySelector,
    it: &mut RywIter<'_>,
    max_key: &[u8],
    read_to_begin: &mut bool,
    read_through_end: &mut bool,
    actual_offset: &mut i32,
) -> Result<(), Error> {
    *key = remove_or_equal(key);
    let already_exhausted = key.offset == 1;
    it.skip(&key.key);
    if key.offset <= 0 && it.begin_key() == ExtKey::from(&key.key) && !key.key.is_empty() {
        it.retreat();
    }
    // `keyNeedsCopy`: the new key, if it changed.
    let mut keykey: Option<Vec<u8>> = None;
    let max = ExtKey::from(max_key);
    while key.offset > 1 && !it.is_unreadable() && !it.is_unknown_range()? && it.end_key() < max {
        if it.is_kv()? {
            key.offset -= 1;
        }
        it.advance();
        keykey = Some(it.begin_key().to_vec());
    }
    while key.offset < 1
        && !it.is_unreadable()
        && !it.is_unknown_range()?
        && it.begin_key() != ExtKey::EMPTY
    {
        if it.is_kv()? {
            key.offset += 1;
            if key.offset == 1 {
                keykey = Some(it.begin_key().to_vec());
                break;
            }
        }
        it.retreat();
        keykey = Some(it.end_key().to_vec());
    }
    if !already_exhausted {
        *actual_offset = key.offset;
    }
    if !it.is_unreadable() && !it.is_unknown_range()? && key.offset < 1 {
        *read_to_begin = true;
        key.key = Vec::new();
        key.offset = 1;
        return Ok(());
    }
    if !it.is_unreadable() && !it.is_unknown_range()? && key.offset > 1 {
        *read_through_end = true;
        key.key = max_key.to_vec();
        key.offset = 1;
        return Ok(());
    }
    while !it.is_unreadable() && it.is_empty_range()? && it.end_key() < max {
        it.advance();
        keykey = Some(it.begin_key().to_vec());
    }
    if let Some(k) = keykey {
        key.key = k;
    }
    Ok(())
}

/// `skipUncached`: from an unknown segment, moves `it` to the unknown segment just before
/// the next known range that is not a single empty key (skipping those), at most
/// `iteration_limit` segments on and not past `end`. Returns the number of single empty
/// keys skipped.
fn skip_uncached<'c>(
    it: &mut RywIter<'c>,
    end: &RywIter<'c>,
    mut iteration_limit: i32,
) -> Result<i32, Error> {
    let mut b = it.begin_key().to_vec();
    let mut e = it.end_key().to_vec();
    let mut single_empty = 0;
    debug_assert!(!it.is_unreadable() && it.is_unknown_range()?);
    while it != end && {
        iteration_limit -= 1;
        iteration_limit >= 0
    } {
        if it.is_unreadable() || it.is_empty_range()? {
            if it.is_unreadable() || !is_key_after(&e, &b) {
                while it.is_unreadable() || !it.is_unknown_range()? {
                    it.retreat();
                }
                return Ok(single_empty);
            }
            single_empty += 1;
        } else {
            b = e;
        }
        it.advance();
        e = it.end_key().to_vec();
    }
    while it.is_unreadable() || !it.is_unknown_range()? {
        it.retreat();
    }
    Ok(single_empty)
}

/// `countUncached`: the number of single empty keys after the unknown segment `it` before
/// the next known range that is not one, at most `max_clears`.
fn count_uncached(mut it: RywIter<'_>, max_key: &[u8], max_clears: i32) -> Result<i32, Error> {
    if max_clears <= 0 {
        return Ok(0);
    }
    let mut b = it.begin_key().to_vec();
    let mut e = it.end_key().to_vec();
    let mut single_empty = 0;
    while e.as_slice() < max_key {
        if it.is_unreadable() || it.is_empty_range()? {
            if it.is_unreadable() || !is_key_after(&e, &b) {
                return Ok(single_empty);
            }
            single_empty += 1;
            if single_empty >= max_clears {
                return Ok(max_clears);
            }
        } else {
            b = e;
        }
        it.advance();
        e = it.end_key().to_vec();
    }
    Ok(single_empty)
}

/// `skipUncachedBack`: [`skip_uncached`] backward.
fn skip_uncached_back<'c>(
    it: &mut RywIter<'c>,
    end: &RywIter<'c>,
    mut iteration_limit: i32,
) -> Result<i32, Error> {
    let mut b = it.begin_key().to_vec();
    let mut e = it.end_key().to_vec();
    let mut single_empty = 0;
    debug_assert!(!it.is_unreadable() && it.is_unknown_range()?);
    while it != end && {
        iteration_limit -= 1;
        iteration_limit >= 0
    } {
        if it.is_unreadable() || it.is_empty_range()? {
            if it.is_unreadable() || !is_key_after(&e, &b) {
                while it.is_unreadable() || !it.is_unknown_range()? {
                    it.advance();
                }
                return Ok(single_empty);
            }
            single_empty += 1;
        } else {
            e = b;
        }
        it.retreat();
        b = it.begin_key().to_vec();
    }
    while it.is_unreadable() || !it.is_unknown_range()? {
        it.advance();
    }
    Ok(single_empty)
}

/// `countUncachedBack`: [`count_uncached`] backward.
fn count_uncached_back(mut it: RywIter<'_>, max_clears: i32) -> Result<i32, Error> {
    if max_clears <= 0 {
        return Ok(0);
    }
    let mut b = it.begin_key().to_vec();
    let mut e = it.end_key().to_vec();
    let mut single_empty = 0;
    while !b.is_empty() {
        if it.is_unreadable() || it.is_empty_range()? {
            if it.is_unreadable() || !is_key_after(&e, &b) {
                return Ok(single_empty);
            }
            single_empty += 1;
            if single_empty >= max_clears {
                return Ok(max_clears);
            }
        } else {
            e = b;
        }
        it.retreat();
        b = it.begin_key().to_vec();
    }
    Ok(single_empty)
}

/// `setRequestLimits`: the limits of a storage read that is to fill the caller's `limits`
/// with `additional_rows` more rows (those the selectors' offsets and the cache's single
/// empty keys will eat) and at least `offset` rows; the byte limit doubles with each
/// request, up to `REPLY_BYTE_LIMIT`.
fn set_request_limits(limits: &mut Limits, additional_rows: i64, offset: i32, request_count: u32) {
    let int_max = i64::from(i32::MAX);
    let offset = i64::from(offset);
    limits.min_rows = (1 + additional_rows).max(offset).min(int_max);
    if limits.has_row_limit() {
        limits.rows = (limits.rows.max(1) + additional_rows)
            .max(offset)
            .min(int_max);
    }
    if limits.bytes == 0 {
        limits.bytes = -1;
        if !limits.has_row_limit() {
            limits.rows = (limits.rows.max(1) + additional_rows)
                .max(offset)
                .min(int_max);
        }
    } else if limits.has_byte_limit() {
        limits.bytes = (limits.bytes << request_count.min(20)).min(REPLY_BYTE_LIMIT);
    }
}

/// `1 << min(requestCount, 20)`, 0 for the first request: how many single empty keys a
/// request may skip.
fn clear_limit(request_count: u32) -> i32 {
    if request_count == 0 {
        0
    } else {
        1 << request_count.min(20)
    }
}

/// `read(GetValueReq)`: the value of `key` as the transaction sees it.
pub(crate) async fn get<S: RywSource>(
    read: &RywRead<'_, S>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    enum First {
        Done(Option<Vec<u8>>),
        Fetch { dependent: bool },
    }
    fn iter<'c>(
        cache: &'c SnapshotCache,
        writes: &WriteMapIter,
        bypass: bool,
        key: &[u8],
    ) -> RywIter<'c> {
        let mut it = RywIter::new(cache, writes.clone());
        if bypass {
            it.bypass_unreadable_protection();
        }
        it.skip(key);
        it
    }
    let first = read.source.with_cache(|cache| -> Result<First, Error> {
        let it = iter(cache, &read.writes, read.bypass_unreadable, key);
        let dependent = it.is_dependent();
        if it.is_kv()? {
            return Ok(First::Done(it.kv()?.map(|kv| kv.value.to_vec())));
        }
        if it.is_empty_range()? {
            return Ok(First::Done(None));
        }
        Ok(First::Fetch { dependent })
    })??;
    let dependent = match first {
        First::Done(value) => return Ok(value),
        First::Fetch { dependent } => dependent,
    };
    let value = read.source.read_value(key.to_vec()).await?;
    read.source.with_cache(|cache| {
        cache.insert_key(key, value.as_deref());
        if !dependent {
            return Ok(value);
        }
        // A dependent write at the key: its operations apply to the value read.
        let it = iter(cache, &read.writes, read.bypass_unreadable, key);
        debug_assert!(it.is_kv().unwrap_or(false));
        Ok(it.kv()?.map(|kv| kv.value.to_vec()))
    })?
}

/// `read(GetKeyReq)`: the key `key` resolves to as the transaction sees it: `""` before
/// the first key, `max_read_key` after the last readable one.
pub(crate) fn get_key<'a, S: RywSource>(
    read: &'a RywRead<'_, S>,
    mut key: KeySelector,
) -> BoxFuture<'a, Result<Vec<u8>, Error>> {
    Box::pin(async move {
        if key.offset > 0 {
            let max = read.max_read_key;
            let result = get_range_value(read, key, fge(max.to_vec()), row_limit(1)).await?;
            if result.read_to_begin {
                return Ok(Vec::new());
            }
            if result.read_through_end || result.rows.is_empty() {
                return Ok(read.current_max_read_key().to_vec());
            }
            Ok(result.rows[0].key.to_vec())
        } else {
            key.offset += 1;
            let result = get_range_value_back(read, fge(Vec::new()), key, row_limit(1)).await?;
            if result.read_through_end {
                return Ok(read.current_max_read_key().to_vec());
            }
            if result.read_to_begin || result.rows.is_empty() {
                return Ok(Vec::new());
            }
            Ok(result.rows[0].key.to_vec())
        }
    })
}

/// What a synchronous step of a range read ends with.
enum Step {
    /// The read is over with this result (C++'s early `return`s).
    Return(RangeOut),
    /// The loop is over (`break`): the result is what was gathered.
    Break,
    /// Resolve the far selector with `getKey`, then go on (the read starts with it).
    ResolveFar,
    /// Resolve the far selector with `getKey`, then end the loop.
    ResolveFarAndBreak,
    /// Read from storage, then go on.
    Fetch {
        begin: KeySelector,
        end: KeySelector,
        limits: Limits,
    },
}

/// What happened since the last step.
enum Phase {
    Start,
    FarResolved(Vec<u8>),
    Fetched {
        read: RangeRead,
        begin: KeySelector,
        end: KeySelector,
    },
}

/// The state `getRangeValue` and `getRangeValueBack` keep over their waits.
struct RangeState {
    begin: KeySelector,
    end: KeySelector,
    limits: Limits,
    result: Vec<KeyValue>,
    more: bool,
    additional_rows: i64,
    /// `itemsPastEnd` (forward) / `itemsPastBegin` (reverse).
    items_past: i64,
    request_count: u32,
    read_to_begin: bool,
    read_through_end: bool,
    actual_begin_offset: i32,
    actual_end_offset: i32,
}

impl RangeState {
    fn new(begin: KeySelector, end: KeySelector, limits: Limits) -> RangeState {
        RangeState {
            actual_begin_offset: begin.offset,
            actual_end_offset: end.offset,
            begin,
            end,
            limits,
            result: Vec::new(),
            more: false,
            additional_rows: 0,
            items_past: 0,
            request_count: 0,
            read_to_begin: false,
            read_through_end: false,
        }
    }

    /// Records a storage read in the cache (`cache.insert(getKnownKeyRange(...), rows)`).
    fn insert(
        cache: &mut SnapshotCache,
        reverse: bool,
        read: &RangeRead,
        begin: &KeySelector,
        end: &KeySelector,
    ) {
        // The rows are slices of the replies (no copy): the cache shares them with the result.
        let rows: Vec<KeyValue> = read.iter_shared().collect();
        let outcome = RangeReadOutcome {
            rows,
            more: read.more(),
            read_to_begin: read.read_to_begin(),
            read_through_end: read.read_through_end(),
            read_through: read.read_through(),
        };
        fn bound(s: &KeySelector) -> SelectorBound<'_> {
            SelectorBound {
                key: &s.key,
                offset: s.offset,
            }
        }
        cache.insert_read(reverse, outcome, bound(begin), bound(end));
    }

    fn resolve(
        &mut self,
        begin_it: &mut RywIter<'_>,
        end_it: &mut RywIter<'_>,
        max: &[u8],
    ) -> Result<(), Error> {
        resolve_key_selector(
            &mut self.begin,
            begin_it,
            max,
            &mut self.read_to_begin,
            &mut self.read_through_end,
            &mut self.actual_begin_offset,
        )?;
        resolve_key_selector(
            &mut self.end,
            end_it,
            max,
            &mut self.read_to_begin,
            &mut self.read_through_end,
            &mut self.actual_end_offset,
        )
    }

    /// `getRangeValue` between two waits.
    fn step_forward(
        &mut self,
        cache: &mut SnapshotCache,
        writes: &WriteMapIter,
        max: &'static [u8],
        phase: Phase,
    ) -> Result<Step, Error> {
        if let Phase::Fetched { read, begin, end } = &phase {
            Self::insert(cache, false, read, begin, end);
        }
        let cache: &SnapshotCache = cache;
        let mut it = RywIter::new(cache, writes.clone());
        let mut it_end = it.clone();
        match phase {
            Phase::Start => {
                self.resolve(&mut it, &mut it_end, max)?;
                if self.actual_begin_offset >= self.actual_end_offset
                    && self.begin.key >= self.end.key
                {
                    return Ok(Step::Return(RangeOut::empty(false, false)));
                } else if (is_first_greater_or_equal(&self.begin) && self.begin.key == max)
                    || (is_first_greater_or_equal(&self.end) && self.end.key.is_empty())
                {
                    return Ok(Step::Return(RangeOut::empty(
                        self.read_to_begin,
                        self.read_through_end,
                    )));
                }
                if !is_first_greater_or_equal(&self.end) && self.begin.key > self.end.key {
                    return Ok(Step::ResolveFar);
                }
            }
            Phase::FarResolved(resolved_end) => {
                if resolved_end.is_empty() {
                    self.read_to_begin = true;
                }
                if resolved_end == max {
                    self.read_through_end = true;
                }
                if self.begin.key >= resolved_end && !is_backward(&self.begin) {
                    return Ok(Step::Return(RangeOut::empty(false, false)));
                } else if resolved_end.is_empty() {
                    return Ok(Step::Return(RangeOut::empty(
                        self.read_to_begin,
                        self.read_through_end,
                    )));
                }
                self.resolve(&mut it, &mut it_end, max)?;
            }
            Phase::Fetched { .. } => self.resolve(&mut it, &mut it_end, max)?,
        }

        loop {
            if self.result.is_empty()
                && self.actual_begin_offset >= self.actual_end_offset
                && self.begin.key >= self.end.key
            {
                return Ok(Step::Return(RangeOut::empty(false, false)));
            }
            if self.end.offset <= 1 && self.end.key.is_empty() {
                return Ok(Step::Return(RangeOut::empty(
                    self.read_to_begin,
                    self.read_through_end,
                )));
            }
            if (self.begin.offset >= self.end.offset && self.begin.key >= self.end.key)
                || (self.begin.offset >= 1 && self.begin.key.as_slice() >= max)
            {
                if is_first_greater_or_equal(&self.end) || self.result.is_empty() {
                    return Ok(Step::Break);
                }
                return Ok(Step::ResolveFarAndBreak);
            }
            if !it.is_unreadable() && !it.is_unknown_range()? && it.begin_key() > it_end.begin_key()
            {
                if is_first_greater_or_equal(&self.end) {
                    return Ok(Step::Break);
                }
                return Ok(Step::Return(RangeOut::empty(
                    self.read_to_begin,
                    self.read_through_end,
                )));
            }
            if self.limits.is_reached() && self.items_past >= i64::from(1 - self.end.offset) {
                return Ok(Step::Break);
            }
            if it == it_end
                && ((!it.is_unreadable() && !it.is_unknown_range()?)
                    || (self.begin.offset > 0
                        && is_first_greater_or_equal(&self.end)
                        && ExtKey::from(&self.end.key) == it.begin_key()))
            {
                return Ok(Step::Break);
            }

            if it.is_unknown_range()? {
                if self.limits.has_byte_limit()
                    && self.limits.has_satisfied_min_rows()
                    && !self.result.is_empty()
                    && self.items_past >= i64::from(1 - self.end.offset)
                {
                    self.more = true;
                    return Ok(Step::Break);
                }
                let mut uc_end = it.clone();
                let mut single_clears = 0;
                let clear_limit = clear_limit(self.request_count);
                if it.begin_key() < it_end.begin_key() {
                    single_clears =
                        skip_uncached(&mut uc_end, &it_end, clear_limit + 100)?.min(clear_limit);
                }
                let mut read_end;
                if uc_end != it_end {
                    read_end = fge(uc_end.end_key().to_vec());
                    if self.end.offset < 1 {
                        self.additional_rows += i64::from(1 - self.end.offset);
                    }
                } else if self.end.offset < 1 {
                    read_end = fge(self.end.key.clone());
                    self.additional_rows += i64::from(1 - self.end.offset);
                } else {
                    read_end = self.end.clone();
                    if self.end.offset > 1 {
                        single_clears += count_uncached(uc_end, max, clear_limit - single_clears)?;
                        read_end.offset += single_clears;
                    }
                }
                self.additional_rows += i64::from(single_clears);

                let mut read_begin;
                if is_first_greater_or_equal(&self.begin) {
                    let k = if it.begin_key() > ExtKey::from(&self.begin.key) {
                        it.begin_key().to_vec()
                    } else {
                        self.begin.key.clone()
                    };
                    self.begin = fge(k);
                    read_begin = self.begin.clone();
                } else if self.begin.offset > 1 {
                    read_begin = fge(self.begin.key.clone());
                    self.additional_rows += i64::from(self.begin.offset - 1);
                } else {
                    read_begin = self.begin.clone();
                    let single_clears = count_uncached_back(it.clone(), clear_limit)?;
                    read_begin.offset -= single_clears;
                    self.additional_rows += i64::from(single_clears);
                }
                if read_end.key < read_begin.key {
                    read_end.key = read_begin.key.clone();
                }
                let mut request = self.limits;
                set_request_limits(
                    &mut request,
                    self.additional_rows,
                    2 - read_begin.offset,
                    self.request_count,
                );
                self.request_count += 1;
                debug_assert!(!request.has_row_limit() || request.rows > 0);
                debug_assert!(request.has_row_limit() || request.has_byte_limit());
                self.additional_rows = 0;
                return Ok(Step::Fetch {
                    begin: read_begin,
                    end: read_end,
                    limits: request,
                });
            } else if it.is_kv()? {
                if it.kv()?.is_none() {
                    it.advance();
                    continue;
                }
                // Not technically correct since this would add `end.getKey()`, but that is
                // protected above (C++'s comment).
                let bound = if is_first_greater_or_equal(&self.end) {
                    self.end.key.clone()
                } else {
                    max.to_vec()
                };
                let rows = it.take_contiguous(&bound)?;
                let max_count = rows.len();
                let mut count = 0;
                while count < max_count && !self.limits.is_reached() {
                    decrement(&mut self.limits, &rows[count]);
                    count += 1;
                }
                self.items_past += (max_count - count) as i64;
                self.result.extend(rows.into_iter().take(count));
                it.advance();
            } else {
                it.advance();
            }
        }
    }

    /// The end of `getRangeValue`, after its loop.
    fn finish_forward(mut self) -> RangeOut {
        let mut more = self.more || self.limits.is_reached();
        if is_first_greater_or_equal(&self.end) {
            let keep = self
                .result
                .partition_point(|kv| &kv.key[..] < self.end.key.as_slice());
            if keep < self.result.len() {
                more = false;
            }
            self.result.truncate(keep);
        }
        RangeOut {
            rows: self.result,
            more,
            read_to_begin: self.read_to_begin,
            read_through_end: !more && self.read_through_end,
        }
    }

    /// `getRangeValueBack` between two waits: `it` walks from the end, `it_end` is at the
    /// begin.
    fn step_back(
        &mut self,
        cache: &mut SnapshotCache,
        writes: &WriteMapIter,
        max: &'static [u8],
        phase: Phase,
    ) -> Result<Step, Error> {
        if let Phase::Fetched { read, begin, end } = &phase {
            Self::insert(cache, true, read, begin, end);
        }
        let cache: &SnapshotCache = cache;
        let mut it = RywIter::new(cache, writes.clone());
        let mut it_end = it.clone();
        match phase {
            Phase::Start => {
                self.resolve(&mut it_end, &mut it, max)?;
                if self.actual_begin_offset >= self.actual_end_offset
                    && self.begin.key >= self.end.key
                {
                    return Ok(Step::Return(RangeOut::empty(false, false)));
                } else if (is_first_greater_or_equal(&self.begin) && self.begin.key == max)
                    || (is_first_greater_or_equal(&self.end) && self.end.key.is_empty())
                {
                    return Ok(Step::Return(RangeOut::empty(
                        self.read_to_begin,
                        self.read_through_end,
                    )));
                }
                if !is_first_greater_or_equal(&self.begin) && self.begin.key > self.end.key {
                    return Ok(Step::ResolveFar);
                }
            }
            Phase::FarResolved(resolved_begin) => {
                if resolved_begin.is_empty() {
                    self.read_to_begin = true;
                }
                if resolved_begin == max {
                    self.read_through_end = true;
                }
                if resolved_begin >= self.end.key && self.end.offset <= 1 {
                    return Ok(Step::Return(RangeOut::empty(false, false)));
                } else if resolved_begin == max {
                    return Ok(Step::Return(RangeOut::empty(
                        self.read_to_begin,
                        self.read_through_end,
                    )));
                }
                self.resolve(&mut it_end, &mut it, max)?;
            }
            Phase::Fetched { .. } => self.resolve(&mut it_end, &mut it, max)?,
        }

        loop {
            let begin_offset_due = i64::from(self.begin.offset - 1);
            if self.result.is_empty()
                && self.actual_begin_offset >= self.actual_end_offset
                && self.begin.key >= self.end.key
            {
                return Ok(Step::Return(RangeOut::empty(false, false)));
            }
            if !is_backward(&self.begin) && self.begin.key.as_slice() >= max {
                return Ok(Step::Return(RangeOut::empty(
                    self.read_to_begin,
                    self.read_through_end,
                )));
            }
            if (self.begin.offset >= self.end.offset && self.begin.key >= self.end.key)
                || (self.end.offset <= 1 && self.end.key.is_empty())
            {
                if is_first_greater_or_equal(&self.begin) || self.result.is_empty() {
                    return Ok(Step::Break);
                }
                return Ok(Step::ResolveFarAndBreak);
            }
            if self.items_past >= begin_offset_due
                && !it.is_unreadable()
                && !it.is_unknown_range()?
                && it.begin_key() < it_end.begin_key()
            {
                if is_first_greater_or_equal(&self.begin) {
                    return Ok(Step::Break);
                }
                return Ok(Step::Return(RangeOut::empty(
                    self.read_to_begin,
                    self.read_through_end,
                )));
            }
            if self.limits.is_reached() && self.items_past >= begin_offset_due {
                return Ok(Step::Break);
            }
            if is_first_greater_or_equal(&self.end) && ExtKey::from(&self.end.key) == it.begin_key()
            {
                if self.items_past >= begin_offset_due && it == it_end {
                    return Ok(Step::Break);
                }
                it.retreat();
            }

            if it.is_unknown_range()? {
                if self.limits.has_byte_limit()
                    && !self.result.is_empty()
                    && self.items_past >= begin_offset_due
                {
                    self.more = true;
                    return Ok(Step::Break);
                }
                let mut uc_end = it.clone();
                let mut single_clears = 0;
                let clear_limit = clear_limit(self.request_count);
                if it.begin_key() > it_end.begin_key() {
                    single_clears = skip_uncached_back(&mut uc_end, &it_end, clear_limit + 100)?
                        .min(clear_limit);
                }
                let mut read_begin;
                if uc_end != it_end {
                    read_begin = fge(uc_end.begin_key().to_vec());
                    if self.begin.offset > 1 {
                        self.additional_rows += i64::from(self.begin.offset - 1);
                    }
                } else if self.begin.offset > 1 {
                    read_begin = fge(self.begin.key.clone());
                    self.additional_rows += i64::from(self.begin.offset - 1);
                } else {
                    read_begin = self.begin.clone();
                    if self.begin.offset < 1 {
                        single_clears += count_uncached_back(uc_end, clear_limit - single_clears)?;
                        read_begin.offset -= single_clears;
                    }
                }
                self.additional_rows += i64::from(single_clears);

                let mut read_end;
                if is_first_greater_or_equal(&self.end) {
                    let k = if it.end_key() < ExtKey::from(&self.end.key) {
                        it.end_key().to_vec()
                    } else {
                        self.end.key.clone()
                    };
                    self.end = fge(k);
                    read_end = self.end.clone();
                } else if self.end.offset < 1 {
                    read_end = fge(self.end.key.clone());
                    self.additional_rows += i64::from(1 - self.end.offset);
                } else {
                    read_end = self.end.clone();
                    let single_clears = count_uncached(it.clone(), max, clear_limit)?;
                    read_end.offset += single_clears;
                    self.additional_rows += i64::from(single_clears);
                }
                if read_begin.key > read_end.key {
                    read_begin.key = read_end.key.clone();
                }
                let mut request = self.limits;
                set_request_limits(
                    &mut request,
                    self.additional_rows,
                    read_end.offset,
                    self.request_count,
                );
                self.request_count += 1;
                debug_assert!(!request.has_row_limit() || request.rows > 0);
                debug_assert!(request.has_row_limit() || request.has_byte_limit());
                self.additional_rows = 0;
                return Ok(Step::Fetch {
                    begin: read_begin,
                    end: read_end,
                    limits: request,
                });
            } else {
                let found = if it.is_kv()? { it.kv()? } else { None };
                if found.is_some() {
                    let bound = if is_first_greater_or_equal(&self.begin) {
                        self.begin.key.clone()
                    } else {
                        Vec::new()
                    };
                    // Ascending, the segment's own key/value last.
                    let rows = it.take_contiguous_back(&bound)?;
                    let max_count = rows.len();
                    let mut count = 0;
                    while count < max_count && !self.limits.is_reached() {
                        decrement(&mut self.limits, &rows[max_count - count - 1]);
                        count += 1;
                    }
                    self.items_past += (max_count - count) as i64;
                    self.result.extend(rows.iter().rev().take(count).cloned());
                }
                if it == it_end {
                    return Ok(Step::Break);
                }
                it.retreat();
            }
        }
    }

    /// The end of `getRangeValueBack`, after its loop.
    fn finish_back(mut self) -> RangeOut {
        let mut more = self.more || self.limits.is_reached();
        if is_first_greater_or_equal(&self.begin) {
            // The rows are descending: those at or after the begin come first.
            let keep = self
                .result
                .partition_point(|kv| &kv.key[..] >= self.begin.key.as_slice());
            if keep < self.result.len() {
                more = false;
            }
            self.result.truncate(keep);
        }
        RangeOut {
            rows: self.result,
            more,
            read_to_begin: !more && self.read_to_begin,
            read_through_end: self.read_through_end,
        }
    }
}

/// `getRangeValue`: the rows from `begin` up to `end` as the transaction sees them, within
/// `limits` (selectors with `orEqual` already removed or not: they are removed here).
pub(crate) fn get_range_value<'a, S: RywSource>(
    read: &'a RywRead<'_, S>,
    begin: KeySelector,
    end: KeySelector,
    limits: Limits,
) -> BoxFuture<'a, Result<RangeOut, Error>> {
    Box::pin(range_value(read, begin, end, limits, false))
}

/// `getRangeValueBack`: [`get_range_value`] read from the end; the rows are descending.
pub(crate) fn get_range_value_back<'a, S: RywSource>(
    read: &'a RywRead<'_, S>,
    begin: KeySelector,
    end: KeySelector,
    limits: Limits,
) -> BoxFuture<'a, Result<RangeOut, Error>> {
    Box::pin(range_value(read, begin, end, limits, true))
}

async fn range_value<S: RywSource>(
    read: &RywRead<'_, S>,
    begin: KeySelector,
    end: KeySelector,
    limits: Limits,
    reverse: bool,
) -> Result<RangeOut, Error> {
    // The first step is the read's synchronous start; the later ones come after a wait,
    // and C++ reads `getMaxReadKey()` again for each.
    let mut max = read.max_read_key;
    let mut state = RangeState::new(begin, end, limits);
    let mut phase = Phase::Start;
    #[cfg(test)]
    let mut steps = 0;
    loop {
        #[cfg(test)]
        {
            steps += 1;
            assert!(steps < 10_000, "a range read does not progress");
        }
        if !matches!(phase, Phase::Start) {
            max = read.current_max_read_key();
        }
        let step = read.source.with_cache(|cache| {
            if reverse {
                state.step_back(cache, &read.writes, max, phase)
            } else {
                state.step_forward(cache, &read.writes, max, phase)
            }
        })??;
        match step {
            Step::Return(out) => return Ok(out),
            Step::Break => break,
            Step::ResolveFar => {
                let far = if reverse {
                    state.begin.clone()
                } else {
                    state.end.clone()
                };
                phase = Phase::FarResolved(get_key(read, far).await?);
            }
            Step::ResolveFarAndBreak => {
                // No worry about the iterators: the loop is over (C++'s comment).
                let far = if reverse {
                    state.begin.clone()
                } else {
                    state.end.clone()
                };
                let resolved = get_key(read, far).await?;
                if resolved.is_empty() {
                    state.read_to_begin = true;
                }
                if resolved == read.current_max_read_key() {
                    state.read_through_end = true;
                }
                if reverse {
                    state.begin = fge(resolved);
                } else {
                    state.end = fge(resolved);
                }
                break;
            }
            Step::Fetch { begin, end, limits } => {
                let rows = read
                    .source
                    .read_range(begin.clone(), end.clone(), limits, reverse)
                    .await?;
                phase = Phase::Fetched {
                    read: rows,
                    begin,
                    end,
                };
            }
        }
    }
    Ok(if reverse {
        state.finish_back()
    } else {
        state.finish_forward()
    })
}

// ---- read conflict ranges ----------------------------------------------------------------

/// `sizeof(KeyRangeRef)`, as `approximateSize` counts a conflict range.
const KEY_RANGE_REF_SIZE: usize = crate::commit::KEY_RANGE_REF_SIZE as usize;

/// `updateConflictMap(key)`: the read of `key` depended on the database if the key is
/// unmodified or has dependent writes (`it` is at the key's segment).
fn update_conflict_map_key(conflicts: &mut ReadConflicts, key: &[u8], it: &WriteMapIter) {
    if it.is_unmodified_range() || (it.is_operation() && it.is_independent() == Some(false)) {
        let mut end = key.to_vec();
        end.push(0);
        // `approximateSize += 2 * key.expectedSize() + 1 + sizeof(KeyRangeRef)`.
        conflicts.add_approximate_size(2 * key.len() + 1 + KEY_RANGE_REF_SIZE);
        conflicts.insert(key, &end);
    }
}

/// `updateConflictMap(keys)`: the parts of `[begin, end)` that are unmodified or have
/// dependent writes (`it` is at `begin`'s segment).
fn update_conflict_map_range(
    conflicts: &mut ReadConflicts,
    begin: &[u8],
    end: &[u8],
    it: &mut WriteMapIter,
) {
    let end_key = ExtKey::from(end);
    while it.begin_key() < end_key {
        if it.is_unmodified_range() || (it.is_operation() && it.is_independent() == Some(false)) {
            let b = ExtKey::from(begin).max(it.begin_key()).to_vec();
            let e = end_key.min(it.end_key()).to_vec();
            if b < e {
                // `approximateSize += keys.expectedSize() + sizeof(KeyRangeRef)`: the whole
                // range read, for each part inserted.
                conflicts.add_approximate_size(begin.len() + end.len() + KEY_RANGE_REF_SIZE);
                conflicts.insert(&b, &e);
            }
        }
        if !it.advance() && it.begin_key() < end_key {
            // Past the last segment: nothing more (C++ cannot get here either).
            break;
        }
    }
}

/// `addConflictRange(GetValueReq)`.
pub(crate) fn add_get_conflict(conflicts: &mut ReadConflicts, key: &[u8], writes: &WriteMapIter) {
    let mut it = writes.clone();
    it.skip(key);
    update_conflict_map_key(conflicts, key, &it);
}

/// `ReadYourWritesTransaction::addReadConflictRange` once checked and shortened: the parts
/// of `[begin, end)` that are unmodified or have dependent writes, now.
pub(crate) fn add_range_conflict(
    conflicts: &mut ReadConflicts,
    begin: &[u8],
    end: &[u8],
    writes: &WriteMapIter,
) {
    let mut it = writes.clone();
    it.skip(begin);
    update_conflict_map_range(conflicts, begin, end, &mut it);
}

/// `KeyRangeRef(begin, end)`, which throws `inverted_range` if `begin > end`.
fn checked_range(begin: Vec<u8>, end: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>), Error> {
    if begin > end {
        return Err(Error::new(INVERTED_RANGE));
    }
    Ok((begin, end))
}

/// `addConflictRange(GetKeyReq)`: from the selector `key` (as given to `getKey`) and the key
/// it resolved to.
pub(crate) fn add_get_key_conflict(
    conflicts: &mut ReadConflicts,
    key: &KeySelector,
    result: &[u8],
    writes: &WriteMapIter,
) -> Result<(), Error> {
    let anchor = if key.or_equal {
        key_after(&key.key)
    } else {
        key.key.clone()
    };
    let (begin, end) = if key.offset <= 0 {
        checked_range(result.to_vec(), anchor)?
    } else {
        checked_range(anchor, key_after(result))?
    };
    add_range_conflict(conflicts, &begin, &end, writes);
    Ok(())
}

/// `addConflictRange(GetRangeReq<false>)` and `addConflictRange(GetRangeReq<true>)`: from
/// the range read's selectors (`orEqual` removed) and its result.
pub(crate) fn add_range_read_conflict(
    conflicts: &mut ReadConflicts,
    begin: &KeySelector,
    end: &KeySelector,
    reverse: bool,
    result: &RangeOut,
    max_read_key: &[u8],
    writes: &WriteMapIter,
) -> Result<(), Error> {
    let (mut range_begin, mut range_end);
    if begin.key < end.key {
        if reverse {
            // If the begin offset is 1 or less, no change before the rows returned can
            // change the outcome.
            range_begin = if begin.offset <= 1 && result.more {
                end.key.clone()
            } else {
                begin.key.clone()
            };
            range_end = end.key.clone();
        } else {
            range_begin = begin.key.clone();
            // If the end offset is 1 or more, no change after the rows returned can change
            // the outcome.
            range_end = if end.offset > 0 && result.more {
                begin.key.clone()
            } else {
                end.key.clone()
            };
        }
    } else {
        range_begin = end.key.clone();
        range_end = begin.key.clone();
    }
    if result.read_to_begin && begin.offset <= 0 {
        range_begin = Vec::new();
    }
    if result.read_through_end && end.offset > 0 {
        range_end = max_read_key.to_vec();
    }
    if let (Some(first), Some(last)) = (result.rows.first(), result.rows.last()) {
        if reverse {
            // The rows are descending: the last one is the lowest key.
            if last.key < range_begin {
                range_begin = last.key.to_vec();
            }
            if end.offset > 0 && range_end <= first.key {
                range_end = key_after(&first.key);
            }
        } else {
            if begin.offset <= 0 && first.key < range_begin {
                range_begin = first.key.to_vec();
            }
            if range_end <= last.key {
                range_end = key_after(&last.key);
            }
        }
    }
    let (b, e) = checked_range(range_begin, range_end)?;
    add_range_conflict(conflicts, &b, &e, writes);
    Ok(())
}

#[cfg(test)]
mod tests;
