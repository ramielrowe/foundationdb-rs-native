//! Range reads of a transaction: [`Transaction::get_range`] (one batch, as
//! `fdb_transaction_get_range`), and the streams of batches and of rows that follow the
//! bindings' iteration ([`RangeStream`], [`KeyValueStream`]).

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fdb_native_schema::domain::KeySelector;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::KeyValue;
use futures_core::Stream;

use super::{ReadGuard, Snapshot, Transaction};
use crate::error::Error;
use crate::error::codes::*;
use crate::read::{RangeOptions, read_conflict_range, remove_or_equal};

/// What [`Transaction::get_range`] returns: one batch of rows, in the order read (descending
/// for a reverse read), and whether the range may have more.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RangeResult {
    /// The rows.
    pub kvs: Vec<KeyValue>,
    /// `more`: the range may have rows beyond these (a limit, the streaming mode's batch
    /// size or a shard boundary cut it); `false` means it is exhausted.
    pub more: bool,
}

impl RangeResult {
    /// The rows and `more`.
    pub fn new(kvs: Vec<KeyValue>, more: bool) -> RangeResult {
        RangeResult { kvs, more }
    }
}

/// A checked range read, for [`Transaction::range_read`].
struct RangeRead {
    begin: KeySelector,
    end: KeySelector,
    options: RangeOptions,
    max_read_key: &'static [u8],
    use_provisional_proxies: bool,
    epoch: u64,
}

impl<R: Runtime> Transaction<R> {
    /// `getRange(begin, end, limit, target_bytes, mode, iteration, snapshot = false,
    /// reverse)`: one batch of the rows from the key `begin` resolves to (included) up to the
    /// key `end` resolves to (excluded), at the transaction's read version, as
    /// `fdb_transaction_get_range` returns them with read-your-writes disabled. **Does not
    /// see the transaction's own writes yet** (read-your-writes is M5; see the [module
    /// documentation](super)). [`get_ranges`](Self::get_ranges) and
    /// [`range_stream`](Self::range_stream) read the whole range.
    ///
    /// * **Limits** ([`RangeOptions`]): `fdb_c`'s. At most `limit` rows (0: no limit); about
    ///   `target_bytes` bytes, lowered to the streaming mode's batch size (each row counted
    ///   as its key and value plus 8 bytes; at least one row comes back if there is one).
    ///   With a byte target the batch ends with the first shard that returned rows.
    ///   [`more`](RangeResult::more) says whether the range may go on.
    /// * **Errors**, in `fdb_c`'s order: `exact_mode_without_limits` (2210) for
    ///   [`StreamingMode::Exact`](crate::read::StreamingMode::Exact) without a limit,
    ///   `client_invalid_operation` (2000) for the iterator mode at iteration 0; then the
    ///   deferred error, `transaction_cancelled`/`transaction_timed_out`,
    ///   `used_during_commit`; `key_outside_legal_range` (2004) for a selector key after
    ///   `\xff` (`\xff\xff` with the `ReadSystemKeys` or `AccessSystemKeys` option); then
    ///   the read's errors (`transaction_too_old`, `future_version`, ...). A selector key
    ///   longer than any key is shortened first (`KeySelectorRef::setKey`).
    /// * **Bounds** (`readThrough` of `ReadYourWrites.actor.cpp` 7.4:186-213, 8.0:195-222):
    ///   `orEqual` selectors become `(keyAfter(key), false, offset)`; an inverted range
    ///   (begin's offset at least end's and its key at least end's) is empty. A reverse read
    ///   whose end offset is above 1 resolves the end first (a `get_key`, with its read
    ///   conflict range), clamped to the readable keys. A forward read never returns keys
    ///   from `\xff` on (`\xff\xff` with the system key options): the rows from there are
    ///   dropped and `more` is then `false`.
    /// * **Read conflict range** (non-snapshot): `getRangeFinished`'s (NativeAPI
    ///   7.4:4849-4895, 8.0:2650-2702), what the result depends on: from the begin
    ///   selector's key (the first row read when a limit cut a reverse read, `""` if the
    ///   read went to the beginning of the database) to the end selector's key (after the
    ///   last row read when a limit cut a forward read, `\xff\xff` if the read went through
    ///   the end of the database); not added if empty or if the read fails. It is added when
    ///   the read ends, and a commit started meanwhile waits for it. As C++'s
    ///   `extraConflictRanges`, it does not count toward the transaction's size.
    pub async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        options: &RangeOptions,
    ) -> Result<RangeResult, Error> {
        self.get_range_with(begin, end, options, false).await
    }

    /// The batches of the rows between `begin` and `end` ([`RangeStream`]): what the
    /// bindings' range iterators read, [`get_range`](Self::get_range) after
    /// [`get_range`](Self::get_range) until the range (or `options.limit`) is exhausted,
    /// with the iteration counted from `options.iteration`. Each batch adds its read
    /// conflict range. The stream holds a handle to the transaction.
    pub fn get_ranges(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> RangeStream<R> {
        RangeStream::new(self.clone(), begin, end, options, false)
    }

    /// The rows between `begin` and `end`, one at a time ([`KeyValueStream`]): the batches
    /// of [`get_ranges`](Self::get_ranges), flattened. An error ends the stream.
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// use fdb_native_client::KeySelector;
    /// use fdb_native_client::read::RangeOptions;
    /// use std::future::poll_fn;
    /// use std::pin::Pin;
    /// use futures_core::Stream;
    ///
    /// let tx = db.create_transaction();
    /// let mut rows = tx.range_stream(
    ///     KeySelector::first_greater_or_equal(b"a".to_vec()),
    ///     KeySelector::first_greater_or_equal(b"b".to_vec()),
    ///     RangeOptions::default(),
    /// );
    /// while let Some(row) = poll_fn(|cx| Pin::new(&mut rows).poll_next(cx)).await {
    ///     let row = row?;
    ///     println!("{:?} = {:?}", row.key, row.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_stream(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> KeyValueStream<R> {
        KeyValueStream::new(self.get_ranges(begin, end, options))
    }

    pub(super) async fn get_range_with(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        options: &RangeOptions,
        snapshot: bool,
    ) -> Result<RangeResult, Error> {
        // `validate_and_update_parameters` comes before anything else.
        options.validate()?;
        let (begin, end, max_read_key, use_provisional_proxies, epoch, guard) = {
            let mut state = self.state();
            self.check_handle(&state)?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_alive(self.rt().now())?;
            state.check_not_committing()?;
            let max_read_key = state.flags.max_read_key();
            let begin = self.set_key(&state, begin);
            let end = self.set_key(&state, end);
            if begin.key.as_slice() > max_read_key || end.key.as_slice() > max_read_key {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            // `limits.isReached()` and `!limits.isValid()` cannot happen: `fdb_c`'s limits
            // are never 0 rows nor negative.
            let begin = remove_or_equal(&begin);
            let end = remove_or_equal(&end);
            if begin.offset >= end.offset && begin.key >= end.key {
                return Ok(RangeResult::default());
            }
            (
                begin,
                end,
                max_read_key,
                state.flags.use_provisional_proxies,
                state.epoch,
                (!snapshot).then(|| self.start_read(&mut state)),
            )
        };
        let read = RangeRead {
            begin,
            end,
            options: *options,
            max_read_key,
            use_provisional_proxies,
            epoch,
        };
        match guard {
            None => self.range_read(read, None).await,
            Some(guard) => {
                let tx = self.clone();
                self.spawn_read(async move {
                    let result = tx.range_read(read, Some(&guard)).await;
                    drop(guard);
                    result
                })
                .await
            }
        }
    }

    /// The part of [`get_range_with`](Self::get_range_with) that reads (and adds the read
    /// conflict range, for a non-snapshot read: `guard`).
    async fn range_read(
        &self,
        read: RangeRead,
        guard: Option<&ReadGuard<R>>,
    ) -> Result<RangeResult, Error> {
        let RangeRead {
            begin,
            end,
            options,
            max_read_key,
            use_provisional_proxies,
            epoch,
        } = read;
        let options = &options;
        let mut end = end;
        if options.reverse && end.offset > 1 {
            // `readThrough`: resolve the end first, so that a reverse read does not start in
            // keys the transaction may not read.
            let key =
                Box::pin(self.native_get_key(&end, guard, epoch, use_provisional_proxies)).await?;
            let key = if key.as_slice() > max_read_key {
                max_read_key.to_vec()
            } else {
                key
            };
            end = KeySelector::first_greater_or_equal(key);
            // `Transaction::getRangeInternal`'s inverted range.
            if begin.offset >= end.offset && begin.key >= end.key {
                return Ok(RangeResult::default());
            }
        }
        let reader = &self.inner.db.inner.reader;
        let read = self
            .guarded(epoch, async {
                let version = self.read_version(false).await?;
                let limits = options.limits(reader.schema().await?)?;
                reader
                    .get_range_with_limits(
                        &begin,
                        &end,
                        limits,
                        options.reverse,
                        version,
                        use_provisional_proxies,
                    )
                    .await
            })
            .await??;
        if let Some(guard) = guard {
            if let Some(range) = read_conflict_range(&begin, &end, options.reverse, &read) {
                self.add_read_conflict(guard, range)?;
            }
        }
        let more = read.more();
        let mut kvs = read.into_key_values();
        if !options.reverse
            && kvs
                .last()
                .is_some_and(|kv| kv.key.as_slice() >= max_read_key)
        {
            // `readThrough`: the rows in keys the transaction may not read are dropped, and
            // the range is then exhausted.
            while kvs
                .last()
                .is_some_and(|kv| kv.key.as_slice() >= max_read_key)
            {
                kvs.pop();
            }
            return Ok(RangeResult { kvs, more: false });
        }
        Ok(RangeResult { kvs, more })
    }
}

impl<R: Runtime> Snapshot<'_, R> {
    /// [`Transaction::get_range`] without a read conflict range.
    pub async fn get_range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        options: &RangeOptions,
    ) -> Result<RangeResult, Error> {
        self.tx.get_range_with(begin, end, options, true).await
    }

    /// [`Transaction::get_ranges`] without read conflict ranges.
    pub fn get_ranges(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> RangeStream<R> {
        RangeStream::new(self.tx.clone(), begin, end, options, true)
    }

    /// [`Transaction::range_stream`] without read conflict ranges.
    pub fn range_stream(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> KeyValueStream<R> {
        KeyValueStream::new(self.get_ranges(begin, end, options))
    }
}

type BatchFuture = Pin<Box<dyn Future<Output = Result<RangeResult, Error>> + Send>>;

/// The batches of a range ([`Transaction::get_ranges`]), as the bindings' range iterators
/// read them: each batch is a [`get_range`](Transaction::get_range) with the
/// [`iteration`](RangeOptions::iteration) one more than the previous one's and the row limit
/// less what was read; after a batch with [`more`](RangeResult::more), the next one starts
/// after its last row (forward: `firstGreaterThan(last)`; reverse: the end becomes
/// `firstGreaterOrEqual(last)`). The stream ends after a batch without `more`, when the row
/// limit is reached, or after an error (which it yields). Empty batches are not yielded.
pub struct RangeStream<R: Runtime> {
    tx: Transaction<R>,
    snapshot: bool,
    begin: KeySelector,
    end: KeySelector,
    options: RangeOptions,
    pending: Option<BatchFuture>,
    done: bool,
}

impl<R: Runtime> fmt::Debug for RangeStream<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeStream")
            .field("begin", &self.begin)
            .field("end", &self.end)
            .field("options", &self.options)
            .field("snapshot", &self.snapshot)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime> RangeStream<R> {
    fn new(
        tx: Transaction<R>,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
        snapshot: bool,
    ) -> RangeStream<R> {
        RangeStream {
            tx,
            snapshot,
            begin,
            end,
            options,
            pending: None,
            done: false,
        }
    }

    /// The options of the next batch (its iteration and remaining row limit).
    pub fn next_options(&self) -> &RangeOptions {
        &self.options
    }

    /// Moves past `batch`; whether there is more to read.
    fn advance(&mut self, batch: &RangeResult) -> bool {
        let Some(last) = batch.kvs.last() else {
            return false;
        };
        if !batch.more {
            return false;
        }
        if self.options.limit != 0 {
            let read = u32::try_from(batch.kvs.len()).unwrap_or(u32::MAX);
            if read >= self.options.limit {
                return false;
            }
            self.options.limit -= read;
        }
        if self.options.reverse {
            self.end = KeySelector::first_greater_or_equal(last.key.clone());
        } else {
            self.begin = KeySelector::first_greater_than(last.key.clone());
        }
        self.options.iteration = self.options.iteration.saturating_add(1);
        true
    }
}

impl<R: Runtime> Unpin for RangeStream<R> {}

impl<R: Runtime> Stream for RangeStream<R> {
    type Item = Result<RangeResult, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            let pending = this.pending.get_or_insert_with(|| {
                let tx = this.tx.clone();
                let (begin, end, options, snapshot) = (
                    this.begin.clone(),
                    this.end.clone(),
                    this.options,
                    this.snapshot,
                );
                Box::pin(async move { tx.get_range_with(&begin, &end, &options, snapshot).await })
            });
            let result = match pending.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            this.pending = None;
            match result {
                Err(e) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Ok(batch) => {
                    this.done = !this.advance(&batch);
                    if !batch.kvs.is_empty() {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
            }
        }
    }
}

/// The rows of a range, one at a time ([`Transaction::range_stream`]): the batches of a
/// [`RangeStream`], flattened.
pub struct KeyValueStream<R: Runtime> {
    batches: RangeStream<R>,
    rows: VecDeque<KeyValue>,
}

impl<R: Runtime> fmt::Debug for KeyValueStream<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyValueStream")
            .field("batches", &self.batches)
            .field("buffered", &self.rows.len())
            .finish()
    }
}

impl<R: Runtime> KeyValueStream<R> {
    fn new(batches: RangeStream<R>) -> KeyValueStream<R> {
        KeyValueStream {
            batches,
            rows: VecDeque::new(),
        }
    }
}

impl<R: Runtime> Unpin for KeyValueStream<R> {}

impl<R: Runtime> Stream for KeyValueStream<R> {
    type Item = Result<KeyValue, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(row) = this.rows.pop_front() {
                return Poll::Ready(Some(Ok(row)));
            }
            match Pin::new(&mut this.batches).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(batch))) => this.rows.extend(batch.kvs),
            }
        }
    }
}
