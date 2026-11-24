//! The stack machine (`Tester` of `bindings/python/tests/tester.py`): one per prefix,
//! `START_THREAD` starts more, and they share the database and the named transactions.
//!
//! The behaviour follows the Python tester, the reference the bindingtester compares the
//! other testers with:
//!
//! * **Errors.** An FDB error of an instruction is caught and the packed tuple
//!   `(b"ERROR", b"<code>")` is pushed with the instruction's number; anything else the
//!   Python tester would not catch (a malformed instruction, a value of the wrong type, an
//!   unknown instruction, `strinc` of a key of `0xff`s, a failed `UNIT_TESTS`) is fatal: the
//!   machine stops and the tester exits with a nonzero status.
//! * **Futures.** `COMMIT`, `ON_ERROR` and `GET_VERSIONSTAMP` push futures, waited for when
//!   they are popped (`WAIT_FUTURE`, or any instruction that pops them): they start when the
//!   instruction runs and make progress on their own. The reads push their values, as the
//!   Python tester (which waits for them at once) does.
//! * **`_DATABASE`** forms run in [`Database::run`], the retry loop `@fdb.transactional`
//!   is, and the writes push `RESULT_NOT_PRESENT`. `_SNAPSHOT` forms are snapshot reads.
//! * **Tenants** are not supported. No tenant is ever active, so the `_TENANT` forms are the
//!   `_DATABASE` ones, `TENANT_CLEAR_ACTIVE` does nothing and `TENANT_GET_ID` pushes
//!   `NO_ACTIVE_TENANT`; the other tenant instructions are fatal. The bindingtester turns
//!   them off for any tester it does not know to support them (`tenants_enabled`), and
//!   release-8.0 removed them.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use fdb_native_client::error::codes::{CLIENT_INVALID_OPERATION, NOT_COMMITTED, UNKNOWN_ERROR};
use fdb_native_client::runtime::TokioRuntime;
use fdb_native_client::{
    Database, Error, KeySelector, KeyValue, KeyValueStream, MutationType, RangeOptions,
    StreamingMode, Transaction, TransactionOption,
};
use fdb_native_tuple::{Element, Tuple};
use futures_core::Stream;
use tokio::task::JoinHandle;

use crate::Fatal;
use crate::stack::{self, Stack};
use crate::unit_tests;
use crate::values::{self, RESULT_NOT_PRESENT, bytes, error_value, into_bytes};

/// `LOG_STACK` truncates the packed items to this many bytes.
const LOG_VALUE_LIMIT: usize = 40_000;
/// `LOG_STACK` writes this many items per transaction.
const LOG_BATCH: usize = 100;

/// What the stack machines share: the database and the named transactions
/// (`Tester.tr_map`). A name maps to `None` until `NEW_TRANSACTION` (a machine's own prefix
/// is registered so when it starts, as in Python).
#[derive(Debug)]
pub(crate) struct Shared {
    db: Database,
    transactions: Mutex<HashMap<Vec<u8>, Option<Transaction>>>,
}

impl Shared {
    pub(crate) fn new(db: Database) -> Arc<Shared> {
        Arc::new(Shared {
            db,
            transactions: Mutex::new(HashMap::new()),
        })
    }

    fn transactions(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Option<Transaction>>> {
        self.transactions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// How an instruction fails: with an FDB error (pushed), or fatally.
#[derive(Debug)]
pub(crate) enum Fail {
    Fdb(Error),
    Fatal(Fatal),
}

impl From<Error> for Fail {
    fn from(e: Error) -> Fail {
        Fail::Fdb(e)
    }
}

impl From<Fatal> for Fail {
    fn from(f: Fatal) -> Fail {
        Fail::Fatal(f)
    }
}

/// The form of an instruction, from its name's suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Variant {
    /// The current transaction.
    Transaction,
    /// `_SNAPSHOT`: a snapshot read of the current transaction.
    Snapshot,
    /// `_DATABASE` (and `_TENANT`, with no tenant active): the database, in a retry loop.
    Database,
}

/// The operation and form of an instruction name (`GET_SNAPSHOT` is `GET`, snapshot).
pub(crate) fn parse_op(name: &str) -> (&str, Variant) {
    if let Some(op) = name.strip_suffix("_DATABASE") {
        (op, Variant::Database)
    } else if let Some(op) = name.strip_suffix("_TENANT") {
        (op, Variant::Database)
    } else if let Some(op) = name.strip_suffix("_SNAPSHOT") {
        (op, Variant::Snapshot)
    } else {
        (name, Variant::Transaction)
    }
}

/// What an instruction acts on.
enum Target {
    Transaction(Transaction),
    Snapshot(Transaction),
    Database(Database),
}

/// `KeySelector.first_greater_or_equal(key)`: how a plain key bounds a range.
fn first_greater_or_equal(key: Vec<u8>) -> KeySelector {
    KeySelector::first_greater_or_equal(key)
}

/// A finished future's value: `RESULT_NOT_PRESENT`, or the error.
fn done_value(result: Result<(), Error>) -> Element {
    match result {
        Ok(()) => bytes(RESULT_NOT_PRESENT),
        Err(e) => error_value(e.code()),
    }
}

/// The rows of a range stream.
async fn collect(mut stream: KeyValueStream<TokioRuntime>) -> Result<Vec<KeyValue>, Error> {
    let mut rows = Vec::new();
    while let Some(row) = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
        rows.push(row?);
    }
    Ok(rows)
}

/// `push_range`: the rows (those whose key starts with `prefix`, if given) packed as one
/// tuple `(k1, v1, k2, v2, ...)`.
fn pack_range(rows: Vec<KeyValue>, prefix: Option<&[u8]>) -> Result<Element, Fatal> {
    let mut items = Vec::with_capacity(rows.len() * 2);
    for row in rows {
        if prefix.is_none_or(|p| row.key.starts_with(p)) {
            items.push(Element::Bytes(row.key.into()));
            items.push(Element::Bytes(row.value.into()));
        }
    }
    Ok(Element::Bytes(values::pack(items)?))
}

/// The key of a transaction name in the shared map (names are stack values; the machine's
/// own prefix is a byte string).
fn name_key(name: Element) -> Result<Vec<u8>, Fatal> {
    values::pack(vec![name])
}

/// One stack machine.
#[derive(Debug)]
pub(crate) struct StackMachine {
    shared: Arc<Shared>,
    prefix: Vec<u8>,
    /// The current transaction's name (its key in [`Shared::transactions`]).
    tr_name: Vec<u8>,
    stack: Stack,
    last_version: i64,
    threads: Vec<JoinHandle<Result<(), Fatal>>>,
}

impl StackMachine {
    /// A machine for the instructions under `prefix`, using the transaction named `prefix`
    /// (registered with no transaction yet).
    pub(crate) fn new(shared: Arc<Shared>, prefix: Vec<u8>) -> Result<StackMachine, Fatal> {
        let tr_name = name_key(Element::Bytes(prefix.clone()))?;
        shared.transactions().insert(tr_name.clone(), None);
        Ok(StackMachine {
            shared,
            prefix,
            tr_name,
            stack: Stack::default(),
            last_version: 0,
            threads: Vec::new(),
        })
    }

    /// Reads the instructions and runs them, then waits for the machines `START_THREAD`
    /// started. (Boxed, with its `Send` bound spelled out: the machines it starts run it
    /// too.)
    pub(crate) fn run(self) -> Pin<Box<dyn Future<Output = Result<(), Fatal>> + Send>> {
        Box::pin(self.run_inner())
    }

    async fn run_inner(mut self) -> Result<(), Fatal> {
        let instructions = self.instructions().await?;
        let mut result = Ok(());
        for (index, row) in instructions.iter().enumerate() {
            let instruction = values::unpack(&row.value)
                .map_err(|e| e.context(format!("instruction {index}")))?;
            if let Err(e) = self.execute(index, &instruction).await {
                result = Err(e);
                break;
            }
        }
        // `[thr.join() for thr in self.threads]`: the first failure wins.
        for thread in std::mem::take(&mut self.threads) {
            let joined = thread
                .await
                .unwrap_or_else(|e| Err(Fatal::new(format!("a thread's task failed: {e}"))));
            if result.is_ok() {
                result = joined;
            }
        }
        result.map_err(|e| {
            e.context(format!(
                "machine {:?}",
                String::from_utf8_lossy(&self.prefix)
            ))
        })
    }

    /// The instructions: the range of `fdb.tuple.range((prefix,))`, read at once.
    async fn instructions(&self) -> Result<Vec<KeyValue>, Fatal> {
        let (begin, end) = values::tuple_range(vec![Element::Bytes(self.prefix.clone())])?;
        let options = RangeOptions::default().mode(StreamingMode::WantAll);
        self.shared
            .db
            .run(|tx, _| {
                collect(tx.get_ranges_keyvalues(
                    first_greater_or_equal(begin.clone()),
                    first_greater_or_equal(end.clone()),
                    options,
                ))
            })
            .await
            .map_err(|e| Fatal::new(format!("reading the instructions: {e}")))
    }

    /// Runs instruction number `index`: pushes its FDB error, if any; fails on anything
    /// else.
    pub(crate) async fn execute(&mut self, index: usize, instruction: &Tuple) -> Result<(), Fatal> {
        let name = match instruction.first() {
            Some(Element::String(name)) => name.clone(),
            other => {
                return Err(Fatal::new(format!(
                    "instruction {index}: the operation is {other:?}, not a string"
                )));
            }
        };
        let (op, variant) = parse_op(&name);
        match self.dispatch(index, op, variant, instruction).await {
            Ok(()) => Ok(()),
            Err(Fail::Fdb(e)) => {
                self.stack.push(index, error_value(e.code()));
                Ok(())
            }
            Err(Fail::Fatal(e)) => Err(e.context(format!("instruction {index} ({name})"))),
        }
    }

    // ---- the stack ---------------------------------------------------------------------

    fn push(&mut self, index: usize, value: Element) {
        self.stack.push(index, value);
    }

    async fn pop(&mut self) -> Result<Element, Fatal> {
        Ok(self.stack.pop().await?.1)
    }

    async fn pop_bytes(&mut self) -> Result<Vec<u8>, Fatal> {
        into_bytes(self.pop().await?)
    }

    async fn pop_i64(&mut self) -> Result<i64, Fatal> {
        values::to_i64(&self.pop().await?)
    }

    async fn pop_i32(&mut self) -> Result<i32, Fatal> {
        values::to_i32(&self.pop().await?)
    }

    async fn pop_count(&mut self) -> Result<usize, Fatal> {
        values::to_count(&self.pop().await?)
    }

    async fn pop_flag(&mut self) -> Result<bool, Fatal> {
        values::truthy(&self.pop().await?)
    }

    /// `N` then `N` items (`TUPLE_PACK` and friends), the top first.
    async fn pop_counted(&mut self) -> Result<Vec<Element>, Fatal> {
        let count = self.pop_count().await?;
        self.stack.pop_many(count).await
    }

    async fn pop_selector(&mut self) -> Result<KeySelector, Fatal> {
        let key = self.pop_bytes().await?;
        let or_equal = self.pop_flag().await?;
        let offset = self.pop_i32().await?;
        Ok(KeySelector::new(key, or_equal, offset))
    }

    // ---- transactions ------------------------------------------------------------------

    /// The current transaction.
    fn current(&self) -> Result<Transaction, Fatal> {
        match self.shared.transactions().get(&self.tr_name) {
            Some(Some(tx)) => Ok(tx.clone()),
            _ => Err(Fatal::new(
                "no current transaction (NEW_TRANSACTION was not run for this name)",
            )),
        }
    }

    /// `NEW_TRANSACTION`.
    fn new_transaction(&self) {
        let tx = self.shared.db.create_transaction();
        self.shared
            .transactions()
            .insert(self.tr_name.clone(), Some(tx));
    }

    /// `USE_TRANSACTION`: switches to `name`, creating its transaction if it has none.
    fn use_transaction(&mut self, name: Element) -> Result<(), Fatal> {
        self.tr_name = name_key(name)?;
        let mut transactions = self.shared.transactions();
        if !transactions.contains_key(&self.tr_name) {
            transactions.insert(
                self.tr_name.clone(),
                Some(self.shared.db.create_transaction()),
            );
        }
        Ok(())
    }

    fn target(&self, variant: Variant) -> Result<Target, Fatal> {
        Ok(match variant {
            Variant::Transaction => Target::Transaction(self.current()?),
            Variant::Snapshot => Target::Snapshot(self.current()?),
            Variant::Database => Target::Database(self.shared.db.clone()),
        })
    }

    /// The transaction of an instruction with no `_SNAPSHOT` or `_DATABASE` form.
    fn transaction(&self, variant: Variant) -> Result<Transaction, Fatal> {
        match variant {
            Variant::Transaction => self.current(),
            _ => Err(Fatal::new(
                "the instruction has no _SNAPSHOT or _DATABASE form",
            )),
        }
    }

    /// The transaction of a read with a `_SNAPSHOT` form but no `_DATABASE` one (the
    /// transaction's snapshot view reads the same way for these).
    fn read_transaction(&self, variant: Variant) -> Result<Transaction, Fatal> {
        match variant {
            Variant::Transaction | Variant::Snapshot => self.current(),
            Variant::Database => Err(Fatal::new("the instruction has no _DATABASE form")),
        }
    }

    /// A write on the current transaction, or (`_DATABASE`) in its own committed
    /// transaction, then `RESULT_NOT_PRESENT`.
    async fn write(
        &mut self,
        index: usize,
        variant: Variant,
        write: impl Fn(&Transaction),
    ) -> Result<(), Fail> {
        match self.target(variant)? {
            Target::Transaction(tx) => write(&tx),
            Target::Snapshot(_) => {
                return Err(Fatal::new("a write has no _SNAPSHOT form").into());
            }
            Target::Database(db) => {
                db.run(|tx, _| {
                    write(&tx);
                    std::future::ready(Ok::<(), Error>(()))
                })
                .await?;
                self.push(index, bytes(RESULT_NOT_PRESENT));
            }
        }
        Ok(())
    }

    /// A range read: the whole range (to the limit), batch after batch with the streaming
    /// mode as the Python binding's range iterator reads it; for `_DATABASE`, its
    /// `to_list()` (the `Iterator` mode becomes `Exact` with a limit, `WantAll` without).
    async fn read_range(
        &self,
        variant: Variant,
        begin: KeySelector,
        end: KeySelector,
        limit: i64,
        reverse: bool,
        mode: i64,
    ) -> Result<Vec<KeyValue>, Fail> {
        let target = self.target(variant)?;
        let limit = i32::try_from(limit)
            .ok()
            .and_then(|l| u32::try_from(l).ok())
            .ok_or_else(|| Fatal::new(format!("invalid row limit {limit}")))?;
        // `fdb_c` refuses an unknown mode (`validate_and_update_parameters`).
        let Some(mode) = i32::try_from(mode).ok().and_then(StreamingMode::from_code) else {
            return Err(Error::new(CLIENT_INVALID_OPERATION).into());
        };
        let mut options = RangeOptions::default()
            .limit(limit)
            .reverse(reverse)
            .mode(mode);
        let rows = match target {
            Target::Transaction(tx) => collect(tx.get_ranges_keyvalues(begin, end, options)).await,
            Target::Snapshot(tx) => {
                collect(tx.snapshot().get_ranges_keyvalues(begin, end, options)).await
            }
            Target::Database(db) => {
                if options.mode == StreamingMode::Iterator {
                    options.mode = if limit > 0 {
                        StreamingMode::Exact
                    } else {
                        StreamingMode::WantAll
                    };
                }
                db.run(|tx, _| {
                    collect(tx.get_ranges_keyvalues(begin.clone(), end.clone(), options))
                })
                .await
            }
        };
        Ok(rows?)
    }

    // ---- the instructions --------------------------------------------------------------

    async fn dispatch(
        &mut self,
        index: usize,
        op: &str,
        variant: Variant,
        instruction: &Tuple,
    ) -> Result<(), Fail> {
        match op {
            // Data operations.
            "PUSH" => {
                let item = instruction
                    .get(1)
                    .cloned()
                    .ok_or_else(|| Fatal::new("PUSH without an item"))?;
                self.push(index, item);
            }
            "DUP" => {
                let top = self.stack.top()?.clone();
                self.stack.push_item(top);
            }
            "EMPTY_STACK" => self.stack.clear(),
            "SWAP" => {
                let depth = self.pop_count().await?;
                self.stack.swap(depth)?;
            }
            "POP" => {
                self.pop().await?;
            }
            "SUB" => {
                let a = self.pop().await?;
                let b = self.pop().await?;
                self.push(index, values::sub(&a, &b)?);
            }
            "CONCAT" => {
                let a = self.pop().await?;
                let b = self.pop().await?;
                self.push(index, values::concat(a, b)?);
            }
            "WAIT_FUTURE" => {
                // Pops (waiting for a future) and pushes the value back, with the number of
                // the instruction that pushed it.
                let (number, value) = self.stack.pop().await?;
                self.stack.push(number, value);
            }
            "LOG_STACK" => self.log_stack().await?,

            // Transactions.
            "NEW_TRANSACTION" => self.new_transaction(),
            "USE_TRANSACTION" => {
                let name = self.pop().await?;
                self.use_transaction(name)?;
            }
            "ON_ERROR" => {
                let code = self.pop_i64().await?;
                // `Error::fromUnvalidatedCode`.
                let code = u16::try_from(code)
                    .ok()
                    .filter(|&c| c <= 30_000)
                    .unwrap_or(UNKNOWN_ERROR);
                let tx = self.transaction(variant)?;
                let entry =
                    stack::start(async move { done_value(tx.on_error(Error::new(code)).await) })
                        .await;
                self.stack.push_entry(index, entry);
            }
            "COMMIT" => {
                let tx = self.transaction(variant)?;
                let entry =
                    stack::start(async move { done_value(tx.commit().await.map(|_| ())) }).await;
                self.stack.push_entry(index, entry);
            }
            "RESET" => self.transaction(variant)?.reset(),
            "CANCEL" => self.transaction(variant)?.cancel(),
            "GET_READ_VERSION" => {
                self.last_version = self.read_transaction(variant)?.get_read_version().await?;
                self.push(index, bytes(b"GOT_READ_VERSION"));
            }
            "SET_READ_VERSION" => self
                .transaction(variant)?
                .set_read_version(self.last_version),
            "GET_COMMITTED_VERSION" => {
                self.last_version = self.transaction(variant)?.committed_version();
                self.push(index, bytes(b"GOT_COMMITTED_VERSION"));
            }
            "GET_APPROXIMATE_SIZE" => {
                self.transaction(variant)?.approximate_size()?;
                self.push(index, bytes(b"GOT_APPROXIMATE_SIZE"));
            }
            "GET_VERSIONSTAMP" => {
                let stamp = self.transaction(variant)?.get_versionstamp();
                let entry = stack::start(async move {
                    match stamp.await {
                        Ok(stamp) => Element::Bytes(stamp.to_vec()),
                        Err(e) => error_value(e.code()),
                    }
                })
                .await;
                self.stack.push_entry(index, entry);
            }

            // Reads.
            "GET" => {
                let key = self.pop_bytes().await?;
                let value = match self.target(variant)? {
                    Target::Transaction(tx) => tx.get(&key).await?,
                    Target::Snapshot(tx) => tx.snapshot().get(&key).await?,
                    Target::Database(db) => db.run(|tx, _| tx.get(&key)).await?,
                };
                self.push(
                    index,
                    value.map_or_else(|| bytes(RESULT_NOT_PRESENT), Element::Bytes),
                );
            }
            "GET_KEY" => {
                let selector = self.pop_selector().await?;
                let prefix = self.pop_bytes().await?;
                let key = match self.target(variant)? {
                    Target::Transaction(tx) => tx.get_key(&selector).await?,
                    Target::Snapshot(tx) => tx.snapshot().get_key(&selector).await?,
                    Target::Database(db) => db.run(|tx, _| tx.get_key(&selector)).await?,
                };
                self.push(
                    index,
                    Element::Bytes(values::clamp_to_prefix(key, &prefix)?),
                );
            }
            "GET_RANGE" => {
                let begin = self.pop_bytes().await?;
                let end = self.pop_bytes().await?;
                let limit = self.pop_i64().await?;
                let reverse = self.pop_flag().await?;
                let mode = self.pop_i64().await?;
                let rows = self
                    .read_range(
                        variant,
                        first_greater_or_equal(begin),
                        first_greater_or_equal(end),
                        limit,
                        reverse,
                        mode,
                    )
                    .await?;
                self.push(index, pack_range(rows, None)?);
            }
            "GET_RANGE_STARTS_WITH" => {
                let prefix = self.pop_bytes().await?;
                let limit = self.pop_i64().await?;
                let reverse = self.pop_flag().await?;
                let mode = self.pop_i64().await?;
                let end = values::strinc(&prefix)?;
                let rows = self
                    .read_range(
                        variant,
                        first_greater_or_equal(prefix),
                        first_greater_or_equal(end),
                        limit,
                        reverse,
                        mode,
                    )
                    .await?;
                self.push(index, pack_range(rows, None)?);
            }
            "GET_RANGE_SELECTOR" => {
                let begin = self.pop_selector().await?;
                let end = self.pop_selector().await?;
                let limit = self.pop_i64().await?;
                let reverse = self.pop_flag().await?;
                let mode = self.pop_i64().await?;
                let prefix = self.pop_bytes().await?;
                let rows = self
                    .read_range(variant, begin, end, limit, reverse, mode)
                    .await?;
                self.push(index, pack_range(rows, Some(&prefix))?);
            }
            "GET_ESTIMATED_RANGE_SIZE" => {
                let begin = self.pop_bytes().await?;
                let end = self.pop_bytes().await?;
                let tx = self.read_transaction(variant)?;
                tx.get_estimated_range_size_bytes(&begin, &end).await?;
                self.push(index, bytes(b"GOT_ESTIMATED_RANGE_SIZE"));
            }
            "GET_RANGE_SPLIT_POINTS" => {
                let begin = self.pop_bytes().await?;
                let end = self.pop_bytes().await?;
                let chunk_size = self.pop_i64().await?;
                if chunk_size <= 0 {
                    // The Python binding raises a plain exception (not an FDB error) before
                    // calling `fdb_c`; the client would fail with `client_invalid_operation`.
                    return Err(Fatal::new(format!("invalid chunk size {chunk_size}")).into());
                }
                let tx = self.read_transaction(variant)?;
                tx.get_range_split_points(&begin, &end, chunk_size).await?;
                self.push(index, bytes(b"GOT_RANGE_SPLIT_POINTS"));
            }

            // Writes.
            "SET" => {
                let key = self.pop_bytes().await?;
                let value = self.pop_bytes().await?;
                self.write(index, variant, |tx| tx.set(&key, &value))
                    .await?;
            }
            "CLEAR" => {
                let key = self.pop_bytes().await?;
                self.write(index, variant, |tx| tx.clear(&key)).await?;
            }
            "CLEAR_RANGE" => {
                let begin = self.pop_bytes().await?;
                let end = self.pop_bytes().await?;
                self.write(index, variant, |tx| tx.clear_range(&begin, &end))
                    .await?;
            }
            "CLEAR_RANGE_STARTS_WITH" => {
                let prefix = self.pop_bytes().await?;
                let end = values::strinc(&prefix)?;
                self.write(index, variant, |tx| tx.clear_range(&prefix, &end))
                    .await?;
            }
            "ATOMIC_OP" => {
                let name = match self.pop().await? {
                    Element::String(name) => name,
                    other => {
                        return Err(Fatal::new(format!("atomic operation {other:?}")).into());
                    }
                };
                let key = self.pop_bytes().await?;
                let operand = self.pop_bytes().await?;
                let op = MutationType::from_name(&name)
                    .ok_or_else(|| Fatal::new(format!("unknown atomic operation {name:?}")))?;
                self.write(index, variant, |tx| tx.atomic_op(&key, &operand, op))
                    .await?;
            }
            "READ_CONFLICT_RANGE" | "WRITE_CONFLICT_RANGE" => {
                let begin = self.pop_bytes().await?;
                let end = self.pop_bytes().await?;
                let tx = self.transaction(variant)?;
                if op == "READ_CONFLICT_RANGE" {
                    tx.add_read_conflict_range(&begin, &end)?;
                } else {
                    tx.add_write_conflict_range(&begin, &end)?;
                }
                self.push(index, bytes(b"SET_CONFLICT_RANGE"));
            }
            "READ_CONFLICT_KEY" | "WRITE_CONFLICT_KEY" => {
                let key = self.pop_bytes().await?;
                let tx = self.transaction(variant)?;
                if op == "READ_CONFLICT_KEY" {
                    tx.add_read_conflict_key(&key)?;
                } else {
                    tx.add_write_conflict_key(&key)?;
                }
                self.push(index, bytes(b"SET_CONFLICT_KEY"));
            }
            "DISABLE_WRITE_CONFLICT" => self
                .transaction(variant)?
                .set_option(TransactionOption::NextWriteNoWriteConflictRange)?,

            // Tuples.
            "TUPLE_PACK" => {
                let items = self.pop_counted().await?;
                self.push(index, Element::Bytes(values::pack(items)?));
            }
            "TUPLE_PACK_WITH_VERSIONSTAMP" => {
                let prefix = self.pop_bytes().await?;
                let items = self.pop_counted().await?;
                for value in values::pack_with_versionstamp(&prefix, items) {
                    self.push(index, value);
                }
            }
            "TUPLE_UNPACK" => {
                let packed = self.pop_bytes().await?;
                for item in values::unpack(&packed)?.into_vec() {
                    self.push(index, Element::Bytes(values::pack(vec![item])?));
                }
            }
            "TUPLE_SORT" => {
                let items = self.pop_counted().await?;
                for packed in values::sort_packed(items)? {
                    self.push(index, Element::Bytes(packed));
                }
            }
            "TUPLE_RANGE" => {
                let items = self.pop_counted().await?;
                let (begin, end) = values::tuple_range(items)?;
                self.push(index, Element::Bytes(begin));
                self.push(index, Element::Bytes(end));
            }
            "ENCODE_FLOAT" => {
                let value = values::encode_float(self.pop().await?)?;
                self.push(index, value);
            }
            "ENCODE_DOUBLE" => {
                let value = values::encode_double(self.pop().await?)?;
                self.push(index, value);
            }
            "DECODE_FLOAT" => {
                let value = values::decode_float(self.pop().await?)?;
                self.push(index, value);
            }
            "DECODE_DOUBLE" => {
                let value = values::decode_double(self.pop().await?)?;
                self.push(index, value);
            }

            // Threads.
            "START_THREAD" => {
                let prefix = self.pop_bytes().await?;
                let machine = StackMachine::new(self.shared.clone(), prefix)?;
                self.threads.push(tokio::spawn(machine.run()));
            }
            "WAIT_EMPTY" => {
                let prefix = self.pop_bytes().await?;
                let end = values::strinc(&prefix)?;
                let (begin, end) = (first_greater_or_equal(prefix), first_greater_or_equal(end));
                let options = RangeOptions::with_limit(1).mode(StreamingMode::Exact);
                // A retry loop that fails with `not_committed` while the range has keys.
                self.shared
                    .db
                    .run(|tx, _| {
                        let rows = tx.get_range(&begin, &end, &options);
                        async move {
                            if rows.await?.kvs.is_empty() {
                                Ok(())
                            } else {
                                Err(Error::new(NOT_COMMITTED))
                            }
                        }
                    })
                    .await?;
                self.push(index, bytes(b"WAITED_FOR_EMPTY"));
            }

            // Miscellaneous.
            "UNIT_TESTS" => unit_tests::run(&self.shared.db)
                .await
                .map_err(|e| Fatal::new(format!("unit tests failed: {e}")))?,
            "TENANT_CLEAR_ACTIVE" => {}
            "TENANT_GET_ID" => self.push(index, bytes(b"NO_ACTIVE_TENANT")),
            op if op.starts_with("TENANT_") => {
                return Err(Fatal::new(
                    "tenants are not supported (the bindingtester's --no-tenants turns them off)",
                )
                .into());
            }
            op if op.starts_with("DIRECTORY_") => {
                return Err(Fatal::new("the directory layer is not supported").into());
            }
            op => return Err(Fatal::new(format!("unknown operation {op:?}")).into()),
        }
        Ok(())
    }

    /// `LOG_STACK`: pops the prefix, then every item, writing
    /// `prefix + pack((stack_index, instruction_number)) = pack((item,))[:40000]`, 100 to a
    /// transaction.
    async fn log_stack(&mut self) -> Result<(), Fail> {
        let prefix = self.pop_bytes().await?;
        let mut entries = Vec::with_capacity(LOG_BATCH);
        while !self.stack.is_empty() {
            let depth = self.stack.len() - 1;
            let (number, item) = self.stack.pop().await?;
            let mut key = prefix.clone();
            key.extend(values::pack(vec![
                Element::Int(depth as i64),
                Element::Int(number as i64),
            ])?);
            let mut value = values::pack(vec![item])?;
            value.truncate(LOG_VALUE_LIMIT);
            entries.push((key, value));
            if entries.len() == LOG_BATCH {
                self.log_entries(&entries).await?;
                entries.clear();
            }
        }
        self.log_entries(&entries).await?;
        Ok(())
    }

    async fn log_entries(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Error> {
        self.shared
            .db
            .run(|tx, _| {
                for (key, value) in entries {
                    tx.set(key, value);
                }
                std::future::ready(Ok(()))
            })
            .await
    }

    /// The stack (for tests).
    #[cfg(test)]
    pub(crate) fn stack(&self) -> &Stack {
        &self.stack
    }
}

#[cfg(test)]
mod tests;
