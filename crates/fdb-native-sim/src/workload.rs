//! Workloads the simulated client runs, what they record, and the invariants checked
//! against the cluster's [log](crate::cluster::SimCluster::log) afterwards.
//!
//! * [`at_most_once`]: transactions driven by hand (`commit`, then `on_error`), each attempt
//!   writing a key of its own and adding 1 to a counter (a non-idempotent operation), half
//!   of them also read-modify-writing a hot key (so that they conflict). Every attempt's
//!   outcome is recorded: committed (with its version), not committed, or maybe committed.
//!   [`Records::check_at_most_once`]: *committed* ⇒ applied exactly once, at that version;
//!   *not committed* ⇒ never applied; *maybe* ⇒ applied at most once, and, for
//!   `commit_unknown_result` (which the client only reports after settling the commit with
//!   a dummy transaction), not applied after it was reported; no attempt applied twice; the
//!   counter equals the number of applied attempts. Each `on_error` is timed: the backoff
//!   stays within FDB's bounds (a fraction of the current backoff, which doubles up to the
//!   max retry delay, or `RESOURCE_CONSTRAINED_MAX_BACKOFF` for the resource-constrained
//!   errors; exactly `FUTURE_VERSION_RETRY_DELAY` for `future_version` and
//!   `transaction_too_old`).
//! * [`bank`]: transfers between accounts with [`Database::run`], each idempotent through a
//!   marker key checked when an earlier attempt may have committed. Every attempt records
//!   its read version and the balances it read. [`Records::check_bank`]: no transfer
//!   applied twice, every transfer `run` reported done applied once, the balances each
//!   applied attempt read equal the balances just before its commit version (no write came
//!   between its reads and its commit: serializability) and the balances at its read version
//!   (the reads were served at the right version), and the total is conserved.
//! * [`reads`]: read-only transactions that check every value and range they read against
//!   the cluster's history at their read version (linearizable reads through the location
//!   cache, shard moves and replica choice), and that their read version is at least every
//!   commit version acknowledged before they started (causal consistency).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::error::codes as client_codes;
use fdb_native_client::{
    Database, Error, KeySelector, MutationType, RangeOptions, TransactionOption, TransactionRetry,
};
use fdb_native_schema::domain::MutationType as Mt;
use fdb_native_transport::runtime::{Instant, Runtime};

use crate::cluster::{LogEntry, SimCluster};
use crate::executor::lock;
use crate::trace::TraceLevel;
use crate::{Sim, SimRng};

/// The counter every at-most-once attempt adds 1 to.
pub const COUNTER: &[u8] = b"c/count";
/// The balance every account starts with.
pub const INITIAL_BALANCE: i64 = 1000;

/// The key of at-most-once attempt `label`.
pub fn attempt_key(label: &str) -> Vec<u8> {
    format!("m/{label}").into_bytes()
}

/// Account `i`'s key.
pub fn account(i: u64) -> Vec<u8> {
    format!("b/{i:02}").into_bytes()
}

/// The marker key of transfer `xfer`.
pub fn marker(xfer: u64) -> Vec<u8> {
    format!("x/{xfer:06}").into_bytes()
}

/// Hot key `i` (read-modify-written by the at-most-once attempts).
pub fn hot_key(i: u64) -> Vec<u8> {
    format!("h/{i}").into_bytes()
}

fn balance(v: Option<&[u8]>) -> i64 {
    match v {
        Some(b) if b.len() == 8 => i64::from_le_bytes(b.try_into().expect("8 bytes")),
        _ => 0,
    }
}

/// What the client said about one commit attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `commit` returned this version.
    Committed(i64),
    /// An error that is not maybe-committed (the commit did not happen).
    NotCommitted(u16),
    /// An error that is maybe-committed.
    Maybe(u16),
}

/// One at-most-once attempt.
#[derive(Clone, Debug)]
pub struct Attempt {
    /// `worker/op/retry`.
    pub label: String,
    /// Its own key.
    pub key: Vec<u8>,
    /// What the client said.
    pub outcome: Outcome,
    /// When it said it.
    pub reported_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct BankRead {
    read_version: i64,
    from: u64,
    to: u64,
    from_balance: i64,
    to_balance: i64,
}

/// Counters of what the workloads did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkloadStats {
    /// Attempts committed.
    pub committed: u64,
    /// Attempts not committed.
    pub not_committed: u64,
    /// Attempts maybe committed.
    pub maybe: u64,
    /// Transfers done.
    pub transfers: u64,
    /// Values and ranges checked by the read workload.
    pub reads_checked: u64,
    /// Client errors seen, by code.
    pub errors: BTreeMap<u16, u64>,
    /// `on_error` waits checked.
    pub backoffs: u64,
}

impl fmt::Display for WorkloadStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "attempts {}/{}/{} (committed/not/maybe), transfers {}, reads {}, backoffs {}, errors {:?}",
            self.committed,
            self.not_committed,
            self.maybe,
            self.transfers,
            self.reads_checked,
            self.backoffs,
            self.errors
        )
    }
}

/// What the workloads observed, shared between them.
#[derive(Debug)]
pub struct Records {
    sim: Sim,
    attempts: Mutex<Vec<Attempt>>,
    bank_reads: Mutex<BTreeMap<(u64, u32), BankRead>>,
    transfers_done: Mutex<Vec<u64>>,
    max_acked: AtomicI64,
    violations: Mutex<Vec<String>>,
    stats: Mutex<WorkloadStats>,
}

impl Records {
    /// Empty records for the workloads of `sim`.
    pub fn new(sim: &Sim) -> Arc<Records> {
        Arc::new(Records {
            sim: sim.clone(),
            attempts: Mutex::new(Vec::new()),
            bank_reads: Mutex::new(BTreeMap::new()),
            transfers_done: Mutex::new(Vec::new()),
            max_acked: AtomicI64::new(0),
            violations: Mutex::new(Vec::new()),
            stats: Mutex::new(WorkloadStats::default()),
        })
    }

    /// Records a broken invariant.
    pub fn violation(&self, what: String) {
        self.sim
            .trace(TraceLevel::Events, "VIOLATION", &[], || what.clone());
        lock(&self.violations).push(what);
    }

    /// The broken invariants recorded so far.
    pub fn violations(&self) -> Vec<String> {
        lock(&self.violations).clone()
    }

    /// The counters.
    pub fn stats(&self) -> WorkloadStats {
        lock(&self.stats).clone()
    }

    /// The at-most-once attempts.
    pub fn attempts(&self) -> Vec<Attempt> {
        lock(&self.attempts).clone()
    }

    fn error(&self, e: &Error) {
        *lock(&self.stats).errors.entry(e.code()).or_insert(0) += 1;
    }

    fn acked(&self, version: i64) {
        self.max_acked.fetch_max(version, Ordering::SeqCst);
    }

    fn attempt(&self, label: String, key: Vec<u8>, outcome: Outcome) {
        let reported_at = self.sim.now();
        self.sim.trace(
            TraceLevel::Events,
            "outcome",
            &[outcome_code(outcome)],
            || format!("attempt {label}: {outcome:?}"),
        );
        {
            let mut stats = lock(&self.stats);
            match outcome {
                Outcome::Committed(_) => stats.committed += 1,
                Outcome::NotCommitted(_) => stats.not_committed += 1,
                Outcome::Maybe(_) => stats.maybe += 1,
            }
        }
        lock(&self.attempts).push(Attempt {
            label,
            key,
            outcome,
            reported_at,
        });
    }

    /// Checks the at-most-once invariants against the cluster's log (see the [module
    /// documentation](self)).
    pub fn check_at_most_once(&self, log: &[LogEntry], counter: Option<Vec<u8>>) {
        let mut applied: BTreeMap<Vec<u8>, Vec<&LogEntry>> = BTreeMap::new();
        let mut adds = 0i64;
        for entry in log {
            for m in &entry.mutations {
                if m.kind == Mt::SET_VALUE && m.param1.starts_with(b"m/") {
                    applied.entry(m.param1.clone()).or_default().push(entry);
                }
                if m.kind == Mt::ADD_VALUE && m.param1 == COUNTER {
                    adds += 1;
                }
            }
        }
        let attempts = self.attempts();
        let mut known = BTreeMap::new();
        for a in &attempts {
            known.insert(a.key.clone(), ());
            let hits = applied.get(&a.key).map_or(&[][..], Vec::as_slice);
            let versions: Vec<i64> = hits.iter().map(|e| e.version).collect();
            match a.outcome {
                Outcome::Committed(v) => {
                    if versions != [v] {
                        self.violation(format!(
                            "at-most-once: attempt {} committed at {v} but the cluster applied \
                             it at {versions:?}",
                            a.label
                        ));
                    }
                }
                Outcome::NotCommitted(code) => {
                    if !versions.is_empty() {
                        self.violation(format!(
                            "at-most-once: attempt {} was reported not committed ({code}) but \
                             the cluster applied it at {versions:?}",
                            a.label
                        ));
                    }
                }
                Outcome::Maybe(code) => {
                    if versions.len() > 1 {
                        self.violation(format!(
                            "at-most-once: attempt {} (maybe committed, {code}) applied {} \
                             times, at {versions:?}",
                            a.label,
                            versions.len()
                        ));
                    }
                    if code == client_codes::COMMIT_UNKNOWN_RESULT {
                        for e in hits {
                            if e.applied_at > a.reported_at {
                                self.violation(format!(
                                    "at-most-once: attempt {} was reported \
                                     commit_unknown_result (settled) at {:?} but applied \
                                     later, at {:?} (version {})",
                                    a.label,
                                    a.reported_at.since_origin(),
                                    e.applied_at.since_origin(),
                                    e.version
                                ));
                            }
                        }
                    }
                }
            }
        }
        for (key, hits) in &applied {
            if !known.contains_key(key) {
                self.violation(format!(
                    "at-most-once: {:?} applied at {:?} by no recorded attempt",
                    String::from_utf8_lossy(key),
                    hits.iter().map(|e| e.version).collect::<Vec<_>>()
                ));
            }
        }
        if let Some(value) = counter {
            let total = balance(Some(&value));
            if total != adds {
                self.violation(format!(
                    "at-most-once: the counter is {total} but {adds} additions were applied"
                ));
            }
        }
    }

    /// Checks the bank invariants (see the [module documentation](self)) for `accounts`
    /// accounts, against the log and the cluster's final state.
    pub fn check_bank(&self, log: &[LogEntry], cluster: &SimCluster, accounts: u64) {
        let reads = lock(&self.bank_reads).clone();
        let done = lock(&self.transfers_done).clone();
        let mut state: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        let mut applied: BTreeMap<u64, Vec<(u32, i64)>> = BTreeMap::new();
        for entry in log {
            let marker_set = entry.mutations.iter().find(|m| {
                m.kind == Mt::SET_VALUE && m.param1.starts_with(b"x/") && m.param2.len() == 4
            });
            if let Some(m) = marker_set {
                let xfer: u64 = String::from_utf8_lossy(&m.param1[2..])
                    .parse()
                    .unwrap_or(u64::MAX);
                let attempt = u32::from_le_bytes(m.param2[..4].try_into().expect("4 bytes"));
                applied
                    .entry(xfer)
                    .or_default()
                    .push((attempt, entry.version));
                match reads.get(&(xfer, attempt)) {
                    None => self.violation(format!(
                        "bank: transfer {xfer} attempt {attempt} applied at {} without a \
                         recorded read",
                        entry.version
                    )),
                    Some(r) => {
                        let before = |i: u64| state.get(&account(i)).copied().unwrap_or(0);
                        if (before(r.from), before(r.to)) != (r.from_balance, r.to_balance) {
                            self.violation(format!(
                                "bank (serializability): transfer {xfer} attempt {attempt} \
                                 read balances {:?} at {} but they were {:?} just before its \
                                 commit at {}",
                                (r.from_balance, r.to_balance),
                                r.read_version,
                                (before(r.from), before(r.to)),
                                entry.version
                            ));
                        }
                        let at_rv = |i: u64| {
                            balance(cluster.value_at(&account(i), r.read_version).as_deref())
                        };
                        if (at_rv(r.from), at_rv(r.to)) != (r.from_balance, r.to_balance) {
                            self.violation(format!(
                                "bank: transfer {xfer} attempt {attempt} read {:?} at version \
                                 {} where the cluster had {:?}",
                                (r.from_balance, r.to_balance),
                                r.read_version,
                                (at_rv(r.from), at_rv(r.to))
                            ));
                        }
                    }
                }
            }
            for m in &entry.mutations {
                if m.kind == Mt::SET_VALUE && m.param1.starts_with(b"b/") {
                    state.insert(m.param1.clone(), balance(Some(&m.param2)));
                }
            }
        }
        for (xfer, attempts) in &applied {
            if attempts.len() > 1 {
                self.violation(format!(
                    "bank: transfer {xfer} applied {} times: {attempts:?}",
                    attempts.len()
                ));
            }
        }
        for xfer in &done {
            if !applied.contains_key(xfer) {
                self.violation(format!(
                    "bank: transfer {xfer} was reported done but never applied"
                ));
            }
        }
        let final_version = cluster.max_version();
        let mut total = 0;
        for i in 0..accounts {
            let have = balance(cluster.value_at(&account(i), final_version).as_deref());
            let replayed = state.get(&account(i)).copied().unwrap_or(0);
            if have != replayed {
                self.violation(format!(
                    "bank: account {i} is {have} but replaying the log gives {replayed}"
                ));
            }
            total += have;
        }
        let expected = INITIAL_BALANCE * accounts as i64;
        if total != expected {
            self.violation(format!("bank: the total is {total}, not {expected}"));
        }
    }
}

fn outcome_code(o: Outcome) -> u64 {
    match o {
        Outcome::Committed(_) => 0,
        Outcome::NotCommitted(c) => 10_000 + u64::from(c),
        Outcome::Maybe(c) => 20_000 + u64::from(c),
    }
}

/// `RESOURCE_CONSTRAINED_MAX_BACKOFF`'s errors (`Transaction::getBackoff`).
fn resource_constrained(code: u16) -> bool {
    matches!(code, 1042 | 1078 | 1235 | 1242)
}

/// What `on_error` may wait, as FDB's `getBackoff` computes it.
#[derive(Debug)]
struct Backoff {
    backoff: Duration,
}

#[derive(Clone, Copy, Debug)]
enum Wait {
    AtMost(Duration),
    Exactly(Duration),
    Fails,
}

impl Backoff {
    const INITIAL: Duration = Duration::from_millis(10);
    const MAX: Duration = Duration::from_secs(1);
    const RESOURCE_CONSTRAINED_MAX: Duration = Duration::from_secs(30);
    const FUTURE_VERSION: Duration = Duration::from_millis(10);

    fn new() -> Backoff {
        Backoff {
            backoff: Backoff::INITIAL,
        }
    }

    fn next(&mut self, error: &Error) -> Wait {
        match error.transaction_retry() {
            Some(TransactionRetry::Backoff) => {
                let bound = self.backoff;
                let cap = if resource_constrained(error.code()) {
                    Backoff::RESOURCE_CONSTRAINED_MAX
                } else {
                    Backoff::MAX
                };
                self.backoff = (self.backoff * 2).min(cap);
                Wait::AtMost(bound)
            }
            Some(TransactionRetry::FutureVersion) => {
                Wait::Exactly(Backoff::FUTURE_VERSION.min(Backoff::MAX))
            }
            _ => Wait::Fails,
        }
    }
}

/// Sleeps a random pause between operations, to spread them over the run.
async fn pause(sim: &Sim, rng: &mut SimRng, max: Duration) {
    let d = rng.duration(Duration::ZERO, max);
    sim.sleep(d).await;
}

/// The at-most-once workload of `worker`: `ops` transactions, retried by hand.
pub async fn at_most_once<R: Runtime>(
    db: Database<R>,
    records: Arc<Records>,
    worker: u64,
    ops: u64,
    mut rng: SimRng,
) {
    let sim = records.sim.clone();
    for op in 0..ops {
        pause(&sim, &mut rng, Duration::from_millis(800)).await;
        let tx = db.create_transaction();
        let timeout = rng
            .chance(0.15)
            .then(|| rng.duration(Duration::from_millis(300), Duration::from_secs(4)));
        if let Some(t) = timeout {
            tx.set_option(TransactionOption::Timeout(t))
                .expect("a valid timeout");
        }
        let mut backoff = Backoff::new();
        for retry in 0u32.. {
            let label = format!("{worker}/{op}/{retry}");
            let key = attempt_key(&label);
            let hot = rng.chance(0.5).then(|| hot_key(rng.below(4)));
            let result = async {
                if let Some(hot) = &hot {
                    let _ = tx.get(hot).await?;
                    tx.set(hot, label.as_bytes());
                }
                tx.set(&key, b"1");
                tx.atomic_op(COUNTER, &1u64.to_le_bytes(), MutationType::Add);
                tx.commit().await
            }
            .await;
            let error = match result {
                Ok(c) => {
                    records.attempt(label, key, Outcome::Committed(c.committed_version()));
                    records.acked(c.committed_version());
                    break;
                }
                Err(e) => e,
            };
            records.error(&error);
            let outcome = if error.is_maybe_committed() {
                Outcome::Maybe(error.code())
            } else {
                Outcome::NotCommitted(error.code())
            };
            records.attempt(label.clone(), key, outcome);
            let wait = backoff.next(&error);
            let t0 = sim.now();
            let retried = tx.on_error(error.clone()).await;
            let waited = sim.now() - t0;
            match retried {
                Ok(()) => {
                    lock(&records.stats).backoffs += 1;
                    let ok = match wait {
                        Wait::AtMost(bound) => waited <= bound,
                        Wait::Exactly(d) => waited == d,
                        Wait::Fails => false,
                    };
                    if !ok {
                        records.violation(format!(
                            "backoff: on_error({}) after attempt {label} waited {waited:?}, \
                             expected {wait:?}",
                            error.code()
                        ));
                    }
                }
                Err(e) if timeout.is_some() && e.code() == client_codes::TRANSACTION_TIMED_OUT => {
                    break;
                }
                Err(e) => {
                    records.violation(format!(
                        "at-most-once: on_error({error:?}) after attempt {label} gave up with \
                         {e:?}"
                    ));
                    break;
                }
            }
        }
    }
}

/// Writes the initial balances of `accounts` accounts (and the counter).
pub async fn bank_setup<R: Runtime>(db: &Database<R>, accounts: u64) -> Result<(), Error> {
    db.run(|tx, _| async move {
        for i in 0..accounts {
            tx.set(&account(i), &INITIAL_BALANCE.to_le_bytes());
        }
        tx.set(COUNTER, &0u64.to_le_bytes());
        Ok(())
    })
    .await
}

/// The bank workload of `worker`: `ops` transfers among `accounts` accounts.
pub async fn bank<R: Runtime>(
    db: Database<R>,
    records: Arc<Records>,
    worker: u64,
    ops: u64,
    accounts: u64,
    mut rng: SimRng,
) {
    let sim = records.sim.clone();
    for op in 0..ops {
        pause(&sim, &mut rng, Duration::from_millis(800)).await;
        let xfer = worker * 1000 + op;
        let from = rng.below(accounts);
        let to = (from + 1 + rng.below(accounts - 1)) % accounts;
        let amount = rng.range(1, 100) as i64;
        let attempts = Arc::new(AtomicU32::new(0));
        let result = db
            .run(|tx, maybe| {
                let attempts = attempts.clone();
                let records = records.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if maybe.0 && tx.get(&marker(xfer)).await?.is_some() {
                        return Ok(());
                    }
                    let read_version = tx.get_read_version().await?;
                    let from_balance = balance(tx.get(&account(from)).await?.as_deref());
                    let to_balance = balance(tx.get(&account(to)).await?.as_deref());
                    tx.set(&account(from), &(from_balance - amount).to_le_bytes());
                    tx.set(&account(to), &(to_balance + amount).to_le_bytes());
                    tx.set(&marker(xfer), &attempt.to_le_bytes());
                    lock(&records.bank_reads).insert(
                        (xfer, attempt),
                        BankRead {
                            read_version,
                            from,
                            to,
                            from_balance,
                            to_balance,
                        },
                    );
                    Ok(())
                }
            })
            .await;
        match result {
            Ok(()) => {
                lock(&records.transfers_done).push(xfer);
                lock(&records.stats).transfers += 1;
            }
            Err(e) => records.violation(format!("bank: transfer {xfer} failed: {e:?}")),
        }
    }
}

/// The read workload: `ops` read-only transactions whose reads are checked against the
/// cluster's history.
pub async fn reads<R: Runtime>(
    db: Database<R>,
    records: Arc<Records>,
    cluster: SimCluster,
    ops: u64,
    accounts: u64,
    mut rng: SimRng,
) {
    let sim = records.sim.clone();
    for _ in 0..ops {
        pause(&sim, &mut rng, Duration::from_millis(800)).await;
        let floor = records.max_acked.load(Ordering::SeqCst);
        let tx = db.create_transaction();
        let ranged = rng.chance(0.3);
        let keys: Vec<Vec<u8>> = (0..rng.range(1, 3))
            .map(|_| match rng.below(3) {
                0 => hot_key(rng.below(4)),
                1 => account(rng.below(accounts)),
                _ => COUNTER.to_vec(),
            })
            .collect();
        let limit = if rng.chance(0.5) {
            0
        } else {
            rng.range(1, 4) as u32
        };
        let reverse = rng.chance(0.3);
        loop {
            let result: Result<(), Error> = async {
                let rv = tx.get_read_version().await?;
                if rv < floor {
                    records.violation(format!(
                        "causality: read version {rv} is below {floor}, a commit version \
                         acknowledged before the transaction started"
                    ));
                }
                if ranged {
                    let rows = read_all(&tx, b"b/", b"b0", limit, reverse).await?;
                    let expected = cluster.range_at(b"b/", b"b0", rv);
                    if rows != expected {
                        records.violation(format!(
                            "reads: range [b/, b0) at {rv} (limit {limit}, reverse {reverse}) \
                             returned {} rows {:?}, the cluster had {} rows {:?}",
                            rows.len(),
                            rows.iter()
                                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                                .collect::<Vec<_>>(),
                            expected.len(),
                            expected
                                .iter()
                                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                                .collect::<Vec<_>>(),
                        ));
                    }
                } else {
                    for key in &keys {
                        let got = tx.get(key).await?;
                        let expected = cluster.value_at(key, rv);
                        if got != expected {
                            records.violation(format!(
                                "reads: {:?} at {rv} is {got:?}, the cluster had {expected:?}",
                                String::from_utf8_lossy(key)
                            ));
                        }
                    }
                }
                lock(&records.stats).reads_checked += 1;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => break,
                Err(e) => {
                    records.error(&e);
                    if let Err(e) = tx.on_error(e).await {
                        records.violation(format!("reads: gave up with {e:?}"));
                        break;
                    }
                }
            }
        }
    }
}

/// Reads every row of `[begin, end)`, `limit` rows a request (0: no limit).
async fn read_all<R: Runtime>(
    tx: &fdb_native_client::Transaction<R>,
    begin: &[u8],
    end: &[u8],
    limit: u32,
    reverse: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
    let mut b = KeySelector::first_greater_or_equal(begin.to_vec());
    let mut e = KeySelector::first_greater_or_equal(end.to_vec());
    let mut out = Vec::new();
    loop {
        let mut options = RangeOptions::default();
        options.limit = limit;
        options.reverse = reverse;
        let batch = tx.get_range(&b, &e, &options).await?;
        let last = batch.kvs.last().map(|kv| kv.key.clone());
        out.extend(batch.kvs.into_iter().map(|kv| (kv.key, kv.value)));
        match last {
            Some(last) if batch.more => {
                if reverse {
                    e = KeySelector::first_greater_or_equal(last);
                } else {
                    b = KeySelector::first_greater_than(last);
                }
            }
            _ => break,
        }
    }
    if reverse {
        out.reverse();
    }
    Ok(out)
}
