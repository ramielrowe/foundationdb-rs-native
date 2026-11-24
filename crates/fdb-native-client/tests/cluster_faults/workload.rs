//! The workloads of the fault tests and their invariant checker.
//!
//! A [`Workload`] runs a set of tasks against a [`Database`] until it is finished:
//!
//! * **bank** workers: random transfers between accounts in `Database::run`, each writing a
//!   transfer record (its idempotency marker) in the same transaction. A retry after a maybe
//!   committed attempt reads the record first and stops if it is there.
//! * **counter** workers: `Add 1` on one counter, with a marker key in the same
//!   transaction, through `Database::run` (unlimited retries) or with `RETRY_LIMIT` 3 (the
//!   *limited* workers, whose errors come back to the caller during long faults).
//! * **raw** workers: the same increment, committed once with `Transaction::commit` and no
//!   retry, so that every commit error reaches the caller and is classified.
//! * **range readers**: the account range read at one read version, by a range stream in a
//!   random streaming mode and direction and by the reader in one unlimited read; the two
//!   must be equal, have every account (and every filler key of the spread layout) and the
//!   conserved total.
//! * **watchers**: watch a key, change it, and wait for the watch with a bound.
//!
//! Every outcome is classified as the caller sees it: committed (`Ok`), not committed (an
//! error that is not [`is_maybe_committed`](Error::is_maybe_committed)) or unknown. The
//! [checker](Workload::finish) reads the database at one version and asserts:
//!
//! * the counter equals the number of markers (no increment lost or applied twice, which a
//!   retry that ignored a maybe-committed attempt would do);
//! * every committed outcome has its marker and **no not-committed outcome has one**; the
//!   unknown ones may or may not (their split is reported);
//! * the balances are what the transfer records say (from the initial balances), so their
//!   total is conserved;
//! * no watch hung past its bound after its key changed, and none failed with an error that
//!   is not retryable; no range read saw an inconsistent snapshot or failed with an error
//!   other than retryable ones and the transaction's own timeout;
//! * the connections stayed bounded (at most one peer per process).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fdb_native_client::error::codes;
use fdb_native_client::read::{RangeLimits, RangeOptions, StreamingMode};
use fdb_native_client::{
    Database, Error, KeySelector, KeyValue, MutationType, TransactionOption, Watch,
};
use futures_core::Stream;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// The initial balance of every account.
pub(crate) const INITIAL_BALANCE: i64 = 1_000;
/// The timeout of every workload transaction (all attempts of a `run`).
pub(crate) const OP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a watch may stay pending after the commit that changed its key.
pub(crate) const WATCH_BOUND: Duration = Duration::from_secs(60);
/// The pause between two operations of a worker (keeps the load moderate on a shared host).
const PACE: Duration = Duration::from_millis(20);
/// How long the workers may take to stop once asked.
const STOP_BOUND: Duration = Duration::from_secs(120);

const COUNTER_KEY: &[u8] = b"ctr/value";
const COUNTER_MARKS: &[u8] = b"ctr/mark/";
const XFER_PREFIX: &[u8] = b"bank/xfer/";
const WATCH_PREFIX: &str = "watch/";

/// Where the accounts live.
#[derive(Clone, Debug)]
pub(crate) enum Layout {
    /// `bank/acct/<i>`, nothing else in the range.
    Plain,
    /// `k<i * stride, 8 digits>/acct`, between the `k<8 digits>` filler keys `0..filler`
    /// that `FdbCluster::write_keys("k", filler, ...)` writes: the accounts spread over every
    /// shard of that data, and a range read of `k..l` crosses them all.
    Spread { stride: usize, filler: usize },
}

/// What a workload runs.
#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) accounts: usize,
    pub(crate) layout: Layout,
    /// Bytes of padding after each balance (makes the accounts weigh on shard sizes).
    pub(crate) padding: usize,
    pub(crate) bank_workers: usize,
    pub(crate) counter_workers: usize,
    pub(crate) limited_workers: usize,
    pub(crate) raw_workers: usize,
    pub(crate) range_readers: usize,
    pub(crate) watchers: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            accounts: 20,
            layout: Layout::Plain,
            padding: 0,
            bank_workers: 3,
            counter_workers: 2,
            limited_workers: 2,
            raw_workers: 2,
            range_readers: 1,
            watchers: 4,
        }
    }
}

impl Config {
    fn account_key(&self, i: usize) -> Vec<u8> {
        match self.layout {
            Layout::Plain => format!("bank/acct/{i:04}").into_bytes(),
            Layout::Spread { stride, .. } => format!("k{:08}/acct", i * stride).into_bytes(),
        }
    }

    fn is_account_key(&self, key: &[u8]) -> bool {
        match self.layout {
            Layout::Plain => key.starts_with(b"bank/acct/"),
            Layout::Spread { .. } => key.ends_with(b"/acct"),
        }
    }

    /// The range the range readers read, and how many filler keys it has.
    fn read_range(&self) -> (Vec<u8>, Vec<u8>, usize) {
        match self.layout {
            Layout::Plain => (b"bank/acct/".to_vec(), b"bank/acct0".to_vec(), 0),
            Layout::Spread { filler, .. } => (b"k".to_vec(), b"l".to_vec(), filler),
        }
    }

    fn total(&self) -> i64 {
        INITIAL_BALANCE * self.accounts as i64
    }

    fn encode_balance(&self, balance: i64) -> Vec<u8> {
        let mut v = balance.to_le_bytes().to_vec();
        v.resize(8 + self.padding, b'.');
        v
    }
}

fn decode_balance(value: &[u8]) -> i64 {
    let bytes: [u8; 8] = value
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .unwrap_or_else(|| panic!("bad balance {value:?}"));
    i64::from_le_bytes(bytes)
}

/// The kinds of workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Kind {
    Bank,
    Counter,
    Limited,
    Raw,
    RangeRead,
    Watch,
}

const KINDS: [Kind; 6] = [
    Kind::Bank,
    Kind::Counter,
    Kind::Limited,
    Kind::Raw,
    Kind::RangeRead,
    Kind::Watch,
];

impl Kind {
    fn index(self) -> usize {
        self as usize
    }
}

/// What a write's caller was told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Committed,
    NotCommitted(u16),
    Unknown(u16),
}

impl Outcome {
    fn of<T>(result: &Result<T, Error>) -> Outcome {
        match result {
            Ok(_) => Outcome::Committed,
            Err(e) if e.is_maybe_committed() => Outcome::Unknown(e.code()),
            Err(e) => Outcome::NotCommitted(e.code()),
        }
    }
}

#[derive(Debug)]
struct Attempt {
    kind: Kind,
    marker: Vec<u8>,
    outcome: Outcome,
}

/// Statistics of the watchers.
#[derive(Debug, Default)]
struct WatchStats {
    fired: u64,
    retryable_errors: u64,
    /// Fired before the change (nothing else writes the key).
    early: u64,
    skipped_rounds: u64,
    max_latency: Duration,
    /// Violations: hung past the bound, or failed with an error that is not retryable.
    bad: Vec<String>,
}

/// Statistics of the range readers.
#[derive(Debug, Default)]
struct ReadStats {
    reads: u64,
    rows: u64,
    retries: u64,
    timeouts: u64,
    bad: Vec<String>,
}

struct Shared {
    db: Database,
    cfg: Config,
    stop: AtomicBool,
    stop_notify: Notify,
    successes: [AtomicU64; 6],
    attempts: Mutex<Vec<Attempt>>,
    /// `kind: error name (code)[ maybe committed]` -> count.
    errors: Mutex<BTreeMap<String, u64>>,
    watch: Mutex<WatchStats>,
    reads: Mutex<ReadStats>,
    max_peers: AtomicUsize,
    token: AtomicU64,
    /// Attempts of a `run` after a maybe-committed one, and how many of them found the
    /// earlier attempt's marker.
    rechecks: AtomicU64,
    rechecks_found: AtomicU64,
}

impl Shared {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    async fn until_stopped(&self) {
        loop {
            let notified = self.stop_notify.notified();
            if self.stopped() {
                return;
            }
            notified.await;
        }
    }

    /// Sleeps `d`, or less if the workload stops; whether it stopped.
    async fn pause(&self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => self.stopped(),
            _ = self.until_stopped() => true,
        }
    }

    fn error(&self, kind: Kind, e: &Error) {
        let key = format!(
            "{kind:?}: {} ({}){}",
            e.name().unwrap_or("?"),
            e.code(),
            if e.is_maybe_committed() {
                " maybe committed"
            } else {
                ""
            }
        );
        // The predicates agree with each other whatever happened.
        assert!(
            !(e.is_maybe_committed() && e.is_retryable_not_committed()),
            "{key}: maybe committed and retryable-not-committed at once"
        );
        *self.errors.lock().unwrap().entry(key).or_default() += 1;
    }

    fn success(&self, kind: Kind) {
        self.successes[kind.index()].fetch_add(1, Ordering::SeqCst);
    }

    fn record<T>(&self, kind: Kind, marker: Vec<u8>, result: &Result<T, Error>) {
        let outcome = Outcome::of(result);
        match result {
            Ok(_) => self.success(kind),
            Err(e) => self.error(kind, e),
        }
        self.attempts.lock().unwrap().push(Attempt {
            kind,
            marker,
            outcome,
        });
    }

    /// After a maybe-committed attempt: whether it committed (its marker is there).
    async fn recheck(
        &self,
        tx: &fdb_native_client::Transaction,
        marker: &[u8],
    ) -> Result<bool, Error> {
        let found = tx.get(marker).await?.is_some();
        self.rechecks.fetch_add(1, Ordering::SeqCst);
        if found {
            self.rechecks_found.fetch_add(1, Ordering::SeqCst);
        }
        Ok(found)
    }

    fn sample_peers(&self) {
        self.max_peers
            .fetch_max(self.db.peers().peer_count(), Ordering::SeqCst);
    }
}

/// A small deterministic generator (splitmix64).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// A running workload.
pub(crate) struct Workload {
    shared: Arc<Shared>,
    tasks: Vec<(String, JoinHandle<()>)>,
    seed: u64,
}

impl std::fmt::Debug for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workload")
            .field("seed", &self.seed)
            .finish()
    }
}

impl Workload {
    /// Writes the initial accounts and starts the workers.
    pub(crate) async fn start(db: &Database, cfg: Config) -> Workload {
        let seed = seed();
        let init: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.accounts)
            .map(|i| (cfg.account_key(i), cfg.encode_balance(INITIAL_BALANCE)))
            .collect();
        let init = Arc::new(init);
        db.run(|tx, _| {
            let init = init.clone();
            async move {
                tx.set_option(TransactionOption::Timeout(Duration::from_secs(60)))?;
                for (k, v) in init.iter() {
                    tx.set(k, v);
                }
                tx.set(COUNTER_KEY, &0i64.to_le_bytes());
                Ok(())
            }
        })
        .await
        .expect("initial accounts");
        let shared = Arc::new(Shared {
            db: db.clone(),
            cfg: cfg.clone(),
            stop: AtomicBool::new(false),
            stop_notify: Notify::new(),
            successes: Default::default(),
            attempts: Mutex::new(Vec::new()),
            errors: Mutex::new(BTreeMap::new()),
            watch: Mutex::new(WatchStats::default()),
            reads: Mutex::new(ReadStats::default()),
            max_peers: AtomicUsize::new(0),
            token: AtomicU64::new(0),
            rechecks: AtomicU64::new(0),
            rechecks_found: AtomicU64::new(0),
        });
        let mut tasks = Vec::new();
        let mut spawn = |name: String, f: Pin<Box<dyn std::future::Future<Output = ()> + Send>>| {
            tasks.push((name, tokio::spawn(f)));
        };
        for w in 0..cfg.bank_workers {
            let s = shared.clone();
            spawn(format!("bank {w}"), Box::pin(bank_worker(s, w, seed)));
        }
        for w in 0..cfg.counter_workers {
            let s = shared.clone();
            spawn(
                format!("counter {w}"),
                Box::pin(counter_worker(s, Kind::Counter, w)),
            );
        }
        for w in 0..cfg.limited_workers {
            let s = shared.clone();
            spawn(
                format!("limited {w}"),
                Box::pin(counter_worker(s, Kind::Limited, w)),
            );
        }
        for w in 0..cfg.raw_workers {
            let s = shared.clone();
            spawn(format!("raw {w}"), Box::pin(raw_worker(s, w)));
        }
        for w in 0..cfg.range_readers {
            let s = shared.clone();
            spawn(format!("reader {w}"), Box::pin(range_reader(s, w, seed)));
        }
        for w in 0..cfg.watchers {
            let s = shared.clone();
            spawn(format!("watcher {w}"), Box::pin(watcher(s, w, seed)));
        }
        {
            let s = shared.clone();
            spawn(
                "peer sampler".to_owned(),
                Box::pin(async move {
                    while !s.pause(Duration::from_millis(200)).await {
                        s.sample_peers();
                    }
                }),
            );
        }
        println!("workload started (seed {seed:#x}): {cfg:?}");
        Workload {
            shared,
            tasks,
            seed,
        }
    }

    /// Successes of `kind` so far.
    pub(crate) fn successes(&self, kind: Kind) -> u64 {
        self.shared.successes[kind.index()].load(Ordering::SeqCst)
    }

    fn write_successes(&self) -> u64 {
        [Kind::Bank, Kind::Counter, Kind::Limited, Kind::Raw]
            .iter()
            .map(|k| self.successes(*k))
            .sum()
    }

    /// Waits until the bank and the counter workers (unlimited retries) both commit a few
    /// more times, within `bound`; returns how long it took. Panics otherwise.
    pub(crate) async fn progress(&self, what: &str, bound: Duration) -> Duration {
        let start = Instant::now();
        let bank = self.successes(Kind::Bank);
        let counter = self.successes(Kind::Counter);
        loop {
            if self.successes(Kind::Bank) >= bank + 3
                && self.successes(Kind::Counter) >= counter + 3
            {
                let took = start.elapsed();
                println!("[{what}] progress after {took:.1?}");
                return took;
            }
            if start.elapsed() > bound {
                panic!("[{what}] no progress within {bound:?}\n{}", self.summary());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Asserts that no write of the workload commits for `window`.
    pub(crate) async fn stalled(&self, what: &str, window: Duration) {
        let before = self.write_successes();
        tokio::time::sleep(window).await;
        let after = self.write_successes();
        assert_eq!(
            before,
            after,
            "[{what}] writes committed while they should not\n{}",
            self.summary()
        );
        println!("[{what}] no write committed for {window:?}");
    }

    /// How many errors of `code` the workers of `kind` got.
    pub(crate) fn error_count(&self, kind: Kind, code: u16) -> u64 {
        let prefix = format!("{kind:?}: ");
        let suffix = format!("({code})");
        self.shared
            .errors
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix) && k.contains(&suffix))
            .map(|(_, n)| n)
            .sum()
    }

    fn summary(&self) -> String {
        let mut s = String::new();
        for kind in KINDS {
            let _ = write!(s, "{kind:?} {} ", self.successes(kind));
        }
        let _ = writeln!(s);
        for (k, n) in self.shared.errors.lock().unwrap().iter() {
            let _ = writeln!(s, "  {k}: {n}");
        }
        s
    }

    /// Stops the workers (each must stop within a bound: none may hang), then checks every
    /// invariant (see the [module documentation](self)). Panics on a violation.
    pub(crate) async fn finish(self, max_peers: usize) {
        let Workload {
            shared,
            tasks,
            seed,
        } = self;
        shared.stop.store(true, Ordering::SeqCst);
        shared.stop_notify.notify_waiters();
        for (name, task) in tasks {
            match tokio::time::timeout(STOP_BOUND, task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => panic!("worker {name} failed: {e}"),
                Err(_) => panic!("worker {name} did not stop within {STOP_BOUND:?}"),
            }
        }
        let report = check(&shared).await;
        println!("workload seed {seed:#x}\n{report}");
        let peers = shared.max_peers.load(Ordering::SeqCst);
        assert!(
            peers <= max_peers,
            "{peers} peers at once, for {max_peers} server processes"
        );
    }
}

// ---- workers --------------------------------------------------------------------------------

async fn bank_worker(s: Arc<Shared>, w: usize, seed: u64) {
    let n = s.cfg.accounts;
    let mut rng = Rng::new(seed ^ ((w as u64 + 1) * 7919));
    let mut seq = 0u64;
    while !s.pause(PACE).await {
        seq += 1;
        let from = rng.below(n);
        let to = (from + 1 + rng.below(n - 1)) % n;
        let amount = 1 + rng.below(50) as i64;
        let marker = [XFER_PREFIX, format!("{w:02}/{seq:08}").as_bytes()].concat();
        let record = format!("{from},{to},{amount}");
        let (ka, kb) = (s.cfg.account_key(from), s.cfg.account_key(to));
        let result =
            s.db.run(|tx, maybe_committed| {
                let (s, marker, record, ka, kb) = (
                    s.clone(),
                    marker.clone(),
                    record.clone(),
                    ka.clone(),
                    kb.clone(),
                );
                async move {
                    tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                    if maybe_committed.0 && s.recheck(&tx, &marker).await? {
                        return Ok(());
                    }
                    let (a, b) = tokio::join!(tx.get(&ka), tx.get(&kb));
                    let a = decode_balance(&a?.expect("account a"));
                    let b = decode_balance(&b?.expect("account b"));
                    tx.set(&ka, &s.cfg.encode_balance(a - amount));
                    tx.set(&kb, &s.cfg.encode_balance(b + amount));
                    tx.set(&marker, record.as_bytes());
                    Ok(())
                }
            })
            .await;
        s.record(Kind::Bank, marker, &result);
    }
}

/// An increment and its marker, by `Database::run` (unlimited retries, or 3 for `Limited`).
async fn counter_worker(s: Arc<Shared>, kind: Kind, w: usize) {
    let mut seq = 0u64;
    let tag = if kind == Kind::Limited { "l" } else { "c" };
    while !s.pause(PACE).await {
        seq += 1;
        let marker = [COUNTER_MARKS, format!("{tag}{w:02}/{seq:08}").as_bytes()].concat();
        let result =
            s.db.run(|tx, maybe_committed| {
                let (s, marker) = (s.clone(), marker.clone());
                async move {
                    tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                    if kind == Kind::Limited {
                        tx.set_option(TransactionOption::RetryLimit(3))?;
                    }
                    // Only an attempt after a maybe-committed one checks: if the client ever
                    // called a commit that was sent "not committed", the counter would count
                    // it twice and the checker would see it.
                    if maybe_committed.0 && s.recheck(&tx, &marker).await? {
                        return Ok(());
                    }
                    tx.atomic_op(COUNTER_KEY, &1i64.to_le_bytes(), MutationType::Add);
                    tx.set(&marker, b"");
                    Ok(())
                }
            })
            .await;
        s.record(kind, marker, &result);
    }
}

/// An increment and its marker, committed once (no retry): every commit error reaches the
/// caller.
async fn raw_worker(s: Arc<Shared>, w: usize) {
    let mut seq = 0u64;
    while !s.pause(PACE).await {
        seq += 1;
        let marker = [COUNTER_MARKS, format!("r{w:02}/{seq:08}").as_bytes()].concat();
        let tx = s.db.create_transaction();
        let result = async {
            tx.set_option(TransactionOption::Timeout(Duration::from_secs(10)))?;
            tx.atomic_op(COUNTER_KEY, &1i64.to_le_bytes(), MutationType::Add);
            tx.set(&marker, b"");
            tx.commit().await
        }
        .await;
        s.record(Kind::Raw, marker, &result);
    }
}

async fn collect_stream<S>(mut stream: S) -> Result<Vec<KeyValue>, Error>
where
    S: Stream<Item = Result<KeyValue, Error>> + Unpin,
{
    let mut out = Vec::new();
    while let Some(row) = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
        out.push(row?);
    }
    Ok(out)
}

/// What a snapshot of the read range must look like; `None` if it does.
fn snapshot_violation(cfg: &Config, rows: &[KeyValue], reverse: bool) -> Option<String> {
    let (_, _, filler) = cfg.read_range();
    let ordered = rows.windows(2).all(|w| {
        if reverse {
            w[0].key > w[1].key
        } else {
            w[0].key < w[1].key
        }
    });
    if !ordered {
        return Some("rows out of order".to_owned());
    }
    let mut accounts = 0;
    let mut fillers = 0;
    let mut total = 0i64;
    for kv in rows {
        if cfg.is_account_key(&kv.key) {
            accounts += 1;
            total += decode_balance(&kv.value);
        } else {
            fillers += 1;
        }
    }
    if accounts != cfg.accounts || fillers != filler || total != cfg.total() {
        return Some(format!(
            "{accounts} accounts (want {}), {fillers} other rows (want {filler}), total {total} \
             (want {})",
            cfg.accounts,
            cfg.total()
        ));
    }
    None
}

async fn range_reader(s: Arc<Shared>, w: usize, seed: u64) {
    let mut rng = Rng::new(seed ^ 0xabcd ^ w as u64);
    let (begin, end, _) = s.cfg.read_range();
    let fge = |k: &[u8]| KeySelector::first_greater_or_equal(k.to_vec());
    let modes = [
        StreamingMode::Iterator,
        StreamingMode::WantAll,
        StreamingMode::Small,
        StreamingMode::Medium,
        StreamingMode::Large,
        StreamingMode::Serial,
    ];
    let tx = s.db.create_transaction();
    while !s.pause(PACE * 5).await {
        let mode = modes[rng.below(modes.len())];
        let reverse = rng.below(2) == 1;
        let attempt = async {
            tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
            let version = tx.get_read_version().await?;
            let options = RangeOptions::default().mode(mode).reverse(reverse);
            let rows =
                collect_stream(tx.get_ranges_keyvalues(fge(&begin), fge(&end), options)).await?;
            // The same snapshot, by the reader in one unlimited read.
            let whole =
                s.db.reader()
                    .get_range_with_limits(
                        &fge(&begin),
                        &fge(&end),
                        RangeLimits::UNLIMITED,
                        reverse,
                        version,
                        false,
                    )
                    .await?
                    .into_key_values();
            Ok::<_, Error>((version, rows, whole))
        }
        .await;
        match attempt {
            Ok((version, rows, whole)) => {
                let mut stats = s.reads.lock().unwrap();
                stats.reads += 1;
                stats.rows += rows.len() as u64;
                if let Some(v) = snapshot_violation(&s.cfg, &rows, reverse) {
                    stats
                        .bad
                        .push(format!("{mode:?} reverse {reverse} at {version}: {v}"));
                } else if rows != whole {
                    stats.bad.push(format!(
                        "{mode:?} reverse {reverse} at {version}: the stream ({} rows) and the \
                         reader ({} rows) differ",
                        rows.len(),
                        whole.len()
                    ));
                }
                drop(stats);
                s.success(Kind::RangeRead);
                tx.reset();
            }
            Err(e) => {
                s.error(Kind::RangeRead, &e);
                let code = e.code();
                match tx.on_error(e).await {
                    Ok(()) => s.reads.lock().unwrap().retries += 1,
                    Err(e) if e.code() == codes::TRANSACTION_TIMED_OUT => {
                        s.reads.lock().unwrap().timeouts += 1;
                        tx.reset();
                    }
                    Err(e) => {
                        s.reads
                            .lock()
                            .unwrap()
                            .bad
                            .push(format!("read failed: {e} (from {code})"));
                        tx.reset();
                    }
                }
            }
        }
    }
}

/// Rounds of: watch a key, change it, wait for the watch (bounded).
async fn watcher(s: Arc<Shared>, w: usize, seed: u64) {
    let mut rng = Rng::new(seed ^ 0x5157 ^ ((w as u64) << 8));
    let key = format!("{WATCH_PREFIX}{w:02}").into_bytes();
    while !s.pause(PACE * 5).await {
        // Register.
        let registered =
            s.db.run(|tx, _| {
                let key = key.clone();
                async move {
                    tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                    tx.watch(&key).await
                }
            })
            .await;
        let mut watch = match registered {
            Ok(watch) => watch,
            Err(e) => {
                s.error(Kind::Watch, &e);
                s.watch.lock().unwrap().skipped_rounds += 1;
                continue;
            }
        };
        // A while with no change: the watch should stay pending.
        let quiet = Duration::from_millis(200 + rng.below(2000) as u64);
        tokio::select! {
            result = &mut watch => {
                let mut stats = s.watch.lock().unwrap();
                match result {
                    Ok(()) => {
                        stats.early += 1;
                        stats.bad.push(format!("watch {w} fired before its key changed"));
                    }
                    Err(e) if e.is_retryable() => stats.retryable_errors += 1,
                    Err(e) => stats.bad.push(format!("watch {w} failed before the change: {e}")),
                }
                continue;
            }
            stopped = s.pause(quiet) => if stopped { break; }
        }
        // Change the key.
        let token = s.token.fetch_add(1, Ordering::SeqCst);
        let changed =
            s.db.run(|tx, _| {
                let key = key.clone();
                async move {
                    tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                    tx.set(&key, format!("{w}/{token}").as_bytes());
                    Ok(())
                }
            })
            .await;
        if let Err(e) = changed {
            // Not changed, or not known to be: no bound to check.
            s.error(Kind::Watch, &e);
            s.watch.lock().unwrap().skipped_rounds += 1;
            continue;
        }
        let changed_at = Instant::now();
        tokio::select! {
            result = tokio::time::timeout(WATCH_BOUND, &mut watch) => {
                let mut stats = s.watch.lock().unwrap();
                match result {
                    Ok(Ok(())) => {
                        stats.fired += 1;
                        stats.max_latency = stats.max_latency.max(changed_at.elapsed());
                        drop(stats);
                        s.success(Kind::Watch);
                    }
                    Ok(Err(e)) if e.is_retryable() => {
                        stats.retryable_errors += 1;
                        drop(stats);
                        s.error(Kind::Watch, &e);
                    }
                    Ok(Err(e)) => {
                        stats.bad.push(format!("watch {w} failed after the change: {e}"));
                    }
                    Err(_) => stats.bad.push(format!(
                        "watch {w} still pending {WATCH_BOUND:?} after its key changed"
                    )),
                }
            }
            _ = s.until_stopped() => break,
        }
    }
}

// ---- watches held through a fault ----------------------------------------------------------

/// Watches registered before a fault, on keys nothing else writes, changed after it (or
/// during it): each must fire (or fail with a retryable error) within [`WATCH_BOUND`] of the
/// change, and none before it.
pub(crate) struct HeldWatches {
    name: String,
    keys: Vec<Vec<u8>>,
    watches: Vec<Watch>,
}

impl HeldWatches {
    /// `n` watches on the keys `held/<name>/<i>`.
    pub(crate) async fn register(db: &Database, name: &str, n: usize) -> HeldWatches {
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("held/{name}/{i:02}").into_bytes())
            .collect();
        let mut watches = Vec::new();
        for key in &keys {
            let watch = tokio::time::timeout(
                Duration::from_secs(60),
                db.run(|tx, _| {
                    let key = key.clone();
                    async move {
                        tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                        tx.watch(&key).await
                    }
                }),
            )
            .await
            .expect("watch registered within 60 s")
            .expect("watch registered");
            watches.push(watch);
        }
        println!("[{name}] {n} watches registered");
        HeldWatches {
            name: name.to_owned(),
            keys,
            watches,
        }
    }

    /// Changes every key in one transaction (lock-aware if `lock_aware`) and waits for every
    /// watch. Panics on a watch that fired early, failed with an error that is not
    /// retryable, or hung.
    pub(crate) async fn change_and_check(self, db: &Database, lock_aware: bool) {
        let HeldWatches {
            name,
            keys,
            mut watches,
        } = self;
        // Nothing has fired yet: nothing changed the keys.
        for (i, w) in watches.iter_mut().enumerate() {
            match tokio::time::timeout(Duration::ZERO, &mut *w).await {
                Err(_) => {}
                Ok(Ok(())) => panic!("[{name}] watch {i} fired before its key changed"),
                // Retryable: the caller would register it again (and see the change).
                Ok(Err(e)) if e.is_retryable() => println!("[{name}] watch {i} failed: {e}"),
                Ok(Err(e)) => panic!("[{name}] watch {i} failed: {e}"),
            }
        }
        let keys = Arc::new(keys);
        tokio::time::timeout(
            Duration::from_secs(60),
            db.run(|tx, _| {
                let keys = keys.clone();
                async move {
                    tx.set_option(TransactionOption::Timeout(OP_TIMEOUT))?;
                    if lock_aware {
                        tx.set_option(TransactionOption::LockAware)?;
                    }
                    for k in keys.iter() {
                        tx.set(k, b"changed");
                    }
                    Ok(())
                }
            }),
        )
        .await
        .expect("change within 60 s")
        .expect("change");
        let changed = Instant::now();
        let mut fired = 0;
        for (i, w) in watches.into_iter().enumerate() {
            let left = WATCH_BOUND.saturating_sub(changed.elapsed());
            match tokio::time::timeout(left, w).await {
                Ok(Ok(())) => fired += 1,
                Ok(Err(e)) if e.is_retryable() => println!("[{name}] watch {i} failed: {e}"),
                Ok(Err(e)) => panic!("[{name}] watch {i} failed: {e}"),
                Err(_) => {
                    panic!("[{name}] watch {i} still pending {WATCH_BOUND:?} after the change")
                }
            }
        }
        println!(
            "[{name}] {fired} of {} held watches fired, {:.1?} after the change",
            keys.len(),
            changed.elapsed()
        );
    }
}

// ---- the checker ----------------------------------------------------------------------------

async fn read_prefix(
    tx: &fdb_native_client::Transaction,
    prefix: &[u8],
) -> Result<Vec<KeyValue>, Error> {
    let mut end = prefix.to_vec();
    *end.last_mut().unwrap() += 1;
    collect_stream(tx.get_ranges_keyvalues(
        KeySelector::first_greater_or_equal(prefix.to_vec()),
        KeySelector::first_greater_or_equal(end),
        RangeOptions::default().mode(StreamingMode::WantAll),
    ))
    .await
}

struct Snapshot {
    counter: i64,
    marks: BTreeSet<Vec<u8>>,
    xfers: BTreeMap<Vec<u8>, Vec<u8>>,
    balances: Vec<i64>,
}

/// Checks the invariants; returns the report.
async fn check(s: &Shared) -> String {
    let keys: Vec<Vec<u8>> = (0..s.cfg.accounts).map(|i| s.cfg.account_key(i)).collect();
    let keys = Arc::new(keys);
    let snap = tokio::time::timeout(
        Duration::from_secs(180),
        s.db.run(|tx, _| {
            let keys = keys.clone();
            async move {
                tx.set_option(TransactionOption::Timeout(Duration::from_secs(60)))?;
                let counter = tx.get(COUNTER_KEY).await?.expect("counter");
                let marks = read_prefix(&tx, COUNTER_MARKS).await?;
                let xfers = read_prefix(&tx, XFER_PREFIX).await?;
                let mut balances = Vec::new();
                for k in keys.iter() {
                    balances.push(decode_balance(&tx.get(k).await?.expect("account")));
                }
                Ok(Snapshot {
                    counter: i64::from_le_bytes(counter[..8].try_into().unwrap()),
                    marks: marks.into_iter().map(|kv| kv.key).collect(),
                    xfers: xfers.into_iter().map(|kv| (kv.key, kv.value)).collect(),
                    balances,
                })
            }
        }),
    )
    .await
    .expect("the checker's read within 180 s")
    .expect("the checker's read");

    let mut violations = Vec::new();
    let mut text = String::new();

    // The counter: one increment per marker.
    if snap.counter != snap.marks.len() as i64 {
        violations.push(format!(
            "counter {} but {} markers: increments lost or applied twice",
            snap.counter,
            snap.marks.len()
        ));
    }

    // The bank: the balances are what the transfer records say.
    let mut expected = vec![INITIAL_BALANCE; s.cfg.accounts];
    for (key, record) in &snap.xfers {
        let record = String::from_utf8_lossy(record);
        let parts: Vec<i64> = record.split(',').map(|p| p.parse().unwrap()).collect();
        let [from, to, amount] = parts[..] else {
            panic!("bad transfer record {key:?}: {record}");
        };
        expected[from as usize] -= amount;
        expected[to as usize] += amount;
    }
    if snap.balances != expected {
        violations.push(format!(
            "balances {:?} but the transfer records say {:?}",
            snap.balances, expected
        ));
    }
    let total: i64 = snap.balances.iter().sum();
    if total != s.cfg.total() {
        violations.push(format!("total {total}, want {}", s.cfg.total()));
    }

    // Every outcome against its marker.
    let attempts = s.attempts.lock().unwrap();
    let mut per_kind: HashMap<Kind, [usize; 4]> = HashMap::new();
    let mut unknown = 0;
    let mut unknown_landed = 0;
    let mut attempted = BTreeSet::new();
    for a in attempts.iter() {
        let landed = match a.kind {
            Kind::Bank => snap.xfers.contains_key(&a.marker),
            _ => snap.marks.contains(&a.marker),
        };
        attempted.insert(a.marker.clone());
        let counts = per_kind.entry(a.kind).or_default();
        match a.outcome {
            Outcome::Committed => {
                counts[0] += 1;
                if !landed {
                    violations.push(format!(
                        "{:?} {}: committed, but its marker is missing (a lost commit)",
                        a.kind,
                        String::from_utf8_lossy(&a.marker)
                    ));
                }
            }
            Outcome::NotCommitted(code) => {
                counts[1] += 1;
                if landed {
                    violations.push(format!(
                        "{:?} {}: reported not committed ({code}), but it committed",
                        a.kind,
                        String::from_utf8_lossy(&a.marker)
                    ));
                }
            }
            Outcome::Unknown(_) => {
                counts[2] += 1;
                unknown += 1;
                if landed {
                    counts[3] += 1;
                    unknown_landed += 1;
                }
            }
        }
    }
    for marker in snap.marks.iter().chain(snap.xfers.keys()) {
        if !attempted.contains(marker) {
            violations.push(format!(
                "marker {} that no attempt wrote",
                String::from_utf8_lossy(marker)
            ));
        }
    }
    let mut kinds: Vec<_> = per_kind.into_iter().collect();
    kinds.sort();
    for (kind, [ok, not, unk, landed]) in kinds {
        let _ = writeln!(
            text,
            "{kind:?}: {ok} committed, {not} not committed, {unk} unknown ({landed} of them landed)"
        );
    }
    let _ = writeln!(
        text,
        "counter {} = {} markers; {} transfers; balances total {total}",
        snap.counter,
        snap.marks.len(),
        snap.xfers.len()
    );
    drop(attempts);
    let _ = writeln!(
        text,
        "retries after a maybe-committed attempt: {} ({} found it committed)",
        s.rechecks.load(Ordering::SeqCst),
        s.rechecks_found.load(Ordering::SeqCst)
    );

    let watch = s.watch.lock().unwrap();
    let _ = writeln!(
        text,
        "watches: {} fired (max {:.1?} after the change), {} retryable errors, {} early, {} \
         rounds skipped",
        watch.fired, watch.max_latency, watch.retryable_errors, watch.early, watch.skipped_rounds
    );
    violations.extend(watch.bad.iter().cloned());
    drop(watch);
    let reads = s.reads.lock().unwrap();
    let _ = writeln!(
        text,
        "range reads: {} consistent ({} rows), {} retries, {} timed out",
        reads.reads, reads.rows, reads.retries, reads.timeouts
    );
    violations.extend(reads.bad.iter().cloned());
    drop(reads);
    let _ = writeln!(text, "errors:");
    for (k, n) in s.errors.lock().unwrap().iter() {
        let _ = writeln!(text, "  {k}: {n}");
    }
    let _ = writeln!(
        text,
        "at most {} peers at once",
        s.max_peers.load(Ordering::SeqCst)
    );
    assert!(
        violations.is_empty(),
        "{} invariant violations:\n  {}\n{text}",
        violations.len(),
        violations.join("\n  ")
    );
    let _ = writeln!(
        text,
        "{unknown} outcomes unknown to the caller, {unknown_landed} of which committed"
    );
    text
}
