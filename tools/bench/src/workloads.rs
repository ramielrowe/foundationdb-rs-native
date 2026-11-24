//! The workloads. Each one emits one JSON value per (variant, concurrency) as it finishes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::Value;

use crate::backend::{Backend, Mode, Op, WatchFuture};
use crate::harness::{self, Iter, Measurement, Stats, closed_loop, key, value};

/// Keys per point-access keyspace (`bench/get/`, `bench/set/`, `bench/mix/`).
pub const KEYSPACE: u64 = 100_000;

/// The default suite of `fdb-bench run`.
pub const WORKLOADS: &[&str] = &["get", "get_seq", "set", "scan", "mixed", "many_tx", "watch"];

/// Workloads `run` does only when asked (`--workloads`): `grv_only` isolates the read version
/// path, `causal` is a correctness check whose cell fails on a violation.
pub const OPT_IN_WORKLOADS: &[&str] = &["grv_only", "causal"];

#[derive(Clone, Copy)]
pub struct Opts {
    pub warmup: Duration,
    pub dur: Duration,
    /// The concurrencies of `get`, `get_seq`, `set`, `mixed`, `grv_only` and `causal` (`--concs`;
    /// default [`CONCS`]).
    pub concs: &'static [usize],
    /// `causal` only (`--causal-negative`): run the negative control, which must find violations.
    pub causal_negative: bool,
}

pub async fn run<B: Backend>(
    b: B,
    name: &str,
    o: Opts,
    emit: &mut dyn FnMut(Value),
) -> Result<(), String> {
    match name {
        "load" => load(&b).await?,
        "get" => get(&b, o, emit).await,
        "get_seq" => get_seq(&b, o, emit).await,
        "set" => set(&b, o, emit).await,
        "scan" => scan(&b, o, emit).await,
        "mixed" => mixed(&b, o, emit).await,
        "many_tx" => many_tx(&b, o, emit).await,
        "watch" => watch(&b, emit).await?,
        "causal" => causal(&b, o, emit).await?,
        "grv_only" => grv_only(&b, o, emit).await,
        other => return Err(format!("unknown workload {other}")),
    }
    Ok(())
}

/// Fills the keyspaces. Idempotent: skipped when the last key of the last keyspace exists.
async fn load<B: Backend>(b: &B) -> Result<(), String> {
    let spaces: [(&str, u64); 5] = [
        ("get", KEYSPACE),
        ("set", KEYSPACE),
        ("mix", KEYSPACE),
        ("scan10k", 10_000),
        ("scan100k", 100_000),
    ];
    if b.get(&key("scan100k", 99_999)).await? {
        eprintln!("load: already loaded");
        return Ok(());
    }
    let t = Instant::now();
    for (space, n) in spaces {
        let chunks: Vec<(u64, u64)> = (0..n)
            .step_by(2000)
            .map(|s| (s, (s + 2000).min(n)))
            .collect();
        let results: Vec<Result<(), String>> = futures::stream::iter(chunks)
            .map(|(s, e)| {
                let b = b.clone();
                async move {
                    let ops: Vec<Op> = (s..e).map(|i| Op::Set(key(space, i), value(i))).collect();
                    b.run_ops(&ops).await
                }
            })
            .buffer_unordered(8)
            .collect()
            .await;
        for r in results {
            r?;
        }
    }
    eprintln!("load: {:.1}s", t.elapsed().as_secs_f64());
    Ok(())
}

fn emit_m(emit: &mut dyn FnMut(Value), m: &Measurement, w: &str, variant: &str, conc: usize) {
    emit(m.json(w, variant, conc));
}

pub const CONCS: &[usize] = &[1, 16, 256];

/// A fresh transaction per get: the read version and the read.
async fn get<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for &conc in o.concs {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |ctx| {
            let k = key("get", ctx.rng.below(KEYSPACE));
            let b = b.clone();
            async move {
                b.get(&k).await?;
                Ok(Iter::ops(1))
            }
        })
        .await;
        emit_m(emit, &m, "get", "fresh_tx", conc);
    }
}

/// 100 gets in one transaction: the read version is paid once, so this is the storage read
/// path. The latency is per get (the first get of each transaction is not sampled).
async fn get_seq<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for &conc in o.concs {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |ctx| {
            let keys: Vec<Vec<u8>> = (0..100)
                .map(|_| key("get", ctx.rng.below(KEYSPACE)))
                .collect();
            let b = b.clone();
            async move {
                let lat = b.get_seq(&keys).await?;
                Ok(Iter {
                    ops: lat.len() as u64,
                    samples: Some(lat),
                })
            }
        })
        .await;
        emit_m(emit, &m, "get_seq", "100_per_tx", conc);
    }
}

async fn set<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for &conc in o.concs {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |ctx| {
            let k = key("set", ctx.rng.below(KEYSPACE));
            let v = value(ctx.rng.next());
            let b = b.clone();
            async move {
                b.set_commit(&k, &v).await?;
                Ok(Iter::ops(1))
            }
        })
        .await;
        emit_m(emit, &m, "set", "1_key_commit", conc);
    }
}

async fn scan<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for (space, n) in [("scan10k", 10_000u64), ("scan100k", 100_000)] {
        for mode in [Mode::WantAll, Mode::Iterator] {
            let b2 = b.clone();
            let (begin, end) = (key(space, 0), key(space, n)); // end: past the last key
            let m = closed_loop(1, o.warmup, o.dur, move |_ctx| {
                let (b, begin, end) = (b2.clone(), begin.clone(), end.clone());
                async move {
                    let (rows, _bytes) = b.scan(&begin, &end, mode).await?;
                    if rows != n {
                        return Err(format!("scan returned {rows} rows, want {n}"));
                    }
                    Ok(Iter::ops(rows))
                }
            })
            .await;
            // ops_per_s is rows/s; the latency samples are whole scans.
            let variant = format!("{}/{}", &space[4..], mode.name());
            emit_m(emit, &m, "scan", &variant, 1);
        }
    }
}

/// 90% read transactions (10 gets) and 10% write transactions (5 gets + 5 sets, commit).
/// One iteration is one transaction.
async fn mixed<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for &conc in o.concs {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |ctx| {
            let write_tx = ctx.rng.below(100) < 10;
            let ops: Vec<Op> = (0..10)
                .map(|j| {
                    let k = key("mix", ctx.rng.below(KEYSPACE));
                    if write_tx && j >= 5 {
                        Op::Set(k, value(j))
                    } else {
                        Op::Get(k)
                    }
                })
                .collect();
            let b = b.clone();
            async move {
                b.run_ops(&ops).await?;
                Ok(Iter::ops(1))
            }
        })
        .await;
        emit_m(emit, &m, "mixed", "90r_10w_10ops", conc);
    }
}

/// Many transactions in flight at once: 1000 and 5000 tasks, each running 5-op transactions
/// (3 gets, 2 sets) back to back. The RSS column is the process's peak.
async fn many_tx<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for conc in [1000, 5000] {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |ctx| {
            let ops: Vec<Op> = (0..5)
                .map(|j| {
                    let k = key("mix", ctx.rng.below(KEYSPACE));
                    if j >= 3 {
                        Op::Set(k, value(j))
                    } else {
                        Op::Get(k)
                    }
                })
                .collect();
            let b = b.clone();
            async move {
                b.run_ops(&ops).await?;
                Ok(Iter::ops(1))
            }
        })
        .await;
        emit_m(emit, &m, "many_tx", "3r_2w_commit", conc);
    }
}

fn stats(mut v: Vec<u32>) -> Stats {
    v.sort_unstable();
    Stats {
        ops: v.len() as u64,
        iters: v.len() as u64,
        errors: 0,
        all_iters: 0,
        samples: v,
    }
}

fn manual(secs: f64, stats: Stats, cpu0: harness::Cpu) -> Measurement {
    let c1 = harness::cpu();
    Measurement {
        secs,
        stats,
        cpu_user: c1.user - cpu0.user,
        cpu_sys: c1.sys - cpu0.sys,
        maxrss_kb: c1.maxrss_kb,
    }
}

async fn watch<B: Backend>(b: &B, emit: &mut dyn FnMut(Value)) -> Result<(), String> {
    // One watch at a time: setup latency (a transaction with the watch, until its commit is
    // done), the writer's commit latency, and the time from the start of the writer's commit
    // to the watch firing.
    let iters = 400usize;
    let warm = 40usize;
    let (mut setup, mut fire, mut writer) = (Vec::new(), Vec::new(), Vec::new());
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ std::process::id() as u64;
    let c0 = harness::cpu();
    let t_all = Instant::now();
    for i in 0..iters {
        let k = key(&format!("watch{nonce:x}"), i as u64);
        let t = Instant::now();
        let w = b.watch(&k).await?;
        let s = t.elapsed();
        let t = Instant::now();
        b.set_commit(&k, &value(i as u64)).await?;
        let c = t.elapsed();
        tokio::time::timeout(Duration::from_secs(30), w)
            .await
            .map_err(|_| "watch did not fire in 30s".to_string())??;
        let f = t.elapsed();
        if i >= warm {
            setup.push(s.as_micros() as u32);
            writer.push(c.as_micros() as u32);
            fire.push(f.as_micros() as u32);
        }
    }
    let secs = t_all.elapsed().as_secs_f64();
    emit_m(emit, &manual(secs, stats(setup), c0), "watch", "setup", 1);
    let c = harness::cpu();
    emit_m(
        emit,
        &manual(secs, stats(writer), c),
        "watch",
        "writer_commit",
        1,
    );
    emit_m(
        emit,
        &manual(secs, stats(fire), c),
        "watch",
        "write_to_fire",
        1,
    );

    // Many watches: 5000 setups at concurrency 256, then one transaction writes all the keys;
    // the samples are, per watch, the time from the start of that write until it fired.
    let total = 5000usize;
    let keys: Vec<Vec<u8>> = (0..total)
        .map(|i| key(&format!("watchm{nonce:x}"), i as u64))
        .collect();
    let c0 = harness::cpu();
    let t = Instant::now();
    let setups: Vec<Result<(u32, WatchFuture), String>> =
        futures::stream::iter(keys.iter().cloned())
            .map(|k| {
                let b = b.clone();
                async move {
                    let t = Instant::now();
                    let w = b.watch(&k).await?;
                    Ok((t.elapsed().as_micros() as u32, w))
                }
            })
            .buffer_unordered(256)
            .collect()
            .await;
    let secs = t.elapsed().as_secs_f64();
    let (mut lat, mut futs) = (Vec::new(), Vec::new());
    for r in setups {
        let (us, w) = r?;
        lat.push(us);
        futs.push(w);
    }
    emit_m(
        emit,
        &manual(secs, stats(lat), c0),
        "watch",
        "setup_5000_c256",
        256,
    );

    let c0 = harness::cpu();
    let t = Instant::now();
    let ops: Vec<Op> = keys.iter().map(|k| Op::Set(k.clone(), value(7))).collect();
    let writer = {
        let b = b.clone();
        tokio::spawn(async move { b.run_ops(&ops).await })
    };
    let fired: Vec<Result<u32, String>> = futures::stream::iter(futs)
        .map(|w| async move {
            tokio::time::timeout(Duration::from_secs(60), w)
                .await
                .map_err(|_| "watch did not fire in 60s".to_string())??;
            Ok(t.elapsed().as_micros() as u32)
        })
        .buffer_unordered(total)
        .collect()
        .await;
    writer.await.map_err(|e| e.to_string())??;
    let secs = t.elapsed().as_secs_f64();
    let lat: Vec<u32> = fired.into_iter().collect::<Result<_, _>>()?;
    emit_m(
        emit,
        &manual(secs, stats(lat), c0),
        "watch",
        "fire_5000",
        total,
    );
    Ok(())
}

/// `grv_only`: a fresh transaction per iteration that only gets its read version, the part of
/// `get` that is the proxy's and the client's batching, without a storage read.
async fn grv_only<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for &conc in o.concs {
        let b = b.clone();
        let m = closed_loop(conc, o.warmup, o.dur, move |_ctx| {
            let b = b.clone();
            async move {
                b.grv().await?;
                Ok(Iter::ops(1))
            }
        })
        .await;
        emit_m(emit, &m, "grv_only", "fresh_tx", conc);
    }
}

/// Writers and readers of the causal check (each a task of the runtime).
const CAUSAL_WRITERS: usize = 16;
const CAUSAL_READERS: usize = 16;

/// The counters of one `causal` run.
#[derive(Default)]
struct CausalCounts {
    background_gets: AtomicU64,
    same_task_checks: AtomicU64,
    cross_task_checks: AtomicU64,
    violations: AtomicU64,
    rv_backwards: AtomicU64,
    errors: AtomicU64,
}

impl CausalCounts {
    /// Counts a violation and prints the first few.
    fn violation(&self, what: std::fmt::Arguments) {
        if self.violations.fetch_add(1, SeqCst) < 20 {
            eprintln!("VIOLATION {what}");
        }
    }
}

fn decode_counter(v: &Option<Vec<u8>>) -> u64 {
    v.as_ref()
        .filter(|v| v.len() >= 8)
        .map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()))
        .unwrap_or(0)
}

/// `causal`: whether a transaction started after a commit was acknowledged always reads it.
///
/// For each concurrency (`--concs`: the background load) `conc` tasks run fresh-transaction
/// gets, so that read version batches are always in flight, while 16 writers commit
/// `bench/causal/<w>` = i, one transaction each. Once a commit is acknowledged the writer
/// publishes `(i, committed version)`. The checks are:
///
/// * the same writer starts a new transaction: its read version is at least the committed
///   version and it reads i;
/// * 16 readers, on any worker thread, take a writer's last published `(i, cv)` and start a
///   new transaction: its read version is at least `cv` and it reads at least i. This is the
///   check a read version shared with transactions that started before the commit was
///   acknowledged would fail;
/// * the read versions of one task never go backwards.
///
/// `--causal-negative` is the negative control: the readers take a read version *before*
/// looking at what was acknowledged, as a transaction that joined an already sent read version
/// request would, and must then be found out (the cell fails if no violation is reported).
/// Otherwise the cell fails on any violation, any backwards read version, or no check at all.
async fn causal<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) -> Result<(), String> {
    let negative = o.causal_negative;
    let mut failures = Vec::new();
    for &background in o.concs {
        let c = Arc::new(CausalCounts::default());
        let stop = Arc::new(AtomicBool::new(false));
        let published: Arc<Vec<Mutex<(u64, i64)>>> =
            Arc::new((0..CAUSAL_WRITERS).map(|_| Mutex::new((0, 0))).collect());
        for w in 0..CAUSAL_WRITERS {
            b.set_commit_version(&key("causal", w as u64), &0u64.to_be_bytes())
                .await?;
        }
        let mut handles = Vec::new();
        for t in 0..background {
            let (b, c, stop) = (b.clone(), c.clone(), stop.clone());
            handles.push(tokio::spawn(async move {
                let mut rng = harness::Rng::new(t as u64 + 1);
                while !stop.load(SeqCst) {
                    match b.get(&key("get", rng.below(KEYSPACE))).await {
                        Ok(_) => c.background_gets.fetch_add(1, SeqCst),
                        Err(_) => c.errors.fetch_add(1, SeqCst),
                    };
                }
            }));
        }
        for w in 0..CAUSAL_WRITERS {
            let (b, c, stop, published) = (b.clone(), c.clone(), stop.clone(), published.clone());
            handles.push(tokio::spawn(async move {
                let k = key("causal", w as u64);
                let (mut last_rv, mut i) = (0i64, 0u64);
                while !stop.load(SeqCst) {
                    i += 1;
                    let cv = match b.set_commit_version(&k, &i.to_be_bytes()).await {
                        Ok(cv) => cv,
                        Err(e) => {
                            // Unknown result: the key may or may not be i. Skip ahead.
                            eprintln!("writer {w}: commit: {e}");
                            c.errors.fetch_add(1, SeqCst);
                            i += 1_000_000;
                            continue;
                        }
                    };
                    *published[w].lock().unwrap() = (i, cv);
                    match b.rv_get(&k).await {
                        Ok((rv, v)) => {
                            c.same_task_checks.fetch_add(1, SeqCst);
                            let got = decode_counter(&v);
                            if rv < cv || got != i {
                                c.violation(format_args!(
                                    "same task, writer {w}: committed i={i} at cv={cv}; \
                                     a new transaction has rv={rv} and read {got}"
                                ));
                            }
                            if rv < last_rv {
                                c.rv_backwards.fetch_add(1, SeqCst);
                                eprintln!("read version backwards, writer {w}: {rv} < {last_rv}");
                            }
                            last_rv = rv;
                        }
                        Err(_) => {
                            c.errors.fetch_add(1, SeqCst);
                        }
                    }
                }
            }));
        }
        for r in 0..CAUSAL_READERS {
            let (b, c, stop, published) = (b.clone(), c.clone(), stop.clone(), published.clone());
            handles.push(tokio::spawn(async move {
                let mut rng = harness::Rng::new(1000 + r as u64);
                let mut last_rv = 0i64;
                while !stop.load(SeqCst) {
                    let w = rng.below(CAUSAL_WRITERS as u64) as usize;
                    let k = key("causal", w as u64);
                    let early = if negative { Some(b.grv().await) } else { None };
                    let (i, cv) = *published[w].lock().unwrap();
                    if i == 0 {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    let res = match early {
                        Some(Ok(rv)) => b.get_at(&k, rv).await.map(|v| (rv, v)),
                        Some(Err(e)) => Err(e),
                        None => b.rv_get(&k).await,
                    };
                    match res {
                        Ok((rv, v)) => {
                            c.cross_task_checks.fetch_add(1, SeqCst);
                            let got = decode_counter(&v);
                            if rv < cv || got < i {
                                c.violation(format_args!(
                                    "cross task, reader {r}: writer {w} acknowledged i={i} at \
                                     cv={cv}; a new transaction has rv={rv} and read {got}"
                                ));
                            }
                            // The negative control's read versions are older than the last.
                            if rv < last_rv && !negative {
                                c.rv_backwards.fetch_add(1, SeqCst);
                                eprintln!("read version backwards, reader {r}: {rv} < {last_rv}");
                            }
                            last_rv = rv;
                        }
                        Err(_) => {
                            c.errors.fetch_add(1, SeqCst);
                        }
                    }
                }
            }));
        }
        let start = Instant::now();
        tokio::time::sleep(o.warmup + o.dur).await;
        stop.store(true, SeqCst);
        for h in handles {
            let _ = h.await;
        }
        let secs = start.elapsed().as_secs_f64();
        let n = |a: &AtomicU64| a.load(SeqCst);
        let checks = n(&c.same_task_checks) + n(&c.cross_task_checks);
        emit(serde_json::json!({
            "workload": "causal",
            "variant": if negative { "negative_control" } else { "checks" },
            "conc": background,
            "secs": secs,
            "background_gets": n(&c.background_gets),
            "background_gets_per_s": n(&c.background_gets) as f64 / secs,
            "same_task_checks": n(&c.same_task_checks),
            "cross_task_checks": n(&c.cross_task_checks),
            "violations": n(&c.violations),
            "rv_backwards": n(&c.rv_backwards),
            "errors": n(&c.errors),
            "negative_control": negative,
        }));
        if negative && n(&c.violations) == 0 {
            failures.push(format!(
                "concurrency {background}: the negative control found no violation (the checker is blind)"
            ));
        } else if !negative && (n(&c.violations) > 0 || n(&c.rv_backwards) > 0 || checks == 0) {
            failures.push(format!(
                "concurrency {background}: {} violations, {} backwards read versions, {checks} checks",
                n(&c.violations),
                n(&c.rv_backwards),
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}
