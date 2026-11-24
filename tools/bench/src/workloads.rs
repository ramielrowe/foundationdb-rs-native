//! The workloads. Each one emits one JSON value per (variant, concurrency) as it finishes.

use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::Value;

use crate::backend::{Backend, Mode, Op, WatchFuture};
use crate::harness::{self, Iter, Measurement, Stats, closed_loop, key, value};

/// Keys per point-access keyspace (`bench/get/`, `bench/set/`, `bench/mix/`).
pub const KEYSPACE: u64 = 100_000;

pub const WORKLOADS: &[&str] = &["get", "get_seq", "set", "scan", "mixed", "many_tx", "watch"];

#[derive(Clone, Copy)]
pub struct Opts {
    pub warmup: Duration,
    pub dur: Duration,
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

const CONCS: [usize; 3] = [1, 16, 256];

/// A fresh transaction per get: the read version and the read.
async fn get<B: Backend>(b: &B, o: Opts, emit: &mut dyn FnMut(Value)) {
    for conc in CONCS {
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
    for conc in CONCS {
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
    for conc in CONCS {
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
    for conc in CONCS {
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
