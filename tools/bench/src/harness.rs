//! Closed-loop load generation, latency samples, CPU accounting.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// xorshift64*; one per task, seeded from the task index, so both clients see the same key
/// sequence.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 11) % n
    }
}

pub fn key(prefix: &str, i: u64) -> Vec<u8> {
    format!("bench/{prefix}/{i:08}").into_bytes()
}

pub const VALUE_LEN: usize = 100;

pub fn value(i: u64) -> Vec<u8> {
    let mut v = vec![b'x'; VALUE_LEN];
    let s = format!("{i:016x}");
    v[..16].copy_from_slice(s.as_bytes());
    v
}

#[derive(Clone, Copy)]
pub struct Cpu {
    pub user: f64,
    pub sys: f64,
    pub maxrss_kb: i64,
}

pub fn cpu() -> Cpu {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    Cpu {
        user: tv(ru.ru_utime),
        sys: tv(ru.ru_stime),
        maxrss_kb: ru.ru_maxrss,
    }
}

/// What one iteration of a workload closure did.
pub struct Iter {
    /// Units of work for throughput (gets, commits, rows ...).
    pub ops: u64,
    /// The latencies of this iteration (microseconds); `None`: the iteration's wall time is
    /// the one sample.
    pub samples: Option<Vec<u32>>,
}

impl Iter {
    pub fn ops(ops: u64) -> Iter {
        Iter { ops, samples: None }
    }
}

pub struct TaskCtx {
    pub rng: Rng,
    pub measuring: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    /// Every iteration started, warmup and stragglers included (what the server saw).
    pub all_iters: u64,
    pub samples: Vec<u32>,
    pub ops: u64,
    pub iters: u64,
    pub errors: u64,
}

impl Stats {
    pub fn pct(&self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let i = ((self.samples.len() as f64 - 1.0) * p).round() as usize;
        self.samples[i] as u64
    }
}

pub struct Measurement {
    pub secs: f64,
    pub stats: Stats,
    pub cpu_user: f64,
    pub cpu_sys: f64,
    pub maxrss_kb: i64,
}

impl Measurement {
    pub fn json(&self, workload: &str, variant: &str, conc: usize) -> Value {
        let ops_s = self.stats.ops as f64 / self.secs;
        let cpu = self.cpu_user + self.cpu_sys;
        let mean = if self.stats.samples.is_empty() {
            0.0
        } else {
            self.stats.samples.iter().map(|&x| x as f64).sum::<f64>()
                / self.stats.samples.len() as f64
        };
        json!({
            "workload": workload,
            "variant": variant,
            "conc": conc,
            "ops": self.stats.ops,
            "iters": self.stats.iters,
            "all_iters": self.stats.all_iters,
            "errors": self.stats.errors,
            "secs": self.secs,
            "ops_per_s": ops_s,
            "p50_us": self.stats.pct(0.50),
            "p90_us": self.stats.pct(0.90),
            "p99_us": self.stats.pct(0.99),
            "p999_us": self.stats.pct(0.999),
            "max_us": self.stats.samples.last().copied().unwrap_or(0),
            "mean_us": mean,
            "cpu_user_s": self.cpu_user,
            "cpu_sys_s": self.cpu_sys,
            "cpu_us_per_op": if self.stats.ops > 0 { cpu * 1e6 / self.stats.ops as f64 } else { 0.0 },
            "cpu_cores": cpu / self.secs,
            "maxrss_mb": self.maxrss_kb as f64 / 1024.0,
        })
    }
}

/// Runs `conc` tasks, each calling `f` in a loop, for `warmup` (discarded) and then `dur`
/// (measured). The closure builds one iteration's future from the task context.
pub async fn closed_loop<F, Fut>(conc: usize, warmup: Duration, dur: Duration, f: F) -> Measurement
where
    F: Fn(&mut TaskCtx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Iter, String>> + Send,
    // The future must not borrow the context: take what it needs before returning it.
{
    let f = Arc::new(f);
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(conc);
    for task in 0..conc {
        let (f, stop, measuring) = (f.clone(), stop.clone(), measuring.clone());
        handles.push(tokio::spawn(async move {
            let mut ctx = TaskCtx {
                rng: Rng::new(task as u64 + 1),
                measuring: false,
            };
            let mut st = Stats::default();
            let mut first_err: Option<String> = None;
            while !stop.load(Ordering::Relaxed) {
                ctx.measuring = measuring.load(Ordering::Relaxed);
                let t = Instant::now();
                st.all_iters += 1;
                let r = f(&mut ctx).await;
                let us = t.elapsed().as_micros().min(u32::MAX as u128) as u32;
                // Only iterations that started and ended inside the window count.
                let counted = ctx.measuring && measuring.load(Ordering::Relaxed);
                match r {
                    Ok(it) => {
                        if counted {
                            st.ops += it.ops;
                            st.iters += 1;
                            match it.samples {
                                Some(mut v) => st.samples.append(&mut v),
                                None => st.samples.push(us),
                            }
                        }
                    }
                    Err(e) => {
                        if counted {
                            st.errors += 1;
                        }
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                        // A failing loop must not spin.
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
            if let Some(e) = first_err {
                eprintln!("task {task}: first error: {e}");
            }
            st
        }));
    }
    tokio::time::sleep(warmup).await;
    let c0 = cpu();
    let t0 = Instant::now();
    measuring.store(true, Ordering::SeqCst);
    tokio::time::sleep(dur).await;
    measuring.store(false, Ordering::SeqCst);
    let secs = t0.elapsed().as_secs_f64();
    let c1 = cpu();
    stop.store(true, Ordering::SeqCst);
    let mut total = Stats::default();
    for h in handles {
        let st = h.await.expect("task panicked");
        total.samples.extend(st.samples);
        total.ops += st.ops;
        total.iters += st.iters;
        total.errors += st.errors;
        total.all_iters += st.all_iters;
    }
    total.samples.sort_unstable();
    Measurement {
        secs,
        stats: total,
        cpu_user: c1.user - c0.user,
        cpu_sys: c1.sys - c0.sys,
        maxrss_kb: c1.maxrss_kb,
    }
}
