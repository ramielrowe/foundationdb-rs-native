//! fdb-bench: fdb-native-client vs libfdb_c on a disposable testkit cluster. Dev-only.
//!
//! ```text
//! fdb-bench run [--cluster single|triple] [--configure "grv_proxies=2 ..."] [--reps 3]
//!               [--duration 8] [--warmup 2] [--backends native,c] [--threads 4]
//!               [--workloads get,set,...] [--concs 1,16,256] [--causal-negative] [--out DIR]
//! fdb-bench serve [--cluster single|triple] [--configure ...]  # a cluster for manual `worker` runs
//! fdb-bench report RESULTS.jsonl
//! fdb-bench worker --backend native|c --cluster-file F --workload W [--threads N]
//!                  [--flavor multi|current] [--c-threads N] [--c-dbs N] [--concs 1,16,256]
//!                  [--causal-negative] ...
//! ```
//!
//! `--backends` takes client specs: `native`, `native:current` (a current-thread runtime: one
//! thread runs the application and the client) and `c`, `c:N` (libfdb_c with N network threads
//! and N database handles).
//!
//! `run` starts the cluster (a single-process container, or a 3-process `FdbCluster`),
//! loads the keyspaces, and runs every (repetition, workload, thread count, client) as a
//! separate `worker` process, so that each client has its own process (CPU, RSS) and the
//! libfdb_c network thread is started fresh. Safety: the only cluster it talks to is the one
//! it starts; it never accepts a cluster file for `run`. See `tools/bench/README.md`.

mod backend;
mod harness;
mod report;
mod workloads;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use backend::{CClient, Native};
use workloads::{OPT_IN_WORKLOADS, Opts, WORKLOADS};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    INTERRUPTED.store(true, Ordering::SeqCst);
}

struct Args(Vec<String>);

impl Args {
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .position(|a| a == name)
            .and_then(|i| self.0.get(i + 1))
            .map(String::as_str)
    }

    fn or<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        self.get(name).unwrap_or(default)
    }

    fn num<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        self.get(name)
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| die(&format!("bad value for {name}")))
            })
            .unwrap_or(default)
    }
}

/// A positive number from the environment (hand runs), else `default`.
fn env_num(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|v| {
            v.parse()
                .ok()
                .filter(|n| *n > 0)
                .unwrap_or_else(|| die(&format!("bad value for {name}")))
        })
        .unwrap_or(default)
}

fn die(msg: &str) -> ! {
    eprintln!("fdb-bench: {msg}");
    std::process::exit(2);
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = argv.first().cloned() else {
        die("usage: fdb-bench run|worker|report ... (see tools/bench/README.md)");
    };
    let args = Args(argv[1..].to_vec());
    match cmd.as_str() {
        "worker" => worker(&args),
        "run" => run(&args),
        "serve" => serve(&args),
        "report" => {
            let path = argv.get(1).unwrap_or_else(|| die("report FILE"));
            let lines = read_lines(Path::new(path));
            print!("{}", report::markdown(&lines));
        }
        _ => die("unknown command"),
    }
}

fn read_lines(p: &Path) -> Vec<Value> {
    std::fs::read_to_string(p)
        .unwrap_or_else(|e| die(&format!("{}: {e}", p.display())))
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

// ---------------------------------------------------------------------------------------------
// worker: one client, one workload

fn worker(args: &Args) {
    let backend = args.or("--backend", "native").to_string();
    let cluster_file = PathBuf::from(
        args.get("--cluster-file")
            .unwrap_or_else(|| die("--cluster-file")),
    );
    let workload = args
        .get("--workload")
        .unwrap_or_else(|| die("--workload"))
        .to_string();
    let threads: usize = args.num("--threads", 4);
    let opts = Opts {
        warmup: Duration::from_secs_f64(args.num("--warmup", 2.0)),
        dur: Duration::from_secs_f64(args.num("--duration", 8.0)),
        concs: args.get("--concs").map_or(workloads::CONCS, |s| {
            let v: Vec<usize> = s
                .split(',')
                .map(|c| c.parse().unwrap_or_else(|_| die("bad value for --concs")))
                .collect();
            Vec::leak(v)
        }),
        causal_negative: args.0.iter().any(|a| a == "--causal-negative"),
    };
    // `--flavor current`: a current-thread runtime, so one thread runs the application and,
    // for native, the whole client. Otherwise a multi-thread runtime of `--threads` workers.
    let current = match args.or("--flavor", "multi") {
        "multi" => false,
        "current" => true,
        _ => die("--flavor multi|current"),
    };
    // libfdb_c: `--c-threads N` (or FDB_BENCH_C_THREADS) runs N client network threads
    // (`ClientThreadsPerVersion`, which needs the multi-version client: the host's libfdb_c is
    // loaded as the external client); the database handles, used round robin, default to N.
    let c_threads: usize = args.num("--c-threads", env_num("FDB_BENCH_C_THREADS", 1));
    let c_dbs: usize = args.num("--c-dbs", env_num("FDB_BENCH_C_DBS", c_threads));
    let rt = if current {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("runtime")
    };
    let mut emit = {
        let backend = backend.clone();
        move |mut v: Value| {
            v["backend"] = json!(backend);
            v["workers"] = json!(if current { 1 } else { threads });
            // Only a non-default configuration says so, so older results keep their shape.
            if current {
                v["flavor"] = json!("current");
            }
            if backend == "c" && c_threads > 1 {
                v["c_threads"] = json!(c_threads);
                v["c_dbs"] = json!(c_dbs);
            }
            println!("{v}");
        }
    };
    let result = match backend.as_str() {
        "native" => rt.block_on(async {
            let b = Native::open(&cluster_file)?;
            let r = workloads::run(b.clone(), &workload, opts, &mut emit).await;
            b.shutdown().await;
            r
        }),
        "c" => {
            // libfdb_c: the network thread(s), started by `boot` (selected API version 740).
            let mut api = foundationdb::api::FdbApiBuilder::default()
                .build()
                .expect("fdb api");
            if c_threads > 1 {
                use foundationdb::options::NetworkOption;
                let lib = std::env::var("FDB_BENCH_LIBFDB_C")
                    .unwrap_or_else(|_| "/usr/lib/libfdb_c.so".into());
                api = api
                    .set_option(NetworkOption::ExternalClientLibrary(lib))
                    .and_then(|a| a.set_option(NetworkOption::DisableLocalClient))
                    .and_then(|a| {
                        a.set_option(NetworkOption::ClientThreadsPerVersion(c_threads as i32))
                    })
                    .expect("multi-version client options");
            }
            let network = unsafe { api.boot() }.expect("boot libfdb_c network thread");
            let r = rt.block_on(async {
                let b = CClient::open(&cluster_file, c_dbs)?;
                workloads::run(b, &workload, opts, &mut emit).await
            });
            drop(rt);
            drop(network);
            r
        }
        _ => die("--backend native|c"),
    };
    if let Err(e) = result {
        eprintln!("worker {backend}/{workload}: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------------------------
// run: the orchestrator

#[allow(clippy::large_enum_variant)]
enum Cluster {
    Single(fdb_native_testkit::FdbContainer),
    Triple(fdb_native_testkit::FdbCluster),
}

impl Cluster {
    fn cluster_file(&self) -> PathBuf {
        match self {
            Cluster::Single(c) => c.cluster_file().to_path_buf(),
            Cluster::Triple(c) => c.cluster_file().to_path_buf(),
        }
    }
}

/// Starts the disposable cluster of `--cluster`. `configure` (`--configure`, triple only) is an
/// `fdbcli configure` argument list, e.g. `grv_proxies=2 commit_proxies=2`; it is applied once
/// the cluster is healthy, and the cluster is given time to recruit the new roles.
fn start_cluster(kind: &str, configure: Option<&str>) -> Cluster {
    let cluster = match kind {
        "single" => Cluster::Single(
            fdb_native_testkit::FdbContainer::start().unwrap_or_else(|e| die(&e.to_string())),
        ),
        "triple" => Cluster::Triple(
            fdb_native_testkit::FdbCluster::builder()
                .processes(3)
                .start()
                .unwrap_or_else(|e| die(&e.to_string())),
        ),
        _ => die("--cluster single|triple"),
    };
    if let Some(cfg) = configure {
        let Cluster::Triple(c) = &cluster else {
            die("--configure needs --cluster triple");
        };
        let out = c
            .fdbcli(&format!("configure {cfg}"))
            .unwrap_or_else(|e| die(&e.to_string()));
        eprintln!("configure {cfg}: {}", out.trim());
        c.wait_healthy(Duration::from_secs(120))
            .unwrap_or_else(|e| die(&e.to_string()));
        std::thread::sleep(Duration::from_secs(5));
    }
    cluster
}

/// Starts a cluster, loads the keyspaces, prints the cluster file and waits for a signal (or
/// stdin to close): for running `worker` by hand, e.g. under `perf record`.
fn serve(args: &Args) {
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as usize);
        libc::signal(libc::SIGTERM, on_signal as *const () as usize);
    }
    let cluster = start_cluster(args.or("--cluster", "single"), args.get("--configure"));
    let cluster_file = cluster.cluster_file();
    let exe = std::env::current_exe().expect("current_exe");
    let ok = Command::new(exe)
        .args([
            "worker",
            "--backend",
            "native",
            "--workload",
            "load",
            "--cluster-file",
        ])
        .arg(&cluster_file)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ok {
        die("load failed");
    }
    println!("cluster-file: {}", cluster_file.display());
    while !INTERRUPTED.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(cluster);
}

fn run(args: &Args) {
    let kind = args.or("--cluster", "single");
    let reps: usize = args.num("--reps", 3);
    let duration: f64 = args.num("--duration", 8.0);
    let warmup: f64 = args.num("--warmup", 2.0);
    let backends: Vec<&str> = args.or("--backends", "native,c").split(',').collect();
    for b in &backends {
        // Only to fail before the cluster starts.
        client_args(b);
    }
    let threads: Vec<&str> = args.or("--threads", "4").split(',').collect();
    let configure = args.get("--configure");
    let default_wl = WORKLOADS.join(",");
    let workload_list: Vec<&str> = args.or("--workloads", &default_wl).split(',').collect();
    for w in &workload_list {
        if !WORKLOADS.contains(w) && !OPT_IN_WORKLOADS.contains(w) {
            die(&format!(
                "unknown workload {w}; known: {WORKLOADS:?} and, when asked for, {OPT_IN_WORKLOADS:?}"
            ));
        }
    }
    // Worker arguments `run` passes on as they are.
    let mut passthrough: Vec<String> = Vec::new();
    if let Some(c) = args.get("--concs") {
        passthrough.extend(["--concs".to_string(), c.to_string()]);
    }
    if args.0.iter().any(|a| a == "--causal-negative") {
        passthrough.push("--causal-negative".to_string());
    }
    let out = PathBuf::from(args.or("--out", "results/latest"));
    std::fs::create_dir_all(&out).expect("create out dir");
    let results_path = out.join("results.jsonl");
    std::fs::write(&results_path, "").expect("truncate results");

    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as usize);
        libc::signal(libc::SIGTERM, on_signal as *const () as usize);
    }

    // Containers are started here, outside any async runtime, and removed when `cluster` drops.
    eprintln!("== starting a {kind} cluster (image: FDB_IMAGE or the testkit default)");
    let cluster = start_cluster(kind, configure);
    let cluster_file = cluster.cluster_file();
    let status = match &cluster {
        Cluster::Single(c) => c.fdbcli("status").unwrap_or_default(),
        Cluster::Triple(c) => c.fdbcli("status").unwrap_or_default(),
    };
    let image = match &cluster {
        Cluster::Single(c) => c.image().to_string(),
        Cluster::Triple(c) => c.image().to_string(),
    };
    std::fs::write(
        out.join("meta.json"),
        serde_json::to_string_pretty(&json!({
            "cluster": kind,
            "configure": configure,
            "image": image,
            "cluster_file": std::fs::read_to_string(&cluster_file).unwrap_or_default().trim(),
            "reps": reps, "duration_s": duration, "warmup_s": warmup,
            "backends": backends, "threads": threads, "workloads": workload_list,
            "status": status,
        }))
        .unwrap(),
    )
    .unwrap();

    let exe = std::env::current_exe().expect("current_exe");
    let mut failed_cells = 0usize;
    let mut cell = |backend: &str, workload: &str, threads: &str, rep: Option<usize>| -> bool {
        if INTERRUPTED.load(Ordering::SeqCst) {
            return false;
        }
        let t = Instant::now();
        // 1-minute load average of the (shared) host when the cell starts.
        let load1: f64 = std::fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|s| s.split(' ').next().and_then(|x| x.parse().ok()))
            .unwrap_or(0.0);
        let mut cmd = Command::new(&exe);
        cmd.arg("worker")
            .args(client_args(backend))
            .args(["--workload", workload])
            .args(["--threads", threads])
            .args(&passthrough)
            .args([
                "--duration",
                &duration.to_string(),
                "--warmup",
                &warmup.to_string(),
            ])
            .arg("--cluster-file")
            .arg(&cluster_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("spawn worker");
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .filter(|l| l.starts_with('{'))
                .collect::<Vec<_>>()
        });
        let deadline = Instant::now() + Duration::from_secs(1800);
        let status = loop {
            match child.try_wait().expect("wait") {
                Some(s) => break s,
                None if Instant::now() > deadline => {
                    let _ = child.kill();
                    break child.wait().expect("wait");
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        };
        let lines = reader.join().unwrap_or_default();
        if !status.success() {
            failed_cells += 1;
            eprintln!("!! worker {backend}/{workload}/{threads}t failed: {status}");
        }
        if let Some(rep) = rep {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&results_path)
                .unwrap();
            for l in lines {
                let mut v: Value = serde_json::from_str(&l).unwrap();
                v["rep"] = json!(rep);
                v["load1"] = json!(load1);
                writeln!(f, "{v}").unwrap();
            }
        }
        eprintln!(
            "   {} rep={:?} {backend:<6} {workload:<8} {threads}t  {:.0}s",
            if status.success() { "ok" } else { "FAILED" },
            rep,
            t.elapsed().as_secs_f64()
        );
        status.success()
    };

    eprintln!("== loading keyspaces");
    if !cell("native", "load", "4", None) {
        die("load failed");
    }
    'outer: for rep in 1..=reps {
        eprintln!("== repetition {rep}/{reps}");
        for w in &workload_list {
            for (ti, t) in threads.iter().enumerate() {
                // Alternate which client goes first so a drifting server favours neither.
                let mut order = backends.clone();
                if rep % 2 == 0 {
                    order.reverse();
                }
                for b in order {
                    // A current-thread runtime has no thread count: once per workload.
                    if b.ends_with(":current") && ti > 0 {
                        continue;
                    }
                    if INTERRUPTED.load(Ordering::SeqCst) {
                        eprintln!("== interrupted");
                        break 'outer;
                    }
                    cell(b, w, t, Some(rep));
                }
            }
        }
    }
    let md = report::markdown(&read_lines(&results_path));
    std::fs::write(out.join("results.md"), &md).unwrap();
    println!("{md}");
    eprintln!("== results in {}", out.display());
    drop(cluster); // removes the containers (and the network)
    if failed_cells > 0 {
        // A failed cell is a failed run: `causal` fails its cell on a violation.
        die(&format!("{failed_cells} cell(s) failed"));
    }
}

/// The worker arguments of a client spec of `--backends`: `native`, `native:current`, `c`,
/// `c:N`.
fn client_args(spec: &str) -> Vec<String> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect();
    match spec.split_once(':') {
        None if spec == "native" || spec == "c" => s(&["--backend", spec]),
        Some(("native", "current")) => s(&["--backend", "native", "--flavor", "current"]),
        Some(("c", n)) if n.parse::<usize>().is_ok_and(|n| n > 0) => {
            s(&["--backend", "c", "--c-threads", n])
        }
        _ => die(&format!(
            "unknown client {spec}; use native, native:current, c or c:N"
        )),
    }
}
