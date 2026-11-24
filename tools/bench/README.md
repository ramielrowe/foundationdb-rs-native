# fdb-bench

Dev-only benchmark of `fdb-native-client` against `libfdb_c` 7.4.10 on a disposable testkit
cluster. Method, hardware, results and analysis are in [docs/benchmarks.md](../../docs/benchmarks.md).

Not a workspace member (its own `[workspace]` table), so CI never builds it and
`cargo ... --workspace` does not need `libfdb_c` or libclang. It needs the host's
`libfdb_c.so` (7.4.x) and libclang, because the C client is the `foundationdb` crate
(0.11, `fdb-7_4`), whose bindings are generated with bindgen.

```bash
tools/bench/run.sh quick                  # ~2 min smoke test
tools/bench/run.sh full                   # standard suite, ~1 h (single-process + 3-process cluster)
tools/bench/run.sh threads                # native vs libfdb_c at 1/2/4 threads, ~20 min (below)
tools/bench/run.sh causal                 # the causal-consistency check and its negative control (below)
tools/bench/run.sh grv                    # grv_only: a read version and nothing else
BENCH_OUT=/some/dir tools/bench/run.sh single

# one cell by hand, against a cluster you started with the testkit
tools/bench/target/release/fdb-bench worker --backend native --threads 4 \
    --cluster-file FILE --workload get --duration 8 --warmup 2
tools/bench/target/release/fdb-bench report results/<run>/single-4t/results.jsonl
```

`fdb-bench run` starts the cluster itself (`--cluster single|triple`; image from `FDB_IMAGE`,
a 7.4 server), loads the keyspaces (`bench/{get,set,mix}/` 100k keys each, `bench/scan10k/`,
`bench/scan100k/`, 100-byte values), and runs each (repetition, workload, thread count,
client) as its own `worker` process. The client order alternates between repetitions.
Output per run directory: `results.jsonl` (one line per measurement and repetition),
`results.md` (medians), `meta.json`. `results/` is git-ignored.

Both clients run under the same tokio runtime (`--threads` worker threads) driving the same
closed-loop tasks and the same key sequences; only the client under test differs.

## Threads: native against libfdb_c's network threads

libfdb_c does its work on one network thread, native on the application's tokio workers, so
which one is "faster" depends on the threads each gets (see "Why native is faster" in
docs/benchmarks.md). The knobs:

```bash
tools/bench/run.sh threads     # ~20 min: native 1/2/4 workers + current_thread, libfdb_c x1/x2/x4

# in `run`, --backends takes client specs; the report tells them apart by name
fdb-bench run --workloads get,get_seq --concs 16,256 --threads 4 \
    --backends native,native:current,c,c:2,c:4
```

* `--flavor current` (worker; `native:current` in `run`): a tokio current-thread runtime, so one
  thread runs the application and the whole native client. Reported as 1 worker, client
  `native/current`.
* `--c-threads N` (worker; `c:N` in `run`; or `FDB_BENCH_C_THREADS=N`): libfdb_c with N client
  network threads (`ClientThreadsPerVersion`), through the multi-version client with the host's
  libfdb_c as the external one (`FDB_BENCH_LIBFDB_C` names it; default `/usr/lib/libfdb_c.so`) and
  the local client disabled. `--c-dbs M` (or `FDB_BENCH_C_DBS=M`; default N) opens M database
  handles that the operations use round robin, so that all N threads have work. Reported as
  client `c xN`. `run`'s thread count (`--threads`) is the application's workers in both.
* `--concs 16,256` (also `run`): the concurrencies to run, for `get`, `get_seq`, `set`,
  `mixed`, `grv_only` and `causal`.
* Every result line carries `all_iters`: the iterations that started, warmup and the
  stragglers included (the number of gets, or transactions, the server saw), for comparing with
  server-side counters.

`grv_only` (`run.sh grv`) is a fresh transaction that only gets its read version, with the same
closed loop as `get`; it isolates the read version path.

## Causal check

```bash
CAUSAL_SECS=20 tools/bench/run.sh causal     # ~8 min; exit status 0 only if every cell is as expected
fdb-bench run --workloads causal --concs 256 --duration 60 --reps 1 [--causal-negative]
```

The `causal` workload checks that a transaction started after a commit was acknowledged reads
that commit, which a client that shares read versions between transactions can get wrong.
Per background concurrency (`--concs`, default 256 in `run.sh`: that many tasks do
fresh-transaction gets, so GRV batches are always in flight), 16 writers each loop
`set bench/causal/<w> = i; commit`, publish `(i, committed version)` once the commit is
acknowledged, and start a new transaction of their own; 16 readers on any worker thread take a
writer's last published pair and start a new transaction. Each new transaction's read version
must be at least the committed version and the value at least i; read versions of one task
never go backwards. The line reports `same_task_checks`, `cross_task_checks`, `violations`,
`rv_backwards` and `errors` (commit or read errors, which are retried and not violations).

* The worker (and so the `run` cell) **exits non-zero on any violation, any backwards read
  version, or if no check ran**.
* `--causal-negative` is the negative control: the readers take a read version *before* they
  look at what was acknowledged (as a transaction that joined a read version request already in
  flight would). It **must** report violations, and the cell fails if it reports none: it
  proves the checker can see the failure.
* `run.sh causal` does both, for both clients, on a single-process cluster and on a 3-process
  cluster configured with 2 GRV and 2 commit proxies (`--configure`, below).

## Clusters

`run` and `serve` start the cluster themselves (`--cluster single|triple`).
`--configure "grv_proxies=2 commit_proxies=2"` (triple only) runs `fdbcli configure ...` on the
fresh cluster, waits for it to be healthy and gives it 5 s to recruit before the keyspaces
are loaded; it is recorded in `meta.json`. `serve` is for running `worker` by hand (and under
`perf`):

```bash
tools/bench/target/release/fdb-bench serve --cluster triple --configure "grv_proxies=2 commit_proxies=2"
# prints "cluster-file: ..."; Ctrl-C removes the containers
```

`run` exits non-zero if a cell failed, after writing `results.md` and removing the containers.

**Safety:** the tool only ever talks to the containers it starts (labelled, removed by ID on
exit; Ctrl-C finishes the current cell, then removes them). Never point it at a shared
cluster: it writes about 500k keys under `bench/` and overwrites them at full speed.
