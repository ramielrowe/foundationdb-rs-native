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

**Safety:** the tool only ever talks to the containers it starts (labelled, removed by ID on
exit; Ctrl-C finishes the current cell, then removes them). Never point it at a shared
cluster: it writes about 500k keys under `bench/` and overwrites them at full speed.
