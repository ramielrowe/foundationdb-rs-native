# Benchmarks: fdb-native-client vs libfdb_c

How this client's throughput, latency and CPU use compare with `libfdb_c` 7.4.10 on the same
disposable cluster, what the numbers say about the client, and how to reproduce them. The tool
is [`tools/bench`](../tools/bench) (dev-only, outside the workspace); `tools/bench/run.sh full`
reproduces every table below except the "fix" column (see [With the GRV timer fix](#with-the-grv-timer-fix)).

Numbers are from one machine that was shared with other development work while the runs went
(see [Host and caveats](#host-and-caveats)). They show the shape of the difference, not
absolute capabilities of either client; treat differences under about 20% as noise.

## Summary

* **Point reads at low concurrency are 8 to 11 times slower in latency** (p50 1.12 ms vs
  98 us for a fresh transaction plus one `get`; 883 vs 9877 ops/s at concurrency 1). The cause
  is one thing: the GRV batcher's `sleep(batch_time)` on tokio rounds a sub-millisecond delay up
  to the timer tick, about 1.05 ms, so every transaction waits a millisecond for its read
  version. A one-line experiment (no sleep below 1 ms) turns 883 ops/s into 13.2k (p50 58 us,
  better than libfdb_c's 98 us) and the 256-concurrency point read from 144k to 329k ops/s.
  Bead `foundationdb-rs-native-8jn` (P2).
* **Once the read version is paid for, reads are on par or faster.** 100 gets in one
  transaction: p50 28 us vs 38 us per get at concurrency 1, and 386k vs 272k gets/s at
  concurrency 256 (native spreads the work over the tokio worker threads; libfdb_c's single
  network thread is its ceiling).
* **Range scans are 1.7 to 2.4 times slower** (2.7 to 4.1M rows/s vs 6.0 to 7.0M), with 2.4 to
  2.8 times the CPU per row. perf: more than half of the samples are malloc/free, from the
  per-row owned key and value copies. Bead `foundationdb-rs-native-s1t` (P3).
* **Commits are bounded by the server, and at par once the GRV floor is gone.** Set + commit
  takes 4 to 6 ms on this cluster's memory engine. At concurrency 256: 29k (native) vs 40k
  (libfdb_c) commits/s on the single-process cluster, with a p50 1.7 ms higher, and 20.3k vs
  21.2k on the 3-process cluster; with the GRV experiment native does 39.6k with the same p50
  as libfdb_c (5.71 vs 5.73 ms). The numbers are noisy (min-max in `results.md`).
* **Many transactions in flight (1000, 5000) are at par** in throughput and tail latency, with
  about twice the memory (265 MB vs 140 MB at 5000) and 1.5 to 2 times the CPU per
  transaction. Beads `foundationdb-rs-native-0et` (P4) and `foundationdb-rs-native-e82` (P3).
* **Watches**: setup latency has the same 1 ms floor (p50 1.16 ms vs 147 us); when one write
  fires 5000 watches the median watch fires 35 ms (native) vs 38 ms (libfdb_c) after the write
  starts, and 5000 setups at concurrency 256 run at 81k/s vs 82k/s.
* **CPU per operation** is 1.5 to 2 times libfdb_c's under load (11.1 vs 6.6 us per fresh-
  transaction get at 256), but native uses several cores where libfdb_c is capped at about one
  network thread, so it wins on throughput at equal application threads when reads are
  GRV-free and the cluster is not the limit.

## What is compared

| | native | libfdb_c |
|---|---|---|
| Client | `fdb-native-client` (this repository, this commit), tokio adapter | `libfdb_c.so` 7.4.10 (the host's `/usr/lib/libfdb_c.so`) through the Rust `foundationdb` 0.11 crate (`fdb-7_4`, API version 740) |
| Where the client's work runs | on the application's tokio worker threads: every connection's I/O, encoding and decoding, location cache, read-your-writes state, GRV and commit batching are tasks of the same runtime | on libfdb_c's **single network thread** (`fdb_run_network`): all I/O, flatbuffer encoding, caches, the RYW layer and batching; application threads only start futures and wake on callbacks |
| Application side | tokio multi-thread runtime, `--threads` workers (4 by default) | the same tokio runtime and the same tasks; libfdb_c's network thread is in addition |
| Retry loop | `Database::run` | `Database::run` of the `foundationdb` crate |

Both processes run the same workload code (`tools/bench/src/workloads.rs`, generic over a
two-implementation `Backend` trait), the same key sequences (one xorshift generator per task,
seeded by the task index), the same value size (100 bytes) and the same closed-loop concurrency
model. Only the client differs. Because the clients have different threading, compare them at
equal application concurrency and equal application threads (the tables label both), and
read the CPU columns: libfdb_c's CPU figure includes its network thread (it is the process's
`getrusage`).

The server is `foundationdb/foundationdb:7.4.10`, started by the testkit as a disposable,
labelled container (never a shared cluster) and removed when the run ends. libfdb_c 7.4.10
has no multi-version client here, so 8.0 servers are not compared.

* **single**: one `fdbserver` process, `single memory`, in a Docker bridge network
  (`FdbContainer`); the client runs on the host.
* **triple**: three `fdbserver` containers (`FdbCluster`, `double memory`, 3 coordinators).

## Workloads

| workload | variant | what one iteration is | concurrency | reported "ops" |
|---|---|---|---|---|
| `get` | `fresh_tx` | new transaction, one `get` of a random key of 100k, dropped (read version + the read) | 1, 16, 256 | gets |
| `get_seq` | `100_per_tx` | one transaction, 100 sequential gets of random keys; latency is per get, the first get (which pays for the read version) is not sampled | 1, 16, 256 | gets |
| `set` | `1_key_commit` | `db.run` with one `set` of a random key of 100k, 100-byte value, commit | 1, 16, 256 | commits |
| `scan` | `10k` / `100k` x `wantall` / `iterator` | one transaction scans a whole range (10 000 or 100 000 rows of 20-byte keys and 100-byte values) row by row with `StreamingMode::WantAll` or `Iterator` | 1 | rows (ops/s is rows/s; latency is per scan) |
| `mixed` | `90r_10w_10ops` | `db.run` of a 10-operation transaction: 90% of transactions are 10 gets (read-only, local commit), 10% are 5 gets + 5 sets with a commit | 1, 16, 256 | transactions |
| `many_tx` | `3r_2w_commit` | `db.run` of 3 gets + 2 sets and a commit, in 1000 and in 5000 concurrent tasks (that many transactions in flight) | 1000, 5000 | transactions |
| `watch` | `setup`, `writer_commit`, `write_to_fire` | one watch at a time on a fresh key: the transaction that registers the watch (to the commit), then a second transaction writes the key; `write_to_fire` runs from the start of the writer's commit to the watch resolving | 1 | watches |
| `watch` | `setup_5000_c256`, `fire_5000` | 5000 watches registered with 256 in flight; then one transaction writes all 5000 keys and each watch's latency is measured from the start of that write | 256, 5000 | watches |

Keyspaces: `bench/get/`, `bench/set/`, `bench/mix/` with 100k keys each, `bench/scan10k/` and
`bench/scan100k/`, all loaded before the first cell (`workload load`). `set` and `mixed`
overwrite existing keys, so the data size is constant. The 90/10 mix writes 5 keys in a write
transaction and reads random keys, so conflicts are rare; `errors` in the raw output counts
iterations that failed after retries (0 in every run reported here).

## Method

* Each cell is a separate process (`fdb-bench worker`): its own `getrusage`, peak RSS, and a
  fresh libfdb_c network thread. A cell runs every concurrency of one workload in turn.
* A concurrency level runs 2 s of warmup (discarded) and then an 8 s measured window of
  closed-loop tasks (each task starts its next iteration when the previous one finishes).
  Latencies (per iteration, or per get for `get_seq`) are recorded for every iteration that
  starts and ends inside the window; percentiles come from the sorted samples.
* CPU is `getrusage(RUSAGE_SELF)` user + system over the measured window only, divided by the
  window's operations (`CPU us/op`; for `scan` per row; for `watch` rows the whole phase).
  Peak RSS is `ru_maxrss` at the end of the cell.
* 3 repetitions; within a repetition each (workload, thread count) runs both clients, and the
  client order alternates between repetitions so a slow drift favours neither. The tables
  report the **median of the 3 repetitions** of each statistic; `results.md` in a run
  directory also has the min-max of the throughput and the host's 1-minute load average at
  the start of every cell is in `results.jsonl` (`load1`).
* Suites (`tools/bench/run.sh full`): `single-4t` (all workloads, 4 application threads),
  `single-1t` (get, get_seq, mixed, many_tx with 1 application thread), `triple-4t` (get, set,
  mixed, many_tx on the 3-process cluster).

## Host and caveats

```
CPU      AMD Ryzen 9 9950X3D2 16-Core (32 threads, 1 socket), up to 5.67 GHz; L2 16 MiB, L3 192 MiB
Memory   59 GB
Kernel   Linux 7.0.0-38-generic; Docker 29.1.3 (overlayfs on ext4), bridge network
Rust     rustc 1.97.1, release profile (thin LTO), glibc malloc
Server   foundationdb/foundationdb:7.4.10, memory storage engine; client and server on the same host
```

* **The host was shared.** Other agents were building, running tests and fault-test clusters
  during the runs: the 1-minute load average at the start of the cells was 4 to 17 (median 8)
  for `single-4t`, 5 to 21 for `single-1t` and 10 to 24 for `triple-4t`, on 32 hardware
  threads. CPU-bound cells (get, get_seq, scans) were steadier (repetition spread usually
  10 to 15%, min-max in `results.md`); commit-bound cells (set, mixed, many_tx) share the
  disk and the docker bridge with that load and varied by up to 2x between repetitions. The medians and the interleaved client
  order are the mitigation; re-run on a quiet machine before quoting an absolute number.
* Commit latency is the server's: a memory-engine commit fsyncs the transaction log in the
  container (4 to 6 ms here), so `set` at low concurrency measures the disk, not the client.
* The client and the server share the host's CPUs (one `fdbserver` process is single-threaded,
  so the server saturates at roughly one core per role: the GRV proxy, the storage server and
  the commit pipeline can each become the limit at high concurrency; the 329k gets/s of the
  GRV experiment shows the server is not the limit at 150k to 180k).
* libfdb_c is reached through the Rust `foundationdb` crate, which adds a small future/waker
  layer over `fdb_future_set_callback`; the C numbers include that layer (it is also what a
  Rust application would use).
* No TLS, 100-byte values, one cluster file, API version 740 for the C client.

## Results

`ratio` is native / libfdb_c throughput. Latencies are p50 / p99. `CPU us/op` is
`libfdb_c / native` (and `/ fix` for the last column). For `scan`, ops/s are rows per second
(M = million, k = thousand) and latencies are per whole scan; for `watch`, `setup`,
`writer_commit` and `write_to_fire` are one-at-a-time latencies (ops/s is the cycle rate).
The `native+fix` column is [the experiment below](#with-the-grv-timer-fix), not the client as
committed.

### Single-process cluster, 4 application threads

| workload | variant | conc | libfdb_c ops/s | native ops/s | ratio | native+fix ops/s | libfdb_c p50 / p99 | native p50 / p99 | native+fix p50 / p99 | CPU us/op c / native / fix |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 9877 | 883 | 0.09 | 13.2k | 98 us / 129 us | 1.12 ms / 2.12 ms | 58 us / 223 us | 29.9 / 32.0 / 29.9 |
| get | fresh_tx | 16 | 110.3k | 13.3k | 0.12 | 160.4k | 142 us / 201 us | 1.19 ms / 1.29 ms | 97 us / 162 us | 7.4 / 12.5 / 15.0 |
| get | fresh_tx | 256 | 176.1k | 143.8k | 0.82 | 329.2k | 1.48 ms / 2.04 ms | 1.88 ms / 2.49 ms | 751 us / 1.60 ms | 6.6 / 11.1 / 10.7 |
| get_seq | 100_per_tx | 1 | 24.5k | 24.0k | 0.98 | 33.9k | 38 us / 48 us | 28 us / 44 us | 28 us / 39 us | 15.3 / 14.2 / 13.3 |
| get_seq | 100_per_tx | 16 | 250.4k | 261.5k | 1.04 | 277.7k | 58 us / 96 us | 50 us / 123 us | 51 us / 146 us | 5.5 / 10.7 / 10.0 |
| get_seq | 100_per_tx | 256 | 272.4k | 386.2k | 1.42 | 418.2k | 928 us / 1.34 ms | 589 us / 1.41 ms | 564 us / 1.10 ms | 5.5 / 9.5 / 8.9 |
| set | 1_key_commit | 1 | 226 | 158 | 0.70 | 224 | 3.87 ms / 10.4 ms | 5.58 ms / 15.5 ms | 3.43 ms / 12.3 ms | 121.3 / 68.2 / 63.2 |
| set | 1_key_commit | 16 | 3539 | 2140 | 0.60 | 4461 | 3.89 ms / 8.41 ms | 5.65 ms / 26.4 ms | 3.47 ms / 6.58 ms | 14.7 / 19.3 / 21.6 |
| set | 1_key_commit | 256 | 39.8k | 29.4k | 0.74 | 39.6k | 5.73 ms / 21.6 ms | 7.91 ms / 20.3 ms | 5.71 ms / 24.4 ms | 9.2 / 12.8 / 14.8 |
| scan | 100k/iterator | 1 | 6.96M | 3.64M | 0.52 |  | 13.9 ms / 21.2 ms | 24.7 ms / 48.8 ms |  | 0.1 / 0.2 /  |
| scan | 100k/wantall | 1 | 7.00M | 4.14M | 0.59 |  | 14.0 ms / 16.9 ms | 23.9 ms / 28.2 ms |  | 0.1 / 0.2 /  |
| scan | 10k/iterator | 1 | 6.01M | 2.71M | 0.45 |  | 1.62 ms / 2.23 ms | 3.46 ms / 4.73 ms |  | 0.1 / 0.2 /  |
| scan | 10k/wantall | 1 | 6.91M | 2.83M | 0.41 |  | 1.37 ms / 1.85 ms | 3.35 ms / 5.30 ms |  | 0.1 / 0.2 /  |
| mixed | 90r_10w_10ops | 1 | 1239 | 507 | 0.41 | 1456 | 467 us / 3.76 ms | 1.42 ms / 5.73 ms | 324 us / 3.71 ms | 173.9 / 180.5 / 151.1 |
| mixed | 90r_10w_10ops | 16 | 13.9k | 6380 | 0.46 | 16.1k | 649 us / 6.28 ms | 1.76 ms / 12.0 ms | 526 us / 6.39 ms | 62.4 / 116.0 / 112.8 |
| mixed | 90r_10w_10ops | 256 | 27.8k | 33.0k | 1.19 | 35.7k | 8.70 ms / 14.8 ms | 6.84 ms / 13.6 ms | 6.60 ms / 12.8 ms | 52.7 / 101.9 / 91.3 |
| many_tx | 3r_2w_commit | 1000 | 34.8k | 40.6k | 1.17 | 41.8k | 21.4 ms / 191.3 ms | 19.5 ms / 105.2 ms | 18.8 ms / 145.5 ms | 25.3 / 48.8 / 44.1 |
| many_tx | 3r_2w_commit | 5000 | 32.7k | 28.2k | 0.86 | 31.4k | 121.3 ms / 402.0 ms | 139.6 ms / 568.0 ms | 132.0 ms / 552.8 ms | 34.4 / 66.4 / 55.8 |
| watch | fire_5000 | 5000 | 108.1k | 114.1k | 1.06 | 122.0k | 37.8 ms / 46.1 ms | 34.8 ms / 42.6 ms | 33.1 ms / 40.9 ms | 8.1 / 9.3 / 8.2 |
| watch | setup | 1 | 72.7 | 54.7 | 0.75 | 58.0 | 147 us / 2.98 ms | 1.16 ms / 3.17 ms | 128 us / 1.51 ms | 232.9 / 276.3 / 317.1 |
| watch | setup_5000_c256 | 256 | 82.5k | 81.0k | 0.98 | 108.3k | 3.02 ms / 5.29 ms | 2.15 ms / 3.65 ms | 1.58 ms / 4.37 ms | 14.5 / 35.7 / 33.2 |
| watch | write_to_fire | 1 | 72.7 | 54.7 | 0.75 | 58.0 | 11.2 ms / 19.0 ms | 15.6 ms / 29.5 ms | 12.5 ms / 26.6 ms | 0.0 / 0.1 / 0.1 |
| watch | writer_commit | 1 | 72.7 | 54.7 | 0.75 | 58.0 | 3.48 ms / 9.43 ms | 4.44 ms / 19.0 ms | 3.44 ms / 20.9 ms | 0.0 / 0.0 / 0.0 |

### Single-process cluster, 1 application thread

One tokio worker thread for the application and, for libfdb_c, its network thread in addition
(so libfdb_c has two threads of CPU, native one).

| workload | variant | conc | libfdb_c ops/s | native ops/s | ratio | libfdb_c p50 / p99 | native p50 / p99 | CPU us/op c / native |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 9171 | 852 | 0.09 | 101 us / 153 us | 1.12 ms / 2.21 ms | 30.3 / 32.8 |
| get | fresh_tx | 16 | 98.0k | 12.6k | 0.13 | 142 us / 347 us | 1.24 ms / 2.26 ms | 6.9 / 8.0 |
| get | fresh_tx | 256 | 166.3k | 110.4k | 0.66 | 1.44 ms / 2.51 ms | 2.15 ms / 3.61 ms | 6.2 / 7.7 |
| get_seq | 100_per_tx | 1 | 24.2k | 25.0k | 1.03 | 38 us / 54 us | 27 us / 43 us | 14.6 / 11.1 |
| get_seq | 100_per_tx | 16 | 218.7k | 108.2k | 0.49 | 65 us / 132 us | 135 us / 241 us | 5.5 / 6.6 |
| get_seq | 100_per_tx | 256 | 243.9k | 145.6k | 0.60 | 944 us / 1.98 ms | 1.56 ms / 3.02 ms | 5.2 / 6.9 |
| mixed | 90r_10w_10ops | 1 | 879 | 490 | 0.56 | 491 us / 8.08 ms | 1.36 ms / 7.60 ms | 204.1 / 116.2 |
| mixed | 90r_10w_10ops | 16 | 7459 | 5688 | 0.76 | 859 us / 13.4 ms | 2.29 ms / 12.2 ms | 80.7 / 67.1 |
| mixed | 90r_10w_10ops | 256 | 22.9k | 15.8k | 0.69 | 9.82 ms / 20.9 ms | 15.8 ms / 22.0 ms | 54.1 / 62.2 |
| many_tx | 3r_2w_commit | 1000 | 30.3k | 28.7k | 0.94 | 25.3 ms / 149.9 ms | 30.3 ms / 137.4 ms | 30.3 / 31.0 |
| many_tx | 3r_2w_commit | 5000 | 20.6k | 20.0k | 0.97 | 156.4 ms / 900.5 ms | 197.4 ms / 886.1 ms | 42.7 / 47.9 |

### Three-process cluster (`double memory`), 4 application threads

| workload | variant | conc | libfdb_c ops/s | native ops/s | ratio | libfdb_c p50 / p99 | native p50 / p99 | CPU us/op c / native |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| get | fresh_tx | 1 | 6068 | 766 | 0.13 | 161 us / 212 us | 1.17 ms / 3.52 ms | 35.4 / 40.2 |
| get | fresh_tx | 16 | 86.4k | 13.0k | 0.15 | 182 us / 258 us | 1.23 ms / 1.37 ms | 9.1 / 15.3 |
| get | fresh_tx | 256 | 178.0k | 148.9k | 0.84 | 1.37 ms / 2.55 ms | 1.85 ms / 2.54 ms | 7.8 / 12.2 |
| set | 1_key_commit | 1 | 147 | 157 | 1.07 | 5.91 ms / 35.0 ms | 5.70 ms / 17.4 ms | 122.6 / 63.3 |
| set | 1_key_commit | 16 | 2000 | 1558 | 0.78 | 6.16 ms / 41.7 ms | 9.47 ms / 22.2 ms | 16.4 / 25.0 |
| set | 1_key_commit | 256 | 21.2k | 20.3k | 0.96 | 9.68 ms / 35.9 ms | 11.0 ms / 49.7 ms | 9.3 / 14.3 |
| mixed | 90r_10w_10ops | 1 | 453 | 425 | 0.94 | 632 us / 13.1 ms | 1.51 ms / 10.1 ms | 251.3 / 204.4 |
| mixed | 90r_10w_10ops | 16 | 9167 | 5946 | 0.65 | 648 us / 15.3 ms | 1.66 ms / 14.7 ms | 104.9 / 147.6 |
| mixed | 90r_10w_10ops | 256 | 23.0k | 29.5k | 1.28 | 9.49 ms / 24.0 ms | 6.56 ms / 30.6 ms | 63.7 / 123.7 |
| many_tx | 3r_2w_commit | 1000 | 28.6k | 30.0k | 1.05 | 26.3 ms / 202.2 ms | 25.5 ms / 163.4 ms | 34.4 / 62.4 |
| many_tx | 3r_2w_commit | 5000 | 29.4k | 41.4k | 1.41 | 107.3 ms / 715.3 ms | 89.7 ms / 521.3 ms | 44.0 / 76.6 |

### Peak RSS

Peak resident set size of the whole process (tokio runtime and the bench's buffers included),
medians, single-process cluster, 4 threads:

| workload | concurrency | libfdb_c | native |
|---|---:|---:|---:|
| get | 1 / 256 | 23 / 37 MB | 17 / 35 MB |
| scan | 100k rows (WantAll / Iterator) | 40 / 40 MB | 48 / 62 MB |
| many_tx | 1000 | 50 MB | 72 MB |
| many_tx | 5000 | 138 MB | 264 MB |

## Analysis

**The 1 ms floor (GRV batcher).** Every number where native loses by a lot at concurrency 1 or
16 (`get`, the `mixed` mix, `watch` setup, `set`) has the same shape: native latency is the
libfdb_c latency plus about one millisecond. `get_seq`, which pays for the read version once and
then only reads, shows native faster (28 us vs 38 us), so the read path itself (location cache,
load balancing, request encoding, reply decoding) is not the problem. The GRV batcher follows
C++'s `readVersionBatcher`: it waits `batch_time` (half the smoothed GRV latency, capped at
5 ms) after the first request before sending. On a LAN `batch_time` is tens of microseconds, and
`Runtime::sleep` on tokio cannot sleep that short: tokio's timer wheel has 1 ms resolution
(measured: `sleep(0)`, `sleep(20us)` and `sleep(100us)` all take 1.05 ms; `sleep(1ms)` takes
2.06 ms). With 256 concurrent transactions the delay is hidden because the batches fill and
the proxy is busy anyway (native 144k vs 176k ops/s, p50 1.9 ms vs 1.5 ms). Bead
`foundationdb-rs-native-8jn`.

**Threading.** libfdb_c does all client work on one thread, so one core caps it: at 256
concurrent gets it uses 1.2 cores and tops out near 176k gets/s, and with 100 gets per
transaction it uses 1.5 cores for 272k gets/s (its server round trips overlap, but the
network thread is saturated). Native spreads its work over the tokio workers (1.6 cores at
256 fresh gets, 3.7 cores for `get_seq`), so with 4 application threads it reaches 386k gets/s
on `get_seq`. With a single application thread (and libfdb_c's network thread as its second)
native is behind in throughput at high concurrency (110k vs 166k gets/s; 146k vs 244k for
`get_seq`) because it then has one core against libfdb_c's two, and it spends more CPU per
operation (below).

**CPU per operation.** Native spends 1.5 to 2 times libfdb_c's CPU for the same work once the
loop is busy (get at 256: 11.1 us vs 6.6; get_seq: 9.5 vs 5.5; mixed at 256: 102 vs 53 us per
transaction; many_tx at 1000: 49 vs 25). The perf profile of the `get` workload (native,
frame-pointer build, 33k samples) puts 32% in libc, of which memcmp 8.6% comes mainly from the
read-your-writes layer's ordered maps (the snapshot cache's `BTreeMap`, the write map's
persistent tree iterator and `RywIter`, the location cache's `BTreeMap`), malloc/free about 15%
(wire `Writer` slot and vtable vectors, per-request state, boxed task cells), SipHash about 4%
and clock reads (vdso) 7.7%. Beads `foundationdb-rs-native-e82`.

**Scans.** The profile of the scan workload is 54% libc, mostly malloc/free: each row is
copied out of the reply into an owned key and an owned value (`KeyValueRef::into_owned`), then
moved through the RYW merge iterator and the batch `Vec`. libfdb_c hands out pointers into the
reply buffer. At 100k rows that is the difference between 14 ms and 24 ms per scan; the
streaming mode makes no measurable difference at these sizes. Bead `foundationdb-rs-native-s1t`.

**Commits, many transactions, watches.** Commit-bound workloads are at par within the noise of
a shared disk, and `many_tx` at 1000 and 5000 in flight shows no scaling problem in the client
(throughput and tail latency are within noise, native sometimes ahead). Native needs about
twice the memory per in-flight transaction (about 50 KB vs 27 KB); bead
`foundationdb-rs-native-0et`. 5000 watches register and fire at libfdb_c's rate.

## With the GRV timer fix

The `native+fix` column is the native client with one change in `read/grv.rs`'s `run_batcher`:
when `batch_time` is under 1 ms the batch timer is an immediately-ready future (the loop still
yields once before sending, as C++'s `delay(0)` does) instead of `rt.sleep(batch_time)`. It was
built from a scratch copy of the crates (the committed client is unchanged), run as the
native-only suite `single-4t-grvfix` with the same method but at a different time than the
`single-4t` run (load average 4.5 to 15, median 6.6), so small differences are not significant.
With it native matches or beats libfdb_c on read latency at every concurrency (get p50 58 us
at 1, 97 us at 16, 751 us at 256 vs 98 us, 142 us, 1.48 ms), and the low-concurrency `mixed`
and `set` gaps close. It is a measurement of what the floor costs, not a proposed patch: the
bead lists what a real fix has to consider (other short sleeps, batching under load, the smol
adapter).

## Reproducing

```bash
tools/bench/run.sh full                 # ~1 h: single-4t, single-1t, triple-4t into tools/bench/results/<time>/
tools/bench/run.sh quick                # ~2 min smoke test
python3 tools/bench/summarize.py tools/bench/results/<run>/single-4t   # side-by-side medians
```

The raw data of the runs in this document (every measurement of every repetition, the server
`status`, the host) is in [benchmarks-data/2026-11-24](benchmarks-data/2026-11-24); regenerate
the tables with `tools/bench/summarize.py docs/benchmarks-data/2026-11-24/single-4t --fix
docs/benchmarks-data/2026-11-24/single-4t-grvfix`.

Needs docker, the host `libfdb_c.so` 7.4.x (the `foundationdb` crate links it dynamically) and
libclang (its bindgen). `FDB_IMAGE` selects the server image (a 7.4 image). Each suite
directory has `results.jsonl` (every measurement of every repetition), `results.md` (medians
with min-max, p90/p99.9, cores busy, peak RSS) and `meta.json` (image, cluster, the server's
`status`). `tools/bench/README.md` describes the tool.

Profiling recipe (perf, user space only; perf_event_paranoid 1 is enough):

```bash
cd tools/bench
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --target-dir /path/to/target-fp
./target/release/fdb-bench serve            # starts a cluster, loads keys, prints "cluster-file: ..."
perf record -F 1999 --call-graph fp -o get.data /path/to/target-fp/release/fdb-bench worker \
    --backend native --workload get --threads 4 --duration 6 --warmup 1 --cluster-file FILE
perf report -i get.data --no-children --sort symbol --stdio -g none | head -40
python3 -I tools/bench/perf_callers.py get.data memcmp   # who calls a leaf (first fdb_native frame)
# Ctrl-C the `serve` process to remove its container.
```

## Findings filed

| bead | priority | what |
|---|---|---|
| `foundationdb-rs-native-8jn` | P2 | GRV batcher sleep rounds up to 1 ms on tokio: every transaction pays about 1 ms before its first read |
| `foundationdb-rs-native-s1t` | P3 | Range scans 1.7 to 2.4 times slower than libfdb_c: per-row key/value allocations |
| `foundationdb-rs-native-e82` | P3 | Client CPU per operation 1.5 to 2 times libfdb_c: RYW iterators, hashing, Writer allocations, clock reads |
| `foundationdb-rs-native-0et` | P4 | Memory per in-flight transaction about 2 times libfdb_c |

## Not covered

TLS, 8.0 servers (libfdb_c 7.4 cannot talk to them), larger values and batches, multi-version
clients, a quiet dedicated machine, a remote (non-loopback) server, long-running soak, and the
`smol` runtime adapter.
