#!/usr/bin/env bash
# Benchmarks of fdb-native-client against libfdb_c 7.4.10 on disposable testkit clusters.
# Dev-only; nothing here is part of the workspace build. See tools/bench/README.md and
# docs/benchmarks.md.
#
# Safety: the only clusters this talks to are the ones `fdb-bench run` starts itself
# (labelled testkit containers, removed on exit). It never takes a cluster file.
#
# Usage: tools/bench/run.sh [suite] [-- extra fdb-bench run args]
#   quick   one repetition, 2 s windows, three workloads (a smoke test, ~2 min)
#   single  the standard suite on a single-process memory cluster (3 reps, 8 s windows):
#           all workloads with 4 application threads, then get/get_seq/mixed/many_tx with 1
#   triple  a 3-process (double, memory) cluster: get/set/mixed/many_tx, 4 threads
#   full    single + triple (default; about an hour)
#   threads why native out-runs libfdb_c: native with a current-thread runtime and 1, 2, 4
#           workers against libfdb_c with 1, 2, 4 network threads (get, get_seq at 16 and 256;
#           single cluster, 3 reps, 8 s windows; about 20 min)
#   grv     grv_only (a fresh transaction's read version, no read) at 256, both clients
#   causal  the causal-consistency check (workload `causal`), both clients, 4 threads, on a
#           single-process cluster and on a 3-process cluster with 2 GRV and 2 commit
#           proxies, each with its negative control, which must find violations. CAUSAL_SECS
#           (default 60) is the measured seconds per cell. Exits non-zero if a check finds
#           a violation or a control finds none.
#
# Needs: docker, the host's libfdb_c.so (7.4.x) and libclang (the `foundationdb` crate's
# bindgen), and the image `foundationdb/foundationdb:7.4.10` (FDB_IMAGE overrides the testkit
# default; libfdb_c 7.4 cannot talk to other server versions, so the image must be a 7.4 server).

set -euo pipefail
FAILED=0

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUITE="${1:-full}"
shift || true
[ "${1:-}" = "--" ] && shift
EXTRA=("$@")

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${BENCH_OUT:-$HERE/results/$STAMP}"
mkdir -p "$OUT"

cd "$HERE"
cargo build --release
BIN="$HERE/target/release/fdb-bench"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "git: $(git -C "$HERE" rev-parse HEAD)"
  echo "rustc: $(rustc --version)"
  echo "fdbcli: $(fdbcli --version | head -1)"
  echo "kernel: $(uname -sr)"
  echo "loadavg: $(cat /proc/loadavg)"
  lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|max MHz'
  free -g | head -2
} >"$OUT/host.txt"

run() { # name, args...
  local name="$1"
  shift
  echo "=== $name -> $OUT/$name" >&2
  "$BIN" run --out "$OUT/$name" "$@" "${EXTRA[@]}" || FAILED=1
}

case "$SUITE" in
  quick)
    run quick --cluster single --reps 1 --duration 2 --warmup 1 --workloads get,set,scan
    ;;
  single)
    run single-4t --cluster single --threads 4
    run single-1t --cluster single --threads 1 --workloads get,get_seq,mixed,many_tx
    ;;
  triple)
    run triple-4t --cluster triple --threads 4 --workloads get,set,mixed,many_tx
    ;;
  full)
    run single-4t --cluster single --threads 4
    run single-1t --cluster single --threads 1 --workloads get,get_seq,mixed,many_tx
    run triple-4t --cluster triple --threads 4 --workloads get,set,mixed,many_tx
    ;;
  threads)
    # native with 1, 2 and 4 workers and with a current-thread runtime; libfdb_c with 1, 2
    # and 4 network threads under 4 application workers. The report tells the clients apart
    # by name (native, native/current, c, c x2, c x4).
    for wl in get get_seq; do
      run "threads-native-$wl" --cluster single --workloads "$wl" --concs 16,256 \
        --threads 1,2,4 --backends native,native:current
      run "threads-c-$wl" --cluster single --workloads "$wl" --concs 16,256 \
        --threads 4 --backends c,c:2,c:4
    done
    ;;
  grv)
    run grv --cluster single --workloads grv_only --concs 256 --threads 4
    ;;
  causal)
    SECS="${CAUSAL_SECS:-60}"
    CAUSAL=(--workloads causal --concs 256 --threads 4 --reps 1 --duration "$SECS" --warmup 2)
    PROXIES=(--configure "grv_proxies=2 commit_proxies=2")
    run causal-single --cluster single "${CAUSAL[@]}"
    run causal-single-negative --cluster single "${CAUSAL[@]}" --causal-negative
    run causal-triple --cluster triple "${PROXIES[@]}" "${CAUSAL[@]}"
    run causal-triple-negative --cluster triple "${PROXIES[@]}" "${CAUSAL[@]}" --causal-negative
    ;;
  *)
    echo "usage: $0 quick|single|triple|full|threads|grv|causal [-- args for fdb-bench run]" >&2
    exit 2
    ;;
esac
echo "results: $OUT" >&2
exit "$FAILED"
