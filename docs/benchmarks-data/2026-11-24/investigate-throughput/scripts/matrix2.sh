#!/usr/bin/env bash
# Threading: per-thread CPU of both clients at several thread configurations.
cd "$(dirname "$0")"
CF=/tmp/fdb-native-testkit-Tak7gJ/fdb.cluster
T() { ./threadcpu.sh $CF "$@" >> runs/matrix2.jsonl 2>>runs/matrix2.err; }
for wl in get get_seq; do
  for conc in 16 256; do
    T native $wl $conc --flavor current
    T native $wl $conc --threads 1
    T native $wl $conc --threads 2
    T native $wl $conc --threads 4
    T c $wl $conc --threads 1
    T c $wl $conc --threads 4
    FDB_BENCH_C_THREADS=2 FDB_BENCH_C_DBS=2 T c $wl $conc --threads 4
    FDB_BENCH_C_THREADS=4 FDB_BENCH_C_DBS=4 T c $wl $conc --threads 4
  done
done
echo done >> runs/matrix2.err
