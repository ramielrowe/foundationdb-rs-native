#!/usr/bin/env bash
# GRV accounting at the benchmark's own cells: both clients, 4 tokio workers.
cd "$(dirname "$0")"
C=9fb21046bba6; CF=/tmp/fdb-native-testkit-Tak7gJ/fdb.cluster
for rep in 1 2; do
  for wl in get get_seq; do
    for conc in 16 256; do
      if [ $rep = 1 ]; then order="native c"; else order="c native"; fi
      for be in $order; do ./grvcell.sh $C $CF $be $wl $conc --threads 4 | sed "s/^{/{\"rep\": $rep, /" >> runs/matrix1.jsonl; done
    done
  done
done
for be in native c; do ./grvcell.sh $C $CF $be grv_only 256 --threads 4 >> runs/matrix1.jsonl; done
echo done
