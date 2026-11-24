#!/usr/bin/env bash
# Runs one worker cell and samples its threads' CPU (from /proc) for 4 s inside the measured
# window. Usage: threadcpu.sh CLUSTER_FILE BACKEND WORKLOAD CONC [worker args...]
# Env (FDB_BENCH_C_THREADS etc.) passes through.
set -euo pipefail
CF="$1"; BE="$2"; WL="$3"; CONC="$4"; shift 4
HERE="$(cd "$(dirname "$0")" && pwd)"
OUTF=$(mktemp -p "$HERE/runs")
"$HERE/bench/target/release/fdb-bench" worker --backend "$BE" --cluster-file "$CF" --workload "$WL" --concs "$CONC" "$@" > "$OUTF" &
PID=$!
sleep 4
snap() { for t in /proc/$PID/task/*; do echo "$(basename $t) $(cat $t/comm | tr ' ' _) $(awk '{print $14+$15}' $t/stat)"; done; }
A=$(snap); sleep 4; B=$(snap)
wait $PID
python3 -I - "$A" "$B" "$(cat $OUTF)" "$BE $WL $CONC $* C_THREADS=${FDB_BENCH_C_THREADS:-1}" <<'PY'
import sys, json, os
hz = os.sysconf("SC_CLK_TCK")
a = {l.split()[0]: (l.split()[1], int(l.split()[2])) for l in sys.argv[1].splitlines()}
b = {l.split()[0]: (l.split()[1], int(l.split()[2])) for l in sys.argv[2].splitlines()}
per = sorted(((b[t][1] - a[t][1]) / hz / 4.0, b[t][0]) for t in b if t in a)
per = [p for p in per if p[0] > 0.01][::-1]
row = json.loads(sys.argv[3].splitlines()[0])
print(json.dumps({"cell": sys.argv[4], "ops_per_s": round(row["ops_per_s"]), "p50_us": row["p50_us"],
                  "cpu_us_per_op": round(row["cpu_us_per_op"], 2), "cpu_cores": round(row["cpu_cores"], 2),
                  "threads_over_1pct": [(n, round(c, 2)) for c, n in per]}))
PY
rm -f "$OUTF"
