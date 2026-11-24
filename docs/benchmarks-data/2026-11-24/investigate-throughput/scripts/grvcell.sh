#!/usr/bin/env bash
# One bench cell with the GRV proxy's counters (GrvProxyMetrics, cumulative totals) read from the
# disposable cluster's trace logs before and after. Usage:
#   grvcell.sh CONTAINER CLUSTER_FILE BACKEND WORKLOAD CONC [extra worker args...]
set -euo pipefail
C="$1"; CF="$2"; BACKEND="$3"; WL="$4"; CONC="$5"; shift 5
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/bench/target/release/fdb-bench"
counters() {
  docker exec "$C" sh -c 'grep -h "Type=\"GrvProxyMetrics\"" /var/fdb/logs/*.xml | tail -1' |
    python3 -I -c '
import re,sys,json
s=sys.stdin.read()
out={}
for k in ["TxnRequestIn","TxnStartIn","TxnStartBatch","TxnSystemPriorityStartIn","TxnDefaultPriorityStartIn","TxnBatchPriorityStartIn"]:
    m=re.search(k+"=\"([^\"]*)\"",s); out[k]=int(float(m.group(1).split()[-1]))
m=re.search("Time=\"([0-9.]+)\"",s); out["time"]=float(m.group(1))
print(json.dumps(out))'
}
B=$(counters)
sleep 5.5
B=$(counters)
OUT=$("$BIN" worker --backend "$BACKEND" --cluster-file "$CF" --workload "$WL" --concs "$CONC" "$@")
sleep 11
A=$(counters)
python3 -I - "$B" "$A" "$OUT" "$BACKEND" "$WL" "$CONC" "$*" <<'PY'
import json,sys
b=json.loads(sys.argv[1]); a=json.loads(sys.argv[2])
d={k:a[k]-b[k] for k in a}
out=[json.loads(l) for l in sys.argv[3].splitlines() if l.strip().startswith("{")]
row=out[0] if out else {}
if "rows" in row: row=row["rows"][0]
reqs=d["TxnRequestIn"]-d["TxnSystemPriorityStartIn"]  # system-priority requests are ~1 tx each
res={"backend":sys.argv[4],"workload":sys.argv[5],"conc":int(sys.argv[6]),"args":sys.argv[7],
     "ops_per_s":row.get("ops_per_s"),"all_iters":row.get("all_iters"),"p50_us":row.get("p50_us"),"cpu_us_per_op":row.get("cpu_us_per_op"),"cpu_cores":row.get("cpu_cores"),
     "srv_default_tx":d["TxnDefaultPriorityStartIn"],"srv_batch_prio_tx":d["TxnBatchPriorityStartIn"],"srv_system_tx":d["TxnSystemPriorityStartIn"],
     "srv_requests_nonsystem":reqs,"tx_per_grv":(d["TxnDefaultPriorityStartIn"]/reqs if reqs>0 else None),
     "counter_window_s":a["time"]-b["time"]}
if not out: res["raw"]=sys.argv[3]
print(json.dumps(res))
PY
