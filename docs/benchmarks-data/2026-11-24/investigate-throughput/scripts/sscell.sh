#!/usr/bin/env bash
# One cell with the storage server's GetValueQueries counter (StorageMetrics) before and after.
# Usage: sscell.sh CONTAINER CLUSTER_FILE BACKEND WORKLOAD CONC [worker args]
set -euo pipefail
C="$1"; CF="$2"; BE="$3"; WL="$4"; CONC="$5"; shift 5
HERE="$(cd "$(dirname "$0")" && pwd)"
q() { docker exec "$C" sh -c 'grep -h "Type=\"StorageMetrics\"" /var/fdb/logs/*.xml | tail -1' | python3 -I -c '
import re,sys; s=sys.stdin.read(); print(int(float(re.search("GetValueQueries=\"([^\"]*)\"",s).group(1).split()[-1])))'; }
sleep 5.5; B=$(q)
OUT=$("$HERE/bench/target/release/fdb-bench" worker --backend "$BE" --cluster-file "$CF" --workload "$WL" --concs "$CONC" "$@")
sleep 11; A=$(q)
python3 -I -c '
import json,sys
r=json.loads(sys.argv[1].splitlines()[0]); per=100 if sys.argv[3]=="get_seq" else 1
print(json.dumps({"backend":sys.argv[2],"workload":sys.argv[3],"all_iters":r["all_iters"],"expected_gets":r["all_iters"]*per,"ss_get_value_queries":int(sys.argv[4])-int(sys.argv[5]),"ops_per_s":round(r["ops_per_s"])}))' "$OUT" "$BE" "$WL" "$A" "$B"
