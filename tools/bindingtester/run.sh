#!/usr/bin/env bash
# Run the upstream FoundationDB bindingtester (Python binding + libfdb_c, in Docker) against the
# native Rust stack tester, or against itself (--self-test) to check the harness.
#
# Dev-only; nothing here is part of the workspace build. See tools/bindingtester/README.md.
#
# Safety: the only cluster this ever talks to is a fresh, disposable FDB container it starts
# itself (labelled, removed by exact ID on exit). A malformed packet can make an FDB 7.4/8.0
# server exit, so never point the tester at a shared or long-lived cluster.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=tools/bindingtester/versions.sh
source "$HERE/versions.sh"

usage() {
  cat <<'EOF'
Usage: tools/bindingtester/run.sh [options]

  -V, --version 7.4|8.0     FDB release to test against (default 7.4; pins 7.4.10 / 8.0.1)
  -s, --suites LIST         comma list of api,scripted,tuple (default api,scripted,tuple)
      --seeds N             number of seeds per suite (default 3)
      --seed-start S        first seed (default 1); seeds are S..S+N-1
      --seed-list A,B,C     explicit seeds (overrides --seeds/--seed-start)
  -n, --num-ops N           operations per api test (default 200; scripted/tuple ignore it)
  -c, --concurrency C       api only: C > 1 runs the tester alone with C threads (upstream
                            forbids --compare with concurrency); default 1 = compare
  -t, --tester PATH         tester binary (default: cargo build --release -p
                            fdb-native-stacktester, then target/release/fdb-native-stacktester)
      --no-build            do not run cargo; use the default path as is
      --self-test           compare the Python tester with itself; no Rust binary needed
      --api-version V       pin the API version (default: bindingtester picks per seed;
                            ignored for scripted, which always uses the latest)
      --max-int-bits N      cap integer width of generated tests (default: tester's, 2040)
      --no-threads          tester does not implement START_THREAD
      --timeout SECS        per tester process (default 600)
      --bt-arg ARG          extra argument for bindingtester.py (repeatable), e.g. --print-test
      --tester-env K=V      environment variable for the tester process (repeatable)
      --int-edge skip|keep  skip (default) rewrites the +-(2^64-1) tuple test values that the
                            Python binding packs differently (README); keep fails on them
      --results-dir DIR     where logs go (default tools/bindingtester/results/<time>-<version>)
      --keep-logs           keep logs of passing seeds too (default: failures only)
      --rebuild-image       rebuild the bindingtester image even if it exists
      --build-image         build the image and exit
  -h, --help

Environment: FDB_SRC=/path/to/foundationdb (a clone with the pinned release commits; read only,
used with `git archive`). Unset: the pinned commit is fetched from GitHub into
tools/bindingtester/.cache.

Exit status: 0 all seeds passed, 1 at least one failed, 2 harness error.
EOF
}

die() {
  echo "run.sh: $*" >&2
  exit 2
}

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

VERSION=7.4
SUITES=api,scripted,tuple
SEED_COUNT=3
SEED_START=1
SEED_LIST=""
NUM_OPS=200
CONCURRENCY=1
TESTER=""
BUILD_TESTER=1
SELF_TEST=0
API_VERSION=""
MAX_INT_BITS=""
THREADS=1
TIMEOUT=600
TESTER_ENV=()
BT_ARGS=()
INT_EDGE=skip
RESULTS_DIR=""
KEEP_LOGS=0
REBUILD_IMAGE=0
BUILD_IMAGE_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -V | --version) VERSION="${2:?}"; shift 2 ;;
    -s | --suites) SUITES="${2:?}"; shift 2 ;;
    --seeds) SEED_COUNT="${2:?}"; shift 2 ;;
    --seed-start) SEED_START="${2:?}"; shift 2 ;;
    --seed-list) SEED_LIST="${2:?}"; shift 2 ;;
    -n | --num-ops) NUM_OPS="${2:?}"; shift 2 ;;
    -c | --concurrency) CONCURRENCY="${2:?}"; shift 2 ;;
    -t | --tester) TESTER="${2:?}"; BUILD_TESTER=0; shift 2 ;;
    --no-build) BUILD_TESTER=0; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    --api-version) API_VERSION="${2:?}"; shift 2 ;;
    --max-int-bits) MAX_INT_BITS="${2:?}"; shift 2 ;;
    --no-threads) THREADS=0; shift ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --bt-arg) BT_ARGS+=("${2:?}"); shift 2 ;;
    --tester-env) TESTER_ENV+=("${2:?}"); shift 2 ;;
    --int-edge) INT_EDGE="${2:?}"; shift 2 ;;
    --results-dir) RESULTS_DIR="${2:?}"; shift 2 ;;
    --keep-logs) KEEP_LOGS=1; shift ;;
    --rebuild-image) REBUILD_IMAGE=1; shift ;;
    --build-image) BUILD_IMAGE_ONLY=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

bt_pins "$VERSION" || die "unknown --version '$VERSION' (expected 7.4 or 8.0)"
[[ "$INT_EDGE" == skip || "$INT_EDGE" == keep ]] || die "--int-edge must be skip or keep"
[[ "$CONCURRENCY" =~ ^[0-9]+$ && "$CONCURRENCY" -ge 1 ]] || die "--concurrency must be >= 1"

IFS=, read -r -a SUITE_ARR <<<"$SUITES"
for s in "${SUITE_ARR[@]}"; do
  case "$s" in api | scripted | tuple) ;; *) die "unknown suite '$s' (api, scripted, tuple)" ;; esac
done

if [[ -n "$SEED_LIST" ]]; then
  IFS=, read -r -a SEEDS <<<"$SEED_LIST"
else
  SEEDS=()
  for ((i = 0; i < SEED_COUNT; i++)); do SEEDS+=("$((SEED_START + i))"); done
fi
[[ ${#SEEDS[@]} -gt 0 ]] || die "no seeds"

command -v docker >/dev/null || die "docker not found"

# ---- containers: tracked by exact ID, removed on exit ---------------------------------------
RUN_ID="bt-$$-$(date +%s%N)"
LABEL="fdb-native-bindingtester-run=$RUN_ID"
CONTAINERS=()
SERVER=""
RUNNER=""

remove_containers() {
  local id
  for id in ${CONTAINERS[@]+"${CONTAINERS[@]}"}; do
    docker rm -f -v "$id" >/dev/null 2>&1 || echo "run.sh: could not remove container $id" >&2
  done
  CONTAINERS=()
}
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  remove_containers
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---- image ------------------------------------------------------------------------------------
CACHE="$HERE/.cache"
mkdir -p "$CACHE"

IMAGE_HASH="$(
  {
    cat "$HERE/Dockerfile" "$HERE/overlay/run_bindingtester.py"
    echo "$BT_BASE_IMAGE $FDB_VERSION $FDB_COMMIT $FDB_IMAGE $WHEEL_SHA256 $LIBFDB_C_SHA256"
  } | sha256sum | cut -c1-12
)"
IMAGE_TAG="fdb-native-bindingtester:$FDB_VERSION-$IMAGE_HASH"

fetch_source() { # <dest dir>: bindings/bindingtester and bindings/python/tests at FDB_COMMIT
  local dest="$1" repo
  if [[ -n "${FDB_SRC:-}" ]]; then
    repo="$FDB_SRC"
    [[ "$(git -C "$repo" rev-parse --verify -q "$FDB_COMMIT^{commit}")" == "$FDB_COMMIT" ]] ||
      die "FDB_SRC=$repo does not contain commit $FDB_COMMIT ($FDB_VERSION); git fetch --tags there"
  else
    repo="$CACHE/fdb-src"
    if [[ "$(git -C "$repo" rev-parse --verify -q "$FDB_COMMIT^{commit}" 2>/dev/null || true)" != "$FDB_COMMIT" ]]; then
      log "fetching $FDB_COMMIT from github.com/apple/foundationdb"
      git init -q "$repo"
      git -C "$repo" fetch -q --depth 1 --filter=blob:none https://github.com/apple/foundationdb.git "$FDB_COMMIT"
    fi
  fi
  mkdir -p "$dest"
  git -C "$repo" archive "$FDB_COMMIT" bindings/bindingtester bindings/python/tests | tar -x -C "$dest"
}

fetch_wheel() { # <dest file>
  local dest="$1" cached="$CACHE/wheels/$WHEEL_SHA256.whl"
  if [[ ! -f "$cached" ]]; then
    mkdir -p "$CACHE/wheels"
    log "downloading $WHEEL_URL"
    curl -fsSL --retry 3 -o "$cached.tmp" "$WHEEL_URL"
    echo "$WHEEL_SHA256  $cached.tmp" | sha256sum -c - >/dev/null || {
      rm -f "$cached.tmp"
      die "checksum mismatch for $WHEEL_URL"
    }
    mv "$cached.tmp" "$cached"
  fi
  echo "$WHEEL_SHA256  $cached" | sha256sum -c - >/dev/null || die "cached wheel $cached is corrupt"
  cp "$cached" "$dest"
}

ensure_image() {
  if [[ "$REBUILD_IMAGE" -eq 0 ]] && docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
    return
  fi
  local ctx="$CACHE/ctx-$FDB_VERSION"
  rm -rf "$ctx"
  mkdir -p "$ctx/overlay"
  fetch_source "$ctx/src"
  fetch_wheel "$ctx/fdb.whl"
  cp "$HERE/Dockerfile" "$ctx/Dockerfile"
  cp "$HERE/overlay/run_bindingtester.py" "$ctx/overlay/"
  log "building $IMAGE_TAG (FDB $FDB_VERSION @ ${FDB_COMMIT:0:12})"
  if ! DOCKER_BUILDKIT=1 docker build --progress=plain -t "$IMAGE_TAG" \
    --build-arg "FDB_IMAGE=$FDB_IMAGE" --build-arg "BASE_IMAGE=$BT_BASE_IMAGE" \
    --build-arg "FDB_VERSION=$FDB_VERSION" --build-arg "LIBFDB_C_SHA256=$LIBFDB_C_SHA256" \
    "$ctx" >"$CACHE/build-$FDB_VERSION.log" 2>&1; then
    tail -40 "$CACHE/build-$FDB_VERSION.log" >&2
    die "image build failed (full log: $CACHE/build-$FDB_VERSION.log)"
  fi
  rm -rf "$ctx"
}

ensure_image
if [[ "$BUILD_IMAGE_ONLY" -eq 1 ]]; then
  log "image $IMAGE_TAG ready"
  exit 0
fi

# ---- tester binary ----------------------------------------------------------------------------
TESTER_SHA=""
if [[ "$SELF_TEST" -eq 0 ]]; then
  if [[ -z "$TESTER" ]]; then
    if [[ "$BUILD_TESTER" -eq 1 ]]; then
      log "cargo build --release -p fdb-native-stacktester"
      (cd "$REPO_ROOT" && cargo build --release -p fdb-native-stacktester) ||
        die "building fdb-native-stacktester failed"
    fi
    TESTER="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release/fdb-native-stacktester"
  fi
  [[ -f "$TESTER" && -x "$TESTER" ]] || die "tester binary '$TESTER' is not an executable file"
  TESTER="$(cd "$(dirname "$TESTER")" && pwd)/$(basename "$TESTER")"
  [[ "$TESTER" != *" "* ]] || die "tester path must not contain spaces (the bindingtester splits the command on spaces)"
  TESTER_SHA="$(sha256sum "$TESTER" | cut -d' ' -f1)"
fi

# ---- results ----------------------------------------------------------------------------------
[[ -n "$RESULTS_DIR" ]] || RESULTS_DIR="$HERE/results/$(date -u +%Y%m%dT%H%M%SZ)-$VERSION"
mkdir -p "$RESULTS_DIR/cluster"
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
SUMMARY="$RESULTS_DIR/summary.tsv"
printf 'suite\tseed\tstatus\texit\tseconds\tdetail\n' >"$SUMMARY"
{
  echo "version:      $VERSION ($FDB_VERSION, commit $FDB_COMMIT)"
  echo "server image: $FDB_IMAGE"
  echo "bt image:     $IMAGE_TAG"
  echo "mode:         $([[ "$SELF_TEST" -eq 1 ]] && echo 'python vs python (self test)' || echo "python vs rust ($TESTER, sha256 $TESTER_SHA)")"
  echo "suites:       $SUITES"
  echo "seeds:        ${SEEDS[*]}"
  echo "num-ops:      $NUM_OPS  concurrency: $CONCURRENCY  api-version: ${API_VERSION:-random}  int-edge: $INT_EDGE"
} >"$RESULTS_DIR/run-info.txt"

# ---- disposable FDB server --------------------------------------------------------------------
start_server() {
  local id ip cf
  id="$(docker create --label "$LABEL" --label fdb-native-bindingtester=server \
    --name "fdb-bt-server-$$-$(date +%s%N)" "$FDB_IMAGE")" || die "docker create failed for $FDB_IMAGE"
  CONTAINERS+=("$id")
  docker start "$id" >/dev/null || die "docker start failed"
  SERVER="$id"
  ip="$(docker inspect -f '{{(index .NetworkSettings.Networks "bridge").IPAddress}}' "$id")"
  [[ -n "$ip" ]] || die "server container has no bridge IP"
  # The image's entrypoint writes the cluster file a moment after the container starts.
  local i
  cf=""
  for ((i = 0; i < 60; i++)); do
    cf="$(docker exec "$id" cat /var/fdb/fdb.cluster 2>/dev/null || true)"
    [[ -z "$cf" ]] || break
    [[ "$(docker inspect -f '{{.State.Running}}' "$id")" == true ]] || die "server container exited during startup"
    sleep 0.5
  done
  [[ "$cf" == "docker:docker@$ip:4500" ]] || die "unexpected cluster file '$cf' (expected docker:docker@$ip:4500)"

  for ((i = 0; i < 60; i++)); do
    if docker exec "$id" fdbcli --timeout 5 --exec "$FDB_CONFIGURE" >/dev/null 2>&1; then break; fi
    [[ "$(docker inspect -f '{{.State.Running}}' "$id")" == true ]] || {
      docker logs "$id" 2>&1 | tail -20 >&2
      die "server container exited during startup"
    }
    sleep 1
  done
  for ((i = 0; i < 60; i++)); do
    if docker exec "$id" fdbcli --timeout 5 --exec 'status minimal' 2>&1 | grep -q 'database is available'; then
      # In-place, so a running tester container sees a replaced server.
      printf '%s\n' "$cf" >"$RESULTS_DIR/cluster/fdb.cluster"
      printf '%s\n' "$RUN_ID" >"$RESULTS_DIR/cluster/.harness-owned"
      log "FDB $FDB_VERSION server ${id:0:12} up at $ip:4500"
      return
    fi
    sleep 1
  done
  die "server did not become available"
}

server_alive() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$SERVER" 2>/dev/null || echo false)" == true ]]
}

# The bindingtester wipes the whole database (`del db[:]`) before every test. Before each run,
# check that the cluster file it is about to be given is the one of the server container this
# script started itself (its labels, its current bridge IP). The runner repeats a cheaper check
# (overlay/run_bindingtester.py).
assert_target_is_ours() {
  local label ip cf
  label="$(docker inspect -f '{{index .Config.Labels "fdb-native-bindingtester-run"}}' "$SERVER")"
  [[ "$label" == "$RUN_ID" ]] || die "refusing to run: $SERVER is not a container started by this run"
  ip="$(docker inspect -f '{{(index .NetworkSettings.Networks "bridge").IPAddress}}' "$SERVER")"
  cf="$(cat "$RESULTS_DIR/cluster/fdb.cluster")"
  [[ "$cf" == "docker:docker@$ip:4500" ]] ||
    die "refusing to run: cluster file '$cf' is not this run's server (docker:docker@$ip:4500)"
}

start_runner() {
  local args=(--init --user "$(id -u):$(id -g)" --label "$LABEL" --label fdb-native-bindingtester=runner
    -e "BT_RUN_ID=$RUN_ID" -e "BT_PY_INT_EDGE=$INT_EDGE" -e "BT_TESTER_THREADS=$THREADS" -e HOME=/tmp
    -v "$RESULTS_DIR/cluster:/cluster")
  if [[ "$SELF_TEST" -eq 0 ]]; then
    args+=(-e "BT_TESTER_CMD=/tester/stacktester" -e "BT_TESTER_NAME=rust"
      -v "$TESTER:/tester/stacktester:ro")
    [[ -z "$MAX_INT_BITS" ]] || args+=(-e "BT_TESTER_MAX_INT_BITS=$MAX_INT_BITS")
  fi
  local kv
  for kv in ${TESTER_ENV[@]+"${TESTER_ENV[@]}"}; do args+=(-e "$kv"); done
  RUNNER="$(docker create "${args[@]}" --name "fdb-bt-runner-$$-$(date +%s%N)" "$IMAGE_TAG")" ||
    die "docker create failed for $IMAGE_TAG"
  CONTAINERS+=("$RUNNER")
  docker start "$RUNNER" >/dev/null || die "docker start failed"
}

start_server
start_runner

# ---- run --------------------------------------------------------------------------------------
OURS=rust
[[ "$SELF_TEST" -eq 0 ]] || OURS=python
NFAIL=0
NPASS=0

run_one() { # <suite> <seed>
  local suite="$1" seed="$2" log_dir="$RESULTS_DIR/$1" logfile start rc status detail
  mkdir -p "$log_dir"
  logfile="$log_dir/seed-$seed.log"

  if ! server_alive; then
    log "server container died; starting a fresh one"
    docker rm -f -v "$SERVER" >/dev/null 2>&1 || true
    start_server
  fi

  assert_target_is_ours
  local args=(--test-name "$suite" --seed "$seed" --cluster-file /cluster/fdb.cluster
    --timeout "$TIMEOUT" --logging-level INFO)
  [[ "$suite" == scripted ]] || [[ -z "$API_VERSION" ]] || args+=(--api-version "$API_VERSION")
  [[ "$suite" != api ]] || args+=(--num-ops "$NUM_OPS")
  [[ -z "$MAX_INT_BITS" ]] || args+=(--max-int-bits "$MAX_INT_BITS")
  [[ "$THREADS" -eq 1 ]] || args+=(--no-threads)
  if [[ "$suite" == api && "$CONCURRENCY" -gt 1 ]]; then
    args=("$OURS" "${args[@]}" --concurrency "$CONCURRENCY")
  else
    args=(python --compare "$OURS" "${args[@]}")
  fi

  args+=(${BT_ARGS[@]+"${BT_ARGS[@]}"})

  start=$SECONDS
  rc=0
  # The inner `timeout` bounds the whole bindingtester run (two tester processes plus generation).
  docker exec "$RUNNER" timeout -k 10 "$((TIMEOUT * 2 + 120))" \
    python3 -u /opt/bt/run_bindingtester.py "${args[@]}" >"$logfile" 2>&1 || rc=$?
  local secs=$((SECONDS - start))

  if [[ "$rc" -eq 0 ]]; then
    status=PASS
    detail=""
    NPASS=$((NPASS + 1))
  else
    status=FAIL
    [[ "$rc" -ne 124 && "$rc" -ne 137 ]] || status=TIMEOUT
    detail="$(grep -m1 -E 'incorrect result|did not complete|ERROR|Traceback|Exception' "$logfile" | tr '\t\n' '  ' | cut -c1-200 || true)"
    NFAIL=$((NFAIL + 1))
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$suite" "$seed" "$status" "$rc" "$secs" "$detail" >>"$SUMMARY"
  printf '  %-9s seed %-6s %-7s %4ss  %s\n' "$suite" "$seed" "$status" "$secs" "$detail"
  if [[ "$status" == PASS && "$KEEP_LOGS" -eq 0 ]]; then rm -f "$logfile"; fi
}

log "results: $RESULTS_DIR"
for suite in "${SUITE_ARR[@]}"; do
  if [[ "$suite" == scripted ]]; then
    # The scripted test is a fixed script (the seed is irrelevant), so run it once.
    run_one scripted "${SEEDS[0]}"
  else
    for seed in "${SEEDS[@]}"; do run_one "$suite" "$seed"; done
  fi
done

echo
log "$NPASS passed, $NFAIL failed (summary: $SUMMARY)"
[[ "$NFAIL" -eq 0 ]] || exit 1
