#!/usr/bin/env bash
# Regenerates the message-level golden vectors, fixtures/release-7.4 and fixtures/release-8.0,
# from the real FDB message types of each release branch, built inside the official
# FoundationDB build image. Needs docker and a FoundationDB checkout; nothing else on the host.
#
# Usage: tools/golden-vectors/regen_messages.sh [--check]
#
#   FDB_SRC        FoundationDB checkout (default: ../../../foundationdb from this directory).
#                  Only read: each branch is exported with `git archive` into build/msg/.
#   MSG_BRANCHES   refs to build (default: "origin/release-7.4 origin/release-8.0"). The fixture
#                  directory is the ref's last path component.
#   BUILD_IMAGE    the build image, pinned by tag and digest (default below).
#   JOBS           parallel compile jobs (default: nproc).
#
# For each branch: export the source, configure FDB's CMake build in the container (with
# messages/targets.cmake injected through CMAKE_PROJECT_foundationdb_INCLUDE), build the
# golden_vectors_messages target (which links fdbclient, flow and fdbrpc), write the fixtures,
# and check that every fixture round-trips through that build's decoder. The build trees and a
# ccache live in build/msg/ (gitignored), so a second run only rebuilds what changed.
#
#   --check   Also regenerate each branch a second time into build/msg/ and require
#             byte-identical output (determinism).
set -euo pipefail

TOOL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# ---------------------------------------------------------------------------
# In-container part: regen_messages.sh --in-container NAME REV JOBS CHECK
if [[ ${1:-} == --in-container ]]; then
	name=$2 rev=$3 jobs=$4 check=$5
	start=$(date +%s)
	cd /build
	echo "== [$name] configure"
	CC=clang CXX=clang++ cmake -G Ninja /src \
		-DUSE_CCACHE=ON \
		-DCMAKE_PROJECT_foundationdb_INCLUDE=/gv/messages/project_include.cmake \
		-DGV_RELEASE_NAME="$name" \
		-DBUILD_JAVA_BINDING=OFF -DBUILD_GO_BINDING=OFF -DBUILD_RUBY_BINDING=OFF \
		-DBUILD_DOCUMENTATION=OFF -DBUILD_AWS_BACKUP=OFF -DWITH_GRPC=OFF -DWITH_ROCKSDB=OFF \
		>/build/configure.log 2>&1 || {
		tail -n 40 /build/configure.log
		exit 1
	}
	echo "== [$name] build golden_vectors_messages (log: build/msg/build-$name/build.log)"
	rm -f /build/golden_vectors_messages/gv_fields.g.h # always re-extract the field names
	ninja -j"$jobs" golden_vectors_messages >/build/build.log 2>&1 || {
		grep -E -A5 'error:|FAILED' /build/build.log | head -n 80
		exit 1
	}
	bin=$(find /build -name golden_vectors_messages -type f -perm -u+x | head -n 1)
	echo "== [$name] built in $(($(date +%s) - start))s: ${bin#/build/}"
	rm -rf "/gv/fixtures/$name"
	"$bin" generate --out /gv/fixtures --fdb-rev "$name@$rev"
	echo "== [$name] round trip through the $name decoder"
	python3 /gv/check_fixtures.py roundtrip "/gv/fixtures/$name" "$bin"
	if [[ $check == 1 ]]; then
		echo "== [$name] determinism: regenerate and compare"
		rm -rf /build/check
		"$bin" generate --out /build/check --fdb-rev "$name@$rev"
		diff -r "/gv/fixtures/$name" "/build/check/$name"
		echo "identical"
	fi
	exit 0
fi

# ---------------------------------------------------------------------------
# Host part.
FDB_SRC=${FDB_SRC:-$(cd "$TOOL_DIR/../../.." && pwd)/foundationdb}
MSG_BRANCHES=${MSG_BRANCHES:-"origin/release-7.4 origin/release-8.0"}
# A dated foundationdb/build rockylinux9 image (2026-11-20). FDB CI uses rockylinux9-latest
# (release-8.0 .github/workflows/tidy.yml); both branches' cmake/ConfigureCompiler.cmake
# describe its toolchain. Both branches build with it unchanged.
BUILD_IMAGE=${BUILD_IMAGE:-foundationdb/build:rockylinux9-20261120210123-c79f7cf1aa@sha256:eabf6edbff5f6e864a574814db0a10eb63f804a6a8856f801f942a0b4be715e1}
JOBS=${JOBS:-$(nproc 2>/dev/null || echo 8)}
MSG_DIR=$TOOL_DIR/build/msg

check=0
for arg in "$@"; do
	case $arg in
	--check) check=1 ;;
	*)
		sed -n '2,22p' "$0" >&2
		exit 2
		;;
	esac
done

# Read-only git: never take the index lock in the FDB checkout.
fdb_git() { git --no-optional-locks -C "$FDB_SRC" "$@"; }

if ! fdb_git rev-parse --git-dir >/dev/null 2>&1; then
	echo "FDB_SRC=$FDB_SRC is not a FoundationDB git checkout; set FDB_SRC" >&2
	exit 1
fi
command -v docker >/dev/null || {
	echo "docker is required" >&2
	exit 1
}

cidfile=""
cleanup() {
	# Remove the build container by ID if it is still there (e.g. after Ctrl-C).
	if [[ -n $cidfile && -s $cidfile ]]; then
		docker rm -f "$(cat "$cidfile")" >/dev/null 2>&1 || true
	fi
	if [[ -n $cidfile ]]; then
		rm -f "$cidfile"
	fi
}
trap cleanup EXIT

mkdir -p "$MSG_DIR/ccache"
for ref in $MSG_BRANCHES; do
	name=${ref##*/}
	rev=$(fdb_git rev-parse --verify --quiet "$ref^{commit}") || {
		echo "MSG_BRANCHES: '$ref' is not a commit in $FDB_SRC (fetch it, or set MSG_BRANCHES)" >&2
		exit 1
	}
	src=$MSG_DIR/src-$name
	if [[ ! -f $src/.gv-rev || $(cat "$src/.gv-rev") != "$rev" ]]; then
		echo "== [$name] export $ref ($rev)"
		rm -rf "$src"
		mkdir -p "$src"
		fdb_git archive "$rev" | tar -x -C "$src"
		echo "$rev" >"$src/.gv-rev"
	fi
	mkdir -p "$MSG_DIR/build-$name"
	cidfile=$(mktemp -u "$MSG_DIR/cid.XXXXXX")
	docker run --rm --cidfile "$cidfile" --name "gv-msg-$name-$$" \
		--user "$(id -u):$(id -g)" -e HOME=/tmp -e CCACHE_DIR=/ccache -e GV_BUILD_IMAGE="$BUILD_IMAGE" \
		-v "$TOOL_DIR:/gv" -v "$src:/src:ro" -v "$MSG_DIR/build-$name:/build" -v "$MSG_DIR/ccache:/ccache" \
		"$BUILD_IMAGE" bash /gv/regen_messages.sh --in-container "$name" "${rev:0:12}" "$JOBS" "$check"
	cleanup
	cidfile=""
done
echo "== all message fixtures regenerated"
