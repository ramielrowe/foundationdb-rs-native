#!/usr/bin/env bash
# Builds tools/golden-vectors and regenerates fixtures/engine from FDB's C++ serializer.
#
# Usage: tools/golden-vectors/regen.sh [--check]
#
#   FDB_SRC        FoundationDB checkout (default: ../../../foundationdb from this directory).
#                  It is only read: headers are compiled in place, and release branches are
#                  exported with `git archive` into build/.
#   FDB_BRANCHES   refs checked by --check (default: "origin/release-7.4 origin/release-8.0").
#
#   --check   After regenerating, also verify that:
#               1. every fixture round-trips through `golden-vectors decode`;
#               2. an ASan/UBSan build regenerates byte-identical fixtures;
#               3. a clang++ build (if installed) decodes to identical fields, and the g++ build
#                  decodes its bytes identically (clang orders vtables differently);
#               4. each release branch's headers encode every positive vector to the same bytes.
set -euo pipefail

TOOL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FDB_SRC=${FDB_SRC:-$(cd "$TOOL_DIR/../../.." && pwd)/foundationdb}
FDB_BRANCHES=${FDB_BRANCHES:-"origin/release-7.4 origin/release-8.0"}
BUILD_DIR=$TOOL_DIR/build
FIXTURES=$TOOL_DIR/fixtures
JOBS=$(nproc 2>/dev/null || echo 4)

check=0
for arg in "$@"; do
	case $arg in
	--check) check=1 ;;
	*)
		sed -n '2,18p' "$0" >&2
		exit 2
		;;
	esac
done

if [[ ! -f $FDB_SRC/flow/include/flow/flat_buffers.h ]]; then
	echo "FDB_SRC=$FDB_SRC is not a FoundationDB checkout; set FDB_SRC" >&2
	exit 1
fi

fdb_rev="unknown"
if git -C "$FDB_SRC" rev-parse --git-dir >/dev/null 2>&1; then
	fdb_rev="$(git -C "$FDB_SRC" rev-parse --abbrev-ref HEAD)@$(git -C "$FDB_SRC" rev-parse --short=12 HEAD)"
	if [[ -n $(git -C "$FDB_SRC" status --porcelain -- flow/include/flow flow/flat_buffers.cpp) ]]; then
		fdb_rev+="+dirty"
	fi
fi

echo "== build ($FDB_SRC, $fdb_rev)"
make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$FDB_SRC" >/dev/null

echo "== regenerate $FIXTURES/engine"
rm -rf "$FIXTURES/engine"
"$BUILD_DIR/golden-vectors" generate --out "$FIXTURES" --fdb-rev "$fdb_rev"

if [[ $check == 0 ]]; then
	exit 0
fi

echo "== check: round trip through decode"
python3 "$TOOL_DIR/check_fixtures.py" roundtrip "$FIXTURES/engine" "$BUILD_DIR/golden-vectors"

echo "== check: ASan/UBSan build"
make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$FDB_SRC" SANITIZE=1 >/dev/null
rm -rf "$BUILD_DIR/check-san"
"$BUILD_DIR/golden-vectors-san" generate --out "$BUILD_DIR/check-san" --fdb-rev "$fdb_rev"
python3 "$TOOL_DIR/check_fixtures.py" compare "$FIXTURES/engine" "$BUILD_DIR/check-san/engine"

if command -v clang++ >/dev/null; then
	echo "== check: clang++ build"
	make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$FDB_SRC" CXX=clang++ BUILD_DIR="$BUILD_DIR/clang" >/dev/null
	rm -rf "$BUILD_DIR/check-clang"
	"$BUILD_DIR/clang/golden-vectors" generate --out "$BUILD_DIR/check-clang" --fdb-rev "$fdb_rev"
	# clang places the vtables in a different order (they are sorted by the
	# address of each vtable's static), so only the decoded fields must match;
	# and the g++ build must decode clang's bytes (and its negatives) identically.
	python3 "$TOOL_DIR/check_fixtures.py" compare "$FIXTURES/engine" "$BUILD_DIR/check-clang/engine" --fields-only
	python3 "$TOOL_DIR/check_fixtures.py" roundtrip "$BUILD_DIR/check-clang/engine" "$BUILD_DIR/golden-vectors"
fi

for ref in $FDB_BRANCHES; do
	name=${ref##*/}
	echo "== check: $ref headers"
	src="$BUILD_DIR/fdb-$name"
	rm -rf "$src"
	mkdir -p "$src"
	git -C "$FDB_SRC" archive --output="$src.tar" "$ref" flow/include/flow
	tar -xf "$src.tar" -C "$src"
	rm -f "$src.tar"
	make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$src" BUILD_DIR="$BUILD_DIR/rel-$name" >/dev/null
	rm -rf "$BUILD_DIR/check-$name"
	# Release-branch readers have no bounds checks, so skip the malformed-input vectors.
	"$BUILD_DIR/rel-$name/golden-vectors" generate --positive-only --out "$BUILD_DIR/check-$name" --fdb-rev "$ref"
	python3 "$TOOL_DIR/check_fixtures.py" compare "$FIXTURES/engine" "$BUILD_DIR/check-$name/engine" --positive-only
done

echo "== all checks passed"
