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
#               4. for each release branch: the stubbed file identifiers and composition codes
#                  match its headers, its headers encode every positive vector to the same
#                  bytes, and its (unchecked) reader decodes every positive fixture identically.
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
		sed -n '2,19p' "$0" >&2
		exit 2
		;;
	esac
done

if [[ ! -f $FDB_SRC/flow/include/flow/flat_buffers.h ]]; then
	echo "FDB_SRC=$FDB_SRC is not a FoundationDB checkout; set FDB_SRC" >&2
	exit 1
fi

# Read-only git: never take the index lock in the FDB checkout.
fdb_git() { git --no-optional-locks -C "$FDB_SRC" "$@"; }

fdb_rev="unknown"
if fdb_git rev-parse --git-dir >/dev/null 2>&1; then
	fdb_rev="$(fdb_git rev-parse --abbrev-ref HEAD)@$(fdb_git rev-parse --short=12 HEAD)"
	if [[ -n $(fdb_git status --porcelain -- flow/include/flow flow/flat_buffers.cpp) ]]; then
		fdb_rev+="+dirty"
	fi
fi

if [[ $check == 1 ]]; then
	for ref in $FDB_BRANCHES; do
		if ! fdb_git rev-parse --verify --quiet "$ref^{commit}" >/dev/null; then
			echo "FDB_BRANCHES: '$ref' is not a commit in $FDB_SRC (fetch it, or set FDB_BRANCHES)" >&2
			exit 1
		fi
	done
fi

# The constants src/stubs.h copies from FDB. Each line: file<TAB>exact text.
STUB_FACTS=$(
	cat <<'EOF'
flow/include/flow/Error.h	constexpr static FileIdentifier file_identifier = 14065384;
flow/include/flow/Error.h	serializer(ar, error_code);
flow/include/flow/flow.h	constexpr static FileIdentifier file_identifier = 2010442;
flow/include/flow/flow.h	class ErrorOr : public ComposedIdentifier<T, 2> {
flow/include/flow/flow.h	using alternatives = pack<Error, T>;
flow/include/flow/error_definitions.h	ERROR( default_error_or, 1035,
flow/include/flow/Arena.h	constexpr static FileIdentifier file_identifier = 13300811;
flow/include/flow/Arena.h	class VectorRef : public ComposedIdentifier<T, 3>, public VectorRefPreserializer<T, SerStrategy> {
flow/include/flow/Arena.h	struct union_like_traits<Optional<T>> : std::true_type {
flow/include/flow/Arena.h	constexpr static auto fb_must_appear_last = true;
flow/include/flow/Optional.h	Optional : public ComposedIdentifier<T, 4> {
flow/include/flow/serialize.h	struct FileIdentifierFor<std::vector<T, Allocator>> : ComposedIdentifierExternal<T, 5> {};
flow/include/flow/serialize.h	: ComposedIdentifierExternal<Key, 6> {};
flow/include/flow/IRandom.h	constexpr static size_t size = sizeof(uint64_t[2]);
flow/include/flow/FileIdentifier.h	(CompositionDepthFor<T>::value == 1) ? 28 : 24;
fdbrpc/include/fdbrpc/fdbrpc.h	class ReplyPromise final : public ComposedIdentifier<T, 1> {
fdbclient/include/fdbclient/FDBTypes.h	struct string_serialized_traits<KeyValueRef> : std::true_type {
EOF
)
STUB_PATHS="flow/include/flow fdbrpc/include/fdbrpc/fdbrpc.h fdbclient/include/fdbclient/FDBTypes.h"

check_stub_facts() { # $1 = tree, $2 = label
	local file text bad=0
	while IFS=$'\t' read -r file text; do
		if ! grep -qF -- "$text" "$1/$file" 2>/dev/null; then
			echo "stub mismatch ($2): '$text' not found in $file; update src/stubs.h" >&2
			bad=1
		fi
	done <<<"$STUB_FACTS"
	if [[ $bad == 1 ]]; then
		exit 1
	fi
	echo "stub constants match ($2)"
}

echo "== build ($FDB_SRC, $fdb_rev)"
make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$FDB_SRC" >/dev/null

echo "== regenerate $FIXTURES/engine"
rm -rf "$FIXTURES/engine"
"$BUILD_DIR/golden-vectors" generate --out "$FIXTURES" --fdb-rev "$fdb_rev"

if [[ $check == 0 ]]; then
	exit 0
fi

echo "== check: stub constants against $FDB_SRC"
check_stub_facts "$FDB_SRC" "$fdb_rev"

echo "== check: round trip through decode"
python3 "$TOOL_DIR/check_fixtures.py" roundtrip "$FIXTURES/engine" "$BUILD_DIR/golden-vectors"

echo "== check: ASan/UBSan build"
make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$FDB_SRC" SANITIZE=1 >/dev/null
rm -rf "$BUILD_DIR/check-san"
UBSAN_OPTIONS="suppressions=$TOOL_DIR/ubsan.supp:halt_on_error=1:print_stacktrace=1" \
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
else
	echo "== check: clang++ build SKIPPED (clang++ not installed)"
fi

for ref in $FDB_BRANCHES; do
	name=${ref##*/}
	echo "== check: $ref"
	src="$BUILD_DIR/fdb-$name"
	rm -rf "$src"
	mkdir -p "$src"
	# shellcheck disable=SC2086 # STUB_PATHS is a list of paths
	fdb_git archive --output="$src.tar" "$ref" $STUB_PATHS
	tar -xf "$src.tar" -C "$src"
	rm -f "$src.tar"
	check_stub_facts "$src" "$ref"
	make -C "$TOOL_DIR" -j"$JOBS" FDB_SRC="$src" BUILD_DIR="$BUILD_DIR/rel-$name" >/dev/null
	rm -rf "$BUILD_DIR/check-$name"
	# Release-branch readers have no bounds checks, so skip the malformed-input vectors.
	"$BUILD_DIR/rel-$name/golden-vectors" generate --positive-only --out "$BUILD_DIR/check-$name" --fdb-rev "$ref"
	python3 "$TOOL_DIR/check_fixtures.py" compare "$FIXTURES/engine" "$BUILD_DIR/check-$name/engine" --positive-only
	# The release reader must decode every committed positive fixture the same way.
	python3 "$TOOL_DIR/check_fixtures.py" roundtrip "$FIXTURES/engine" "$BUILD_DIR/rel-$name/golden-vectors" --positive-only
done

echo "== all checks passed"
