# golden-vectors

A dev-only C++ tool that produces golden test vectors for FoundationDB's flatbuffers
dialect (the `ObjectWriter`/`ObjectReader` wire format) by compiling FDB's own serializer
headers, and that decodes arbitrary bytes back into the same JSON so Rust-encoded output
can be checked against the C++ reader.

Nothing in the Rust workspace builds or runs this tool. The fixtures it writes are checked
in under [`fixtures/`](fixtures/), and Rust tests read them as plain files.

There are two kinds of fixtures:

- `fixtures/engine/`: engine-level vectors (test types modelled on FDB's flatbuffers unit
  tests, compiled against stand-ins for the flow types), built on the host by `regen.sh`.
- `fixtures/release-7.4/` and `fixtures/release-8.0/`: message-level vectors (real
  `GetValueRequest`, `CommitTransactionRequest`, `ClientDBInfo`, ...) serialized by each
  release branch's own code, built inside the official FDB build image by
  `regen_messages.sh`. See [Message-level vectors](#message-level-vectors).

- [Build and regenerate](#build-and-regenerate)
- [Decode mode](#decode-mode)
- [Fixture format](#fixture-format)
- [Corpus accept classes](#corpus-accept-classes)
- [Vector catalog](#vector-catalog)
- [Wire format notes for the codec author](#wire-format-notes-for-the-codec-author)
- [Types not covered](#types-not-covered)
- [Message-level vectors](#message-level-vectors)
- [How the tool is put together](#how-the-tool-is-put-together)

## Build and regenerate

Requirements: a C++20 compiler (tested with g++ 15.2 and clang++), GNU make, Boost headers
(`boost/container/flat_map.hpp`, included by `flat_buffers.h`), Python 3 for the checks, and
a FoundationDB checkout. No FDB build is needed.

```bash
# FDB_SRC defaults to ../../../foundationdb relative to this directory, which is a sibling
# checkout of foundationdb-rs-native. Set it explicitly when working in a git worktree.
export FDB_SRC=/path/to/foundationdb

make                      # builds build/golden-vectors
./regen.sh                # rebuilds, then rewrites fixtures/engine/
./regen.sh --check        # also runs every check below (about two minutes)
```

`CXXFLAGS`, `CPPFLAGS` and `LDFLAGS` given to `make` are kept; the flags the tool needs
(`-std=c++20`, include paths, sanitizer flags) are always appended (`override`).

`regen.sh --check` verifies that:

1. the constants `src/stubs.h` copies from FDB (file identifiers, composition codes,
   `default_error_or`, UID size, ...) still appear verbatim in the FDB headers;
2. every fixture round-trips through `golden-vectors decode` (positives and corpus accepts
   decode to the recorded `root`, negatives and corpus rejects are rejected with the
   recorded `reason`);
3. an ASan/UBSan build regenerates byte-identical fixtures. UBSan aborts on everything
   except one suppressed report (`ubsan.supp`): FDB's reader loads vtable entries through an
   unaligned `uint16_t*` when a hostile soffset makes the vtable address odd. That is
   undefined behaviour in FDB's reader on hostile input, not in this tool, so the
   suppression is limited to `flat_buffers.h`;
4. a clang++ build decodes every positive vector to identical fields, and the g++ build
   decodes clang's bytes identically. **clang's bytes differ from g++'s**: the vtable block
   is ordered by the address of a static per vtable, which depends on the compiler (see
   [Non-canonical output](#non-canonical-output)). If clang++ is missing the step prints
   `SKIPPED`;
5. for each of `origin/release-7.4` and `origin/release-8.0` (exported with `git archive`
   into `build/`; the FDB checkout is never modified, and git runs with
   `--no-optional-locks`): the stubbed constants match that branch, its `flat_buffers.h`
   encodes every positive vector to the same bytes as `main`, and its reader decodes every
   committed positive fixture to the same `root`. Release readers have no bounds checks, so
   they never see the malformed-input vectors (`generate --positive-only`,
   `check_fixtures.py roundtrip --positive-only`).

Regeneration with the same compiler is deterministic: running `regen.sh` twice, or with the
sanitizer build, gives byte-identical files. The only expected churn in a diff is
`fixtures/engine/index.json`'s `fdb_source` (FDB branch and commit). Fixture bytes change
only if the C++ compiler changes vtable order or FDB changes the format.

All build output goes to `build/` (gitignored).

## Decode mode

```bash
# hex on stdin (whitespace ignored)
echo 1c0000000adb0f00... | build/golden-vectors decode Inner
# raw bytes
build/golden-vectors decode --raw Inner < message.bin
# skip the file identifier check
build/golden-vectors decode --ignore-file-identifier Inner < hex.txt
# list the accepted TYPE names and their file identifiers
build/golden-vectors list-types
# every TYPE's root members and every reachable table's fields (= fixtures/engine/schemas.json)
build/golden-vectors list-types --schema
```

`TYPE` is a fixture's `cpp_type`. On success it prints
`{"cpp_type", "file_identifier", "file_identifier_hex", "root"}` (with `root` in exactly the
fixture format) and exits 0. If the reader rejects the input it prints
`{"cpp_type", "error": "<reason>"}` and exits 1. Usage errors and unparsable input exit 2.

The decoder mirrors `ArenaObjectReader` on FDB `main`: buffers shorter than 8 bytes and
mismatched file identifiers are rejected, and every read is bounds-checked
(`_ObjectReader::checkRange`).

**Checking a Rust encoder.** Encode the value described by a fixture's `root`, pipe the hex
to `golden-vectors decode <cpp_type>`, and compare the printed `root` with the fixture's
`root`; never compare bytes (the C++ output is not canonical). Do this with all three
decoders: `build/golden-vectors` (main) and `build/rel-release-7.4/golden-vectors` and
`build/rel-release-8.0/golden-vectors` (built by `regen.sh --check`). The release decoders
compile the release branches' readers, which have no bounds checks: only feed them output
that the `main` decoder has already accepted.

## Fixture format

```
fixtures/engine/
  index.json                 # format version, FDB source, list of every file below
  schemas.json               # the schema of every cpp_type (see "Schemas")
  <name>.json                # one positive vector per file
  negative/<name>.json       # one negative vector per file
  corpus/<name>.jsonl        # mutation corpora, one JSON object per line
```

All integers in JSON are exact (64-bit values are written as JSON integers; parse them as
`u64`/`i64`, not `f64`). Byte strings are lowercase hex with no separators.

### Positive record

```jsonc
{
  "format_version": 1,
  "name": "fid_plain",              // == file stem
  "group": "file_identifier",       // catalog group, see below
  "kind": "positive",
  "description": "...",
  "cpp_type": "Inner",              // TYPE for `golden-vectors decode`; the schema to decode with
  "writer_cpp_type": "SchemaV1",    // only when the bytes were written with a different schema
  "file_identifier": 1039114,       // the u32 at bytes 4..8 (decimal)
  "file_identifier_hex": "0x000fdb0a",
  "size": 56,                       // byte length
  "bytes": "1c0000000adb0f00...",   // the message as written by C++ (g++ on main)
  "root": { ...node... }            // the decoded message
}
```

A Rust test should decode `bytes` with the schema named by `cpp_type` and compare the result
with `root`. It may also re-encode and compare the decoded result of its own bytes (via its
own decoder, or `golden-vectors decode`) with `root`. It must not compare its encoded bytes
or `size` with the fixture.

### Negative record

```jsonc
{
  "format_version": 1, "name": "...", "group": "malformed", "kind": "negative",
  "description": "...", "cpp_type": "Untrusted",
  "file_identifier": 4242,          // the identifier the reader EXPECTS (cpp_type's identifier);
  "file_identifier_hex": "0x00001092", // bytes 4..8 may differ (wrong_file_identifier)
  "size": 224, "bytes": "...",
  "mutation": {"op": "overwrite_u32", "pos": 0, "value": 2147483632},
  "expect": "reject",
  "reason": "read outside of buffer"
}
```

The reader must reject `bytes`. `reason` is the C++ reader's message, for information only;
the Rust error need not match it. `mutation` says how the bytes were derived from a valid
message: `overwrite_u32` / `overwrite_u16` / `overwrite_u8` (little-endian value at byte
`pos`), `set_bytes` (a list of `{pos, value}` byte writes applied in order), `truncate`
(first `len` bytes kept), `file_identifier` (written with that identifier instead), or
`schema_mismatch` (written as `writer_cpp_type`).

### Corpus line

Each line of `corpus/*.jsonl`:

```jsonc
{"name": "word_12_0xffffffff", "cpp_type": "Untrusted", "file_identifier": 4242,
 "bytes": "...", "mutation": {...}, "expect": "reject", "reason": "..."}
{"name": "word_96_0x00000001", "cpp_type": "Untrusted", "file_identifier": 4242,
 "bytes": "...", "mutation": {...}, "expect": "accept", "accept_class": "data", "root": {...}}
{"name": "word_16_0x00010000", "cpp_type": "Untrusted", "file_identifier": 4242,
 "bytes": "...", "mutation": {...}, "expect": "accept", "accept_class": "structural",
 "violations": ["vtable_entry"], "root": {...}}
```

`file_identifier` is the expected identifier, as for negatives. See
[Corpus accept classes](#corpus-accept-classes) for what `expect`, `accept_class` and
`violations` require of a Rust decoder.

### Schemas

`schemas.json` (also printed by `golden-vectors list-types --schema`) gives the schema of
every `cpp_type` in the same vocabulary as the dumps, so a Rust test can find the schema
for a fixture without reading the C++:

```jsonc
{
  "format_version": 1,
  "types": {
    "Untrusted": {"file_identifier": 4242, "file_identifier_hex": "0x00001092",
                  "root": [{"field": "0", "slot": 0, "type": "table<Untrusted>"}]},
    ...
  },
  "tables": {
    "table<Untrusted>": [{"field": "s", "slot": 0, "type": "bytes"},
                         {"field": "v", "slot": 1, "type": "vector<i64>"},
                         {"field": "o", "slot": 2, "type": "optional<i32>"}, ...],
    ...
  }
}
```

`types.<cpp_type>.root` is the member list of the synthetic root table; `tables` holds the
member list of every table type reachable from any `cpp_type` (union alternatives and vector
element types included), keyed by its dump type name. Table type names are unique.

### Nodes

Every value is a node:

```jsonc
{
  "field": "debugID",   // table members and struct members only: the serializer argument name
  "slot": 2,            // table members only: index of the member's first vtable slot, or null
  "offset": 8,          // struct members only: byte offset inside the struct
  "type": "optional<uid>",
  "tag": 1,             // union types only: 0 = empty, k = alternative k-1
  "value": ...          // depends on type, see below
}
```

The root node is always `{"type": "table<root>", "value": [...]}`: the synthetic wrapper
table every message starts with. Its members are named `"0"`, `"1"`, ... (usually just
`"0"`, holding the real message). Members of a table appear in serializer argument order,
which is vtable slot order. `slot` advances by 1 per member, by 2 after a union member or a
vector-of-unions member, and by 0 after an `arena` member (whose `slot` is `null`).

Types and their `value`:

| `type` | wire form | `value` |
| --- | --- | --- |
| `bool` | 1 byte, 0 or 1 | `true`/`false` |
| `u8` `i8` `u16` `i16` `u32` `i32` `u64` `i64` | little-endian integer | integer |
| `f32` `f64` | IEEE 754 little-endian | number, see below |
| `uid` | 16-byte scalar: `first` u64 LE then `second` u64 LE, alignment 8 | `{"first": u64, "second": u64}` |
| `bytes` | `std::string` or `StringRef`: offset to u32 length + bytes | hex string |
| `preserialized_kv_vector` | `VectorRef<KeyValueRef, VecSerStrategy::String>`: one blob, see notes | array of `{"key": hex, "value": hex}` |
| `vector<T>` | offset to u32 count + elements (also `VectorRef`, `std::set`, `std::map`, `std::deque`) | array of nodes of type `T` |
| `vector<bool>` | offset to u32 count + one byte per element | array of `bool` nodes |
| `array<T,N>` | `std::array`: same wire form as `vector<T>`; count must equal N | array of N nodes |
| `struct<T1,...>` | `std::tuple`, stored inline | array of member nodes with `field` "0", "1", ... and `offset` |
| `table<Name>` | offset to a table (`Name` is the C++ type; `pair<K,V>` for map entries, `reply_promise` for `ReplyPromise<T>`) | array of member nodes |
| `optional<T>` | union, 1 alternative | `tag` 0 and `null`, or `tag` 1 and a node of type `T` |
| `variant<T1,...>` | union | `tag` k ≥ 1 and a node of type `Tk` |
| `error_or<T>` | union with alternatives `table<Error>`, `T` | `tag` 1 and an `Error` table node, or `tag` 2 and a `T` node |
| `ensure_table<T>` | `T` if `T` is a table, else a one-slot table holding `T` | a node of type `T` (the wrapper table is implicit) |
| `arena` | nothing | `null` |

`table<Error>` has one member `error_code` (`u16`). `table<Void>` has no members.
`table<reply_promise>` has one member `token` (`uid`).

**Floats.** Values are printed with `%.17g` (round-trip exact, not necessarily the
shortest form), with `.0` appended to integral values and `-0.0` kept. An `f32` is widened
to `f64` before printing, so compare an `f32` as `(v as f64).to_bits()` against the parsed
JSON number's bits. NaN and infinities cannot appear in fixtures (the JSON writer refuses
them).

**Absent fields.** A field whose slot is absent (or a union whose tag is 0 or out of range)
is dumped as the value C++ default-constructs: `0`, `false`, `""`, `[]`, an absent optional
(`tag` 0, `null`), a table whose members are all defaults, `std::variant` alternative 0
default-constructed (dumped as `tag` 1), an `ErrorOr` holding `Error 1035`
(`default_error_or`, dumped as `tag` 1), and a bare `Error` with `error_code` 65535
(`invalid_error_code`). A Rust decoder should produce the same values for absent fields.

## Corpus accept classes

`expect` and `accept_class` come from running the bytes through this tool's decoder, which
uses FDB `main`'s real `flat_buffers.h` reader code with a load context that mirrors `main`'s
`ArenaObjectReader` (header check, file identifier check, `checkRange`). They do not come
from a full FDB build. FDB `release-7.4` and `release-8.0` readers do **no** bounds checks
at all, so on hostile input they read out of bounds instead of rejecting: the Rust decoder
is the component that needs hardening, and it must be at least as strict as `main`.

Each accepted corpus entry is classified by the generator:

- `structural`: at least one of the permitted strict checks below fails. The failing checks
  are listed in `violations`.
- `padding`: every permitted check passes and the decoded `root` equals the unmodified
  source message's `root` (the mutation only touched bytes the decoded value does not
  depend on: padding, vtable slack, a table-size field that is still large enough, or a
  truncated tail of padding).
- `data`: every permitted check passes and the decoded `root` differs from the source:
  scalar values, string/vector lengths and counts, offsets, vtable entries or union tags
  were changed to other in-bounds, structurally valid values. This includes fields made
  absent by a zeroed vtable entry (a legal schema-evolution shape) and `vector<bool>` bytes
  other than 0/1 (read as `true`).

**Policy for the Rust decoder:**

1. Every `reject` (corpus or negative vector) is binding: the Rust decoder must return an
   error. It must never panic, overflow, allocate based on an unchecked count, read out of
   bounds or loop forever, on any input.
2. Every `data` accept must be accepted, with a `root` identical to the recorded one.
3. `structural` accepts may be rejected, but only by one of the permitted checks named in
   that entry's `violations`. If the Rust decoder accepts it, the `root` must be identical.
4. `padding` accepts should be accepted with an identical `root`. A decoder may reject them
   only by validating bytes the decoded value does not depend on (e.g. requiring zero
   padding, or an exact vtable table-size field).

Permitted strict checks (the `violations` vocabulary, implemented in `src/strict.h`):

| check | a stricter decoder may reject when |
| --- | --- |
| `offset_into_header` | the root offset, any followed uoffset, or a vtable position lands below byte 8 (inside the header) |
| `zero_uoffset` | a followed uoffset (other than the root offset) is 0, so it points at itself |
| `vtable_alignment` | a vtable starts at an odd position (C++ then performs an unaligned `u16` load: UB) |
| `vtable_size` | the vtable size field is < 4 or odd |
| `table_size` | the table size field is < 4, the table runs past the buffer, or a present field extends past the table size |
| `vtable_entry` | a vtable entry is 1, 2 or 3 (C++ treats these as absent; they would overlap the soffset) |
| `union_tag_range` | a union tag exceeds the number of alternatives, or is 0 for a union that cannot be empty (`std::variant`, `ErrorOr`); C++ substitutes the default value |
| `union_slots` | a union's tag slot is present with a nonzero tag but its offset slot is absent (C++ reads it as empty) |
| `union_vector_count_mismatch` | a vector of unions has a tags vector count different from its offsets vector count (C++ ignores the tags count) |
| `bool_value` | a scalar `bool` byte is not 0 or 1 (C++ UB; `vector<bool>` elements are exempt) |
| `empty_table_offset` | the offset to a zero-field table (e.g. `Void`) is out of bounds (C++ never follows it) |
| `out_of_bounds` | an offset the C++ reader does not follow (e.g. a zero-field table's) is out of bounds |
| `too_deep` | nesting deeper than 64 tables |

No other check is permitted: in particular not object overlap or aliasing (the C++ writer
itself points every empty string/vector at one shared blob), not alignment of tables,
strings, vectors or union payloads (union scalars are unaligned by design), and not
trailing or unreferenced bytes.

Current corpora (`fixtures/engine/corpus/`):

| file | lines | reject | data | padding | structural |
| --- | --- | --- | --- | --- | --- |
| `untrusted_truncations.jsonl`: every truncation of `untrusted_good` | 224 | 223 | 0 | 1 | 0 |
| `untrusted_word_overwrites.jsonl`: every aligned u32 set to 0, 1, 0xffffffff, 0x7ffffff0, 0x80000000, 0xfffffff0, 0x10000 (no-op overwrites omitted) | 382 | 170 | 157 | 6 | 49 |
| `untrusted_random_flips.jsonl`: the `RejectsMalformedBuffers` random-damage loop, 500 iterations with a fixed seed (splitmix64, seed 0x0fdb5eed), 1–3 random bytes each (no-ops omitted) | 499 | 354 | 100 | 19 | 26 |
| `untrusted_targeted.jsonl`: `Optional` union tag set to 2 and to 255 | 2 | 0 | 0 | 0 | 2 |

## Vector catalog

Positive vectors (`fixtures/engine/*.json`), by group:

| group | vectors | covers |
| --- | --- | --- |
| scalars | `scalars_typical`, `scalars_extremes`, `scalars_zero`, `root_scalar_i32` | every integer width and signedness, bool, f32, f64; extremes and -0.0; all-default values (still written); a scalar as the root member |
| strings | `strings` | `std::string`, `StringRef`, empty strings (shared empty blob), binary bytes, length multiple of 4 |
| vectors | `vectors`, `containers`, `root_vector_bool`, `vector_bool_nonzero` | vectors of u8/u16/i32/u64/f64/strings/tables, empty vectors of each kind, `vector<bool>`, `array<u8,16>`, `std::map`, `std::set`, vector of vectors; `vector<bool>` element bytes 02, ff, 80 read as true |
| nested | `nested_root`, `root_multiple_members`, `structs`, `root_vector_of_structs` | the unit-test `Root`/`Nested`/`Nested2` tables, a three-member root, structs inline in tables and in vectors |
| unions | `optionals`, `variant_i32`, `variant_f64`, `variant_table`, `variant_blob`, `variant_u8`, `root_variant_i32`, `root_variant_f64`, `root_variant_table` | `Optional` present/absent of scalar, string, table, f64, UID; vector of `Optional` (union vector); `std::variant` holding each of i32, f64, table, string blob, u8; a variant as the root member |
| error_or | `error_or_success`, `error_or_error`, `error_or_void_success`, `error_or_void_broken_promise`, `error_or_without_ensure_table`, `ensure_table` | replies as sent (`ErrorOr<EnsureTable<T>>`), errors, `Void` replies, `EnsureTable` around scalar/table/string/vector and inside a variant |
| file_identifier | `fid_plain`, `fid_reply_promise`, `fid_error_or`, `fid_vectorref`, `fid_optional`, `fid_std_vector`, `fid_error_or_ensure_table_optional`, `request_like` | each composition wrapper, a two-level composition, a request-shaped message with `ReplyPromise` and `Arena` |
| layout | `serializer_order`, `arena_refs`, `root_vectorref_stringref`, `preserialized_kv_vector`, `vtable_dedup` | slot order ≠ declaration order, `VectorRef`/`StringRef` + `Arena`, pre-serialized KV vector, one vtable shared by four types |
| tolerance | `tolerance_old_writer_new_reader`, `tolerance_new_writer_old_reader`, `schema_v2`, `nested_compat_y1_as_y2`, `nested_compat_y2_as_y1`, `handbuilt_alternate_layout`, `truncated_vtable` | missing trailing slots read as defaults; unknown trailing slots ignored; a hand-built layout C++ never writes (vtables after tables, negative soffset, declaration-order fields, no sharing); a shortened vtable |
| malformed | `untrusted_good`, `untrusted_string_only`, `array_length_16` | the valid sources of the negative vectors |

Every positive vector also passes all the permitted strict checks.

Negative vectors (`fixtures/engine/negative/*.json`). The first seven rows are ported from the
`Rejects*` and `BoundsChecksKnob` tests in `flow/flat_buffers.cpp`; the rest are targeted
additions. All are mutations of `untrusted_good` unless stated.

| vectors | mutation |
| --- | --- |
| `root_offset_0x7ffffff0`, `root_offset_0xffffffff`, `root_offset_0x000000e0` (= size), `root_offset_0x000000dd` (= size - 3) | root offset points outside the buffer |
| `string_offset_0x7ffffff0`, `string_offset_0xffffffff`, `string_offset_0x00000040`, `string_offset_0x00000041` | a string's relative offset points outside the buffer (source: `untrusted_string_only`) |
| `string_length_0x7ffffff0`, `string_length_0xffffffff`, `string_length_0x0000000d` | a string's length prefix runs past the end (source: `untrusted_string_only`) |
| `array_length_15`, `array_length_17`, `array_length_4096` | a vector of the wrong length read as `array<u8,16>` |
| `wrong_file_identifier` | file identifier 4243 where 4242 is expected |
| `truncated_to_header`, `too_short_0`, `too_short_4`, `too_short_7` | buffer cut to 8, 0, 4 and 7 bytes |
| `union_vector_missing_offsets_slot` | a union vector's types slot present but its offsets slot absent |
| `vtable_soffset_out_of_bounds` | a table's soffset puts its vtable 64 bytes past the end |
| `vtable_soffset_i32_min` | a table's soffset is 0x80000000 (`i32::MIN`; do not negate it as an i32) |
| `vtable_size_past_end` | a vtable's size field runs past the end of the buffer |
| `vector_i64_count_wraps_u32` | a `vector<i64>` count of 0x20000000 (count × 8 wraps to 0 in u32 arithmetic) |
| `vector_bool_count_oversized` | a `vector<bool>` count of 0x7ffffff0 |
| `union_offset_out_of_bounds` | a present `Optional<i32>`'s offset points at the end of the buffer |

## Wire format notes for the codec author

These are facts about the C++ implementation (`flow/include/flow/flat_buffers.h`,
`flow/flat_buffers.cpp`, `flow/include/flow/FileIdentifier.h`), verified against the
fixtures. The encoder is identical on `release-7.4`, `release-8.0` and `main` (check 5
above); `main` adds bounds checks to the reader only.

### Buffer layout

```
offset 0   u32  root uoffset            -> the synthetic root table
offset 4   u32  file identifier          little-endian number, not ASCII
offset 8   0..7 zero bytes               padding so the total size is a multiple of 8
           vtable block                  all vtables, each u16-aligned only, no padding between
           tables, vectors, strings      everything else
```

Worked example, `fid_plain` (`Inner{a: u32 = 1, b: string = "x"}`, 56 bytes):

```
00: 1c000000          root uoffset -> 0x1c
04: 0adb0f00          file identifier 0x000fdb0a
08: 00000000 0000     padding (6 bytes)
0e: 0800 0c00 0400 0800   vtable of Inner: 8 bytes, table 12 bytes, a at +4, b at +8
16: 0600 0800 0400        vtable of the root table: 6 bytes, table 8 bytes, slot 0 at +4
1c: 06000000          root table: soffset 6 -> vtable at 0x1c - 6 = 0x16
20: 04000000          slot 0: uoffset -> 0x24
24: 16000000          Inner table: soffset 0x16 -> vtable at 0x24 - 0x16 = 0x0e
28: 01000000          a = 1
2c: 04000000          b: uoffset -> 0x30
30: 01000000 78000000 b: length 1, "x", 3 zero bytes of padding
```

- **Root.** The root offset points at a synthetic table (`detail::FakeRoot`) whose slots are
  the items passed to `ObjectWriter::serialize` (nearly always one). A table-typed message
  is therefore two tables deep: root table, slot 0, uoffset, message table. A scalar root
  is stored inline in the root table (`root_scalar_i32`); a union root takes two slots
  (`root_variant_*`).
- **No protocol version prefix** in these vectors. `ObjectWriter` with `IncludeVersion`
  would prepend a u64 protocol version and all offsets would be relative to the byte after
  it; FlowTransport uses `AssumeVersion`, so packets have no prefix.
- **uoffset** (u32) is relative to its own position: target = position + value. The C++
  writer always emits children after (at higher addresses than) their parents, so values
  are positive and point forward; treat them as unsigned.
- **soffset** (i32) at the start of every table: vtable = table − soffset. C++ always
  places vtables before tables (positive soffset), but its reader accepts a negative
  soffset (`handbuilt_alternate_layout`). Compute in 64-bit; `i32::MIN` is a valid bit
  pattern (`vtable_soffset_i32_min`).

### Vtables

- Layout: u16 vtable byte size (4 + 2 × slots), u16 table byte size, then one u16 per slot:
  the field's byte offset from the table start (≥ 4 because of the soffset). The table byte
  size is the end of the last field and is **not** rounded up (e.g. 47 for `Scalars`, 6 for
  `Error`); the reader never uses it.
- A slot is present iff `index < slots_in_vtable && entry >= 4`; otherwise the field reads as
  its default. That is the entire schema-evolution mechanism (`tolerance_*`,
  `truncated_vtable`). The C++ writer never omits a field: defaults are written.
- **One slot per `serializer()` argument, in argument order**, not declaration order
  (`serializer_order`). A union member takes two slots (tag, then offset); a vector of
  unions takes two (tags vector, then offsets vector); `Arena` takes none and must be the
  last argument.
- **Field placement** (`generate_vtable`): the slot fields are stable-sorted by size,
  descending. Walking them in that order with a running offset `o` starting at 0 (measured
  from the end of the soffset, i.e. from table start + 4), each field gets
  `o' = align_up(o, a)`, `entry = o' + 4`, then `o = o' + size`. Alignment is computed on
  `o`, which excludes the 4-byte soffset, so the first field always has entry 4 even if it
  is 8-aligned (the i64 in `error_or_success` sits at table offset 4). Sizes: a scalar is
  its `sizeof` (bool 1, UID 16), every offset is 4, a struct is its struct size, a union is
  two fields (tag: 1, offset: 4). Alignment `a` is the size rounded up to 1, 2, 4 or 8 (UID:
  8); a struct's alignment is its largest member's. So 8-byte fields come first and union
  tags and bools last (`scalars_typical`, `optionals`). The table size field is
  `final o + 4`.
- **Table placement**: a table is placed so that table start + 4 is aligned to the largest
  alignment among its fields (at least 4), which is what makes the field alignments above
  hold in the buffer. Bytes between the table end and the next object are zero.
- **Deduplication**: a vtable is identified by the (size, alignment) list of its fields
  alone, so different types with the same shape share one vtable. `{6, 8, 4}` (one 4-byte
  field) is shared by the root table, `DedupA{i32}`, `DedupB{u32}`, `DedupC{f32}` and every
  table holding a single offset (`vtable_dedup`).
- **The vtable block holds every vtable reachable from the message type**, whether or not
  this message uses it: union alternatives not taken, element types of empty vectors,
  `Error` inside every `ErrorOr` (`error_or_success` carries the `Error` vtable,
  `variant_i32` carries the `Nested2` vtable).

### Non-canonical output

Vtables are written in the order of the addresses of their `thread_local` statics, which
depends on the compiler and linker. g++ and clang produce different bytes for the same value
(same size, same fields, different vtable order and soffsets). Repeated runs of one binary
are stable. Consumers must compare decoded fields, never bytes. The child objects of a
table are written in serializer order, each at a lower address than the previous, so they
appear in reverse slot order in the buffer; nothing depends on that either.

### Strings, blobs and vectors

- `std::string` / `StringRef`: u32 length, bytes, zero padding to a 4-byte boundary. **No NUL
  terminator** (`strings`: "abcd" is `04000000 61626364` with nothing after it). The length
  prefix is 4-aligned.
- Vectors: u32 count, then the elements. The writer aligns the start of the element data to
  max(4, element alignment) and puts the count immediately before it, so a vector of
  u64/f64/UID/8-aligned structs has its data 8-aligned and its count at 4 mod 8; the gap
  this leaves after the previously written object is zero-filled. Scalars and structs are
  stored inline; tables, strings and vectors are u32 uoffsets, each relative to its own
  element position. An empty vector is aligned to 4 only.
- **Shared empty blob**: the writer emits a single `00000000` (count/length 0) per message and
  points every empty string, `StringRef`, vector, `VectorRef`, set and map at it, whatever its
  element type. `vector<bool>`, union vectors and pre-serialized vectors always write their
  own. Readers need not care; writers need not share.
- `vector<bool>`: count, then one byte per element. The writer writes 0/1; the reader maps
  any nonzero byte to `true` (`vector_bool_nonzero`). Contrast a *scalar* `bool`, which the
  C++ reader `memcpy`s into a `bool`: a byte other than 0/1 there is undefined behaviour in
  C++ (the `bool_value` strict check).
- `std::array<T, N>`: identical to a vector. The `main` reader rejects a count other than N
  (`array_length_*`); **the 7.4 and 8.0 readers write past the array** instead.
- `std::map<K,V>`: a vector of offsets to two-slot tables `{first, second}`, in key order.
  `std::set<K>`: a vector of keys in order.
- Pre-serialized vectors (`VectorRef<KeyValueRef, VecSerStrategy::String>`, used by
  `GetKeyValuesReply::data`): one blob with string framing, whose payload is u32 item count
  then, per item, u32 key length, key, u32 value length, value (`preserialized_kv_vector`).
  A zero-item vector is a 4-byte blob (`04000000 00000000`), not the shared empty blob. The
  C++ `main` loader trusts the embedded lengths and only `ASSERT`s afterwards that they sum
  to the blob size, so it reads past the blob on bad input; the Rust decoder must reject a
  blob whose item lengths overrun it or leave trailing bytes (this tool's stub does).

### Structs

Only `std::tuple` is struct-like in flow (FDB's `Tag` too, via a trait). Members are laid
out in declaration order, each aligned to its own size; the struct size is rounded up to its
largest member alignment (minimum 1). `struct<i16,bool,i64>`: i16 at 0, bool at 2, i64 at
8, size 16. `struct<u8,u32>`: u8 at 0, u32 at 4, size 8. Nested structs are not supported.
Structs are inline in tables and vectors (`structs`, `root_vector_of_structs`).

### Unions (`std::variant`, `Optional`, `ErrorOr`)

- Two slots: a u8 tag and a u32 uoffset. Tag 0 = empty (only `Optional` is ever empty;
  its offset field is then zero but still present in the vtable). Tag k ≥ 1 = alternative
  k − 1, in declaration order of the C++ alternatives.
- The offset points at the alternative's encoding:
  - a table: the table;
  - a string or vector: its length/count prefix;
  - **a scalar or struct: its raw bytes, out of line, unaligned and unpadded**: they are
    written flush against the previously written object, with no alignment of their own.
    An f64 or UID payload can be at 4 mod 8 (`optionals`), a u8 payload at any address
    (`variant_u8`).
- A union cannot directly hold a union (`ErrorOr<Optional<T>>` does not compile). That is
  what `EnsureTable` is for.
- Vectors of unions (`std::vector<Optional<int>>`): the two slots hold uoffsets to a
  `vector<u8>` of tags and to a vector of u32 uoffsets (0 for an empty entry). The `main`
  reader trusts the offsets vector's count and bounds-checks the tags vector against it;
  a present tags slot with an absent offsets slot is rejected
  (`union_vector_missing_offsets_slot`).
- Reader quirks: a tag past the last alternative, a zero tag on a never-empty union, or
  absent slots all produce the default value (absent `Optional`; alternative 0
  default-constructed for `std::variant`; `Error 1035 default_error_or` for `ErrorOr`).
  None of these is an error in C++ (see `union_tag_range` for what Rust may do).

### ErrorOr, EnsureTable, Error, Void, ReplyPromise, UID, Arena

- `ErrorOr<T>`: union with alternatives (`Error`, `T`): tag 1 = error, tag 2 = value.
  Replies travel as `ErrorOr<EnsureTable<Reply>>`.
- `EnsureTable<T>`: if `T` is a table, nothing changes (`error_or_without_ensure_table` has
  the same bytes as `error_or_success`). Otherwise `T` is wrapped in a one-slot table, the
  same shape as the root table, and the field or union offset points at that wrapper
  (`ensure_table`: compare `wrapped_alt` and `bare_alt`).
- `Error`: table with one u16 field, `error_code` (vtable `{6, 6, 4}`). The flags are not
  serialized.
- `Void`: a table with no fields: vtable `{4, 4}`, and the table is just its soffset. The
  reader does not even follow the offset to a zero-field table.
- `ReplyPromise<T>`: table with one field, the reply endpoint token (`uid`).
- `UID`: 16-byte scalar, alignment 8: `first` then `second`, little-endian.
- `Arena`: no slot, no bytes.

### File identifiers

- A numeric little-endian u32 at bytes 4..8. Plain identifiers are below 2^24.
- Wrappers compose (`ComposedIdentifier`): ReplyPromise = 1, ErrorOr = 2, VectorRef = 3,
  Optional = 4, `std::vector` = 5, `std::unordered_set` = 6.
  - `T` plain (depth 0): `Wrapper<T>` = `(code << 24) | id(T)`, depth 1.
    `ErrorOr<Inner>` = `0x020fdb0a`.
  - `T` at depth 1: `Wrapper<T>` = `(code << 28) | id(T)`, depth 2.
    `ErrorOr<Optional<Inner>>` = `0x240fdb0a`.
  - `T` at depth 2: no identifier.
- `EnsureTable<T>` has `T`'s identifier, and its composition depth for every depth that can
  occur under a wrapper (< 2). FDB declares `EnsureTable`'s `composition_depth` as `bool`,
  so a depth-2 `T` would appear as depth 1; that case never composes anyway. So a reply's
  identifier is `(2 << 24) | id(Reply)` (`error_or_void_success`: `0x021ead4a` =
  `(2 << 24) | 2010442`).
- The reader compares the identifier with the expected one and rejects a mismatch
  (`wrong_file_identifier`), except during a 7.0 → 6.3 downgrade.

### Reader hardening

- Only FDB `main` bounds-checks reads (`FLATBUFFERS_BOUNDS_CHECKS_ENABLED`); the 7.4 and 8.0
  readers follow any offset. A malformed packet can crash a 7.4/8.0 server, so never send
  one to anything but a disposable test server.
- The `main` reader checks: the 8-byte header; that every offset target, vtable, scalar,
  string and vector lies inside the buffer; vector counts × element size (in 64-bit, so a
  count that wraps in u32 arithmetic is still caught); `std::array` counts; union vector
  slot pairs. It does **not** check vtable alignment (UBSan flags an unaligned u16 load for
  corrupted soffsets: read everything with byte-wise little-endian loads), union tag
  ranges, the vtable's table-size field, overlap between objects, or trailing bytes
  (`truncate_223` in the truncation corpus is accepted: only padding was cut). See
  [Corpus accept classes](#corpus-accept-classes) for which of these Rust may check.

## Types not covered

These real FDB wire types have no engine vector:

- **Enums** are scalars of `sizeof(enum)` (`scalar_traits` covers `std::is_enum_v`).
- **`ProtocolVersion`** is a u64 scalar (`scalar_traits<ProtocolVersion>` in `serialize.h`).
- **`std::unordered_map`, `std::unordered_set`, `boost::container::flat_map`** have the
  same wire form as `std::map` / `std::set`; for the unordered ones the element order is
  hash order, so it is not canonical either.
- **`std::deque`** has vector traits and the `vector<T>` wire form, but is untested.
- **`Standalone<T>`** (encodes as `T`), **`Tag`** (a struct via `struct_like_traits<Tag>`),
  **`IPAddress`** and **`NetworkAddress`** need fdbclient/fdbrpc headers and are out of scope
  for engine vectors.

The message-level vectors cover all of these except `ProtocolVersion`,
`std::unordered_set` and `boost::container::flat_map` (see the next section).

## Message-level vectors

`fixtures/release-7.4/` and `fixtures/release-8.0/` hold real FDB messages, serialized by
each release branch's own types and serializer: the branch is configured with its own CMake
build inside the official FoundationDB build image, and a generator target that links
`fdbclient` (and through it `fdbrpc` and `flow`) writes the fixtures. Nothing is stubbed or
transcribed except FlowTransport's two-line `PingRequest`, which lives in a `.cpp` file (it is
mirrored and checked against that file, see below).

### Regenerate

Requirements: docker, and a FoundationDB checkout that has the `origin/release-7.4` and
`origin/release-8.0` refs. Nothing else is needed on the host.

```bash
FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen_messages.sh           # both releases
FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen_messages.sh --check   # + determinism
MSG_BRANCHES=origin/release-8.0 FDB_SRC=... tools/golden-vectors/regen_messages.sh  # one release
```

For each branch the script:

1. exports the branch with `git archive` into `build/msg/src-<release>/` (only when the
   commit changed; the FDB checkout is only read, with `git --no-optional-locks`);
2. starts a disposable build container (`docker run --rm`, also removed by ID on exit or
   interrupt) from the pinned image, with the source mounted read-only, the build tree in
   `build/msg/build-<release>/` and a ccache in `build/msg/ccache/` (all gitignored);
3. configures FDB with CMake (`-G Ninja`, clang; Java, Go, Ruby, docs, gRPC, RocksDB and
   AWS off) and injects this tool's target with
   `-DCMAKE_PROJECT_foundationdb_INCLUDE=messages/project_include.cmake`, which defers an
   `include(messages/targets.cmake)` to the end of FDB's top-level `CMakeLists.txt`, so the
   exported tree is not modified;
4. builds `golden_vectors_messages` (FDB's generated headers, the actor compiler output,
   `flow`, `fdbrpc` and `fdbclient`, then `messages/message_vectors.cpp` and `src/main.cpp`);
5. writes `fixtures/<release>/` and feeds every fixture back through that build's `decode`
   command (`check_fixtures.py roundtrip`);
6. with `--check`, generates again and requires byte-identical output.

The build image is pinned by tag and digest in `regen_messages.sh` (`BUILD_IMAGE`):
`foundationdb/build:rockylinux9-20261120210123-c79f7cf1aa@sha256:eabf6edbff5f6e864a574814db0a10eb63f804a6a8856f801f942a0b4be715e1`,
a dated build of the `rockylinux9` image that FDB's CI uses
(`foundationdb/build:rockylinux9-latest` in release-8.0's `.github/workflows/tidy.yml`; both
branches' `cmake/ConfigureCompiler.cmake` describe its toolchain, and both build with it
unchanged). It has clang 19 with libc++, CMake 3.31, Ninja and mono. Each fixture directory's `index.json` records the branch commit (`fdb_source`),
the FDB version and the image.

Cost on a 32-core host: a cold build of one branch (configure plus `fdbclient`, 200 to 350
Ninja steps) takes about 2.5 minutes, and a rebuild after editing the generator about 45
seconds. Each build tree is about 600 MB.

### Decode mode

The generator has the same CLI as the engine tool (`generate`, `decode`, `list-types
[--schema]`). Its `decode` runs the branch's real reader (`ArenaObjectReader`, as
FlowTransport does) after checking the 8-byte header and the file identifier itself. The
binary links libc++ statically and runs on the host:

```bash
build/msg/build-release-7.4/bin/golden_vectors_messages list-types
echo <hex> | build/msg/build-release-7.4/bin/golden_vectors_messages decode GetValueRequest
echo <hex> | build/msg/build-release-8.0/bin/golden_vectors_messages decode 'ErrorOr<EnsureTable<CommitID>>'
```

**Checking a Rust encoder**: encode the value a fixture's `root` describes, decode the bytes
with the matching release's binary and compare the printed `root` with the fixture's. The
release readers have no bounds checks: only feed them well-formed buffers (for example ones
the Rust decoder has already accepted); malformed input can crash them.

### Format and vocabulary

The records use the [fixture format](#fixture-format) unchanged; message directories have
positive records only (no `negative/` or `corpus/`), plus `index.json` and `schemas.json`.
`cpp_type` is the C++ type name (`GetValueRequest`, `ErrorOr<EnsureTable<CommitID>>`,
`UID`). Field names are the `serializer()` argument names in the branch's headers: the last
identifier of each argument (`LoadBalancedReply::penalty` is `penalty`,
`const_cast<KeyRef&>(begin)` is `begin`).

Additions to the [node types](#nodes):

| `type` | C++ | wire form | `value` |
| --- | --- | --- | --- |
| `table<Name>` | every FDB struct with a `serialize()`; `Name` is its C++ name | table | members |
| `table<request_stream>` | `RequestStream<T>` / `PublicRequestStream<T>`, any `T` | one-slot table: `endpoint` (`table<Endpoint>`: `addresses`, `token`) | members |
| `table<reply_promise>` | `ReplyPromise<T>`, any `T` (as in the engine fixtures) | one-slot table: `token` (`uid`) | members |
| `bytes` | also `Standalone<StringRef>` (`Key`, `Value`), `IdempotencyIdRef`, `WipedString`'s string | string framing | hex |
| `tag_set` | `TagSet` | one blob (string framing): per tag, u8 length then the tag bytes; no count | array of hex tags, in order |
| `version_vector` | `VersionVector` | one blob, see below | `{"versions": [{"locality": i8, "id": u16, "version": i64}], "maxVersion": i64}`, `versions` in `Tag` order (locality, then id) |
| `preserialized_kv_vector` | `VectorRef<KeyValueRef, VecSerStrategy::String>` (`GetKeyValuesReply::data`) | as in the engine fixtures | as in the engine fixtures |
| `struct<u16,i8>` | `Tag` (`struct_like_traits<Tag>`: `id` at 0, `locality` at 2) | inline struct, size 4 | members `"0"` (id), `"1"` (locality) |
| integer types | enums (`TraceFlags` u8, `ReadType` i32 on 7.4 and u32 on 8.0, `ClusterType` i32, `TaskPriority` i32) | scalar of the enum's size | integer |

`Standalone<T>` is serialized exactly as `T` (`LoadSaveHelper<Standalone<T>>` in
`flow/ObjectSerializer.h`), so the dumps never show it. `Arena` members appear as before
(`arena`, slot `null`). `schemas.json` lists every reachable table; several C++ types may
share a dump name only if their fields are identical (`reply_promise`, `request_stream`).

**`VersionVector` blob** (`dynamic_size_traits<VersionVector>`, identical on both branches;
little-endian, `size_t` is 8 bytes): u64 count of distinct tag localities; per locality run,
in `Tag` order, i8 locality and u16 run length; if there is at least one entry, u8 tag-id
width (1 if every id is at most 255, else 2), u8 version-delta width (1, 2, 4 or 8: the
smallest that holds max version minus min version), i64 min version, u64 entry count, then
per entry the tag id and (version minus min version) in those widths; finally i64
`maxVersion`. The vectors cover the empty form and the width pairs 1/1, 1/2, 1/4 and 2/8.

### How names and values are checked

- **Field names** come from `messages/extract_fields.py`, which the build runs on the
  branch's headers (and on `fdbrpc/FlowTransport(.actor).cpp` for `PingRequest`). It finds
  each table type's `serialize()` (or `serializable_traits<T>`), picks the flatbuffers
  `serializer()` call (the `if constexpr (is_fb_function<Ar>)` branch where there is one),
  and generates `gv_table_info<T>` (`src/hooks.h`): the names, the address of the data member
  each argument names, and a `static_assert` on the file identifier. At dump time the dumper
  checks that every argument it receives is at that address and that the argument count
  equals the name count, so a name cannot be attached to the wrong member. The target is
  compiled with `-fno-access-control` so private members can be named.
- **Each vector** is encoded twice with different fill bytes (no uninitialized output), must
  equal FDB's own `ObjectWriter` output byte for byte (with `AssumeVersion`, as FlowTransport
  serializes packets), is decoded by the branch's `ArenaObjectReader` to the same field dump
  as the value it was built from, and passes the permitted
  [strict checks](#corpus-accept-classes).
- **Determinism**: the output is a function of the source and the compiler. `--check`, two
  full runs, and a run from an empty `build/msg/` give byte-identical fixtures. The values
  avoid the nondeterministic parts: `std::unordered_map` fields (`TransactionTagMap`) hold
  one entry, and `ClientTagThrottleLimits` serializes `expiration - now()` (the network clock
  does not advance in the generator, so `duration` is exactly 30.0).

### Wire notes for the codec author

- **Envelopes.** A request is sent as the request table itself (root slot 0, the request's
  file identifier). A reply is sent as `ErrorOr<EnsureTable<Reply>>`: union tag 2 and the
  reply table, or tag 1 and `Error`; file identifier `(2 << 24) | id(Reply)`. The
  `*_envelope` and `*_error` vectors are exactly these. The bare reply vectors (`cpp_type`
  is the reply type) are the same table without the envelope, for unit tests.
- **Single-key ranges are compressed.** `KeyRangeRef` [k, k\x00) is written as
  (begin = k\x00, end = "") and a `ClearRange` `MutationRef` over [k, k\x00) as (type,
  param1 = k\x00, param2 = ""); the reader restores [k, k\x00). The fixtures' `root` shows
  the wire form (`commit_transaction_request*`), because the dumper runs the writer side of
  `serialize()`. A Rust encoder must compress the same way, and its decoder must expand.
  `MutationRef` also has checksum forms (a flag bit in the type, a suffix on param2) that are
  only written when the `ENABLE_MUTATION_CHECKSUM` client knob is set; no vector covers them.
- **`ClientTagThrottleLimits`** is written as (tpsRate, duration = expiration - now()).
- **`Endpoint`** is (addresses, token); `NetworkAddressList` is (address, secondaryAddress);
  `NetworkAddress` is (ip, port, flags, fromHostname) with flags bit 0 = private and bit 1 =
  TLS; `IPAddress` is a one-slot table holding `variant<u32, array<u8,16>>` (tag 1 IPv4, tag
  2 IPv6; the u32 is the address as a number, 10.0.0.1 = 0x0a000001). The reader calls
  `Endpoint::choosePrimaryAddress()`, which swaps primary and secondary when only the
  secondary matches the local TLS setting; in these vectors the primary is not TLS and the
  secondary is, so nothing moves.
- **Interfaces carry one endpoint.** `CommitProxyInterface` sends only `commit`,
  `GrvProxyInterface` only `getConsistentReadVersion`, `StorageServerInterface` only
  `getValue`; the other endpoints are `getAdjustedEndpoint(i)`: token (first + (i << 32),
  (second & 0xffffffff00000000) | (u32(second) + i)). The indices a client needs are the same
  on both branches: commit proxy 2 = `getKeyServersLocations`; storage server 1 = `getKey`,
  2 = `getKeyValues`, 10 = `watchValue`. Later commit proxy indices differ (8.0 dropped
  `getHealthMetrics`, `getTenantId` and `getBlobGranuleLocations`, so for example
  `expireIdempotencyId` is 10 on 7.4 and 9 on 8.0).
- **Well-known tokens** are `UID(-1, n)`: 0 endpoint not found, 1 ping, 2 unauthorized
  endpoint, 4 `ClientLeaderRegInterface::openDatabase` (where a coordinator receives
  `OpenDatabaseCoordRequest`). The not-found and unauthorized payload is a bare `UID` root
  (`endpoint_not_found`).
- **`OpenDatabaseCoordRequest`'s reply** is declared as
  `ReplyPromise<CachedSerialization<ClientDBInfo>>`, but `CachedSerialization` writes exactly
  `ErrorOr<EnsureTable<ClientDBInfo>>` (`client_db_info_envelope`).
- **`WatchValueReply`'s file identifier is 3**, so its envelope's is `0x02000003`.
- A C++ writer that fills `GetKeyValuesReply::data` by appending must invalidate its cached
  size first (`VectorRef<_, String>` keeps a cached serialized size that `push_back` leaves at
  0); this does not affect readers.

### 7.4 versus 8.0

The file identifiers of all covered types are the same on both branches. Schema differences
(field@slot; a union takes two slots, an `Arena` none; bold fields exist on one side only):

| type | release-7.4 | release-8.0 |
| --- | --- | --- |
| `GetValueRequest` | key@0 version@1 tags@2 reply@4 spanContext@5 **tenantInfo@6** options@7 ssLatestCommitVersions@9 | key@0 version@1 tags@2 reply@4 spanContext@5 options@6 ssLatestCommitVersions@8 |
| `GetKeyValuesRequest` | ... spanContext@8 **tenantInfo@9** options@10 ssLatestCommitVersions@12 arena | ... spanContext@8 options@9 ssLatestCommitVersions@11 **taskID@12** (`optional<i32>`, a `TaskPriority`) arena |
| `GetKeyRequest` | sel@0 version@1 tags@2 reply@4 spanContext@5 **tenantInfo@6** options@7 ssLatestCommitVersions@9 arena | ... spanContext@5 options@6 ssLatestCommitVersions@8 arena |
| `WatchValueRequest` | key@0 value@1 version@3 tags@4 debugID@6 reply@8 spanContext@9 **tenantInfo@10** | the same without tenantInfo |
| `CommitTransactionRequest` | transaction@0 reply@1 flags@2 debugID@3 commitCostEstimation@5 tagSet@7 spanContext@9 **tenantInfo@10** idempotencyId@11 arena | ... spanContext@9 idempotencyId@10 arena |
| `GetKeyServerLocationsRequest` | begin@0 end@1 limit@3 reverse@4 reply@5 spanContext@6 **tenant@7** **minTenantVersion@8** arena | ... spanContext@6 **legacyVersion@7** (i64) arena |
| `GetReadVersionRequest` | transactionCount@0 flags@1 tags@2 debugID@3 reply@5 spanContext@6 maxVersion@7 | the same plus **maxGrvQueueDelayMS@8** (`optional<i64>`) |
| `GetReadVersionReply` | processBusyTime@0 ... proxyId@10 **proxyTagThrottledDuration@11** (f64) | ends at proxyId@10 |
| `ClientDBInfo` | grvProxies@0 commitProxies@1 id@2 forward@3 history@5 **tenantMode@6** (`table<TenantMode>`: mode u32) **encryptKeyProxy@7** clusterId@9 clusterType@10 **metaclusterName@11** | grvProxies@0 commitProxies@1 id@2 forward@3 history@5 clusterId@6 clusterType@7 **nativeCdcEnabled@8 nativeCdcTagCount@9 cdcProxies@10 streamToCDCProxyId@11** |
| `ReadOptions` | type@0 is `i32` | type@0 is `u32` (same size) |

`TenantInfo` (tenantId i64, token `optional<table<WipedString>>`, arena) exists only on
7.4. Because 8.0 removed it from the middle of the read and commit requests, a 7.4 reader
sees an 8.0 request's later fields in the wrong slots and the reverse: a client must choose
the schema by the server's protocol version. Everything else covered (the replies other than
`GetReadVersionReply`, `CommitID`, `OpenDatabaseCoordRequest`, the interfaces, `Endpoint`,
`MutationRef`, `KeyRangeRef`, `SpanContext`, ...) is identical.

### Message vector catalog

The same 33 vectors exist for both releases; the values differ only in fields that exist on
one branch. Group `storage`:

| vector | `cpp_type` | covers |
| --- | --- | --- |
| `get_value_request` | `GetValueRequest` | every field set; `tag_set` with two tags; `ReadOptions` with every field; `version_vector` with 1-byte deltas; 7.4: `TenantInfo` with a token |
| `get_value_request_minimal` | `GetValueRequest` | optionals absent, default `SpanContext` (and `TenantInfo` on 7.4); `version_vector` with 2-byte deltas |
| `get_value_reply` | `GetValueReply` | bare reply: penalty, error (absent), value, cached |
| `get_value_reply_envelope` | `ErrorOr<EnsureTable<GetValueReply>>` | success envelope |
| `get_value_reply_absent_value` | `ErrorOr<EnsureTable<GetValueReply>>` | value absent, `LoadBalancedReply::error` present (1037) |
| `get_value_reply_error` | `ErrorOr<EnsureTable<GetValueReply>>` | error envelope (1009 `future_version`) |
| `get_key_values_request` | `GetKeyValuesRequest` | `KeySelectorRef` begin and end; `version_vector` with a negative locality, a tag id over 255 and 8-byte deltas; 8.0: `taskID` |
| `get_key_values_reply`, `get_key_values_reply_envelope` | `GetKeyValuesReply`, `ErrorOr<...>` | pre-serialized KV vector with three rows (one empty value) |
| `get_key_values_reply_empty` | `ErrorOr<EnsureTable<GetKeyValuesReply>>` | zero rows (a 4-byte blob) |
| `get_key_request` | `GetKeyRequest` | empty `version_vector` |
| `get_key_reply`, `get_key_reply_envelope` | `GetKeyReply`, `ErrorOr<...>` | `KeySelector` (`Standalone<KeySelectorRef>`) |
| `watch_value_request` | `WatchValueRequest` | value and debugID present |
| `watch_value_reply`, `watch_value_reply_envelope` | `WatchValueReply`, `ErrorOr<...>` | |

Group `proxy`:

| vector | `cpp_type` | covers |
| --- | --- | --- |
| `get_read_version_request` | `GetReadVersionRequest` | priority and flags, `TransactionTagMap<u32>` (`vector<table<pair<bytes,u32>>>`), debugID; 8.0: maxGrvQueueDelayMS |
| `get_read_version_reply`, `get_read_version_reply_envelope` | `GetReadVersionReply`, `ErrorOr<...>` | metadataVersion, tagThrottleInfo (`ClientTagThrottleLimits`), `version_vector` with two localities and 4-byte deltas; 7.4: proxyTagThrottledDuration |
| `commit_transaction_request` | `CommitTransactionRequest` | `CommitTransactionRef` with conflict ranges, four mutation types and every optional; cost estimation (a `std::deque` of pairs), tag set, idempotency id; single-key range compression |
| `commit_transaction_request_single_key_clear` | `CommitTransactionRequest` | single-key `ClearRange` compression; everything else default |
| `commit_id`, `commit_id_envelope` | `CommitID`, `ErrorOr<...>` | metadataVersion, conflictingKRIndices (`optional<vector<i32>>`) |
| `commit_id_error` | `ErrorOr<EnsureTable<CommitID>>` | 1020 `not_committed` |
| `get_key_server_locations_request` | `GetKeyServerLocationsRequest` | 7.4: tenant and minTenantVersion; 8.0: legacyVersion |
| `get_key_server_locations_reply`, `get_key_server_locations_reply_envelope` | `GetKeyServerLocationsReply`, `ErrorOr<...>` | four `StorageServerInterface`s (`LocalityData` map with an absent value, tssPairID, acceptingRequests), the TSS mapping, the tag mapping (`Tag` structs) |

Group `coordinator`:

| vector | `cpp_type` | covers |
| --- | --- | --- |
| `open_database_coord_request` | `OpenDatabaseCoordRequest` | issues, supportedVersions (`ClientVersionRef`), hostnames, IPv4 and IPv6 coordinators |
| `client_db_info`, `client_db_info_envelope` | `ClientDBInfo`, `ErrorOr<...>` | two GRV and two commit proxies (one of each with a TLS secondary address), forward, history (`VersionHistory`); 7.4: tenantMode, encryptKeyProxy, metaclusterName; 8.0: the CDC fields and a `std::map<u64, UID>` |

Group `transport`:

| vector | `cpp_type` | covers |
| --- | --- | --- |
| `ping_request` | `PingRequest` | FlowTransport's ping (mirrored from `FlowTransport(.actor).cpp`; file identifier and fields checked) |
| `ping_reply` | `ErrorOr<EnsureTable<Void>>` | the ping reply |
| `endpoint_not_found` | `UID` | the endpoint-not-found and unauthorized-endpoint payload |

Not covered: malformed-input vectors for message types (the release readers have no bounds
checks; the engine corpora cover the reader rules), `MutationRef` checksum forms, streams
(`GetKeyValuesStreamRequest`, `ReplyPromiseStream`), mapped range reads, tenant management
and metacluster requests, and FDB's binary (non-flatbuffers) serializer.

## How the tool is put together

| file | role |
| --- | --- |
| `src/stubs.h` | stand-ins for `Arena`, `StringRef`, `VectorRef`, `Optional`, `ErrorOr`, `Error`, `Void`, `UID`, `ReplyPromise` and the pre-serialized KV vector, each reproducing exactly the trait specialization and file identifier composition of the FDB type cited next to it (the constants are re-checked against each branch by `regen.sh --check`) |
| `src/engine_support.cpp` | `generate_vtable`, copied verbatim from `flow/flat_buffers.cpp`, and load-failure hooks that throw |
| `src/codec.h` | encode (`save_members`, run twice with different fill bytes to catch uninitialized output), decode (`load_members` with a context that mirrors `ArenaObjectReader` on `main`), and the type registry |
| `src/dump.h`, `src/json.h` | the field dumper and a small ordered JSON printer |
| `src/schema.h` | the schema lister behind `schemas.json` |
| `src/strict.h` | the permitted strict checks used to classify corpus accepts; every positive vector must pass them |
| `src/fixtures.h` | record builders; every positive vector is decoded again and must equal the dump of its input |
| `src/engine_vectors.cpp` | test types (modelled on the `flow/flat_buffers.cpp` unit tests) and the catalog |
| `src/main.cpp` | CLI (shared by both generators) |
| `src/hooks.h`, `src/gv_types.h` | customization points (`gv_unwrap`, `gv_leaf`, `gv_table_info`) and the switch between `stubs.h` and the real FDB types |
| `check_fixtures.py` | round-trip and tree-compare checks used by `regen.sh --check` and `regen_messages.sh` |
| `ubsan.supp` | the single UBSan suppression (FDB reader alignment UB on hostile input) |
| `regen_messages.sh` | message vectors: exports each release branch, builds and runs the generator in the FDB build image |
| `messages/project_include.cmake`, `messages/targets.cmake` | inject the `golden_vectors_messages` target into FDB's CMake build |
| `messages/real_types.h` | the real types behind the names the dumper uses, leaf dumps (`tag_set`, `version_vector`, ...), template table names, the `PingRequest` mirror |
| `messages/extract_fields.py` | generates `gv_table_info` (field names, member addresses, file identifier checks) from the branch's headers |
| `messages/message_vectors.cpp` | the message catalog, and the FDB network setup the real types need |

Only `flow/include/flow/flat_buffers.h`, `FileIdentifier.h` and `ObjectSerializerTraits.h`
are compiled from `FDB_SRC`; the real Boost `flat_map` header is used. If a release changes
one of the stubbed trait specializations, update `src/stubs.h` to match.

To add a vector, add a type with `TABLE(Name, fid(n))` and `FIELDS(...)` to
`src/engine_vectors.cpp`, call `positive(...)` (or `negative(...)`) in the catalog, and run
`./regen.sh --check`.

To add a message vector, build the value in `messages/message_vectors.cpp` and call
`message(...)`. If it reaches a table type that is not listed yet, add the type to `TABLES` in
`messages/extract_fields.py` (until then the build fails with the `type_name` static
assertion). Run `./regen_messages.sh --check`, and `./regen.sh --check` too if you changed
anything in `src/`: both generators share those headers.
