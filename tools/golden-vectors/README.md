# golden-vectors

A dev-only C++ tool that produces golden test vectors for FoundationDB's flatbuffers
dialect (the `ObjectWriter`/`ObjectReader` wire format) by compiling FDB's own serializer
headers, and that decodes arbitrary bytes back into the same JSON so Rust-encoded output
can be checked against the C++ reader.

Nothing in the Rust workspace builds or runs this tool. The fixtures it writes are checked
in under [`fixtures/`](fixtures/), and Rust tests read them as plain files.

- [Build and regenerate](#build-and-regenerate)
- [Decode mode](#decode-mode)
- [Fixture format](#fixture-format)
- [Vector catalog](#vector-catalog)
- [Wire format notes for the codec author](#wire-format-notes-for-the-codec-author)
- [Message-level vectors (not done)](#message-level-vectors-not-done)
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
./regen.sh --check        # also runs every check below (about a minute)
```

`regen.sh --check` verifies that:

1. every fixture round-trips through `golden-vectors decode` (positives decode to the
   recorded `root`, negatives are rejected with the recorded `reason`);
2. an ASan/UBSan build regenerates byte-identical fixtures;
3. a clang++ build decodes every positive vector to identical fields, and the g++ build
   decodes clang's bytes identically. **clang's bytes differ from g++'s**: the vtable block
   is ordered by the address of a static per vtable, which depends on the compiler (see
   [Non-canonical output](#non-canonical-output));
4. the `flat_buffers.h` of `origin/release-7.4` and `origin/release-8.0` (exported with
   `git archive` into `build/`; the FDB checkout is never modified) encode every positive
   vector to the same bytes as `main`. Release-branch readers have no bounds checks, so the
   malformed-input vectors are skipped for them (`generate --positive-only`).

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
```

`TYPE` is a fixture's `cpp_type`. On success it prints
`{"cpp_type", "file_identifier", "file_identifier_hex", "root"}` (with `root` in exactly the
fixture format) and exits 0. If the reader rejects the input it prints
`{"cpp_type", "error": "<reason>"}` and exits 1. Usage errors and unparsable input exit 2.

To check a Rust encoder: encode the value described by a fixture's `root`, pipe the hex to
`golden-vectors decode <cpp_type>`, and compare the printed `root` with the fixture's
`root`. Do not compare bytes; the C++ output is not canonical.

The decoder mirrors `ArenaObjectReader` on FDB `main`: buffers shorter than 8 bytes and
mismatched file identifiers are rejected, and every read is bounds-checked
(`_ObjectReader::checkRange`).

## Fixture format

```
fixtures/engine/
  index.json                 # format version, FDB source, list of every file below
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
  "description": "...", "cpp_type": "Untrusted", "file_identifier": 4242,
  "file_identifier_hex": "0x00001092", "size": 224, "bytes": "...",
  "mutation": {"op": "overwrite_u32", "pos": 0, "value": 2147483632},
  "expect": "reject",
  "reason": "read outside of buffer"
}
```

The reader must reject `bytes`. `reason` is the C++ reader's message, for information only;
the Rust error need not match it. `mutation` says how the bytes were derived from a valid
message: `overwrite_u32` / `overwrite_u16` (little-endian value at byte `pos`), `truncate`
(first `len` bytes kept), `file_identifier` (written with that identifier instead), or
`schema_mismatch` (written as `writer_cpp_type`).

### Corpus line

Each line of `corpus/*.jsonl`:

```jsonc
{"name": "word_12_0xffffffff", "cpp_type": "Untrusted", "file_identifier": 4242,
 "bytes": "...", "mutation": {...}, "expect": "reject", "reason": "..."}
{"name": "word_96_0x00000000", "cpp_type": "Untrusted", "file_identifier": 4242,
 "bytes": "...", "mutation": {...}, "expect": "accept", "root": {...}}
```

`expect` records what the C++ reader on FDB `main` did. For `reject`, the Rust reader must
reject too. For `accept` the Rust reader should produce the same `root`; it may be stricter
only for a documented reason (for example, rejecting an out-of-range union tag that C++
silently maps to the default value).

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
which is vtable slot order. `slot` advances by 1 per member, by 2 after a union member and
by 0 after an `arena` member (whose `slot` is `null`).

Types and their `value`:

| `type` | wire form | `value` |
| --- | --- | --- |
| `bool` | 1 byte, 0 or 1 | `true`/`false` |
| `u8` `i8` `u16` `i16` `u32` `i32` `u64` `i64` | little-endian integer | integer |
| `f32` `f64` | IEEE 754 little-endian | number (shortest exact decimal, 17 significant digits; `-0.0` kept) |
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

`tag` is the value the C++ writer puts on the wire. Two C++ decoding quirks show up in the
decoded `root`: a `std::variant` whose slots are absent (or whose tag is 0 or out of range)
decodes as alternative 0 default-constructed and is dumped as `tag` 1; the same situation
for `ErrorOr` decodes as `Error 1035` (`default_error_or`) and for `Optional` as absent.

## Vector catalog

Positive vectors (`fixtures/engine/*.json`), by group:

| group | vectors | covers |
| --- | --- | --- |
| scalars | `scalars_typical`, `scalars_extremes`, `scalars_zero`, `root_scalar_i32` | every integer width and signedness, bool, f32, f64; extremes and -0.0; all-default values (still written); a scalar as the root member |
| strings | `strings` | `std::string`, `StringRef`, empty strings (shared empty blob), binary bytes, length multiple of 4 |
| vectors | `vectors`, `containers`, `root_vector_bool` | vectors of u8/u16/i32/u64/f64/strings/tables, empty vectors of each kind, `vector<bool>`, `array<u8,16>`, `std::map`, `std::set`, vector of vectors |
| nested | `nested_root`, `root_multiple_members`, `structs`, `root_vector_of_structs` | the unit-test `Root`/`Nested`/`Nested2` tables, a three-member root, structs inline in tables and in vectors |
| unions | `optionals`, `variant_i32`, `variant_f64`, `variant_table`, `variant_blob`, `variant_u8`, `root_variant_i32`, `root_variant_f64`, `root_variant_table` | `Optional` present/absent of scalar, string, table, f64, UID; vector of `Optional` (union vector); `std::variant` holding each of i32, f64, table, string blob, u8; a variant as the root member |
| error_or | `error_or_success`, `error_or_error`, `error_or_void_success`, `error_or_void_broken_promise`, `error_or_without_ensure_table`, `ensure_table` | replies as sent (`ErrorOr<EnsureTable<T>>`), errors, `Void` replies, `EnsureTable` around scalar/table/string/vector and inside a variant |
| file_identifier | `fid_plain`, `fid_reply_promise`, `fid_error_or`, `fid_vectorref`, `fid_optional`, `fid_std_vector`, `fid_error_or_ensure_table_optional`, `request_like` | each composition wrapper, a two-level composition, a request-shaped message with `ReplyPromise` and `Arena` |
| layout | `serializer_order`, `arena_refs`, `root_vectorref_stringref`, `preserialized_kv_vector`, `vtable_dedup` | slot order ≠ declaration order, `VectorRef`/`StringRef` + `Arena`, pre-serialized KV vector, one vtable shared by four types |
| tolerance | `tolerance_old_writer_new_reader`, `tolerance_new_writer_old_reader`, `schema_v2`, `nested_compat_y1_as_y2`, `nested_compat_y2_as_y1`, `handbuilt_alternate_layout`, `truncated_vtable` | missing trailing slots read as defaults; unknown trailing slots ignored; a hand-built layout C++ never writes (vtables after tables, negative soffset, declaration-order fields, no sharing); a shortened vtable |
| malformed | `untrusted_good`, `untrusted_string_only`, `array_length_16` | the valid sources of the negative vectors |

Negative vectors (`fixtures/engine/negative/*.json`), ported from the `Rejects*` and
`BoundsChecksKnob` tests in `flow/flat_buffers.cpp`:

| vectors | mutation |
| --- | --- |
| `root_offset_0x7ffffff0`, `root_offset_0xffffffff`, `root_offset_0x000000e0` (= size), `root_offset_0x000000dd` (= size - 3) | root offset points outside the buffer |
| `string_offset_0x7ffffff0`, `string_offset_0xffffffff`, `string_offset_0x00000040`, `string_offset_0x00000041` | a string's relative offset points outside the buffer |
| `string_length_0x7ffffff0`, `string_length_0xffffffff`, `string_length_0x0000000d` | a string's length prefix runs past the end |
| `array_length_15`, `array_length_17`, `array_length_4096` | a vector of the wrong length read as `array<u8,16>` |
| `wrong_file_identifier` | file identifier 4243 where 4242 is expected |
| `truncated_to_header`, `too_short_0`, `too_short_4`, `too_short_7` | buffer cut to 8, 0, 4 and 7 bytes |
| `union_vector_missing_offsets_slot` | a union vector's types slot present but its offsets slot absent |

Corpora (`fixtures/engine/corpus/`): `untrusted_truncations.jsonl` (every truncation of
`untrusted_good`, 224 lines: 223 reject, 1 accept) and `untrusted_word_overwrites.jsonl`
(every aligned u32 overwritten with 0, 1, 0xffffffff, 0x7ffffff0, 0x80000000, 0xfffffff0,
0x10000; 392 lines: 170 reject, 222 accept).

## Wire format notes for the codec author

These are facts about the C++ implementation (`flow/include/flow/flat_buffers.h`,
`flow/flat_buffers.cpp`, `flow/include/flow/FileIdentifier.h`), verified against the
fixtures. The encoder is identical on `release-7.4`, `release-8.0` and `main` (check 4
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
  soffset (`handbuilt_alternate_layout`).

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
  descending, and laid out in that order, each aligned to its own alignment, starting at
  table offset 4. Sizes: a scalar is its `sizeof` (bool 1, UID 16), every offset is 4, a
  struct is its struct size, a union is two fields (tag: 1, offset: 4). Alignment is the
  size rounded up to 1, 2, 4 or 8 (UID: 8); a struct's alignment is its largest member's.
  So 8-byte fields come first and union tags and bools last (`scalars_typical`,
  `optionals`).
- **Table placement**: a table is placed so that table start + 4 is aligned to the largest
  alignment among its fields (at least 4), i.e. the 8-byte fields are 8-aligned in the
  buffer. Bytes between the table end and the next object are zero.
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
- Vectors: u32 count, then the elements. Element data is aligned to max(4, element
  alignment), so a vector of u64/f64/UID/8-aligned structs has its data 8-aligned and its
  count at 4 mod 8. Scalars and structs are stored inline; tables, strings and vectors are
  u32 uoffsets, each relative to its own element position. Zero padding to 4 after the data.
- **Shared empty blob**: the writer emits a single `00000000` (count/length 0) per message and
  points every empty string, `StringRef`, vector, `VectorRef`, set and map at it, whatever its
  element type. `vector<bool>` and union vectors always write their own. Readers need not
  care; writers need not share.
- `vector<bool>`: count, then one byte per element (0/1; the reader treats nonzero as true).
- `std::array<T, N>`: identical to a vector. The `main` reader rejects a count other than N
  (`array_length_*`); **the 7.4 and 8.0 readers write past the array** instead.
- `std::map<K,V>`: a vector of offsets to two-slot tables `{first, second}`, in key order.
  `std::set<K>`: a vector of keys in order.
- Pre-serialized vectors (`VectorRef<KeyValueRef, VecSerStrategy::String>`, used by
  `GetKeyValuesReply::data`): one blob with string framing, whose payload is u32 item count
  then, per item, u32 key length, key, u32 value length, value (`preserialized_kv_vector`).

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
  - **a scalar or struct: its raw bytes, stored out of line with no alignment** beyond the
    4-byte alignment of what precedes it. An f64 or UID payload can be at 4 mod 8
    (`optionals`), a u8 payload at any address (`variant_u8`).
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
  None of these is an error.

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
- `EnsureTable<T>` has exactly `T`'s identifier and depth, so a reply's identifier is
  `(2 << 24) | id(Reply)` (`error_or_void_success`: `0x021ead4a` = `(2 << 24) | 2010442`).
- The reader compares the identifier with the expected one and rejects a mismatch
  (`wrong_file_identifier`), except during a 7.0 → 6.3 downgrade.

### Reader hardening

- Only FDB `main` bounds-checks reads (`FLATBUFFERS_BOUNDS_CHECKS_ENABLED`); the 7.4 and 8.0
  readers follow any offset. A malformed packet can crash a 7.4/8.0 server, so never send
  one to anything but a disposable test server.
- The `main` reader checks: the 8-byte header; that every offset target, vtable, scalar,
  string and vector lies inside the buffer; vector counts × element size; `std::array`
  counts; union vector slot pairs. It does **not** check vtable alignment (UBSan flags an
  unaligned u16 load for corrupted soffsets: read everything with byte-wise little-endian
  loads), union tag ranges, the vtable's table-size field, overlap between objects, or
  trailing bytes (`truncate_223` in the truncation corpus is accepted: only padding was
  cut).

## Message-level vectors (not done)

The plan was to add `fixtures/release-7.4/` and `fixtures/release-8.0/` with real messages
(`GetValueRequest`, `CommitTransactionRequest`, `ClientDBInfo`, ...) compiled from each
branch's `fdbclient` headers with stubs. It is not feasible without an FDB build
environment. Blockers found while probing `fdbclient/StorageServerInterface.h`:

- Every message header reaches `flow/flow.h`, `fdbrpc/fdbrpc.h` and `fdbclient/FDBTypes.h`,
  which pull in most of flow: tracing (needs `{fmt}`, not vendored in the FDB tree and not
  installed here), knobs, the network and simulator interfaces, `FlowTransport`, Swift
  bridging headers, and so on.
- `release-7.4` (and 8.0) headers include actor-compiled files (`fdbrpc/LoadBalance.actor.h`,
  `fdbclient/TagThrottle.actor.h`, `flow/genericactors.actor.h`) that only exist after the
  C# actor compiler runs (needs mono or dotnet).
- Generated headers: `fdbclient/FDBOptions.g.h` (vexillographer, also C#),
  `flow/ProtocolVersion.h` (generated from `ProtocolVersion.h.cmake` by
  `flow/protocolversion/protocol_version.py`), `flow/config.h` and `SourceVersion.h`
  (CMake `configure_file`).

Stubbing all of that would mean re-implementing a large part of flow, and transcribing the
message definitions into the tool instead would only duplicate the Rust schema work without
checking it.

Recommended approach for a follow-up issue: generate message vectors inside FDB's own build
environment. Use the `foundationdb/build` Docker image (docker is available on this host),
configure each release branch with CMake, build only the generated-header targets plus
`fdbclient` (or the whole of `fdbserver`), and compile a small TU, like this tool's
`main.cpp` with real types in place of the stubs, against the build tree. The output format
can stay the same: the dumper (`src/dump.h`) works for any type with a `serialize()`
method, and only needs field names (which it would take from the `serializer()` call
arguments with a small script, or leave as `f0`, `f1`, ...). An alternative that avoids C++
entirely is to capture real packets between `fdbcli` and a test server per release and
record them as decode-only vectors.

## How the tool is put together

| file | role |
| --- | --- |
| `src/stubs.h` | stand-ins for `Arena`, `StringRef`, `VectorRef`, `Optional`, `ErrorOr`, `Error`, `Void`, `UID`, `ReplyPromise` and the pre-serialized KV vector, each reproducing exactly the trait specialization and file identifier composition of the FDB type cited next to it |
| `src/engine_support.cpp` | `generate_vtable`, copied verbatim from `flow/flat_buffers.cpp`, and load-failure hooks that throw |
| `src/codec.h` | encode (`save_members`, run twice with different fill bytes to catch uninitialized output) and decode (`load_members` with a context that mirrors `ArenaObjectReader` on `main`) |
| `src/dump.h`, `src/json.h` | the field dumper and a small ordered JSON printer |
| `src/fixtures.h` | record builders; every positive vector is decoded again and must equal the dump of its input |
| `src/engine_vectors.cpp` | test types (modelled on the `flow/flat_buffers.cpp` unit tests) and the catalog |
| `src/main.cpp` | CLI |
| `check_fixtures.py` | round-trip and tree-compare checks used by `regen.sh --check` |

Only `flow/include/flow/flat_buffers.h`, `FileIdentifier.h` and `ObjectSerializerTraits.h`
are compiled from `FDB_SRC`; the real Boost `flat_map` header is used. If a release changes
one of the stubbed trait specializations, update `src/stubs.h` to match.

To add a vector, add a type with `TABLE(Name, fid(n))` and `FIELDS(...)` to
`src/engine_vectors.cpp`, call `positive(...)` (or `negative(...)`) in the catalog, and run
`./regen.sh --check`.
