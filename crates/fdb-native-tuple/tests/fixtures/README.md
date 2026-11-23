# Fixture provenance

`tuples.golden` is copied unmodified from the Apache FoundationDB repository
(`bindings/go/src/fdb/tuple/testdata/tuples.golden`,
https://github.com/apple/foundationdb), which is licensed under the Apache
License 2.0.

Format: a Go `encoding/gob` stream holding a `map[string][]byte` from test-case
name to the packed tuple bytes produced by the Go binding. The Go test cases
that generate the entries live in `bindings/go/src/fdb/tuple/tuple_test.go`
(`testCases`). Entries named `Integers`, `Floats`, `Doubles` and `Nested` use
Go's seeded `math/rand` stream, so the Rust test checks their structure and
re-packs the decoded value instead of rebuilding it.

## python_vectors.txt and gen_vectors.py

`python_vectors.txt` holds 1635 tuples and their packed bytes (1385 plain `pack`,
250 `pack_with_versionstamp`) produced by the Python binding's
`bindings/python/fdb/tuple.py` (Apache-2.0, apple/foundationdb) with a stub
`fdb` module. `gen_vectors.py` regenerates it deterministically (fixed seed);
the format is documented in its docstring. It is a dev-only tool and is not run
by the tests. `tests/differential.rs` checks both pack and unpack against it.
The values +/-(2^64 - 1), where Python deviates from the specification, are
excluded.
