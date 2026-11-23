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
