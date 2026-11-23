# foundationdb-rs-native

A pure-Rust, native [FoundationDB](https://www.foundationdb.org/) client. It speaks
FoundationDB's wire protocol directly and does not link `libfdb_c`.

**Status: pre-alpha.** Nothing is usable yet; the repository currently contains only the
workspace scaffold.

## Supported servers

FoundationDB 7.4 and 8.0.

## Crates

| Crate | Purpose |
| --- | --- |
| `fdb-native-wire` | Sans-IO codec for FDB's flatbuffers dialect and the legacy binary format |
| `fdb-native-schema` | Per-protocol-version message schemas (7.4, 8.0); depends on `wire` |
| `fdb-native-transport` | Runtime-agnostic connection layer; depends on `wire` |
| `fdb-native-client` | Public client API; depends on `schema` and `transport` |
| `fdb-native-tuple` | Tuple and subspace layers |
| `fdb-native-testkit` | Docker test harness (not published) |
| `fdb-native-stacktester` | Binding tester stack machine binary (not published) |

## Golden vectors

[`tools/golden-vectors/`](tools/golden-vectors/) is a dev-only C++ tool that builds against
FoundationDB's own serializer headers and writes golden flatbuffers-dialect vectors (bytes plus
decoded field dumps, and malformed-input cases) to `tools/golden-vectors/fixtures/`. The fixtures
are checked in; the Rust crates never build or run the tool. See its
[README](tools/golden-vectors/README.md) for the fixture format and how to regenerate.

## Testing

Requires [cargo-nextest](https://nexte.st/).

```bash
# Unit tests
cargo nextest run --workspace

# Integration tests that need a live FoundationDB (requires Docker)
cargo nextest run --workspace --run-ignored only --no-tests=pass
```

Tests that need a live FoundationDB container are marked `#[ignore = "requires docker"]`.
The `FDB_IMAGE` environment variable selects the server image
(default `foundationdb/foundationdb:7.4.10`):

```bash
FDB_IMAGE=foundationdb/foundationdb:8.0.1 cargo nextest run --workspace --run-ignored only
```

Tests must only target disposable containers started by `fdb-native-testkit`, never a
shared or long-lived cluster: a malformed packet can make an FDB 7.4/8.0 server process exit.

Lint and format checks, as run in CI:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

## License

Apache-2.0. See [LICENSE](LICENSE).
