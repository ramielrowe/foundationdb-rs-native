# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:1105d646 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->


## Build & Test

```bash
cargo fmt --all --check                                        # formatting
cargo clippy --workspace --all-targets -- -D warnings          # lints (warnings are errors in CI)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps     # docs
cargo nextest run --workspace                                  # unit tests
cargo nextest run --workspace --features fdb-native-transport/smol,fdb-native-client/smol  # unit tests incl. the smol adapter and tests
cargo nextest run --workspace --run-ignored only --no-tests=pass -E 'not (binary(cpp_crosscheck) | test(cpp_crosscheck))'  # Docker-backed integration tests
cargo test --doc --workspace                                   # doc tests (nextest skips them)
cargo build --workspace --examples                             # examples (also checked by --all-targets)
```

The TLS code is behind features of two crates: `fdb-native-transport/tls` and
`fdb-native-client/tls` (the client's `cfg(feature = "tls")` code, the `tls` example and the
client's TLS docker tests need the client's own feature). CI runs clippy, docs, MSRV checks,
tests and the docker job with `--features fdb-native-transport/tls,fdb-native-client/tls` too.

The second runtime adapter, smol (`SmolRuntime`: async-io, async-executor, `blocking`), is behind
`fdb-native-transport/smol` and `fdb-native-client/smol` (a feature of both: the client's `smol`
example and its smol tests need the client's own). The smol tests (`tests/smol_*.rs`, `smol_fake`,
`docker_smol`, `smol_docker`) are `cfg(feature = "smol")`, so they run only with
`--features fdb-native-transport/smol,fdb-native-client/smol` (CI's `test-smol` job; the docker job has
them on too). The client under test runs on smol there; the fake servers of `smol_fake` and
`smol_tls` stay on tokio (they share only streams and channels with it), and `MemRuntime<R>` in
`crates/fdb-native-client/tests/support` takes its time and tasks from `R`. A smol build without
tokio: `cargo build -p fdb-native-client --no-default-features --features smol --example smol`.
Its `Runtime` safety constants (`WRITE_ERRORS_TAKE_NOTHING`, `PENDING_TAKES_NOTHING`) are
justified against async-io's source in `runtime/smol_rt.rs`; a change of async-io's `poll_write`
needs that re-checked.

`fdb-native-client/examples/` are runnable examples (`cargo run -p fdb-native-client --example
basic -- CLUSTER_FILE`); `tests/docker_examples.rs` includes each example's `run` with
`#[path]` and calls it against a testkit container, so an example change is a test change.

`fdb-native-wire`'s encoder is also checked against FDB's C++ readers by an ignored test binary,
`cpp_crosscheck` (`#[ignore = "requires golden-vectors tool"]`, excluded from the Docker CI job).
It needs the dev-only `tools/golden-vectors` decoder; `FDB_GOLDEN_VECTORS_BIN` is a `:`-separated
list of decoders to check against. `regen.sh --check` builds the release-7.4 and release-8.0
readers under `tools/golden-vectors/build/rel-*`; in a git worktree, set `FDB_SRC` explicitly:

```bash
FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen.sh --check   # or: make -C tools/golden-vectors FDB_SRC=...
GV=$PWD/tools/golden-vectors/build
FDB_GOLDEN_VECTORS_BIN=$GV/golden-vectors:$GV/rel-release-7.4/golden-vectors:$GV/rel-release-8.0/golden-vectors \
  cargo nextest run -p fdb-native-wire --run-ignored only -E 'binary(cpp_crosscheck)'
```

The blob codecs (`KeyValueVectorRef`, `VersionVector`, `TagSetRef`, `IdempotencyIdRef`) have the same
kind of check in `tests/blob_codecs.rs`: `cpp_crosscheck_release_readers_decode_reencoded_blobs` sends the
re-encoded blob fields of the message fixtures to each release's own reader. It is ignored (and
excluded from the Docker CI job by the `test(cpp_crosscheck)` filter) and needs the message decoders
built by `FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen_messages.sh` (docker, ~5 min):

```bash
MSG=$PWD/tools/golden-vectors/build/msg
FDB_GOLDEN_MESSAGES_BIN_7_4=$MSG/build-release-7.4/bin/golden_vectors_messages \
FDB_GOLDEN_MESSAGES_BIN_8_0=$MSG/build-release-8.0/bin/golden_vectors_messages \
  cargo nextest run -p fdb-native-wire --run-ignored only -E 'test(cpp_crosscheck_release_readers)'
```

`fdb-native-wire` has [divan](https://docs.rs/divan) benchmarks of encode and decode (eager and
`VectorView`) for representative messages: a small request, a `GetKeyValuesReply`-like 1000-pair
reply, a 1000-mutation commit, a 1 MiB `Vec<u8>`, 200 strings, and vtable-heavy tables. They are a
dev-dependency only (divan's MSRV, 1.80, is below the workspace's 1.85) and `cargo bench` is not
part of CI. Run them in release mode, filter by name after `--`, and compare runs by saving the
output (divan has no baseline store; the median column is the stable one):

```bash
cargo bench -p fdb-native-wire --bench wire                    # everything (~1 min)
cargo bench -p fdb-native-wire --bench wire -- commit          # one group: small, kv_reply, kv_table, commit, blob, strings_200, vtables, reuse
DIVAN_MIN_TIME=4 perf record -g target/release/deps/wire-<hash> --bench commit::encode_   # profile one (perf is the tool used so far)
```

Any change to the writer or reader internals should keep encoder output byte-identical (the fixture
round-trips and the C++ crosscheck do not catch a layout drift that still decodes) and the work-budget
charges unchanged (`tools/golden-vectors/strict.h` mirrors them); `tests/bulk_vectors.rs` pins the
bulk vector paths to the element-wise ones, and `tests/writer_reuse.rs` pins a reused `Writer` to a new one and
the bytes of seeded messages (digests recorded from the pre-vvh.8 encoder).

**Deterministic simulation** (`crates/fdb-native-sim`, dev-only, `publish = false`): the real
client runs on `SimRuntime` (a seeded single-threaded executor with virtual time) against a model
cluster on a fault-injecting in-memory network, all from one seed. `tests/sweep.rs` runs a seed
sweep in the normal `cargo nextest run --workspace` (200 seeds, ~1 s on a laptop in debug; every
tenth seed runs twice and must give the same trace hash) and asserts the invariants: at-most-once
commits checked against the cluster's commit log, the bank workload's serializability, reads
checked against the history, `on_error` backoff bounds, failure-monitor and load-balancing
convergence, liveness after the faults heal, and no client task left after `Database::shutdown`.
A failing seed prints its replay command:

```bash
FDB_SIM_SEEDS=2000 FDB_SIM_SEED_START=0 cargo nextest run -p fdb-native-sim --no-capture -E 'test(seed_sweep)'  # a bigger sweep (--release for 10^4+ seeds)
FDB_SIM_SEED=1234 FDB_SIM_TRACE=1 cargo nextest run -p fdb-native-sim --no-capture -E 'test(replay_seed)'     # replay one seed with its trace (2: every poll and segment)
```

The seed decides the cluster's shape, the network's fault rates, the chaos schedule, the client's
seeds and the stream's write promises (`seed % 4`: both `Runtime` safety constants true, both
false, or one of each); `tests/faults.rs` turns write-time resets up under both settings. A client
change that makes a seed nondeterministic (iterating a `HashMap` with effects, a `RandomState` RNG
without a seed option) shows up as `NONDETERMINISTIC` in the sweep. The harness tolerates one known
gap: the GRV batcher tasks outlive `Database::shutdown` until the database is dropped
(`foundationdb-rs-native-x4q`).

Tests run with [cargo-nextest](https://nexte.st/); config is in `.config/nextest.toml`
(`default` and `ci` profiles). CI is `.github/workflows/ci.yml`.

## Architecture Overview

Pure-Rust native FoundationDB client that speaks the wire protocol directly (no `libfdb_c`),
targeting FDB 7.4 and 8.0 servers. Cargo workspace; all crates live under `crates/`:

- `fdb-native-wire`: sans-IO codec for FDB's flatbuffers dialect and the legacy binary format.
- `fdb-native-schema`: per-protocol-version message schemas (7.4, 8.0); depends on `wire`.
- `fdb-native-transport`: runtime-agnostic connection layer (`Runtime` trait, tokio adapter later); depends on `wire`.
- `fdb-native-client`: public client API; depends on `schema` and `transport`.
- `fdb-native-tuple`: tuple and subspace layers; independent of the other crates.
- `fdb-native-testkit`: Docker test harness (`publish = false`).
- `fdb-native-stacktester`: binary for the bindingtester stack machine (`publish = false`).
- `fdb-native-sim`: deterministic simulation (a seeded `Runtime`, a faulty network, a model cluster, workloads and invariants; `publish = false`); depends on `client`, `schema`, `transport`, `wire`.

Outside the workspace, `tools/golden-vectors/` is a dev-only C++ tool that compiles FDB's
flatbuffers headers and writes golden vectors to `tools/golden-vectors/fixtures/`. The fixtures
are checked in and Rust tests read them as files; nothing in the workspace builds the tool (only
the opt-in `cpp_crosscheck` test runs it). Its README documents the fixture format, the
wire-format details and the decoder strictness policy the `wire` codec follows.
Regenerate with `FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen.sh --check`.
The message-level fixtures (`fixtures/release-7.4/`, `fixtures/release-8.0/`) come from
`FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen_messages.sh --check` (needs docker).
The legacy `BinaryWriter` vectors (`fixtures/binary-release-7.4/`, `fixtures/binary-release-8.0/`) are written by the same command; the types they test live in `fdb-native-transport` (`ConnectPacket`) and `fdb-native-schema` (span contexts, health metrics, idempotency records, conflict keys), the generic codec in `fdb-native-wire::binary`.

## Conventions & Patterns

- Shared package metadata comes from `[workspace.package]`; every crate sets `lints.workspace = true`.
  Workspace lints: `unsafe_code = "forbid"`, `missing_debug_implementations`, `unreachable_pub`, clippy `all`.
- Edition 2024, MSRV 1.85. No unstable rustfmt options.
- **Client API**: `fdb-native-client`'s crate root re-exports the application API (`Database`,
  `Transaction`, options, `Error`, ...); the lower layers (`read`, `commit`, `cluster`,
  `cluster_file`, `watch`, `atomic`) are public modules for tools and tests and are not
  re-exported. Its crate docs have the API design notes; record API changes in `CHANGELOG.md`.
- **Docker-backed tests**: tests that need a live FDB container are marked
  `#[ignore = "requires docker"]` and run with `cargo nextest run --run-ignored only`
  (excluding the `cpp_crosscheck` binary, which needs the golden-vectors tool instead).
  The `FDB_IMAGE` env var selects the image (default `foundationdb/foundationdb:7.4.10`).
  Docker tests live in an integration-test binary named `docker` (or are named `docker_*`) so the
  nextest `docker` test group in `.config/nextest.toml` caps them at 4 concurrent containers.
  The testkit API is blocking; start containers outside an async runtime or via `spawn_blocking`.
- **Multi-process clusters** (fault tests): `fdb_native_testkit::FdbCluster` starts 3-5 `fdbserver`
  containers on a dedicated, labelled docker network with static IPs (so restarts keep their
  addresses), `double` redundancy, and has by-ID helpers: `kill`/`stop`/`start_process`/`restart`,
  `pause`/`unpause`, `exclude`/`include`, `set_coordinators`, `write_keys` + `small_shards()` for
  shard splits/moves, `status_json`/`wait_healthy`. A start takes ~40 s. Its tests live in the
  `cluster` integration-test binary (nextest `cluster` group, max 2 at a time).
- **Safety rule**: tests must only ever target disposable containers started by
  `fdb-native-testkit`, never a shared or long-lived cluster. A malformed packet can make an
  FDB 7.4/8.0 server process exit.
