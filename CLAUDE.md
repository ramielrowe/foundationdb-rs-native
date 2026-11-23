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
cargo nextest run --workspace --run-ignored only --no-tests=pass  # Docker-backed integration tests (flag needed only while none exist)
```

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

## Conventions & Patterns

- Shared package metadata comes from `[workspace.package]`; every crate sets `lints.workspace = true`.
  Workspace lints: `unsafe_code = "forbid"`, `missing_debug_implementations`, `unreachable_pub`, clippy `all`.
- Edition 2024, MSRV 1.85. No unstable rustfmt options.
- **Docker-backed tests**: tests that need a live FDB container are marked
  `#[ignore = "requires docker"]` and run with `cargo nextest run --run-ignored only`.
  The `FDB_IMAGE` env var selects the image (default `foundationdb/foundationdb:7.4.10`).
- **Safety rule**: tests must only ever target disposable containers started by
  `fdb-native-testkit`, never a shared or long-lived cluster. A malformed packet can make an
  FDB 7.4/8.0 server process exit.
