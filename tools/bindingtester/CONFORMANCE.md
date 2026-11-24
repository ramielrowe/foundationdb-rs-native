# Bindingtester conformance

Results of running the upstream bindingtester (Python binding + `libfdb_c`) against
`fdb-native-stacktester` with `tools/bindingtester/run.sh` (see `README.md`), on FDB 7.4.10 and
8.0.1, and the divergences found and fixed (beads `blr.6`, November 2026). Every run is
`python --compare rust` unless it says otherwise; a seed passes when the bindingtester reports 0
incorrect results and 0 errors.

## Summary

After the fixes below, **every seed passes on both versions**, with one documented exception that
the harness rewrites by default (the `+-(2^64-1)` tuple encoding).

The sweep ran in three rounds while the fixes went in; each row gives the first result of its
seeds with the tester of that round. Round 1: the timeout and database option fixes
(`c7b2698`, `ce133c1`); round 2: plus the commit's deferred error (`6df24b3`); round 3: all
fixes (`e6bc53f`).

| Suite | Configuration | Seeds | Round | 7.4.10 | 8.0.1 |
|---|---|---|---|---|---|
| `api` | 200 ops, API version picked by the seed | 1-100 | 1 | 100/100 | 100/100 |
| `api` | 2000 ops | 1001-1020 | 1 | 19/20 (1004) | 19/20 (1004) |
| `api` | 2000 ops | 1021-1120 | 2 | 96/100 (1059, 1068, 1073, 1106) | 95/100 (same, and 1114) |
| `api` | 2000 ops | 1121-1220 | 3 | 100/100 | 100/100 |
| `api` | 2000 ops, the seeds that failed | 1004, 1059, 1068, 1073, 1106, 1114 | 3 | 6/6 | 6/6 |
| `api` | 10000 ops | 5001-5020 | 3 | 20/20 | 20/20 |
| `api` | 200 ops, `--api-version` 520, 610, 630, 700, 710, latest (740 / 800) | 2001-2010 for each | 1 | 60/60 | 60/60 |
| `api` | concurrency 3 (tester alone), 200 ops | 3001-3020 | 1 | 20/20 | 20/20 |
| `api` | concurrency 5 (tester alone), 1000 ops | 6001-6030 | 3 | 30/30 | 30/30 |
| `tuple` | default (`--int-edge skip`) | 1-30 | 1 | 30/30 | 30/30 |
| `tuple` | `--int-edge keep` | 1 | 3 | the 2 expected differences | the 2 expected differences |
| `scripted` | latest API version (740 / 800) | 3 runs | 1 | 3/3 | 3/3 |

Every failure is one of the divergences below; none was a flake. The final clean pass, with the
tester built after merging `main` (b35.4) on top of all fixes:

| Final pass | Seeds | 7.4.10 | 8.0.1 |
|---|---|---|---|
| `api`, 200 ops | 1-30 | 30/30 | 30/30 |
| `api`, 2000 ops | 1001-1010 | 10/10 | 10/10 |
| `api`, each `--api-version` (520, 610, 630, 700, 710, latest) | 2001-2003 | 18/18 | 18/18 |
| `api`, concurrency 3 | 3001-3005 | 5/5 | 5/5 |
| `tuple` | 1-5 | 5/5 | 5/5 |
| `scripted` | 1 run | 1/1 | 1/1 |

In all: 559 runs per version (1118 in total), of which 5 (7.4) and 6 (8.0) failed on a
divergence that is now fixed, plus the `--int-edge keep` check.

The stacktester's `UNIT_TESTS` (the Python binding's unit tests that apply: options, watches,
cancellation, retry limits, timeouts, error predicates, size limits) run inside the `scripted`
suite, and in `crates/fdb-native-stacktester/tests/docker.rs` on both images; they now include
`test_timeouts` case 10 and the database options `LOCATION_CACHE_SIZE`, `MACHINE_ID` and
`DATACENTER_ID` (`test_db_options`), which were left out before.

Larger op counts matter: 100 seeds at 200 ops found nothing, while 2000 ops found the first
divergence in 20 seeds and three more (in five seeds) in the next 100. The random `ON_ERROR`
codes (0 to 5000) and the rarely combined `CANCEL` / oversized writes only come together in long
tests.

Reproduce a row with, e.g.,
`tools/bindingtester/run.sh -V 8.0 -s api --seeds 100 --seed-start 1021 -n 2000`.

## Divergences found and fixed

All five were client bugs (`fdb-native-client`); neither the tuple layer nor the stacktester
needed a change. Each fix has a regression test in `crates/fdb-native-client/tests/transaction.rs`
(and `src/error.rs` for the last).

| Found by | Python | Rust (before) | Root cause | Fix |
|---|---|---|---|---|
| Python `UNIT_TESTS` `test_timeouts` case 10 (blr.4); not in the random suites | `transaction_timed_out` (1031) on commit | commit succeeded | The timeout was only looked at when an operation ran: `set_option(Timeout(0))` after a deadline that passed while idle lifted it. C++ arms a timer (`timebomb`) that sets `resetPromise` at the deadline. | Every look at the transaction's state fires the timeout first if its deadline has passed (`State::fire_timebomb`), as if the timer had fired then; a later `cancel` no longer replaces `timed_out` either. Test `a_timeout_that_fired_while_idle_cannot_be_unset`. |
| `api` seed 1004, 2000 ops (both versions) | `version_invalid` (2011) on `COMMIT` | `transaction_cancelled` (1025) | `GET_COMMITTED_VERSION` of a new transaction (-1), `SET_READ_VERSION(-1)` (deferred `version_invalid`), `CANCEL`, `COMMIT`. `ThreadSafeTransaction::commit` calls `checkDeferredError()` first; our commit checked the cancellation first. | `Transaction::commit` returns the deferred error before anything else, like every other operation. Test `the_deferred_error_comes_before_the_cancellation`. |
| `api` seeds 1059, 1073, 2000 ops (both) | `value_too_large` (2103) on reads after a write | `transaction_cancelled` | `CANCEL`, then `SET` of a value over 100 000 bytes, then reads. C++'s RYW writes never look at `resetPromise`: the write is checked (its error deferred) and applied on a dead transaction. Ours skipped writes on a dead transaction, losing the error. | Writes on a cancelled or timed-out transaction are checked and applied (only `approximate_size` sees them), and trigger no watch. Same test. |
| `api` seeds 1068, 1106, 2000 ops (both) | `transaction_cancelled` (1025) from `ON_ERROR(1100)` | `broken_promise` (1100) | `RYWImpl::onError` ends with `if (e.code() == error_code_broken_promise) throw transaction_cancelled();`. | `Transaction::on_error` maps `broken_promise` to `transaction_cancelled`. Test `the_retry_limit_and_errors_that_are_not_retried_cancel_the_transaction`. |
| `api` seed 1114, 2000 ops (8.0 only) | `blob_granule_request_failed` (1079) from `ON_ERROR(1079)` | retried (`RESULT_NOT_PRESENT`) | 7.4's `Transaction::onError` retries 1079 and `proxy_tag_throttled` (1223); 8.0 removed both codes and retries neither. The client used the union for every cluster. | `on_error` uses the list of the cluster's release (from the cluster monitor's schema version). Tests `on_error_retries_what_the_release_of_the_cluster_retries`, `error::tests::on_error_semantics`. |

Also found, in the harness: `run.sh` read the server's cluster file right after `docker start`,
before the image's entrypoint had written it (`cat: /var/fdb/fdb.cluster: No such file or
directory`, a harness error on a loaded host); it now waits for the file.

Found by review while triaging, not reachable by compared runs (the generator makes every
`COMMIT` blocking when it compares): C++'s RYW checks `used_during_commit` before the
cancellation in reads, `watch`, `commit` and `getVersionstamp`, and `getReadVersion` has no
used-during-commit check at all; ours differs in both. Filed as bead `2au`.

## Known exceptions

* **`+-(2^64-1)` in the tuple layer.** The Python binding packs exactly `2^64-1` and `-(2^64-1)`
  with the long integer forms (`1d 08 ff*8`, `0b f7 00*8`); the tuple spec, Java, Go and
  `fdb-native-tuple` use the 8-byte codes (`1c ff*8`, `0c 00*8`); every decoder accepts both. The
  `tuple` suite always generates both values, so with `--int-edge keep` it reports exactly those
  two keys (on both versions: `Test with seed 1 ... had 2 incorrect result(s)`). By default the
  harness rewrites the two values to `+-(2^64-2)` in the generated test (`README.md`). The `api`
  suite hits them with probability about `2^-65` per random integer.

## Not covered

* The `directory`, `directory_hca` and `directory_partition` suites (no directory layer), tenant
  operations (7.4 bindingtester: `tenants_enabled=False`), and API versions below 520 (the tuple
  layer has only the 4-byte versionstamp offsets of 520 and later).
* The client implements the semantics of the latest API version. Behaviour that C++ changes by API
  version below 610 (for example `onError` resetting instead of cancelling a transaction whose
  error is not retried) is not modelled; the bindingtester cannot observe it, because the generator
  always follows `ON_ERROR` with `NEW_TRANSACTION`, and the `--api-version` runs above pass.
* Runs with concurrency above 1 are the tester's own checks only (upstream forbids `--compare`
  with concurrency).
