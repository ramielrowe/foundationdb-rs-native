# Bindingtester harness

Dev-only. Runs FoundationDB's upstream **bindingtester** (`bindings/bindingtester`) with its Python
binding and `libfdb_c` in Docker, and compares the Python stack machine against our Rust one
(`crates/fdb-native-stacktester`) on the `api`, `scripted` and `tuple` suites, for FDB 7.4 and 8.0.
Nothing here is part of the Cargo workspace or of the default CI.

```bash
tools/bindingtester/run.sh                         # 7.4, api+scripted+tuple, 3 seeds
tools/bindingtester/run.sh -V 8.0 --seeds 10 -n 500
tools/bindingtester/run.sh -V 7.4 -s api --seed-list 17,42 --keep-logs
tools/bindingtester/run.sh --self-test -V 8.0      # python vs python: checks the harness itself
tools/bindingtester/run.sh --help
```

`run.sh` builds the tester (`cargo build --release -p fdb-native-stacktester`, or pass `-t PATH`
/ `--no-build`), starts a fresh `foundationdb/foundationdb` container, starts a bindingtester
container on the same Docker bridge network with the cluster file and the tester binary mounted
in, and for every suite and seed runs

```
bindingtester.py python --compare rust --test-name <suite> --seed S --num-ops N --cluster-file /cluster/fdb.cluster
```

Per seed it records PASS / FAIL / TIMEOUT. Exit status: 0 all passed, 1 some failed, 2 harness
error. Results go to `tools/bindingtester/results/<time>-<version>/` (gitignored): `summary.tsv`,
`run-info.txt` (versions, image tags, tester sha256) and `<suite>/seed-<S>.log` for failing seeds
(all seeds with `--keep-logs`). The log of a failure contains the full bindingtester output,
including the differing keys.

## Requirements

* Docker (BuildKit), `git`, `curl`, `sha256sum`, bash; a Rust toolchain unless `--self-test` or `-t`.
* Network on the first run: image pulls (`ubuntu`, `foundationdb/foundationdb`), the PyPI wheel, and
  either `FDB_SRC` or a shallow fetch of the pinned commit from GitHub (about 15 MB, cached in
  `tools/bindingtester/.cache`, gitignored).
* `FDB_SRC=/path/to/foundationdb` (optional): a clone that has the pinned release commits. It is
  only read, with `git archive`.
* The tester binary runs inside an `ubuntu:26.04` container; a binary built on a host with a newer
  glibc than that (2.43) will not start.

## What is pinned

`versions.sh` holds all pins; the build verifies the checksums.

```
7.4: FDB 7.4.10, API 740
  commit  eed7ac4e51743afe22255d9f42fd6abfd34161ef
  image   foundationdb/foundationdb:7.4.10@sha256:c75fa3531b9f8c0bda320645f1583a8376fd276f7fd060307d252111d29c3bce
  libfdb_c.so  0a78338ba7897b1fd6ef18d24d02b7b79856207f2506806cbba96e28a28c67b1
  fdb wheel    df1e81649c554d2dbba0e866b29491c9366a0a3996edf6df06782e88b909d856 (PyPI foundationdb-7.4.10-py3-none-any.whl)
8.0: FDB 8.0.1, API 800
  commit  9aaa93799b2b897c518ce6ffc91d8ed129c9a6d4
  image   foundationdb/foundationdb:8.0.1@sha256:8ee19baa1a9f8232d52423971901a0e72e9910aa05f2a3130813cef86fe5e569
  libfdb_c.so  3934e236a33297c8d3a6b7cf3768c96c79df32c7303b459b48fd9183fe85f8fd
  fdb wheel    c314aba0eba76471ee1bb489cf772773bde8cd776288fd1a887bdd19dc5c2c35 (PyPI foundationdb-8.0.1-py3-none-any.whl)
base    ubuntu:26.04@sha256:f144425ff09be612d6d9ad965196e9cdc23dae1f42110a8a11a3e9a8198759f7
```

The commits are what the `7.4.10` and `8.0.1` tags point to. The `fdb` package is the official
wheel, which is the same code as `bindings/python/fdb` of that commit plus the generated
`fdboptions.py`/`apiversion.py`. Base image: `ubuntu:26.04` by digest, `python3` from apt.

The image is tagged `fdb-native-bindingtester:<fdb version>-<hash of Dockerfile, overlay and pins>`
and rebuilt automatically when any of those change (`--rebuild-image` forces it, `--build-image`
only builds). A build takes about 15 s.

## How our tester is registered

The upstream bindingtester gets testers by name from `bindingtester.known_testers.testers`; an
unknown name becomes a default `Tester` (API versions from 0, no versionstamps, 64-bit ints). The
image therefore runs the bindingtester through `overlay/run_bindingtester.py`, a wrapper that adds
one entry and then runs upstream `bindingtester.py` unchanged (no upstream file is patched):

```python
Tester("rust", "/tester/stacktester", max_int_bits=2040, min_api_version=520,
       max_api_version=<latest>, threads_enabled=True, types=ALL_TYPES,
       directory_snapshot_ops_enabled=False, tenants_enabled=False)   # last flag: 7.4 only
```

* min API version 520: `fdb-native-tuple` only implements the 4-byte versionstamp offset;
* all value types including versionstamps, integers up to 2040 bits;
* no tenants (7.4: `tenants_enabled=False`, so no tenant operations are generated; 8.0 has none) and
  no directory layer (the `directory*` suites are not run);
* `scripted` always runs at the latest API version (740 / 800), so the client must accept it.

The tester is invoked as the bindingtester does: `<tester> test_spec <api_version> /cluster/fdb.cluster`.
`--no-threads` (tester has no `START_THREAD`) and `--max-int-bits N` map to the bindingtester flags.
Other tester settings are the `BT_*` environment variables documented in the wrapper;
`--tester-env K=V` passes variables (such as `RUST_LOG`) to the tester process.

## Cluster setup

A fresh `fdbserver` container per run (default entrypoint, `configure new single memory`, ready in
about 3 s; labelled `fdb-native-bindingtester-run=<id>` and removed by exact ID on exit, including
on Ctrl-C/TERM; if the server process dies during a run, e.g. from a malformed packet, a new one
is started and the run continues). The container is on the default bridge network, like
`fdb-native-testkit`. On 7.4 the cluster is configured with `tenant_mode=optional_experimental`,
because the 7.4 bindingtester clears the tenant map before every test and fails with error 2136
otherwise; 8.0 needs nothing.

**The bindingtester clears the whole database** (`del db[:]`) before every test. The harness
therefore only passes it the cluster file of its own container: `run.sh` checks the container's
label and IP against the cluster file before every run, and the wrapper refuses to start unless
`--cluster-file /cluster/fdb.cluster` is given and `/cluster/.harness-owned` carries the run's ID.
Never run the bindingtester by hand against a shared cluster (`mes-fdb-dev` in particular).
Containers leaked by a SIGKILL can be found with
`docker ps -a --filter label=fdb-native-bindingtester` and removed by ID.

## Suites, seeds, concurrency

* `api`: random operations (`--num-ops`), the seed also picks the API version unless `--api-version`
  is given. `--concurrency C` with C > 1 runs the tester alone with C threads (upstream forbids
  `--compare` with concurrency, so the result is only the tester's own checks and no comparison).
* `scripted`: a fixed script; it ignores num-ops and seed, so it runs once per invocation (about
  50 s, mostly waiting on watches and timeouts).
* `tuple`: fixed instructions (integer boundary cases up to 2^2040); the seed only picks the API
  version. About 12 s.

## Known expected difference: +-(2^64 - 1)

The Python binding packs exactly the integers `2^64-1` and `-(2^64-1)` with the long forms
(`1d 08 ff*8`, `0b f7 00*8`); the tuple spec, Java, Go and `fdb-native-tuple` use the 8-byte codes
(`1c ff*8`, `0c 00*8`). Decoding accepts both. The `tuple` suite always generates both values
when `max_int_bits >= 64`, so a plain comparison always reports exactly 2 incorrect results at those
two keys (`Test with seed S and concurrency 1 had 2 incorrect result(s) ...`).

By default (`--int-edge skip`) the wrapper avoids it: it rewrites those two integers in the generated
`tuple` test to `+-(2^64-2)`, which both implementations pack identically; both testers still see the
same instructions, and the key labels are unchanged. The `api` suite hits the values with probability
about 2^-65 per random integer and is not changed. The two values themselves are covered by the unit
tests of `fdb-native-tuple`. `--int-edge keep` removes the rewrite, and then the `tuple` suite fails
with those 2 results; use it to confirm a failure is only this difference.

## Debugging a failing seed

Rerun the seed with `--keep-logs -s <suite> --seed-list S`. `--bt-arg ARG` (repeatable) appends
arguments to the bindingtester command: `--bt-arg --print-test` prints the generated instructions
instead of running them (add `--bt-arg --all` for the setup too), `--bt-arg --bisect` finds the
smallest failing `--num-ops`. `--tester-env RUST_LOG=...` configures the tester process.

## Runtime

On a 32-core host (images and server image present): image build 15 s, server start 3 s, `api`
200 ops 2-4 s per seed, `tuple` 12 s per seed, `scripted` 50 s. The default invocation (3 seeds,
all suites) takes about 100 s per version.

## Layout

* `run.sh`: driver (options in `--help`).
* `versions.sh`: pins.
* `Dockerfile`: bindingtester image (built from a context `run.sh` assembles under `.cache/`).
* `overlay/run_bindingtester.py`: tester registration, the `tuple` int-edge rewrite and the
  safety check.
* CI: `.github/workflows/bindingtester.yml`, manual trigger only.
