#!/usr/bin/env python3
"""Run the upstream bindingtester with the native Rust stack tester registered.

This is the only change made to the upstream bindingtester, and it is a wrapper, not a patch:
the upstream files in the image are byte-for-byte the release's. It

1. registers one extra tester in `bindingtester.known_testers.testers` (bindingtester.py takes
   testers by name from that dict; an unknown name becomes a default `Tester` that allows API
   versions from 0, which our tester rejects), and
2. optionally rewrites two integers of the `tuple` test (see below),

and then runs upstream `bindingtester.py` unchanged with the arguments it was given.

Configuration (environment, set by run.sh):

  BT_TESTER_CMD          command line of the tester, e.g. /tester/stacktester (no spaces: the
                         bindingtester splits the command on spaces). Unset: nothing is registered
                         (used for the python-vs-python self test).
  BT_TESTER_NAME         name to register it under (default `rust`).
  BT_TESTER_MAX_INT_BITS largest integer width the tester supports (default 2040, like Python).
  BT_TESTER_MIN_API      lowest API version the tester accepts (default 520: fdb-native-tuple only
                         implements the 4-byte versionstamp offset of API >= 520).
  BT_TESTER_THREADS      `1` (default) if the tester implements START_THREAD.
  BT_PY_INT_EDGE         `skip` (default) or `keep`, see below.

Registered capabilities: all value types including versionstamps; no directory layer (the
`directory*` tests are not run) and no tenants (`tenants_enabled=False` on 7.4; 8.0 has no tenant
operations in the bindingtester at all).

Known expected difference (`BT_PY_INT_EDGE=skip`). The Python binding packs exactly the integers
2**64-1 and -(2**64-1) with the long forms (0x1d 0x08 ff*8 and 0x0b f7 00*8). The tuple spec, the
Java and Go bindings and fdb-native-tuple use the 8-byte codes (0x1c ff*8 and 0x0c 00*8); every
decoder accepts both. The `tuple` test always generates both values (when max_int_bits >= 64),
so comparing against Python would always fail on these two keys. With `skip` the wrapper replaces
them in the generated test by +-(2**64-2), which both encoders pack identically; the labels of the
keys stay the same. The `api` test hits them with probability ~2**-65 per random integer, so it
needs no change. With `keep` the mismatch shows up as a failure in the `tuple` suite.
"""

import os
import runpy
import sys

ROOT = os.environ.get("BT_ROOT", "/opt/fdb/bindings")
BINDINGTESTER_DIR = os.path.join(ROOT, "bindingtester")

# Same sys.path the upstream script ends up with: the bindings directory (so that the
# `bindingtester` package wins over bindingtester.py) and the script's own directory (for
# `import util`).
sys.path[:0] = [ROOT, BINDINGTESTER_DIR]

from bindingtester import known_testers  # noqa: E402


def register_tester():
    cmd = os.environ.get("BT_TESTER_CMD")
    if not cmd:
        return
    name = os.environ.get("BT_TESTER_NAME", "rust")
    kwargs = dict(
        max_int_bits=int(os.environ.get("BT_TESTER_MAX_INT_BITS", "2040")),
        min_api_version=int(os.environ.get("BT_TESTER_MIN_API", "520")),
        max_api_version=known_testers.MAX_API_VERSION,
        threads_enabled=os.environ.get("BT_TESTER_THREADS", "1") == "1",
        types=known_testers.ALL_TYPES,
        directory_snapshot_ops_enabled=False,
    )
    # 7.4 has the flag (and the bindingtester then runs no tenant operations); 8.0 removed it.
    if "tenants_enabled" in known_testers.Tester.__init__.__code__.co_varnames:
        kwargs["tenants_enabled"] = False
    known_testers.testers[name] = known_testers.Tester(name, cmd, **kwargs)


def patch_tuple_int_edge():
    if os.environ.get("BT_PY_INT_EDGE", "skip") != "skip":
        return
    from bindingtester.tests import InstructionSet
    from bindingtester.tests import tuple as tuple_test

    edge = (1 << 64) - 1
    original_generate = tuple_test.TupleTest.generate

    def replace(value):
        if isinstance(value, int) and not isinstance(value, bool):
            if value == edge:
                return edge - 1
            if value == -edge:
                return -(edge - 1)
        return value

    def generate(self, args, thread_number):
        original_push_args = InstructionSet.push_args

        def push_args(instructions, *values):
            return original_push_args(instructions, *[replace(v) for v in values])

        InstructionSet.push_args = push_args
        try:
            return original_generate(self, args, thread_number)
        finally:
            InstructionSet.push_args = original_push_args

    tuple_test.TupleTest.generate = generate


def check_target():
    """The bindingtester wipes the whole database. Only run against run.sh's own cluster file."""
    argv = sys.argv[1:]
    cluster_file = None
    for i, arg in enumerate(argv):
        if arg == "--cluster-file" and i + 1 < len(argv):
            cluster_file = argv[i + 1]
        elif arg.startswith("--cluster-file="):
            cluster_file = arg.split("=", 1)[1]
    if cluster_file != "/cluster/fdb.cluster":
        sys.exit("refusing to run: pass --cluster-file /cluster/fdb.cluster (run.sh's cluster file)")
    try:
        with open("/cluster/.harness-owned") as f:
            marker = f.read().strip()
    except OSError:
        marker = None
    if not marker or marker != os.environ.get("BT_RUN_ID"):
        sys.exit("refusing to run: /cluster was not written by this run.sh invocation")


check_target()
register_tester()
patch_tuple_int_edge()

script = os.path.join(BINDINGTESTER_DIR, "bindingtester.py")
sys.argv[0] = script
runpy.run_path(script, run_name="__main__")
