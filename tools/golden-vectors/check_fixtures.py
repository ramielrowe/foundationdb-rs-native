#!/usr/bin/env python3
"""Checks golden-vector fixtures.

  check_fixtures.py roundtrip FIXTURE_DIR BINARY [--positive-only]
      Feeds every fixture's bytes to `BINARY decode <cpp_type>` and checks the
      output against the fixture: positive and corpus "accept" records must
      decode to the recorded root; negative and corpus "reject" records must be
      rejected with the recorded reason. --positive-only feeds only positive
      records (for release-branch readers, which have no bounds checks).

  check_fixtures.py compare EXPECTED_DIR ACTUAL_DIR [--positive-only] [--fields-only]
      Compares two generated fixture trees record by record (ignoring
      index.json and schemas.json). --positive-only compares only positive records (ACTUAL_DIR
      may lack the rest). --fields-only implies --positive-only and ignores the
      bytes, comparing everything else (decoded root, size, file identifier):
      use it for a writer whose vtable order legitimately differs, e.g. another
      compiler.
"""

import json
import subprocess
import sys
from pathlib import Path


def records(engine_dir: Path):
    """Yields (label, record) for every fixture record under engine_dir."""
    for path in sorted(engine_dir.glob("*.json")):
        if path.name not in ("index.json", "schemas.json"):
            yield path.name, json.loads(path.read_text())
    for path in sorted((engine_dir / "negative").glob("*.json")):
        yield "negative/" + path.name, json.loads(path.read_text())
    for path in sorted((engine_dir / "corpus").glob("*.jsonl")):
        for line in path.read_text().splitlines():
            rec = json.loads(line)
            yield f"corpus/{path.name}:{rec['name']}", rec


def expectation(rec):
    if rec.get("kind") == "positive":
        return "accept"
    return rec["expect"]


def roundtrip(engine_dir: Path, binary: str, positive_only: bool) -> int:
    failures = 0
    count = 0
    for label, rec in records(engine_dir):
        if positive_only and rec.get("kind") != "positive":
            continue
        count += 1
        proc = subprocess.run(
            [binary, "decode", rec["cpp_type"]],
            input=rec["bytes"].encode(),
            capture_output=True,
        )
        expect = expectation(rec)
        try:
            out = json.loads(proc.stdout)
        except json.JSONDecodeError:
            print(f"FAIL {label}: unparseable output (exit {proc.returncode}): {proc.stderr.decode()}")
            failures += 1
            continue
        if expect == "accept":
            ok = proc.returncode == 0 and out.get("root") == rec["root"]
            if ok and out["file_identifier"] != rec["file_identifier"]:
                ok = False
        else:
            ok = proc.returncode == 1 and out.get("error") == rec["reason"]
        if not ok:
            failures += 1
            print(f"FAIL {label}: expected {expect}, exit {proc.returncode}, output {proc.stdout.decode()[:300]}")
    print(f"roundtrip: {count} records, {failures} failures")
    return 1 if failures else 0


def compare(expected_dir: Path, actual_dir: Path, positive_only: bool, fields_only: bool) -> int:
    expected = dict(records(expected_dir))
    actual = dict(records(actual_dir))
    failures = 0
    compared = 0
    for label, rec in expected.items():
        if positive_only and rec.get("kind") != "positive":
            continue
        if label not in actual:
            print(f"MISSING {label}")
            failures += 1
            continue
        compared += 1
        if fields_only:
            rec = {k: v for k, v in rec.items() if k != "bytes"}
            actual[label] = {k: v for k, v in actual[label].items() if k != "bytes"}
        if actual[label] != rec:
            other = actual[label]
            what = "bytes" if other.get("bytes") != rec.get("bytes") else "record"
            print(f"DIFF {label}: {what} differ")
            failures += 1
    for label in actual:
        if positive_only and actual[label].get("kind") != "positive":
            continue
        if label not in expected:
            print(f"EXTRA {label}")
            failures += 1
    print(f"compare: {compared} records compared, {failures} differences")
    return 1 if failures else 0


def main(argv):
    if len(argv) >= 4 and argv[1] == "roundtrip":
        return roundtrip(Path(argv[2]), argv[3], "--positive-only" in argv[4:])
    if len(argv) >= 4 and argv[1] == "compare":
        fields_only = "--fields-only" in argv[4:]
        positive_only = fields_only or "--positive-only" in argv[4:]
        return compare(Path(argv[2]), Path(argv[3]), positive_only, fields_only)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
