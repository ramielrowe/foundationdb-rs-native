#!/usr/bin/env python3
"""Checks golden-vector fixtures.

  check_fixtures.py roundtrip FIXTURE_DIR BINARY [--positive-only]
      Feeds every fixture's bytes to `BINARY decode <cpp_type>` and checks the
      output against the fixture: positive and corpus "accept" records must
      decode to the recorded root; negative and corpus "reject" records must be
      rejected with the recorded reason. --positive-only feeds only positive
      records (for release-branch readers, which have no bounds checks).
      Unless --positive-only, it also lints the corpora (see lint_corpora).

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


# The `violations` vocabulary of README.md ("Corpus accept classes").
VIOLATIONS = {
    "offset_into_header", "zero_uoffset", "vtable_alignment", "vtable_size", "table_size",
    "vtable_entry", "union_tag_range", "union_slots", "union_vector_count_mismatch",
    "bool_value", "empty_table_offset", "out_of_bounds", "too_deep", "work_limit",
}
ACCEPT_CLASSES = {"structural", "padding", "data"}
# Only the aliasing bombs may reach the work limit (README.md: `work_limit`).
WORK_LIMIT_FILES = {"aliasing_bombs.jsonl"}


def lint_corpora(engine_dir: Path) -> int:
    """Checks the shape of every corpus line and index.json's corpus list."""
    problems = []
    index = json.loads((engine_dir / "index.json").read_text())
    listed = {c["file"]: c["entries"] for c in index["corpus"]}
    on_disk = {f"corpus/{p.name}" for p in (engine_dir / "corpus").glob("*.jsonl")}
    if set(listed) != on_disk:
        problems.append(f"index.json lists {sorted(listed)}, directory has {sorted(on_disk)}")
    count = 0
    for file in sorted(on_disk & set(listed)):
        path = engine_dir / file
        seen = set()
        lines = [l for l in path.read_text().splitlines() if l.strip()]
        if len(lines) != listed[file]:
            problems.append(f"{file}: {len(lines)} lines, index.json says {listed[file]}")
        for line in lines:
            rec = json.loads(line)
            count += 1
            where = f"{file}:{rec.get('name')}"
            for key in ("name", "cpp_type", "file_identifier", "bytes", "mutation", "expect"):
                if key not in rec:
                    problems.append(f"{where}: missing {key}")
            if rec.get("name") in seen:
                problems.append(f"{where}: duplicate name")
            seen.add(rec.get("name"))
            violations = rec.get("violations", [])
            if rec.get("expect") == "reject":
                if "reason" not in rec or "accept_class" in rec or violations:
                    problems.append(f"{where}: a reject has a reason and nothing else")
            elif rec.get("expect") == "accept":
                cls = rec.get("accept_class")
                if cls not in ACCEPT_CLASSES or "root" not in rec:
                    problems.append(f"{where}: an accept has an accept_class and a root")
                if (cls == "structural") != bool(violations):
                    problems.append(f"{where}: violations if and only if structural")
            else:
                problems.append(f"{where}: expect is {rec.get('expect')!r}")
            unknown = set(violations) - VIOLATIONS
            if unknown:
                problems.append(f"{where}: unknown violations {sorted(unknown)}")
            if "work_limit" in violations and Path(file).name not in WORK_LIMIT_FILES:
                problems.append(f"{where}: work_limit outside {sorted(WORK_LIMIT_FILES)}")
    for p in problems:
        print(f"LINT {p}")
    print(f"lint: {count} corpus records, {len(problems)} problems")
    return 1 if problems else 0


def expectation(rec):
    if rec.get("kind") == "positive":
        return "accept"
    return rec["expect"]


def roundtrip(engine_dir: Path, binary: str, positive_only: bool) -> int:
    failures = 0 if positive_only else lint_corpora(engine_dir)
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
