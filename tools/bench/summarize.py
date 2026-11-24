#!/usr/bin/env python3
"""Side-by-side Markdown tables (medians over repetitions) for docs/benchmarks.md.

    tools/bench/summarize.py RUN_DIR [--fix FIX_RUN_DIR]

RUN_DIR holds results.jsonl of a `fdb-bench run` (both clients). --fix names a second run of
the native client only (a patched build), added as a third column set.
"""
import json
import statistics
import sys
from collections import defaultdict

ORDER = ["get", "get_seq", "set", "scan", "mixed", "many_tx", "watch"]


def load(path):
    cells = defaultdict(list)
    for line in open(path):
        r = json.loads(line)
        cells[(r["workload"], r["variant"], r["conc"], r["backend"])].append(r)
    return cells


def med(rs, f):
    return statistics.median(r[f] for r in rs)


def us(x):
    if x >= 10000:
        return f"{x / 1000:.1f} ms"
    if x >= 1000:
        return f"{x / 1000:.2f} ms"
    return f"{x:.0f} us"


def rate(x):
    if x >= 1e6:
        return f"{x / 1e6:.2f}M"
    if x >= 1e4:
        return f"{x / 1e3:.1f}k"
    if x >= 100:
        return f"{x:.0f}"
    return f"{x:.1f}"


def main():
    args = sys.argv[1:]
    fix = None
    if "--fix" in args:
        i = args.index("--fix")
        fix = load(args[i + 1] + "/results.jsonl")
        del args[i : i + 2]
    cells = load(args[0] + "/results.jsonl")
    keys = sorted(
        {k[:3] for k in cells},
        key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1], k[2]),
    )
    cols = ["workload", "variant", "conc", "libfdb_c ops/s", "native ops/s", "ratio"]
    if fix:
        cols.append("native+fix ops/s")
    cols += ["libfdb_c p50 / p99", "native p50 / p99"]
    if fix:
        cols.append("native+fix p50 / p99")
    cols += ["CPU us/op c / native"]
    if fix:
        cols[-1] += " / fix"
    print("| " + " | ".join(cols) + " |")
    print("|" + "|".join(["---", "---", "---:"] + ["---:"] * (len(cols) - 3)) + "|")
    for k in keys:
        c, n = cells.get(k + ("c",)), cells.get(k + ("native",))
        if not c or not n:
            continue
        f = fix.get(k + ("native",)) if fix else None
        row = [k[0], k[1], str(k[2]), rate(med(c, "ops_per_s")), rate(med(n, "ops_per_s")),
               f"{med(n, 'ops_per_s') / med(c, 'ops_per_s'):.2f}"]
        if fix:
            row.append(rate(med(f, "ops_per_s")) if f else "")
        row.append(f"{us(med(c, 'p50_us'))} / {us(med(c, 'p99_us'))}")
        row.append(f"{us(med(n, 'p50_us'))} / {us(med(n, 'p99_us'))}")
        if fix:
            row.append(f"{us(med(f, 'p50_us'))} / {us(med(f, 'p99_us'))}" if f else "")
        cpu = f"{med(c, 'cpu_us_per_op'):.1f} / {med(n, 'cpu_us_per_op'):.1f}"
        if fix:
            cpu += f" / {med(f, 'cpu_us_per_op'):.1f}" if f else " / "
        row.append(cpu)
        print("| " + " | ".join(row) + " |")


main()
