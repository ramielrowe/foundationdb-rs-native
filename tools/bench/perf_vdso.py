"""Who reads the clock: the callers of the samples whose IP is in the vDSO.

    python3 -I tools/bench/perf_vdso.py perf.data [DEPTH]

perf's `[vdso] 0x...a06` entry is the `rdtscp` in the vDSO's clock_gettime. `perf script`
prints a frame-pointer chain without that leaf (the vDSO function sets up no frame), so the
sample IPs (`-G`) are matched to the chains by (tid, time). The chain starts at the caller of
the function that called clock_gettime (std's `Timespec::now`, or what it was inlined into);
inlined frames at that return address can be the statement after the clock read. Prints the
first DEPTH frames that are the client's, the bench's or tokio's time and metrics code.
"""
import collections
import re
import subprocess
import sys

data = sys.argv[1]
depth = int(sys.argv[2]) if len(sys.argv) > 2 else 1


def script(fields, *extra):
    return subprocess.run(["perf", "script", "-i", data, "-F", fields, *extra],
                          capture_output=True, text=True).stdout


vdso = set()
total = 0
for line in script("tid,time,ip,dso", "-G").splitlines():
    if not line.strip():
        continue
    total += 1
    if "[vdso]" in line:
        p = line.split()
        vdso.add((p[0], p[1]))

callers = collections.Counter()
for block in script("tid,time,ip,sym,dso").split("\n\n"):
    lines = [l for l in block.split("\n") if l.strip()]
    if not lines:
        continue
    head = lines[0].split()
    if len(head) < 2 or (head[0], head[1]) not in vdso:
        continue
    frames = []
    for l in lines[1:]:
        s = l.strip().split(None, 1)
        s = s[1] if len(s) > 1 else ""
        if re.search(r"fdb_native|fdb_bench|tokio::runtime::(time|metrics)|multi_thread::stats", s):
            s = re.sub(r"<fdb_native_[a-z_]+::runtime::tokio_rt::TokioRuntime"
                       r"(, fdb_native_client::database::ClientTls)?>", "<R>", s)
            frames.append(re.sub(r" \(/.*\)$", "", s)[:150])
            if len(frames) >= depth:
                break
    callers[" <- ".join(frames) or "(none)"] += 1

n = sum(callers.values())
print(f"vdso samples: {n} of {total} ({100 * n / max(total, 1):.1f}%)")
for k, v in callers.most_common(20):
    print(f"  {100 * v / total:5.2f}%  {k}")
