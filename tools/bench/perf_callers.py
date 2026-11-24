import re, subprocess, sys, collections
data, leaf_pat = sys.argv[1], sys.argv[2]
out = subprocess.run(["perf", "script", "-i", data, "-F", "ip,sym"], capture_output=True, text=True).stdout
stacks = [s.strip().split("\n") for s in out.split("\n\n") if s.strip()]
total = len(stacks)
c = collections.Counter()
n = 0
def clean(line):
    parts = line.strip().split(None, 1)
    return parts[1] if len(parts) > 1 else ""
for st in stacks:
    syms = [clean(l) for l in st]
    if not syms or not re.search(leaf_pat, syms[0]):
        continue
    n += 1
    # first frame that is in our code (fdb_native_*) after the leaf
    for s in syms[1:]:
        if "fdb_native" in s or "rpds" in s:
            c[re.sub(r"<fdb_native_[a-z_]+::runtime::tokio_rt::TokioRuntime, fdb_native_client::database::ClientTls>", "<R>", s)[:170]] += 1
            break
    else:
        c["(none)"] += 1
print(f"leaf {leaf_pat}: {n} of {total} samples ({100*n/total:.1f}%)")
for k, v in c.most_common(12):
    print(f"  {100*v/total:5.2f}%  {k}")
