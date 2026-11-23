#!/usr/bin/env python3
"""Checks that the types this tool mirrors still match the FDB branch being built.

  check_mirrors.py FDB_SRC_DIR

Today there is one: ConnectPacket (messages/connect_packet.h), which lives in a .cpp file of
fdbrpc and cannot be included. Compares the struct in the branch's FlowTransport source with
the mirror, ignoring comments, blank lines, spacing and TraceEvent lines (the mirror has no
tracing). Exits 1, printing a diff, if they differ.
"""

import difflib
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def normalize(lines):
    out = []
    for line in lines:
        line = re.sub(r"//.*", "", line)
        line = " ".join(line.split())
        if not line or "TraceEvent" in line:
            continue
        out.append(line)
    return out


def source_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("#pragma pack(push, 1)"))
    end = next(i for i, l in enumerate(lines) if i > start and l.startswith("#pragma pack(pop)"))
    return lines[start : end + 1]


def mirror_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if "BEGIN MIRROR" in l)
    end = next(i for i, l in enumerate(lines) if "END MIRROR" in l)
    return lines[start + 1 : end]


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    src = Path(sys.argv[1])
    candidates = [src / "fdbrpc" / "FlowTransport.actor.cpp", src / "fdbrpc" / "FlowTransport.cpp"]
    source = next((c for c in candidates if c.exists()), None)
    if source is None:
        print("check_mirrors: no FlowTransport source under " + str(src), file=sys.stderr)
        return 1
    want = normalize(source_region(source.read_text()))
    have = normalize(mirror_region((HERE / "connect_packet.h").read_text()))
    if want != have:
        print(f"check_mirrors: messages/connect_packet.h differs from ConnectPacket in {source}:", file=sys.stderr)
        for line in difflib.unified_diff(want, have, "source", "mirror", lineterm=""):
            print(line, file=sys.stderr)
        return 1
    print(f"check_mirrors: ConnectPacket matches {source.relative_to(src)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
