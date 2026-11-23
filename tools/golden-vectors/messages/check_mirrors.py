#!/usr/bin/env python3
"""Checks that what this tool mirrors or documents still matches the FDB branch being built.

  check_mirrors.py FDB_SRC_DIR [--print]

1. The ConnectPacket struct (messages/connect_packet.h) lives in a .cpp file of fdbrpc and
   cannot be included, so the generator mirrors it. The struct in the branch's FlowTransport
   source is compared with the mirror, ignoring comments, blank lines, spacing and whole
   TraceEvent statements (the mirror has no tracing). A difference fails the build and prints
   a diff.

2. The code around it is not mirrored but the Rust side implements it (fdb-native-transport's
   ConnectPacket::check, connection ids, the client's address fields): the normalized text of
   Peer::prependConnectPacket and of the connectionReader block that reads and validates the
   packet is fingerprinted, and a change in either fails the build. When that happens, read
   the change, update the Rust code and docs if the behavior changed, then update the
   fingerprints below (--print shows them).
"""

import difflib
import hashlib
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# sha256 of the normalized text, per source file name.
FINGERPRINTS = {
    "FlowTransport.actor.cpp": {  # release-7.4
        "prependConnectPacket": "6c31de51b6f91e38ab21c597f9f2e93d191df75561ee4499031bdc170074affc",
        "connectPacketReader": "326acf6aa8bf94c13fa208acc72acd111178f76c8b51cacc14d7f56f0bbbb9cb",
    },
    "FlowTransport.cpp": {  # release-8.0 (the reader differs from 7.4's only in contains() and co_await)
        "prependConnectPacket": "6c31de51b6f91e38ab21c597f9f2e93d191df75561ee4499031bdc170074affc",
        "connectPacketReader": "959c06e485d8b281df1af2f5d3f6ba5671185488e399726258cbbf63a946f99b",
    },
}


def normalize(lines):
    """Strips comments, whitespace differences, blank lines and whole TraceEvent statements."""
    out = []
    in_trace = False
    for line in lines:
        line = re.sub(r"//.*", "", line)
        line = " ".join(line.split())
        if not line:
            continue
        if in_trace or "TraceEvent" in line:
            # A trace statement runs to the next line that ends with a semicolon.
            in_trace = not line.endswith(";")
            continue
        out.append(line)
    return out


def struct_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("#pragma pack(push, 1)"))
    end = next(i for i, l in enumerate(lines) if i > start and l.startswith("#pragma pack(pop)"))
    return lines[start : end + 1]


def mirror_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if "BEGIN MIRROR" in l)
    end = next(i for i, l in enumerate(lines) if "END MIRROR" in l)
    return lines[start + 1 : end]


def prepend_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("void Peer::prependConnectPacket()"))
    end = next(i for i, l in enumerate(lines) if i > start and l == "}")
    return lines[start : end + 1]


def reader_region(text):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if "if (expectConnectPacket &&" in l)
    end = next(i for i, l in enumerate(lines) if i > start and "peer->protocolVersion->set(peerProtocolVersion);" in l)
    return lines[start : end + 1]


def fingerprint(lines):
    return hashlib.sha256("\n".join(normalize(lines)).encode()).hexdigest()


def main():
    args = [a for a in sys.argv[1:] if a != "--print"]
    show = "--print" in sys.argv[1:]
    if len(args) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    src = Path(args[0])
    candidates = [src / "fdbrpc" / "FlowTransport.actor.cpp", src / "fdbrpc" / "FlowTransport.cpp"]
    source = next((c for c in candidates if c.exists()), None)
    if source is None:
        print("check_mirrors: no FlowTransport source under " + str(src), file=sys.stderr)
        return 1
    text = source.read_text()

    prints = {
        "prependConnectPacket": fingerprint(prepend_region(text)),
        "connectPacketReader": fingerprint(reader_region(text)),
    }
    if show:
        print(source.name, prints)
        return 0

    want = normalize(struct_region(text))
    have = normalize(mirror_region((HERE / "connect_packet.h").read_text()))
    if want != have:
        print(f"check_mirrors: messages/connect_packet.h differs from ConnectPacket in {source}:", file=sys.stderr)
        for line in difflib.unified_diff(want, have, "source", "mirror", lineterm=""):
            print(line, file=sys.stderr)
        return 1

    expected = FINGERPRINTS[source.name]
    for name, got in prints.items():
        if expected[name] != got:
            print(
                f"check_mirrors: {name} in {source} changed (sha256 {got}, expected {expected[name]}).\n"
                "  The Rust ConnectPacket (crates/fdb-native-transport) and the binary_vectors.cpp descriptions\n"
                "  follow this code: read the change, adjust them if the behavior changed, then update\n"
                "  FINGERPRINTS in messages/check_mirrors.py (run it with --print).",
                file=sys.stderr,
            )
            return 1
    print(f"check_mirrors: ConnectPacket and its send/receive code match {source.relative_to(src)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
