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

3. The packet framing (messages/framing_vectors.cpp; fdb-native-transport's framing module).
   The generator runs the framing core of FlowTransport's sendPacket() verbatim: the two regions
   it marks MIRROR A and MIRROR B are compared with sendPacket() (same normalization). The
   Rust codec also follows sendPacket()'s length/limit code, scanPackets() (the reader), and
   getNewBufferSize() with the limit helpers of release-8.0: the normalized text of each of
   them is fingerprinted like the code in 2.
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
        "sendPacket": "8489f0024dfee1aabb649a06f73d652a33c9e8a54d11b80e0933aaad822fe695",
        "scanPackets": "247c32a3851303392a9eaa5d158f139e1a8fed51754f3f5ebf6cf6bd2fbc942e",
        "packetLimit": "6479b9a5aa7ffcc88148accfa28f53c339bfd8333bc17a3983b94d0e0cc8ef80",
    },
    "FlowTransport.cpp": {  # release-8.0 (the reader differs from 7.4's only in contains() and co_await)
        "prependConnectPacket": "6c31de51b6f91e38ab21c597f9f2e93d191df75561ee4499031bdc170074affc",
        "connectPacketReader": "959c06e485d8b281df1af2f5d3f6ba5671185488e399726258cbbf63a946f99b",
        "sendPacket": "bf0a36bdee7d01325714fbcc3388a5805c9ce1cb3affee2363fd3968102711d6",
        "scanPackets": "8cd9e79949b01335ecde362a716f48cf8f70e12e1b5bad1e141e64a564ffd951",
        "packetLimit": "3d98e1c70c0cd7d2a3d455ac737dd374dadca176ffa60f971034bc62ea6b0319",
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


def mirror_region(text, tag=""):
    lines = text.splitlines()
    start = next(i for i, l in enumerate(lines) if ("BEGIN MIRROR" + tag) in l)
    end = next(i for i, l in enumerate(lines) if ("END MIRROR" + tag) in l)
    return lines[start + 1 : end]


def function_region(text, signature):
    """The lines of the function whose definition starts with `signature` (at column 0), up to its
    closing brace. A forward declaration (a header ending in `);`) is skipped."""
    lines = text.split("\n")
    for start, line in enumerate(lines):
        if not line.startswith(signature):
            continue
        header = next(l for l in lines[start:] if l.rstrip().endswith((");", "{")))
        if header.rstrip().endswith(");"):
            continue
        end = next(i for i in range(start + 1, len(lines)) if lines[i] == "}")
        return lines[start : end + 1]
    raise StopIteration(signature)


def send_packet_regions(text):
    """The two regions of sendPacket() that framing_vectors.cpp mirrors: A, from the PacketWriter's
    construction to finish(); B, the checksum and the writing of the length and the checksum."""
    body = function_region(text, "static ReliablePacket* sendPacket(")
    a_start = next(i for i, l in enumerate(body) if l.strip().startswith("PacketWriter wr(pb,"))
    a_end = next(i for i, l in enumerate(body) if i > a_start and l.strip() == "pb = wr.finish();")
    find = next(i for i, l in enumerate(body) if "Find the correct place to start calculating checksum" in l)
    b_start = find - 1
    b_end = next(i for i, l in enumerate(body) if "packetInfoBuffer.write(&checksum, sizeof(checksum), sizeof(len));" in l) + 1
    return body[a_start : a_end + 1], body[b_start : b_end + 1]


def packet_limit_region(text):
    """getNewBufferSize() and, where they exist (release-8.0), the helpers that compute the limit."""
    lines = function_region(text, "static int getNewBufferSize(")
    for sig in ("static constexpr int packetFramingBytes(", "static int maxPacketPayloadSize("):
        if any(l.startswith(sig) for l in text.split("\n")):
            lines = function_region(text, sig) + lines
    return lines


def prepend_region(text):
    lines = text.split("\n")
    start = next(i for i, l in enumerate(lines) if l.startswith("void Peer::prependConnectPacket()"))
    end = next(i for i, l in enumerate(lines) if i > start and l == "}")
    return lines[start : end + 1]


def reader_region(text):
    lines = text.split("\n")
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
        "sendPacket": fingerprint(function_region(text, "static ReliablePacket* sendPacket(")),
        "scanPackets": fingerprint(function_region(text, "static void scanPackets(")),
        "packetLimit": fingerprint(packet_limit_region(text)),
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

    framing = (HERE / "framing_vectors.cpp").read_text()
    for tag, region in zip((" A", " B"), send_packet_regions(text)):
        want = normalize(region)
        have = normalize(mirror_region(framing, tag))
        if want != have:
            print(f"check_mirrors: messages/framing_vectors.cpp (MIRROR{tag}) differs from sendPacket() in {source}:", file=sys.stderr)
            for line in difflib.unified_diff(want, have, "source", "mirror", lineterm=""):
                print(line, file=sys.stderr)
            return 1

    expected = FINGERPRINTS[source.name]
    for name, got in prints.items():
        if expected[name] != got:
            print(
                f"check_mirrors: {name} in {source} changed (sha256 {got}, expected {expected[name]}).\n"
                "  The Rust ConnectPacket and framing codec (crates/fdb-native-transport) and the binary_vectors.cpp /\n"
                "  framing_vectors.cpp descriptions follow this code: read the change, adjust them if the behavior\n"
                "  changed, then update\n"
                "  FINGERPRINTS in messages/check_mirrors.py (run it with --print).",
                file=sys.stderr,
            )
            return 1
    print(f"check_mirrors: ConnectPacket, the framing mirrors and the send/receive code match {source.relative_to(src)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
