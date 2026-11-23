#!/usr/bin/env python3
"""Generates gv_table_info<T> specializations (src/hooks.h) for real FDB types.

  extract_fields.py SRC_ROOT OUT_HEADER

For every table type in TABLES, finds its definition in SRC_ROOT's flow, fdbrpc and
fdbclient headers, then the serializer(ar, ...) call in its serialize() member function
(or in its serializable_traits<T> specialization), and writes:

  - the dump name (the C++ type name),
  - the field names: one per serializer argument, the last identifier of the argument
    (`LoadBalancedReply::penalty` -> `penalty`, `v.tenantId` -> `tenantId`),
  - an addresses() function returning, per argument, the address of the data member the
    argument names (nullptr for locals and expressions). The dumper checks at runtime that
    every argument it receives is at that address, so a name can never be attached to the
    wrong member, and that the argument count equals the name count.

The message-vector target is compiled with -fno-access-control so private members can be
named. Nothing here is hand-transcribed: field names follow each branch's headers.
"""

import re
import sys
from pathlib import Path

# Table types reachable from the message types in message_vectors.cpp. Options:
#   call=N   the serialize() body has several serializer() calls; the N-th (0-based) gives
#            the field names (the flatbuffers one, or the one for the general case)
#   alt=N    another call with the same slots that the writer takes for some values (a
#            single-key KeyRangeRef or ClearRange is written as (end, "")); its member
#            addresses are accepted too
#   optional the type may not exist on every branch
TABLES = {
    # flow
    "Error": {},
    "Void": {},
    # requests and replies
    "GetValueRequest": {},
    "GetValueReply": {},
    "GetKeyValuesRequest": {},
    "GetKeyValuesReply": {},
    "GetKeyRequest": {},
    "GetKeyReply": {},
    "WatchValueRequest": {},
    "WatchValueReply": {},
    "PingRequest": {},  # FlowTransport's ping (well-known token WLTOKEN_PING_PACKET)
    "GetReadVersionRequest": {},
    "GetReadVersionReply": {},
    "CommitTransactionRequest": {},
    "CommitID": {},
    "GetKeyServerLocationsRequest": {},
    "GetKeyServerLocationsReply": {},
    "OpenDatabaseCoordRequest": {},
    "ClientDBInfo": {},
    # nested tables
    "SpanContext": {},
    "TenantInfo": {"optional": True},
    "WipedString": {"optional": True},
    "ReadOptions": {},
    "KeySelectorRef": {},
    "KeyRangeRef": {"call": 1, "alt": 0},
    "KeyValueRef": {},
    "MutationRef": {"call": 3, "alt": 1},  # 0 and 2 add checksums (knobs, off by default)
    "CommitTransactionRef": {},
    "ClientTrCommitCostEstimation": {},
    "ClientTagThrottleLimits": {},
    "ClientVersionRef": {},
    "Hostname": {},
    "VersionHistory": {},
    "Endpoint": {},
    "NetworkAddressList": {},
    "NetworkAddress": {},
    "IPAddress": {},
    "LocalityData": {},
    "StorageServerInterface": {},
    "GrvProxyInterface": {},
    "CommitProxyInterface": {},
    "EncryptKeyProxyInterface": {"optional": True},  # ClientDBInfo on 7.4 only
    "TenantMode": {"optional": True},  # ClientDBInfo on 7.4 only
    "CDCProxyInterface": {"optional": True},  # ClientDBInfo on 8.0 only
}

INCLUDE_DIRS = ["flow/include/flow", "fdbrpc/include/fdbrpc", "fdbclient/include/fdbclient"]
# PingRequest is defined in FlowTransport's source file, not in a header. message_vectors.cpp
# mirrors it; the generated static_assert and address checks tie the mirror to this source.
EXTRA_SOURCES = ["fdbrpc/FlowTransport.actor.cpp", "fdbrpc/FlowTransport.cpp"]


def strip_comments(text: str) -> str:
    """Replaces comments, string and char literals with spaces (offsets are kept)."""
    out = list(text)
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            for k in range(i, j):
                out[k] = " "
            i = j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        elif c in "\"'":
            j = i + 1
            while j < n and text[j] != c:
                j += 2 if text[j] == "\\" else 1
            for k in range(i + 1, min(j, n)):
                out[k] = " "
            i = j + 1
        else:
            i += 1
    return "".join(out)


def matching(text: str, open_pos: int) -> int:
    """Index of the bracket closing the one at open_pos."""
    pairs = {"{": "}", "(": ")"}
    o = text[open_pos]
    c = pairs[o]
    depth = 0
    for i in range(open_pos, len(text)):
        if text[i] == o:
            depth += 1
        elif text[i] == c:
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced")


def split_args(s: str):
    args, depth, cur = [], 0, ""
    for ch in s:
        if ch in "(<[{":
            depth += 1
        elif ch in ")>]}":
            depth -= 1
        if ch == "," and depth == 0:
            args.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        args.append(cur.strip())
    return args


class Source:
    def __init__(self, root: Path):
        self.root = root
        self.files = {}
        for d in INCLUDE_DIRS:
            for p in sorted((root / d).glob("*.h")):
                self.files[p.relative_to(root).as_posix()] = strip_comments(p.read_text(errors="replace"))
        for rel in EXTRA_SOURCES:
            if (root / rel).exists():
                self.files[rel] = strip_comments((root / rel).read_text(errors="replace"))

    def find_struct(self, name):
        pat = re.compile(r"\b(?:struct|class)\s+" + re.escape(name) + r"\b(?:\s+final)?\s*(?::[^{;]*)?\{")
        hits = []
        for path, text in self.files.items():
            for m in pat.finditer(text):
                hits.append((path, text, m.end() - 1))
        return hits

    def find_traits(self, name):
        pat = re.compile(r"struct\s+serializable_traits\s*<\s*" + re.escape(name) + r"\s*>\s*:\s*std::true_type\s*\{")
        hits = []
        for path, text in self.files.items():
            for m in pat.finditer(text):
                hits.append((path, text, m.end() - 1))
        return hits


SERIALIZE_MEMBER = re.compile(r"\bvoid\s+serialize\s*\(\s*\w+\s*&\s*(\w+)\s*\)\s*\{")
SERIALIZE_TRAITS = re.compile(r"\bvoid\s+serialize\s*\(\s*\w+\s*&\s*(\w+)\s*,\s*[\w:<>\s]+&\s*(\w+)\s*\)\s*\{")


def top_level_matches(body: str, start: int, end: int, regex):
    """Matches of regex at brace depth 0 within body[start:end] (skips nested structs)."""
    depth = 0
    i = start
    hits = []
    while i < end:
        ch = body[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        elif depth == 0:
            m = regex.match(body, i)
            if m:
                hits.append(m)
                i = matching(body, m.end() - 1) + 1 if body[m.end() - 1] == "{" else m.end()
                continue
        i += 1
    return hits


def serializer_calls(text: str, start: int, end: int, ar: str):
    calls = []
    for m in re.finditer(r"\bserializer\s*\(\s*" + re.escape(ar) + r"\s*(,|\))", text[start:end]):
        open_pos = start + m.start() + text[start + m.start():].index("(")
        close = matching(text, open_pos)
        args = split_args(text[open_pos + 1:close])[1:]
        calls.append(args)
    return calls


IDENT = r"[A-Za-z_]\w*"


def classify(arg: str, param: str, body: str):
    """Returns (field_name, member_expression or None)."""
    a = re.sub(r"\s+", "", arg)
    m = re.fullmatch(r"const_cast<[^()]*>\((.*)\)", a)
    if m:
        a = m.group(1)
    name_m = re.findall(IDENT, re.sub(r"\(\)$", "", a))
    name = name_m[-1] if name_m else arg
    if param:
        m = re.fullmatch(re.escape(param) + r"\.(" + IDENT + ")", a)
        return name, (m.group(1) if m else None)
    m = re.fullmatch(r"(?:" + IDENT + r"::)*" + IDENT, a)
    if not m:
        return name, None
    if "::" not in a:
        # A local variable declared in serialize() is not a member.
        decl = (r"(?:^|[;{}])\s*(?:const\s+)?(?:auto|[A-Za-z_][\w:]*(?:<[^;{}]*>)?)\s*[&*]?\s+"
                + re.escape(a) + r"\s*(?:=(?!=)|;|\{|\()")
        if re.search(decl, body, re.M):
            return name, None
    return name, a


def extract(src: Source, name: str, opts):
    candidates = []
    fids = set()
    fid_re = re.compile(r"constexpr\s+static\s+FileIdentifier\s+file_identifier\s*=\s*(\d+)\s*;|"
                        r"static\s+constexpr\s+FileIdentifier\s+file_identifier\s*=\s*(\d+)\s*;")
    for path, text, brace in src.find_struct(name):
        close = matching(text, brace)
        for m in top_level_matches(text, brace + 1, close, SERIALIZE_MEMBER):
            candidates.append((path, text, m, None))
            for f in top_level_matches(text, brace + 1, close, fid_re):
                fids.add(int(f.group(1) or f.group(2)))
    for path, text, brace in src.find_traits(name):
        close = matching(text, brace)
        for m in top_level_matches(text, brace + 1, close, SERIALIZE_TRAITS):
            candidates.append((path, text, m, m.group(2)))
    if not candidates:
        if opts.get("optional"):
            return None
        raise SystemExit(f"{name}: no serialize() found")
    if len(candidates) > 1:
        where = ", ".join(c[0] for c in candidates)
        raise SystemExit(f"{name}: several serialize() definitions ({where})")
    path, text, m, param = candidates[0]
    ar = m.group(1)
    body_end = matching(text, m.end() - 1)
    body = text[m.end():body_end]
    calls = serializer_calls(text, m.end(), body_end, ar)
    if not calls:
        raise SystemExit(f"{name}: no serializer({ar}, ...) call in serialize()")
    fb = re.search(r"\bif\s+constexpr\s*\(\s*is_fb_function\s*<\s*\w+\s*>\s*\)\s*\{", text[m.end():body_end])
    if len(calls) > 1 and "call" not in opts and fb:
        # `if constexpr (is_fb_function<Ar>) { serializer(...); } else { <binary format> }`
        fb_open = m.end() + fb.end() - 1
        fb_calls = serializer_calls(text, fb_open, matching(text, fb_open), ar)
        if len(fb_calls) == 1:
            calls = fb_calls
    if len(calls) > 1 and "call" not in opts:
        raise SystemExit(f"{name}: {len(calls)} serializer() calls; choose one with call=N")
    args = calls[opts.get("call", 0)]
    line = text[: m.start()].count("\n") + 1
    fields = [classify(a, param, body) for a in args]
    alt = None
    if "alt" in opts:
        alt = [classify(a, param, body) for a in calls[opts["alt"]]]
        if len(alt) != len(fields):
            raise SystemExit(f"{name}: alt call has {len(alt)} arguments, not {len(fields)}")
    if len(fids) > 1:
        raise SystemExit(f"{name}: several file identifiers {fids}")
    return {"name": name, "source": f"{path}:{line}", "fields": fields, "alt": alt, "fid": fids.pop() if fids else None}


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    src = Source(Path(argv[1]))
    out = [
        "// Generated by tools/golden-vectors/messages/extract_fields.py from this branch's headers.",
        "// Do not edit. See src/hooks.h (gv_table_info).",
        "#pragma once",
        "",
    ]
    for name, opts in TABLES.items():
        info = extract(src, name, opts)
        if info is None:
            out.append(f"// {name}: not defined on this branch")
            out.append("")
            continue
        names = ",".join(f for f, _ in info["fields"])
        addrs = ", ".join(f"&o.{e}" if e else "nullptr" for _, e in info["fields"])
        alt = ", ".join(f"&o.{e}" if e else "nullptr" for _, e in (info["alt"] or []))
        out += [
            f"// {info['source']}",
            "template <>",
            f"struct gv_table_info<::{name}> : std::true_type {{",
            f'\tstatic constexpr const char* name = "{name}";',
            f'\tstatic constexpr const char* field_names = "{names}";',
            f'\tstatic constexpr const char* source = "{info["source"]}";',
            f"\tstatic std::vector<const void*> addresses(const ::{name}& o) {{ return {{ {addrs} }}; }}",
            f"\tstatic std::vector<const void*> alt_addresses(const ::{name}& o) {{ return {{ {alt} }}; }}",
            "};",
        ]
        if info["fid"] is not None:
            out.append(f"static_assert(FileIdentifierFor<::{name}>::value == {info['fid']}, "
                       f'"{name}: file identifier differs from {info["source"]}");')
        out.append("")
    Path(argv[2]).write_text("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
