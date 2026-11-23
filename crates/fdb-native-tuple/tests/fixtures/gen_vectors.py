#!/usr/bin/env python3
"""Generate tests/fixtures/python_vectors.txt from the FoundationDB Python binding.

Dev-only tool; it is not run by the test suite. The checked-in output is
consumed by tests/differential.rs. Regenerate with:

    python3 -I gen_vectors.py [PATH_TO_FOUNDATIONDB_CHECKOUT] > python_vectors.txt

PATH defaults to ../foundationdb relative to the repository root (that is, a
checkout of apple/foundationdb next to this repository). Only
bindings/python/fdb/tuple.py is read; it is copied, unmodified, next to a stub
`fdb` package (tuple.py only needs `fdb.is_api_version_selected()` and
`fdb.get_api_version()`), so the C client library is not needed. tuple.py is
Apache-2.0, from apple/foundationdb.

Output format, one case per line, tab separated:

    KIND  PREFIX_HEX  PACKED_HEX  TOKENS

KIND is `P` (plain `pack`) or `V` (`pack_with_versionstamp` with the API >= 520
4-byte offset suffix included in PACKED_HEX). PREFIX_HEX is a key prefix that is
prepended to the packed bytes (always empty for `P`). TOKENS is the
whitespace-separated tuple description:

    N            null            B:<hex>    byte string      S:<hex>   utf-8 string (hex)
    I:<decimal>  integer         F:<8 hex>  f32 bit pattern  D:<16 hex> f64 bit pattern
    Z:0 / Z:1    boolean         U:<32 hex> uuid             V:<24 hex> versionstamp
    (  ...  )    nested tuple    (the 12 versionstamp bytes; ten 0xff means incomplete)

Values on which the Python binding is known to differ from the specification
(+/-(2**64 - 1), which Python packs with the long 0x1d/0x0b form) are left out.
"""

import math
import os
import random
import struct
import sys
import tempfile
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SRC = os.path.normpath(os.path.join(HERE, "../../../../../foundationdb"))
SRC = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SRC

STUB_INIT = """\
class _Impl:
    class Value:
        pass
impl = _Impl()
_version = 730
def is_api_version_selected():
    return True
def get_api_version():
    return 730
"""


def load_tuple_module():
    tmp = tempfile.mkdtemp()
    pkg = os.path.join(tmp, "fdb")
    os.mkdir(pkg)
    with open(os.path.join(pkg, "__init__.py"), "w") as f:
        f.write(STUB_INIT)
    with open(os.path.join(SRC, "bindings/python/fdb/tuple.py"), "rb") as f:
        data = f.read()
    with open(os.path.join(pkg, "tuple.py"), "wb") as f:
        f.write(data)
    sys.path.insert(0, tmp)
    from fdb import tuple as T  # noqa: E402

    return T


T = load_tuple_module()
rng = random.Random(20240607)
BAD_INTS = (2**64 - 1, -(2**64 - 1))


def f32_bits(bits):
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def f64_bits(bits):
    return struct.unpack(">d", struct.pack(">Q", bits))[0]


SPECIAL_F32 = [0x00000000, 0x80000000, 0x7F800000, 0xFF800000, 0x7FC00000, 0xFFC00000,
               0x00000001, 0x80000001, 0x007FFFFF, 0x00800000, 0x7F7FFFFF, 0xFF7FFFFF,
               0x3F800000, 0xBF800000, 0x7FC00001, 0xFFC12345]
SPECIAL_F64 = [0x0000000000000000, 0x8000000000000000, 0x7FF0000000000000, 0xFFF0000000000000,
               0x7FF8000000000000, 0xFFF8000000000000, 0x0000000000000001, 0x8000000000000001,
               0x000FFFFFFFFFFFFF, 0x0010000000000000, 0x7FEFFFFFFFFFFFFF, 0xFFEFFFFFFFFFFFFF,
               0x3FF0000000000000, 0xBFF0000000000000, 0x7FF8000000001234, 0xFFF8000000005678]


def boundary_ints():
    out = set()
    for k in range(0, 72):
        for d in (-2, -1, 0, 1, 2):
            out.add(2**k + d)
            out.add(-(2**k) + d)
    for nbytes in (8, 9, 10, 100, 254, 255):
        for d in (-2, -1, 0, 1):
            out.add(2 ** (8 * nbytes) + d)
            out.add(-(2 ** (8 * nbytes)) + d)
    out.add(2 ** (8 * 255) - 1)
    out.add(-(2 ** (8 * 255) - 1))
    return sorted(v for v in out if v not in BAD_INTS and abs(v) < 2 ** (8 * 255))


def rint():
    nb = rng.randint(0, 64)
    mx = (1 << nb) - 1
    v = rng.randint(-mx - 1, mx)
    return v if v not in BAD_INTS else v - 1


def rbigint():
    return rng.choice([-1, 1]) * rng.getrandbits(rng.randint(60, 600))


def rfloat(single):
    if single:
        return T.SingleFloat(f32_bits(rng.choice(SPECIAL_F32)) if rng.random() < 0.2
                             else struct.unpack(">f", struct.pack(">f", rng.uniform(-1e6, 1e6)))[0])
    if rng.random() < 0.2:
        return f64_bits(rng.choice(SPECIAL_F64))
    return rng.choice([-1, 1]) * math.ldexp(rng.random(), rng.randint(-1074, 1023))


def rchar():
    while True:
        c = rng.randint(0, 0x10FFFF)
        if not 0xD800 <= c <= 0xDFFF:
            return chr(c)


def rstring():
    return "".join(rng.choice([rchar(), "\x00", "a", "ÿ"]) for _ in range(rng.randint(0, 8)))


def rbytes():
    return bytes(rng.choice([0, 255, rng.randint(0, 255)]) for _ in range(rng.randint(0, 16)))


def rvs(incomplete=False):
    if incomplete:
        return T.Versionstamp.incomplete(rng.randint(0, 0xFFFF))
    return T.Versionstamp(bytes(rng.getrandbits(8) for _ in range(10)), rng.randint(0, 0xFFFF))


def relem(depth, allow_vs=True):
    kinds = ["int", "int", "null", "bytes", "string", "uuid", "bool", "float", "double", "bigint"]
    if allow_vs:
        kinds.append("vs")
    if depth > 0:
        kinds += ["tuple", "tuple"]
    k = rng.choice(kinds)
    if k == "int":
        return rint()
    if k == "bigint":
        return rbigint()
    if k == "null":
        return None
    if k == "bytes":
        return rbytes()
    if k == "string":
        return rstring()
    if k == "uuid":
        return uuid.UUID(int=rng.getrandbits(128))
    if k == "bool":
        return rng.random() < 0.5
    if k == "float":
        return rfloat(True)
    if k == "double":
        return rfloat(False)
    if k == "vs":
        return rvs()
    return tuple(relem(depth - 1, allow_vs) for _ in range(rng.randint(0, 4)))


def rtuple(depth=3, n=None):
    return tuple(relem(depth) for _ in range(rng.randint(0, 6) if n is None else n))


def tokens(v):
    if v is None:
        return "N"
    if isinstance(v, bool):
        return "Z:%d" % int(v)
    if isinstance(v, int):
        return "I:%d" % v
    if isinstance(v, bytes):
        return "B:" + v.hex()
    if isinstance(v, str):
        return "S:" + v.encode("utf-8").hex()
    if isinstance(v, T.SingleFloat):
        return "F:" + struct.pack(">f", v.value).hex()
    if isinstance(v, float):
        return "D:" + struct.pack(">d", v).hex()
    if isinstance(v, uuid.UUID):
        return "U:" + v.bytes.hex()
    if isinstance(v, T.Versionstamp):
        tr = v.tr_version if v.tr_version is not None else b"\xff" * 10
        return "V:" + (tr + struct.pack(">H", v.user_version)).hex()
    if isinstance(v, tuple):
        return "( " + " ".join(tokens(x) for x in v) + " )" if v else "( )"
    raise TypeError(type(v))


def emit(kind, prefix, packed, t):
    print("%s\t%s\t%s\t%s" % (kind, prefix.hex(), packed.hex(), " ".join(tokens(x) for x in t)))


def main():
    cases = [()]
    cases += [(v,) for v in boundary_ints()]
    cases += [(T.SingleFloat(f32_bits(b)),) for b in SPECIAL_F32]
    cases += [(f64_bits(b),) for b in SPECIAL_F64]
    cases += [
        (None,), (b"",), ("",), ((),), ((None,),), ((), ()), (((),),), ((None, (), None),),
        (b"\x00",), (b"\x00\xff\x00",), ("\x00",), ("a\x00b", b"c\x00d"), (True, False),
        ((b"foo\x00bar", None, ()),), (uuid.UUID(int=0),), (uuid.UUID(int=(1 << 128) - 1),),
        (T.Versionstamp(b"\x00" * 10, 0),), (T.Versionstamp(b"\xff" * 9 + b"\xfe", 0xFFFF),),
    ]
    cases += [rtuple(n=1) for _ in range(150)]
    cases += [rtuple() for _ in range(450)]
    for t in cases:
        emit("P", b"", T.pack(t), t)

    # pack_with_versionstamp: exactly one incomplete versionstamp, anywhere.
    for _ in range(250):
        elems = list(rtuple(depth=2))
        stamp = rvs(incomplete=True)
        nested = [i for i, e in enumerate(elems) if isinstance(e, tuple)]
        if nested and rng.random() < 0.5:
            i = rng.choice(nested)
            inner = list(elems[i])
            inner.insert(rng.randint(0, len(inner)), stamp)
            elems[i] = tuple(inner)
        else:
            elems.insert(rng.randint(0, len(elems)), stamp)
        t = tuple(elems)
        prefix = bytes(rng.getrandbits(8) for _ in range(rng.choice([0, 0, 1, 3, 20])))
        emit("V", prefix, T.pack_with_versionstamp(t, prefix=prefix), t)

main()
