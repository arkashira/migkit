"""A vector - an embedding - as the numbers it holds, each exactly.

Every engine here that has a vector type keeps its elements as 32-bit
floats (pgvector's `vector`, MySQL 9's `VECTOR`, ClickHouse's
`Array(Float32)`, Cassandra 5's `vector<float, n>`, DuckDB's `FLOAT[n]`,
Oracle's `VECTOR(n, FLOAT32)`); pgvector's `halfvec` keeps 16-bit ones and
a MongoDB array keeps 64-bit doubles. Each element is carried as the
double that holds it - exact for all three widths - and a vector is the
same vector on two sides when every element is the same float, sign of
a zero included, and there are as many of them.

Text is where that goes wrong: MySQL writes a vector's text at six
significant digits (`VECTOR_TO_STRING`, the research's measurement), so it
is read as its bytes; pgvector writes the shortest decimal that reads
back to the same 32-bit float, which is exact once it is read back as
one - read as a double, `0.1` is a different number from the float
pgvector holds (`0.10000000149011612`).
"""
import math
import struct


def _f32(x):
    """The 32-bit float nearest to `x`, as the double that holds it."""
    return struct.unpack("<f", struct.pack("<f", x))[0]


def _f16(x):
    return struct.unpack("<e", struct.pack("<e", x))[0]


def of(value, width=None):
    """The elements of a vector as doubles, or None for NULL. `width` 32
    or 16 reads text as the floats of that width it was printed from;
    bytes are MySQL's (little-endian 32-bit floats); a MongoDB `Binary`
    of subtype 9 is BSON's vector."""
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = bytes(value)
    if getattr(value, "subtype", None) == 9:
        return _bson_vector(bytes(value))
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        if len(raw) % 4:
            raise ValueError(f"{len(raw)} bytes are no whole number of"
                             " 32-bit floats")
        return list(struct.unpack(f"<{len(raw) // 4}f", raw))
    if isinstance(value, str):
        return _text(value, width or 32)
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        out = []
        for x in value:
            if isinstance(x, bool) or not isinstance(x, (int, float)) \
                    and not hasattr(x, "__float__"):
                raise ValueError(f"{x!r} is no number of a vector")
            out.append(float(x))
        return out
    raise ValueError(f"not a vector: {value!r}")


def _text(text, width):
    """pgvector's text: `[1,2.5]`, or a sparse `{1:2.5,4:1}/6`."""
    s = text.strip()
    cast = _f16 if width == 16 else _f32 if width == 32 else float
    if s.startswith("{"):
        body, _, dims = s.rpartition("/")
        out = [0.0] * int(dims)
        inner = body.strip()[1:-1].strip()
        for part in (p for p in inner.split(",") if p.strip()):
            at, _, v = part.partition(":")
            out[int(at) - 1] = cast(float(v))
        return out
    if not (s.startswith("[") and s.endswith("]")):
        raise ValueError(f"not a vector: {text!r}")
    inner = s[1:-1].strip()
    return [cast(float(x)) for x in inner.split(",")] if inner else []


def _bson_vector(raw):
    """BSON's vector (subtype 9): a type byte, a padding byte, the data -
    0x27 32-bit floats, 0x03 signed bytes, 0x10 packed bits."""
    dtype, padding, data = raw[0], raw[1], raw[2:]
    if dtype == 0x27:
        return list(struct.unpack(f"<{len(data) // 4}f", data))
    if dtype == 0x03:
        return [float(b) for b in struct.unpack(f"<{len(data)}b", data)]
    if dtype == 0x10:
        bits = "".join(f"{b:08b}" for b in data)
        return [float(b) for b in bits[:len(bits) - padding]]
    raise ValueError(f"a BSON vector of type {dtype:#x} is not one migkit"
                     " reads")


def text(value):
    """The canonical text: every element as the shortest text of its
    double, in order - `[0.10000000149011612,-0.0,2.0]`. A 32-bit float
    and the double that holds it are one number, so a vector from a
    float32 engine and the same numbers from MongoDB meet; a sign of zero
    is kept, and two vectors of different lengths never meet."""
    got = of(value)
    return "[" + ",".join(repr(x) for x in got) + "]"


def width(values):
    """The narrowest float - 16, 32 or 64 bits - holding every element
    exactly."""
    if all(math.isnan(x) or _f16(x) == x for x in values):
        return 16
    if all(math.isnan(x) or _f32(x) == x for x in values):
        return 32
    return 64


def finite(values):
    return all(math.isfinite(x) for x in values)


def pg_text(value):
    """pgvector's input: the shortest text of each double, which pgvector
    reads back as the same 32-bit float for any element that is one."""
    return "[" + ",".join(repr(x) for x in of(value)) + "]"


def sparse_text(value):
    """pgvector's `sparsevec` input: the elements that are not zero, by
    their place from 1, and how many there are - `{1:0.5,4:2.0}/6`."""
    got = of(value)
    kept = ",".join(f"{i}:{x!r}" for i, x in enumerate(got, 1) if x != 0)
    return "{" + kept + "}/" + str(len(got))


def packed(value):
    """MySQL's `VECTOR` bytes: little-endian 32-bit floats."""
    got = of(value)
    return struct.pack(f"<{len(got)}f", *got)
