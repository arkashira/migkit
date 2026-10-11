"""A shape - a geometry or a geography - as its SRID and its well-known
binary, with x the longitude and y the latitude.

The engines agree on the binary (OGC/ISO well-known binary) and disagree
on everything around it: PostGIS writes it with the SRID folded into the
type word (EWKB) and in whichever byte order it was given, MySQL keeps a
four-byte SRID in front of it, SQL Server has a serialisation of its own
and gives the binary only when asked, and MongoDB keeps GeoJSON. Worse,
the order of the two numbers of a point: for SRID 4326, EPSG says
latitude first, and MySQL 8 takes it at its word - `ST_GeomFromText(
'POINT(13.75 100.5)', 4326)` is latitude 13.75 - where PostGIS, GeoJSON
and SQL Server's binary put the longitude first. A point carried as its
bytes from one to the other lands on the other side of the world, and
compares equal to itself on the way.

So a shape is parsed into its parts and written again one way: ISO WKB,
little-endian, x the longitude. Two sides holding the same shape write
the same bytes, and `text` is them with the SRID in front.
"""
import json
import math
import struct

#: ISO type codes; +1000 Z, +2000 M, +3000 ZM
KINDS = {1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPoint",
         5: "MultiLineString", 6: "MultiPolygon", 7: "GeometryCollection"}
CODES = {v: k for k, v in KINDS.items()}

#: SRIDs whose axis order, by EPSG, is latitude first, and which MySQL 8
#: stores and reads that way: the geographic ones. Asked of the server
#: where it can be (`mysql_axis`); these are the ones every server has
LAT_FIRST = {4326, 4258, 4269, 4230, 4283, 4617, 4674, 4759, 4148}

#: Oracle's own numbers for coordinate systems EPSG numbers too, from
#: Oracle Spatial's catalogue (`MDSYS.CS_SRS`, its "Longitude / Latitude"
#: systems): one system, two numbers, so a shape Oracle keeps at 8307
#: meets the same shape PostGIS keeps at 4326. Both are longitude first.
#: Written into any other engine at the EPSG number, which is the one
#: those engines know; into Oracle as it was read
EPSG = {8307: 4326, 8265: 4269, 8267: 4267}


class Shape(tuple):
    """(srid, kind, has_z, has_m, body): body is a point's numbers, a list
    of points, a list of rings, or a list of shapes of one less level."""
    __slots__ = ()

    def __new__(cls, srid, kind, has_z, has_m, body):
        return super().__new__(cls, (int(srid or 0), kind, bool(has_z),
                                     bool(has_m), body))

    srid = property(lambda self: self[0])
    kind = property(lambda self: self[1])

    def __str__(self):
        return text(self)


def _read(raw, at=0):
    """(srid or None, kind, z, m, body, next offset) from WKB or EWKB."""
    order = "<" if raw[at] == 1 else ">"
    (code,) = struct.unpack_from(order + "I", raw, at + 1)
    at += 5
    srid = None
    if code & 0x20000000:
        (srid,) = struct.unpack_from(order + "I", raw, at)
        at += 4
    z = bool(code & 0x80000000)
    m = bool(code & 0x40000000)
    code &= 0x0FFFFFFF
    iso, base = divmod(code, 1000)
    if iso in (1, 3):
        z = True
    if iso in (2, 3):
        m = True
    kind = KINDS.get(base)
    if kind is None:
        raise ValueError(f"well-known binary of type {code} is not one"
                         " migkit reads")
    n = 2 + z + m
    point = order + "d" * n

    def pt():
        nonlocal at
        got = list(struct.unpack_from(point, raw, at))
        at += 8 * n
        return got

    def count():
        nonlocal at
        (k,) = struct.unpack_from(order + "I", raw, at)
        at += 4
        return k
    if kind == "Point":
        body = pt()
    elif kind == "LineString":
        body = [pt() for _ in range(count())]
    elif kind == "Polygon":
        body = [[pt() for _ in range(count())] for _ in range(count())]
    else:
        parts = []
        for _ in range(count()):
            _, pk, pz, pm, pbody, at = _read(raw, at)
            parts.append((pk, pz, pm, pbody))
        body = parts
    return srid, kind, z, m, body, at


def _write(kind, z, m, body):
    out = bytearray(struct.pack("<BI", 1, CODES[kind]
                                + (1000 * (1 * z + 2 * m))))
    n = 2 + z + m

    def pt(p):
        # -0.0 is a coordinate every engine keeps, and is left as it is;
        # an empty point is NaN, NaN, as PostGIS writes one
        out.extend(struct.pack("<" + "d" * n, *[float(v) for v in p[:n]]))
    if kind == "Point":
        pt(body)
    elif kind == "LineString":
        out.extend(struct.pack("<I", len(body)))
        for p in body:
            pt(p)
    elif kind == "Polygon":
        out.extend(struct.pack("<I", len(body)))
        for ring in body:
            out.extend(struct.pack("<I", len(ring)))
            for p in ring:
                pt(p)
    else:
        out.extend(struct.pack("<I", len(body)))
        for pk, pz, pm, pbody in body:
            out.extend(_write(pk, pz, pm, pbody))
    return bytes(out)


def _swap(kind, body):
    """The same shape with each point's first two numbers exchanged."""
    if kind == "Point":
        return [body[1], body[0]] + body[2:] if len(body) >= 2 else body
    if kind == "LineString":
        return [_swap("Point", p) for p in body]
    if kind == "Polygon":
        return [[_swap("Point", p) for p in ring] for ring in body]
    return [(pk, pz, pm, _swap(pk, pb)) for pk, pz, pm, pb in body]


def from_wkb(raw, srid=None, lat_first=False):
    """A shape from WKB or EWKB bytes (or their hex): an SRID in the EWKB
    wins over `srid`; `lat_first` where the bytes put the latitude
    first."""
    if isinstance(raw, str):
        raw = bytes.fromhex(raw.strip())
    raw = bytes(raw)
    got, kind, z, m, body, _ = _read(raw)
    if lat_first:
        body = _swap(kind, body)
    return Shape(got if got is not None else srid, kind, z, m, body)


def from_mysql(raw):
    """MySQL's own storage: a four-byte little-endian SRID and the WKB.
    For a geographic SRID MySQL stores the latitude first (`LAT_FIRST`,
    measured on 8.4 and 9.4)."""
    raw = bytes(raw)
    (srid,) = struct.unpack_from("<I", raw, 0)
    return from_wkb(raw[4:], srid, srid in LAT_FIRST)


def to_mysql(shape):
    """The bytes MySQL stores for a shape - which it takes as they are
    into a column of its SRID."""
    s = of(shape)
    body = _swap(s.kind, s[4]) if s.srid in LAT_FIRST else s[4]
    return struct.pack("<I", s.srid) + _write(s.kind, s[2], s[3], body)


def from_geojson(doc, srid=4326):
    """A shape from GeoJSON, which is longitude first and WGS 84
    (RFC 7946); MongoDB keeps it as a document."""
    if isinstance(doc, str):
        doc = json.loads(doc)
    kind = doc.get("type")
    if kind == "GeometryCollection":
        parts = [from_geojson(g, srid) for g in doc.get("geometries") or []]
        z = any(p[2] for p in parts)
        return Shape(srid, kind, z, False,
                     [(p[1], p[2], p[3], p[4]) for p in parts])
    if kind not in CODES:
        raise ValueError(f"not a GeoJSON geometry: {doc!r}")
    coords = doc.get("coordinates")

    def depth(c):
        d = 0
        while isinstance(c, list) and c and isinstance(c[0], list):
            c, d = c[0], d + 1
        return c
    first = depth(coords) if coords else []
    z = isinstance(first, list) and len(first) >= 3

    def pts(c):
        return [[float(v) for v in p] for p in c]
    if kind == "Point":
        body = [float(v) for v in coords] if coords else \
            [float("nan")] * (2 + z)
    elif kind == "LineString":
        body = pts(coords)
    elif kind == "Polygon":
        body = [pts(r) for r in coords]
    elif kind == "MultiPoint":
        body = [("Point", z, False, [float(v) for v in p]) for p in coords]
    elif kind == "MultiLineString":
        body = [("LineString", z, False, pts(ls)) for ls in coords]
    else:
        body = [("Polygon", z, False, [pts(r) for r in poly])
                for poly in coords]
    return Shape(srid, kind, z, False, body)


def to_geojson(shape):
    """GeoJSON of a shape, members in the order RFC 7946 lists them. A
    shape with M, or of an SRID other than 4326, has none: raises."""
    s = of(shape)
    if s[3]:
        raise ValueError("GeoJSON has no measure (M) coordinate")
    if s.srid not in (4326, 0):
        raise ValueError(f"GeoJSON is WGS 84 (SRID 4326), not SRID"
                         f" {s.srid}")

    def one(kind, z, body):
        if kind == "GeometryCollection":
            return {"type": kind, "geometries": [one(pk, pz, pb)
                                                 for pk, pz, pm, pb in body]}
        if kind == "Point":
            coords = body
        elif kind in ("LineString",):
            coords = body
        elif kind == "Polygon":
            coords = body
        else:
            coords = [pb for _, _, _, pb in body]
        return {"type": kind, "coordinates": coords}
    return one(s.kind, s[2], s[4])


def of(value, lat_first=False):
    """A `Shape` from what a reader hands over, or None for NULL: one
    already; PostGIS's EWKB (bytes, or the hex text it prints); `srid:hex`
    as SQL Server's read gives it; a GeoJSON document."""
    if value is None:
        return None
    if isinstance(value, Shape):
        return value
    if isinstance(value, dict):
        return from_geojson(value)
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("{"):
            return from_geojson(s)
        if s.startswith("mysql:"):
            return from_mysql(bytes.fromhex(s[len("mysql:"):]))
        if s.upper().startswith("SRID="):
            head, _, hexed = s.partition(";")
            return from_wkb(hexed, int(head[5:]), lat_first)
        if ":" in s:
            srid, _, hexed = s.partition(":")
            return from_wkb(hexed, int(srid), lat_first)
        return from_wkb(s, None, lat_first)
    if isinstance(value, (bytes, bytearray)):
        return from_wkb(value, None, lat_first)
    raise ValueError(f"not a shape: {value!r}")


def wkb(value):
    """ISO WKB, little-endian, longitude first."""
    s = of(value)
    return _write(s.kind, s[2], s[3], s[4])


def ewkb_hex(value):
    """PostGIS's input: EWKB with the SRID in it, as hex."""
    s = of(value)
    raw = bytearray(wkb(s))
    if s.srid:
        (code,) = struct.unpack_from("<I", raw, 1)
        base = code % 1000
        z, m = s[2], s[3]
        code = base | 0x20000000 | (0x80000000 if z else 0) \
            | (0x40000000 if m else 0)
        raw = bytearray(struct.pack("<BI", 1, code)) \
            + struct.pack("<I", s.srid) + raw[5:]
    return bytes(raw).hex().upper()


def as_epsg(value):
    """The shape at the EPSG number of its coordinate system, where its
    SRID is Oracle's own number for one (`EPSG`)."""
    s = of(value)
    srid = EPSG.get(s.srid)
    return s if srid is None else Shape(srid, s[1], s[2], s[3], s[4])


def text(value):
    """The canonical text: `SRID=4326;` and the ISO WKB in capitals - the
    SRID as EPSG numbers it (`EPSG`)."""
    s = as_epsg(value)
    return f"SRID={s.srid};{wkb(s).hex().upper()}"


def wkt(value):
    """Well-known text of a shape as SQL Server reads it, which takes Z
    and M from text and not from binary: each number the shortest text
    that reads back as the same double, a point's Z and M after its X and
    Y - `NULL` for the Z of one with an M and no Z - and a point with no
    coordinates `EMPTY`."""
    s = of(value)

    def pt(p, z, m):
        parts = [repr(float(p[0])), repr(float(p[1]))]
        if z or m:
            parts.append(repr(float(p[2])) if z else "NULL")
        if m:
            parts.append(repr(float(p[2 + z])))
        return " ".join(parts)

    def ring(points, z, m):
        return "(" + ", ".join(pt(p, z, m) for p in points) + ")"

    def one(kind, z, m, body):
        name = kind.upper()
        if kind == "Point":
            if not body or all(math.isnan(float(v)) for v in body[:2]):
                return "POINT EMPTY"
            return f"POINT ({pt(body, z, m)})"
        if not body:
            return f"{name} EMPTY"
        if kind == "LineString":
            return f"{name} {ring(body, z, m)}"
        if kind == "Polygon":
            return f"{name} (" + ", ".join(ring(r, z, m) for r in body) + ")"
        if kind == "MultiPoint":
            inner = [f"({pt(pb, pz, pm)})" for _, pz, pm, pb in body]
        elif kind == "MultiLineString":
            inner = [ring(pb, pz, pm) for _, pz, pm, pb in body]
        elif kind == "MultiPolygon":
            inner = ["(" + ", ".join(ring(r, pz, pm) for r in pb) + ")"
                     for _, pz, pm, pb in body]
        else:
            inner = [one(pk, pz, pm, pb) for pk, pz, pm, pb in body]
        return f"{name} (" + ", ".join(inner) + ")"
    return one(s.kind, s[2], s[3], s[4])


def _turn(points):
    """Twice the area a ring encloses, positive where it runs
    counter-clockwise, with x the longitude: each step across the
    antimeridian taken the short way, so a ring around the date line is
    the small ring it is."""
    xs, ys, shift = [], [], 0.0
    for p in points:
        x, y = float(p[0]), float(p[1])
        if xs:
            step = x + shift - xs[-1]
            if step > 180:
                shift -= 360
            elif step < -180:
                shift += 360
        xs.append(x + shift)
        ys.append(y)
    return sum(xs[i] * ys[(i + 1) % len(xs)] - xs[(i + 1) % len(xs)] * ys[i]
               for i in range(len(xs)))


def _polygons(kind, body):
    if kind == "Polygon":
        yield body
    elif kind in ("MultiPolygon", "GeometryCollection"):
        for pk, _, _, pb in body:
            yield from _polygons(pk, pb)


def misoriented(value):
    """Whether a shape holds a polygon whose outer ring runs clockwise, or
    a hole that runs counter-clockwise (`_turn`). PostGIS and MySQL take
    a polygon's rings either way round; SQL Server's `geography` takes
    the inside of a ring to be on its left, so an outer ring drawn
    clockwise is the rest of the globe there, and Oracle holds a polygon
    so drawn as not valid."""
    s = of(value)
    for rings in _polygons(s.kind, s[4]):
        for i, r in enumerate(rings):
            area = _turn(r)
            if (i == 0 and area < 0) or (i > 0 and area > 0):
                return True
    return False
