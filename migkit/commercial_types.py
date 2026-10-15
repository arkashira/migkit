import re
import struct
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from . import geo


@dataclass(frozen=True)
class SDOGeometry:
    gtype: int
    srid: Optional[int] = None
    point: Optional[Tuple[Optional[float], Optional[float], Optional[float]]] = None
    elem_info: Optional[Tuple[int, ...]] = None
    ordinates: Optional[Tuple[float, ...]] = None

    def as_tuple(self) -> Tuple[Any, ...]:
        return (self.gtype, self.srid, self.point, self.elem_info, self.ordinates)


def parse_sdo_geometry(val: Any) -> Optional[SDOGeometry]:
    if val is None:
        return None
    if isinstance(val, SDOGeometry):
        return val
    if isinstance(val, (tuple, list)):
        gtype = int(val[0])
        srid = int(val[1]) if len(val) > 1 and val[1] is not None else None
        pt = None
        if len(val) > 2 and val[2] is not None:
            raw_pt = val[2]
            if isinstance(raw_pt, (tuple, list)):
                pt = (
                    float(raw_pt[0]) if len(raw_pt) > 0 and raw_pt[0] is not None else None,
                    float(raw_pt[1]) if len(raw_pt) > 1 and raw_pt[1] is not None else None,
                    float(raw_pt[2]) if len(raw_pt) > 2 and raw_pt[2] is not None else None,
                )
            elif hasattr(raw_pt, "x") and hasattr(raw_pt, "y"):
                pt = (float(raw_pt.x), float(raw_pt.y), float(getattr(raw_pt, "z", 0.0) or 0.0))
        elem = tuple(int(x) for x in val[3]) if len(val) > 3 and val[3] is not None else None
        ords = tuple(float(x) for x in val[4]) if len(val) > 4 and val[4] is not None else None
        return SDOGeometry(gtype, srid, pt, elem, ords)
    if hasattr(val, "sdo_gtype"):
        gtype = int(val.sdo_gtype)
        srid = int(val.sdo_srid) if getattr(val, "sdo_srid", None) is not None else None
        pt = None
        raw_pt = getattr(val, "sdo_point", None)
        if raw_pt is not None:
            pt = (
                float(raw_pt.x) if getattr(raw_pt, "x", None) is not None else None,
                float(raw_pt.y) if getattr(raw_pt, "y", None) is not None else None,
                float(raw_pt.z) if getattr(raw_pt, "z", None) is not None else None,
            )
        elem = tuple(int(x) for x in val.sdo_elem_info) if getattr(val, "sdo_elem_info", None) is not None else None
        ords = tuple(float(x) for x in val.sdo_ordinates) if getattr(val, "sdo_ordinates", None) is not None else None
        return SDOGeometry(gtype, srid, pt, elem, ords)
    raise ValueError(f"Cannot parse SDO_GEOMETRY from {val!r}")


def sdo_to_shape(val: Any) -> geo.Shape:
    sdo = parse_sdo_geometry(val)
    if sdo is None:
        raise ValueError("SDO_GEOMETRY value is None")
    raw_srid = sdo.srid or 0
    srid = geo.EPSG.get(raw_srid, raw_srid)
    tt = sdo.gtype % 100
    d = sdo.gtype // 1000
    has_z = d >= 3
    has_m = (sdo.gtype % 1000) // 100 == 1

    if tt == 1:
        if sdo.point is not None and sdo.point[0] is not None and sdo.point[1] is not None:
            pt = [sdo.point[0], sdo.point[1]]
            if has_z:
                pt.append(sdo.point[2] if len(sdo.point) > 2 and sdo.point[2] is not None else 0.0)
            return geo.Shape(srid, "Point", has_z, has_m, pt)
        if sdo.ordinates:
            step = 2 + int(has_z) + int(has_m)
            pt = list(sdo.ordinates[:step])
            return geo.Shape(srid, "Point", has_z, has_m, pt)
        raise ValueError(f"Point SDO_GEOMETRY missing coordinates: {sdo}")

    if tt == 2:
        step = 2 + int(has_z) + int(has_m)
        coords = [list(sdo.ordinates[i:i + step]) for i in range(0, len(sdo.ordinates or ()), step)]
        return geo.Shape(srid, "LineString", has_z, has_m, coords)

    if tt == 3:
        step = 2 + int(has_z) + int(has_m)
        coords = [list(sdo.ordinates[i:i + step]) for i in range(0, len(sdo.ordinates or ()), step)]
        return geo.Shape(srid, "Polygon", has_z, has_m, [coords])

    if tt == 4:
        step = 2 + int(has_z) + int(has_m)
        coords = [list(sdo.ordinates[i:i + step]) for i in range(0, len(sdo.ordinates or ()), step)]
        return geo.Shape(srid, "MultiPoint", has_z, has_m, coords)

    raise ValueError(f"Unsupported SDO_GTYPE {sdo.gtype}")


def shape_to_sdo(shape: geo.Shape) -> SDOGeometry:
    inv_epsg = {v: k for k, v in geo.EPSG.items()}
    srid = inv_epsg.get(shape.srid, shape.srid)
    has_z = bool(shape[2])
    has_m = bool(shape[3])
    d = 3 if has_z else 2
    l0 = 1 if has_m else 0
    kind = shape.kind

    if kind == "Point":
        gtype = d * 1000 + l0 * 100 + 1
        coords = shape[4]
        pt = (coords[0], coords[1], coords[2] if len(coords) > 2 else None)
        return SDOGeometry(gtype, srid, pt, None, None)

    if kind == "LineString":
        gtype = d * 1000 + l0 * 100 + 2
        ords = []
        for p in shape[4]:
            ords.extend(p)
        elem = (1, 2, 1)
        return SDOGeometry(gtype, srid, None, elem, tuple(ords))

    if kind == "Polygon":
        gtype = d * 1000 + l0 * 100 + 3
        ords = []
        for ring in shape[4]:
            for p in ring:
                ords.extend(p)
        elem = (1, 1003, 1)
        return SDOGeometry(gtype, srid, None, elem, tuple(ords))

    raise ValueError(f"Cannot convert shape kind {kind} to SDO_GEOMETRY")


def sdo_to_canonical_text(val: Any) -> str:
    shape = sdo_to_shape(val)
    return geo.text(shape)


def is_xml_fragment(xml_val: Any) -> bool:
    if xml_val is None:
        return False
    if isinstance(xml_val, (bytes, bytearray, memoryview)):
        text = bytes(xml_val).decode("utf-8")
    else:
        text = str(xml_val).strip()
    try:
        ET.fromstring(text)
        return False
    except ET.ParseError:
        return True


def canonical_xml(xml_val: Any) -> str:
    if xml_val is None:
        return ""
    if isinstance(xml_val, (bytes, bytearray, memoryview)):
        text = bytes(xml_val).decode("utf-8")
    else:
        text = str(xml_val).strip()
    try:
        root = ET.fromstring(text)
    except ET.ParseError as err:
        raise ValueError(f"Invalid XML document: {err}")

    def _canonical_node(elem: ET.Element) -> str:
        tag = elem.tag
        attribs = elem.attrib
        attr_str = ""
        if attribs:
            sorted_items = sorted(attribs.items())
            attr_str = " " + " ".join(f'{k}="{v}"' for k, v in sorted_items)
        inner = (elem.text or "").strip()
        for child in elem:
            inner += _canonical_node(child)
            if child.tail:
                inner += child.tail.strip()
        if inner:
            return f"<{tag}{attr_str}>{inner}</{tag}>"
        return f"<{tag}{attr_str}/>"

    return _canonical_node(root)


class HierarchyId:
    __slots__ = ("_path", "_nodes")

    def __init__(self, path: Union[str, Sequence[Tuple[int, ...]]]):
        if isinstance(path, str):
            clean = path.strip()
            if not clean.startswith("/") or not clean.endswith("/"):
                raise ValueError(f"HierarchyId path must start and end with '/': {path}")
            self._path = clean
            parts = [p for p in clean.split("/") if p]
            nodes = []
            for p in parts:
                subparts = tuple(int(x) for x in p.split("."))
                nodes.append(subparts)
            self._nodes = tuple(nodes)
        else:
            self._nodes = tuple(path)
            parts = [".".join(str(x) for x in node) for node in self._nodes]
            self._path = "/" + "/".join(parts) + ("/" if parts else "")

    @property
    def path(self) -> str:
        return self._path

    def get_level(self) -> int:
        return len(self._nodes)

    def get_ancestor(self, n: int) -> "HierarchyId":
        if n < 0:
            raise ValueError(f"Ancestor level must be non-negative: {n}")
        if n > len(self._nodes):
            raise ValueError(f"Cannot get ancestor {n} above root for path {self._path}")
        remain = self._nodes[:len(self._nodes) - n]
        return HierarchyId(remain)

    def is_descendant_of(self, parent: Union["HierarchyId", str]) -> bool:
        if isinstance(parent, str):
            parent = HierarchyId(parent)
        if len(parent._nodes) > len(self._nodes):
            return False
        return self._nodes[:len(parent._nodes)] == parent._nodes

    def get_reparented_value(self, old_root: Union["HierarchyId", str], new_root: Union["HierarchyId", str]) -> "HierarchyId":
        if isinstance(old_root, str):
            old_root = HierarchyId(old_root)
        if isinstance(new_root, str):
            new_root = HierarchyId(new_root)
        if not self.is_descendant_of(old_root):
            raise ValueError(f"{self._path} is not a descendant of old_root {old_root.path}")
        suffix = self._nodes[len(old_root._nodes):]
        return HierarchyId(new_root._nodes + suffix)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, HierarchyId):
            return NotImplemented
        return self._nodes < other._nodes

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            try:
                other = HierarchyId(other)
            except Exception:
                return False
        if not isinstance(other, HierarchyId):
            return False
        return self._nodes == other._nodes

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"HierarchyId({self._path!r})"


def parse_hierarchyid(val: Any) -> HierarchyId:
    if isinstance(val, HierarchyId):
        return val
    if isinstance(val, str):
        return HierarchyId(val)
    if isinstance(val, (bytes, bytearray, memoryview)):
        raw = bytes(val)
        if raw.startswith(b"/") and raw.endswith(b"/"):
            return HierarchyId(raw.decode("ascii"))
        try:
            return HierarchyId(raw.decode("utf-8"))
        except Exception:
            pass
    raise ValueError(f"Cannot parse HierarchyId from {val!r}")


def geography_to_shape(val: Any, srid: int = 4326) -> geo.Shape:
    if isinstance(val, geo.Shape):
        return val
    if isinstance(val, (bytes, bytearray, memoryview)):
        raw = bytes(val)
        return geo.shape(raw)
    if isinstance(val, str):
        text = val.strip()
        if text.startswith("0x") or all(c in "0123456789abcdefABCDEF" for c in text):
            hex_str = text[2:] if text.startswith("0x") else text
            return geo.shape(bytes.fromhex(hex_str))
        if text.upper().startswith("SRID="):
            parts = text.split(";", 1)
            srid_part = parts[0].split("=")[1]
            srid = int(srid_part)
            text = parts[1]
        m = re.match(r"^POINT\s*\(\s*([^\s]+)\s+([^\s\)]+)\s*\)$", text, re.I)
        if m:
            v1, v2 = float(m.group(1)), float(m.group(2))
            if srid in geo.LAT_FIRST:
                lon, lat = v2, v1
            else:
                lon, lat = v1, v2
            return geo.Shape(srid, "Point", False, False, [lon, lat])
    raise ValueError(f"Cannot parse SQL Server geography from {val!r}")


def geography_to_canonical_text(val: Any, srid: int = 4326) -> str:
    shp = geography_to_shape(val, srid=srid)
    return geo.text(shp)


def canonical_convert(engine: str, type_name: str, value: Any) -> Any:
    if value is None:
        return None
    eng = engine.lower()
    t = type_name.strip().lower()

    if eng == "oracle":
        if "sdo_geometry" in t:
            return sdo_to_canonical_text(value)
        if "xmltype" in t:
            return canonical_xml(value)

    if eng in ("mssql", "sqlserver"):
        if "geography" in t or "geometry" in t:
            return geography_to_canonical_text(value)
        if "hierarchyid" in t:
            return parse_hierarchyid(value).path
        if "xml" in t:
            return canonical_xml(value)

    return value


def to_canonical(type_name: str, value: Any, engine: Optional[str] = None) -> Any:
    t = type_name.strip().lower()
    if "sdo_geometry" in t:
        return sdo_to_canonical_text(value)
    if "hierarchyid" in t:
        return parse_hierarchyid(value).path
    if "geography" in t or "geometry" in t:
        return geography_to_canonical_text(value)
    if "xml" in t or "xmltype" in t:
        return canonical_xml(value)
    return value
