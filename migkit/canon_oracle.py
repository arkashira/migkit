import decimal
import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

MODULUS_256 = 1 << 256


class ScalePolicy:
    EXACT = "exact"
    NUMERIC = "numeric"
    DECLARED = "declared"
    PRESERVE = "preserve"


class SpacePolicy:
    EXACT = "exact"
    PAD_SPACE = "pad_space"
    TYPE_AWARE = "type_aware"


class CollationPolicy:
    BINARY = "binary"
    NFC = "nfc"
    CASE_INSENSITIVE = "case_insensitive"
    TYPE_AWARE = "type_aware"


class SignedZeroPolicy:
    NUMERIC_EQUAL = "numeric_equal"
    STRICT = "strict"


class DriftKind:
    NONE = "none"
    REPRESENTATIONAL = "representational"
    SCALE_DRIFT = "scale_drift"
    SPACE_DRIFT = "space_drift"
    COLLATION_DRIFT = "collation_drift"
    SIGN_DRIFT = "sign_drift"
    REAL_DRIFT = "real_drift"


@dataclass(frozen=True)
class DriftResult:
    equal: bool
    drift_kind: str
    semantic_equal: bool
    reason: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TypeSpec:
    raw: str
    base: str
    cls: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    is_char: bool = False
    is_binary: bool = False


_TYPE_RE = re.compile(
    r"^(?P<base>[a-zA-Z0-9_ ]+?)(?:\s*\(\s*(?P<p1>\d+)(?:\s*,\s*(?P<p2>\d+))?\s*\))?(?:\s+(?P<extra>.*))?$"
)


def parse_type(engine: str = "", declared: str = "") -> TypeSpec:
    raw = (declared or "").strip()
    if not raw:
        return TypeSpec(raw="", base="text", cls="text")
    m = _TYPE_RE.match(raw)
    base = m.group("base").strip().lower() if m else raw.lower()
    p1 = int(m.group("p1")) if m and m.group("p1") else None
    p2 = int(m.group("p2")) if m and m.group("p2") else None
    extra = m.group("extra").strip().lower() if m and m.group("extra") else ""

    cls = "text"
    precision = None
    scale = None
    length = None
    is_char = False
    is_binary = "binary" in base or "binary" in extra or "bytea" in base or "blob" in base

    if base in ("decimal", "numeric", "number", "dec"):
        cls = "decimal"
        precision = p1
        scale = p2
    elif base in ("float", "double", "real", "float4", "float8", "binary_float", "binary_double"):
        cls = "float"
        precision = p1
    elif base in ("int", "bigint", "smallint", "tinyint", "integer", "int2", "int4", "int8", "int64"):
        cls = "integer"
    elif base in ("char", "character", "bpchar", "nchar"):
        cls = "text"
        is_char = True
        length = p1
    elif base in ("varchar", "character varying", "nvarchar", "text", "string", "clob"):
        cls = "text"
        length = p1
    elif base in ("bool", "boolean"):
        cls = "boolean"
    elif base in ("json", "jsonb"):
        cls = "json"
    elif base in ("date",):
        cls = "date"
    elif base in ("timestamp", "datetime", "timestamptz"):
        cls = "timestamp"
    elif base in ("time", "timetz"):
        cls = "time"
    elif base in ("bytes", "bytea", "blob", "varbinary"):
        cls = "bytes"
        is_binary = True

    return TypeSpec(
        raw=raw,
        base=base,
        cls=cls,
        precision=precision,
        scale=scale,
        length=length,
        is_char=is_char,
        is_binary=is_binary,
    )


def _is_neg_zero(v: Any) -> bool:
    if isinstance(v, float) and v == 0.0:
        return math.copysign(1.0, v) < 0.0
    if isinstance(v, Decimal) and v == 0:
        return v.is_signed()
    return False


def _decimal_scale(d: Decimal) -> int:
    exp = d.as_tuple().exponent
    return abs(exp) if isinstance(exp, int) and exp < 0 else 0


def _normalize_decimal_str(val_str: str) -> str:
    s = val_str.strip()
    is_neg = s.startswith("-")
    if is_neg:
        s = s[1:]
    parts = s.split(".")
    integer_part = parts[0].lstrip("0") or "0"
    if len(parts) > 1:
        return f"{'-' if is_neg else ''}{integer_part}.{parts[1]}"
    return f"{'-' if is_neg else ''}{integer_part}"


def _canonical_json_value(x: Any) -> Any:
    if x is None or isinstance(x, (bool, int)):
        return x
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return str(x)
        if x == 0.0 and math.copysign(1.0, x) < 0.0:
            return "-0.0"
        return x
    if isinstance(x, Decimal):
        return format(x, "f")
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        return {str(k): _canonical_json_value(x[k]) for k in sorted(x, key=lambda k: (len(str(k)), str(k)))}
    if isinstance(x, (list, tuple)):
        return [_canonical_json_value(e) for e in x]
    return str(x)


class CanonicalOracle:
    def __init__(
        self,
        engine: str = "",
        engine_version: str = "",
        declared_type: str = "",
        collation: str = "",
        scale_policy: str = ScalePolicy.NUMERIC,
        space_policy: str = SpacePolicy.TYPE_AWARE,
        collation_policy: str = CollationPolicy.TYPE_AWARE,
        signed_zero_policy: str = SignedZeroPolicy.NUMERIC_EQUAL,
    ):
        self.engine = engine.lower()
        self.engine_version = engine_version
        self.collation = collation
        self.scale_policy = scale_policy
        self.space_policy = space_policy
        self.collation_policy = collation_policy
        self.signed_zero_policy = signed_zero_policy
        self.type_spec = parse_type(self.engine, declared_type)

    def display(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, float):
            if _is_neg_zero(value):
                return "-0.0"
            if value == 0.0:
                return "0.0"
            return str(value)
        if isinstance(value, Decimal):
            if _is_neg_zero(value):
                exp = value.as_tuple().exponent
                zeros = "0" * (abs(exp) if isinstance(exp, int) and exp < 0 else 0)
                return f"-0.{zeros}" if zeros else "-0"
            return format(value, "f")
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).hex().upper()
        if isinstance(value, dict):
            canon_val = _canonical_json_value(value)
            return json.dumps(canon_val, sort_keys=True, ensure_ascii=False)
        if isinstance(value, (list, tuple)):
            canon_val = _canonical_json_value(value)
            return json.dumps(canon_val, ensure_ascii=False)
        return str(value)

    def sort_key(self, value: Any) -> Tuple[Any, ...]:
        if value is None:
            return (0,)
        if isinstance(value, bool):
            return (1, 1 if value else 0)
        if isinstance(value, (int,)):
            return (2, Decimal(value), 0)
        if isinstance(value, float):
            if math.isnan(value):
                return (3, 2, 0.0, 0)
            if math.isinf(value):
                return (3, 1 if value > 0 else -1, abs(value), 0)
            sign_tie = -1 if _is_neg_zero(value) else 1
            return (3, 0, value, sign_tie)
        if isinstance(value, Decimal):
            sign_tie = -1 if _is_neg_zero(value) else 1
            return (2, value, _decimal_scale(value), sign_tie)
        if isinstance(value, str):
            text = value
            if self.space_policy == SpacePolicy.PAD_SPACE or (
                self.space_policy == SpacePolicy.TYPE_AWARE and self.type_spec.is_char
            ):
                text_primary = text.rstrip(" ")
            else:
                text_primary = text

            if self.collation_policy == CollationPolicy.CASE_INSENSITIVE or (
                self.collation_policy == CollationPolicy.TYPE_AWARE and "ci" in self.collation.lower()
            ):
                text_primary = text_primary.casefold()

            if self.collation_policy in (CollationPolicy.NFC, CollationPolicy.TYPE_AWARE):
                text_primary = unicodedata.normalize("NFC", text_primary)

            raw_nfc = unicodedata.normalize("NFC", text)
            return (4, text_primary, raw_nfc, tuple(ord(c) for c in text), len(text))
        if isinstance(value, (bytes, bytearray, memoryview)):
            b = bytes(value)
            return (5, b)
        if isinstance(value, dict):
            items = []
            for k in sorted(value, key=lambda k: (len(str(k)), str(k))):
                items.append((str(k), self.sort_key(value[k])))
            return (6, tuple(items))
        if isinstance(value, (list, tuple)):
            return (7, tuple(self.sort_key(x) for x in value))
        return (8, str(value))

    def equals(self, v1: Any, v2: Any) -> bool:
        return self.diff(v1, v2).equal

    def diff(self, v1: Any, v2: Any) -> DriftResult:
        if v1 is None and v2 is None:
            return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)
        if v1 is None or v2 is None:
            return DriftResult(
                equal=False,
                drift_kind=DriftKind.REAL_DRIFT,
                semantic_equal=False,
                reason="null mismatch",
            )

        if isinstance(v1, (int, float, Decimal, str)) and isinstance(v2, (int, float, Decimal, str)):
            if self.type_spec.cls == "decimal" or isinstance(v1, Decimal) or isinstance(v2, Decimal):
                try:
                    d1 = v1 if isinstance(v1, Decimal) else Decimal(str(v1).strip())
                    d2 = v2 if isinstance(v2, Decimal) else Decimal(str(v2).strip())
                except decimal.InvalidOperation:
                    d1, d2 = None, None

                if d1 is not None and d2 is not None:
                    if d1 != d2:
                        return DriftResult(
                            equal=False,
                            drift_kind=DriftKind.REAL_DRIFT,
                            semantic_equal=False,
                            reason=f"decimal real drift: {d1} vs {d2}",
                        )
                    s1 = _decimal_scale(d1)
                    s2 = _decimal_scale(d2)
                    if s1 != s2:
                        if self.scale_policy in (ScalePolicy.EXACT, ScalePolicy.PRESERVE):
                            return DriftResult(
                                equal=False,
                                drift_kind=DriftKind.SCALE_DRIFT,
                                semantic_equal=False,
                                reason=f"scale mismatch: {s1} vs {s2}",
                                details={"scale1": s1, "scale2": s2},
                            )
                        return DriftResult(
                            equal=True,
                            drift_kind=DriftKind.REPRESENTATIONAL,
                            semantic_equal=True,
                            reason=f"scale difference: {s1} vs {s2}",
                            details={"scale1": s1, "scale2": s2},
                        )

                    z1 = _is_neg_zero(d1)
                    z2 = _is_neg_zero(d2)
                    if z1 != z2:
                        if self.signed_zero_policy == SignedZeroPolicy.STRICT:
                            return DriftResult(
                                equal=False,
                                drift_kind=DriftKind.SIGN_DRIFT,
                                semantic_equal=False,
                                reason="signed zero mismatch",
                            )
                        return DriftResult(
                            equal=True,
                            drift_kind=DriftKind.REPRESENTATIONAL,
                            semantic_equal=True,
                            reason="signed zero difference",
                        )
                    return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)

        if isinstance(v1, (float, int)) and isinstance(v2, (float, int)):
            f1 = float(v1)
            f2 = float(v2)
            if math.isnan(f1) and math.isnan(f2):
                return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)
            z1 = _is_neg_zero(f1)
            z2 = _is_neg_zero(f2)
            if f1 == 0.0 and f2 == 0.0 and z1 != z2:
                if self.signed_zero_policy == SignedZeroPolicy.STRICT:
                    return DriftResult(
                        equal=False,
                        drift_kind=DriftKind.SIGN_DRIFT,
                        semantic_equal=False,
                        reason="signed zero mismatch (-0.0 vs +0.0)",
                    )
                return DriftResult(
                    equal=True,
                    drift_kind=DriftKind.REPRESENTATIONAL,
                    semantic_equal=True,
                    reason="signed zero difference (-0.0 vs +0.0)",
                )
            if f1 != f2:
                return DriftResult(
                    equal=False,
                    drift_kind=DriftKind.REAL_DRIFT,
                    semantic_equal=False,
                    reason=f"float value drift: {f1} vs {f2}",
                )
            return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)

        if isinstance(v1, str) and isinstance(v2, str):
            s1, s2 = v1, v2
            if s1 == s2:
                return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)

            t1 = s1.rstrip(" ")
            t2 = s2.rstrip(" ")
            if t1 == t2:
                if self.space_policy == SpacePolicy.PAD_SPACE or (
                    self.space_policy == SpacePolicy.TYPE_AWARE and self.type_spec.is_char
                ):
                    return DriftResult(
                        equal=True,
                        drift_kind=DriftKind.REPRESENTATIONAL,
                        semantic_equal=True,
                        reason=f"trailing space differ under pad collation: len {len(s1)} vs {len(s2)}",
                    )
                return DriftResult(
                    equal=False,
                    drift_kind=DriftKind.SPACE_DRIFT,
                    semantic_equal=False,
                    reason=f"trailing space mismatch: len {len(s1)} vs {len(s2)}",
                )

            u1 = unicodedata.normalize("NFC", s1)
            u2 = unicodedata.normalize("NFC", s2)
            if u1 == u2:
                if self.collation_policy == CollationPolicy.BINARY:
                    return DriftResult(
                        equal=False,
                        drift_kind=DriftKind.COLLATION_DRIFT,
                        semantic_equal=False,
                        reason="unicode normalization mismatch: nfc vs nfd",
                    )
                return DriftResult(
                    equal=True,
                    drift_kind=DriftKind.REPRESENTATIONAL,
                    semantic_equal=True,
                    reason="unicode normalization difference: nfc vs nfd",
                )

            c1 = u1.casefold()
            c2 = u2.casefold()
            if c1 == c2:
                if self.collation_policy == CollationPolicy.CASE_INSENSITIVE or (
                    self.collation_policy == CollationPolicy.TYPE_AWARE and "ci" in self.collation.lower()
                ):
                    return DriftResult(
                        equal=True,
                        drift_kind=DriftKind.REPRESENTATIONAL,
                        semantic_equal=True,
                        reason=f"case difference under case-insensitive collation: '{s1}' vs '{s2}'",
                    )
                return DriftResult(
                    equal=False,
                    drift_kind=DriftKind.REAL_DRIFT,
                    semantic_equal=False,
                    reason=f"case mismatch under binary/case-sensitive collation: '{s1}' vs '{s2}'",
                )

            return DriftResult(
                equal=False,
                drift_kind=DriftKind.REAL_DRIFT,
                semantic_equal=False,
                reason=f"text real drift: '{s1}' vs '{s2}'",
            )

        if isinstance(v1, dict) and isinstance(v2, dict):
            if set(v1.keys()) != set(v2.keys()):
                return DriftResult(
                    equal=False,
                    drift_kind=DriftKind.REAL_DRIFT,
                    semantic_equal=False,
                    reason=f"dict key set mismatch: {set(v1.keys()) ^ set(v2.keys())}",
                )
            for k in v1:
                child_res = self.diff(v1[k], v2[k])
                if not child_res.equal:
                    return DriftResult(
                        equal=False,
                        drift_kind=DriftKind.REAL_DRIFT,
                        semantic_equal=False,
                        reason=f"dict value mismatch for key '{k}': {child_res.reason}",
                    )
            reordered = list(v1.keys()) != list(v2.keys())
            return DriftResult(
                equal=True,
                drift_kind=DriftKind.REPRESENTATIONAL if reordered else DriftKind.NONE,
                semantic_equal=True,
                reason="reordered keys" if reordered else "",
            )

        if isinstance(v1, (list, tuple)) and isinstance(v2, (list, tuple)):
            if len(v1) != len(v2):
                return DriftResult(
                    equal=False,
                    drift_kind=DriftKind.REAL_DRIFT,
                    semantic_equal=False,
                    reason=f"collection length mismatch: {len(v1)} vs {len(v2)}",
                )
            all_equal = True
            for x, y in zip(v1, v2):
                item_diff = self.diff(x, y)
                if not item_diff.equal:
                    all_equal = False
                    break
            if all_equal:
                return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)

            h1 = self.multiset_hash(v1)
            h2 = self.multiset_hash(v2)
            if h1 == h2:
                return DriftResult(
                    equal=True,
                    drift_kind=DriftKind.REPRESENTATIONAL,
                    semantic_equal=True,
                    reason="reordered multiset elements",
                )
            return DriftResult(
                equal=False,
                drift_kind=DriftKind.REAL_DRIFT,
                semantic_equal=False,
                reason="collection element mismatch",
            )

        if v1 == v2:
            return DriftResult(equal=True, drift_kind=DriftKind.NONE, semantic_equal=True)

        return DriftResult(
            equal=False,
            drift_kind=DriftKind.REAL_DRIFT,
            semantic_equal=False,
            reason=f"value mismatch: {v1!r} vs {v2!r}",
        )

    def multiset_hash(self, items: Any, hash_alg: str = "sha256") -> str:
        return multiset_hash(items, hash_alg=hash_alg)


def _elem_hash(val: Any) -> int:
    if isinstance(val, dict):
        canon_obj = _canonical_json_value(val)
        raw = json.dumps(canon_obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    elif isinstance(val, (list, tuple)):
        canon_obj = _canonical_json_value(val)
        raw = json.dumps(canon_obj, ensure_ascii=False).encode("utf-8")
    elif isinstance(val, Decimal):
        raw = format(val, "f").encode("utf-8")
    elif isinstance(val, float) and _is_neg_zero(val):
        raw = b"-0.0"
    elif isinstance(val, (bytes, bytearray, memoryview)):
        raw = bytes(val)
    else:
        raw = str(val).encode("utf-8")
    h = hashlib.sha256(raw).digest()
    return int.from_bytes(h, "big")


def multiset_hash(items: Any, hash_alg: str = "sha256") -> str:
    if isinstance(items, dict):
        total = 0
        for k in items:
            kh = _elem_hash(str(k))
            vh = _elem_hash(items[k])
            pair_h = int.from_bytes(
                hashlib.sha256(kh.to_bytes(32, "big") + vh.to_bytes(32, "big")).digest(),
                "big",
            )
            total = (total + pair_h) % MODULUS_256
        return format(total, "064x")

    if isinstance(items, (list, tuple, set, frozenset)):
        total = 0
        for item in items:
            eh = _elem_hash(item)
            total = (total + eh) % MODULUS_256
        return format(total, "064x")

    return format(_elem_hash(items) % MODULUS_256, "064x")


@dataclass(frozen=True)
class TestVector:
    name: str
    category: str
    engine: str
    engine_version: str
    declared_type: str
    val1: Any
    val2: Any
    expected_equal: bool
    expected_drift_kind: str
    policies: Dict[str, Any] = field(default_factory=dict)


CANONICAL_TEST_VECTORS: List[TestVector] = [
    TestVector(
        name="decimal_leading_zeros",
        category="decimal",
        engine="postgres",
        engine_version="16",
        declared_type="numeric(10,2)",
        val1=Decimal("01.50"),
        val2=Decimal("1.50"),
        expected_equal=True,
        expected_drift_kind=DriftKind.NONE,
    ),
    TestVector(
        name="decimal_scale_numeric_policy",
        category="decimal",
        engine="postgres",
        engine_version="16",
        declared_type="numeric(10,2)",
        val1=Decimal("1.0"),
        val2=Decimal("1.00"),
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
        policies={"scale_policy": ScalePolicy.NUMERIC},
    ),
    TestVector(
        name="decimal_scale_exact_policy",
        category="decimal",
        engine="mysql",
        engine_version="8.0",
        declared_type="decimal(10,2)",
        val1=Decimal("1.0"),
        val2=Decimal("1.00"),
        expected_equal=False,
        expected_drift_kind=DriftKind.SCALE_DRIFT,
        policies={"scale_policy": ScalePolicy.EXACT},
    ),
    TestVector(
        name="decimal_real_drift",
        category="decimal",
        engine="postgres",
        engine_version="16",
        declared_type="numeric(10,2)",
        val1=Decimal("1.00"),
        val2=Decimal("1.01"),
        expected_equal=False,
        expected_drift_kind=DriftKind.REAL_DRIFT,
    ),
    TestVector(
        name="signed_zero_float_numeric_policy",
        category="float",
        engine="mysql",
        engine_version="8.0",
        declared_type="float",
        val1=0.0,
        val2=math.copysign(0.0, -1.0),
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
        policies={"signed_zero_policy": SignedZeroPolicy.NUMERIC_EQUAL},
    ),
    TestVector(
        name="signed_zero_float_strict_policy",
        category="float",
        engine="postgres",
        engine_version="16",
        declared_type="float8",
        val1=0.0,
        val2=math.copysign(0.0, -1.0),
        expected_equal=False,
        expected_drift_kind=DriftKind.SIGN_DRIFT,
        policies={"signed_zero_policy": SignedZeroPolicy.STRICT},
    ),
    TestVector(
        name="trailing_space_varchar_real_drift",
        category="text",
        engine="postgres",
        engine_version="16",
        declared_type="varchar(64)",
        val1="hello",
        val2="hello  ",
        expected_equal=False,
        expected_drift_kind=DriftKind.SPACE_DRIFT,
        policies={"space_policy": SpacePolicy.TYPE_AWARE},
    ),
    TestVector(
        name="trailing_space_char_pad_space",
        category="text",
        engine="mysql",
        engine_version="8.0",
        declared_type="char(64)",
        val1="hello",
        val2="hello  ",
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
        policies={"space_policy": SpacePolicy.TYPE_AWARE},
    ),
    TestVector(
        name="unicode_nfc_vs_nfd_binary_collation",
        category="unicode",
        engine="postgres",
        engine_version="16",
        declared_type="text",
        val1="\u00e9",
        val2="e\u0301",
        expected_equal=False,
        expected_drift_kind=DriftKind.COLLATION_DRIFT,
        policies={"collation_policy": CollationPolicy.BINARY},
    ),
    TestVector(
        name="unicode_nfc_vs_nfd_nfc_collation",
        category="unicode",
        engine="postgres",
        engine_version="16",
        declared_type="text",
        val1="\u00e9",
        val2="e\u0301",
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
        policies={"collation_policy": CollationPolicy.NFC},
    ),
    TestVector(
        name="case_folding_case_insensitive",
        category="text",
        engine="mysql",
        engine_version="8.0",
        declared_type="varchar(64)",
        val1="Alpha",
        val2="alpha",
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
        policies={"collation_policy": CollationPolicy.CASE_INSENSITIVE},
    ),
    TestVector(
        name="case_folding_binary",
        category="text",
        engine="postgres",
        engine_version="16",
        declared_type="text",
        val1="Alpha",
        val2="alpha",
        expected_equal=False,
        expected_drift_kind=DriftKind.REAL_DRIFT,
        policies={"collation_policy": CollationPolicy.BINARY},
    ),
    TestVector(
        name="unordered_json_keys",
        category="json",
        engine="postgres",
        engine_version="16",
        declared_type="jsonb",
        val1={"a": 1, "b": 2},
        val2={"b": 2, "a": 1},
        expected_equal=True,
        expected_drift_kind=DriftKind.REPRESENTATIONAL,
    ),
    TestVector(
        name="multiset_count_mismatch",
        category="multiset",
        engine="postgres",
        engine_version="16",
        declared_type="json",
        val1=["x", "y", "x"],
        val2=["x", "y"],
        expected_equal=False,
        expected_drift_kind=DriftKind.REAL_DRIFT,
    ),
]


def verify_test_vectors(
    oracle: Optional[CanonicalOracle] = None,
) -> Tuple[List[TestVector], List[Tuple[TestVector, DriftResult]]]:
    passed = []
    failed = []
    for tv in CANONICAL_TEST_VECTORS:
        orc = CanonicalOracle(
            engine=tv.engine,
            engine_version=tv.engine_version,
            declared_type=tv.declared_type,
            scale_policy=tv.policies.get("scale_policy", ScalePolicy.NUMERIC),
            space_policy=tv.policies.get("space_policy", SpacePolicy.TYPE_AWARE),
            collation_policy=tv.policies.get("collation_policy", CollationPolicy.TYPE_AWARE),
            signed_zero_policy=tv.policies.get("signed_zero_policy", SignedZeroPolicy.NUMERIC_EQUAL),
        ) if oracle is None else oracle
        res = orc.diff(tv.val1, tv.val2)
        if res.equal == tv.expected_equal and res.drift_kind == tv.expected_drift_kind:
            passed.append(tv)
        else:
            failed.append((tv, res))
    return passed, failed
