import decimal
import math
from decimal import Decimal
import pytest

from migkit import canon_oracle as co
from migkit import canon
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("canon_oracle", "canonical_oracle_rungs", "numeric_scale_policy", "holds"),
    ("canon_oracle", "canonical_oracle_rungs", "numeric_scale_policy", "refuses"),
    ("canon_oracle", "canonical_oracle_rungs", "type_aware_space_policy", "holds"),
    ("canon_oracle", "canonical_oracle_rungs", "type_aware_space_policy", "refuses"),
    ("canon_oracle", "canonical_oracle_rungs", "type_aware_collation_policy", "holds"),
    ("canon_oracle", "canonical_oracle_rungs", "type_aware_collation_policy", "refuses"),
    ("canon_oracle", "canonical_oracle_rungs", "multiset_order_independent_hash", "holds"),
    ("canon_oracle", "canonical_oracle_rungs", "multiset_order_independent_hash", "refuses"),
    ("canon_oracle", "canonical_oracle_rungs", "refuse_incompatible_type_coercion", "holds"),
    ("canon_oracle", "canonical_oracle_rungs", "refuse_incompatible_type_coercion", "refuses"),
    ("canon_oracle", "canonical_policy_rungs", "preserve_exact_scale", "holds"),
    ("canon_oracle", "canonical_policy_rungs", "preserve_exact_scale", "refuses"),
    ("canon_oracle", "canonical_policy_rungs", "pad_fixed_space", "holds"),
    ("canon_oracle", "canonical_policy_rungs", "pad_fixed_space", "refuses"),
    ("canon_oracle", "canonical_policy_rungs", "normalize_unicode_collation", "holds"),
    ("canon_oracle", "canonical_policy_rungs", "normalize_unicode_collation", "refuses"),
    ("canon_oracle", "canonical_policy_rungs", "signed_zero_numeric_equal", "holds"),
    ("canon_oracle", "canonical_policy_rungs", "signed_zero_numeric_equal", "refuses"),
    ("canon_oracle", "canonical_policy_rungs", "refuse_lossy_downcast", "holds"),
    ("canon_oracle", "canonical_policy_rungs", "refuse_lossy_downcast", "refuses"),
]



def test_decimal_scale_and_leading_zeros():
    oracle_num = co.CanonicalOracle(
        declared_type="decimal(10,2)",
        scale_policy=co.ScalePolicy.NUMERIC,
    )
    res = oracle_num.diff(Decimal("01.50"), Decimal("1.50"))
    assert res.equal is True
    assert res.drift_kind == co.DriftKind.NONE

    res_scale = oracle_num.diff(Decimal("1.0"), Decimal("1.00"))
    assert res_scale.equal is True
    assert res_scale.drift_kind == co.DriftKind.REPRESENTATIONAL
    assert "scale" in res_scale.reason

    oracle_exact = co.CanonicalOracle(
        declared_type="decimal(10,2)",
        scale_policy=co.ScalePolicy.EXACT,
    )
    res_exact = oracle_exact.diff(Decimal("1.0"), Decimal("1.00"))
    assert res_exact.equal is False
    assert res_exact.drift_kind == co.DriftKind.SCALE_DRIFT

    res_real = oracle_exact.diff(Decimal("1.00"), Decimal("1.01"))
    assert res_real.equal is False
    assert res_real.drift_kind == co.DriftKind.REAL_DRIFT


def test_signed_zero_float_and_decimal():
    pos_zero = 0.0
    neg_zero = math.copysign(0.0, -1.0)
    assert str(pos_zero) == "0.0"
    assert math.copysign(1.0, neg_zero) < 0.0

    oracle = co.CanonicalOracle(
        declared_type="float",
        signed_zero_policy=co.SignedZeroPolicy.NUMERIC_EQUAL,
    )
    assert oracle.display(pos_zero) == "0.0"
    assert oracle.display(neg_zero) == "-0.0"

    k_pos = oracle.sort_key(pos_zero)
    k_neg = oracle.sort_key(neg_zero)
    assert k_neg < k_pos

    res = oracle.diff(pos_zero, neg_zero)
    assert res.equal is True
    assert res.drift_kind == co.DriftKind.REPRESENTATIONAL
    assert "signed zero" in res.reason

    oracle_strict = co.CanonicalOracle(
        declared_type="float",
        signed_zero_policy=co.SignedZeroPolicy.STRICT,
    )
    res_strict = oracle_strict.diff(pos_zero, neg_zero)
    assert res_strict.equal is False
    assert res_strict.drift_kind == co.DriftKind.SIGN_DRIFT


def test_trailing_spaces_and_char_vs_varchar():
    oracle_varchar = co.CanonicalOracle(
        declared_type="varchar(32)",
        space_policy=co.SpacePolicy.TYPE_AWARE,
    )
    res_vc = oracle_varchar.diff("alice", "alice  ")
    assert res_vc.equal is False
    assert res_vc.drift_kind == co.DriftKind.SPACE_DRIFT

    oracle_char = co.CanonicalOracle(
        declared_type="char(32)",
        space_policy=co.SpacePolicy.TYPE_AWARE,
    )
    res_char = oracle_char.diff("alice", "alice  ")
    assert res_char.equal is True
    assert res_char.drift_kind == co.DriftKind.REPRESENTATIONAL

    k1 = oracle_char.sort_key("alice")
    k2 = oracle_char.sort_key("alice  ")
    assert k1 != k2


def test_unicode_and_collation_normalization():
    nfc = "\u00e9"
    nfd = "e\u0301"
    assert nfc != nfd

    oracle_bin = co.CanonicalOracle(
        declared_type="text",
        collation_policy=co.CollationPolicy.BINARY,
    )
    res_bin = oracle_bin.diff(nfc, nfd)
    assert res_bin.equal is False
    assert res_bin.drift_kind == co.DriftKind.COLLATION_DRIFT

    oracle_nfc = co.CanonicalOracle(
        declared_type="text",
        collation_policy=co.CollationPolicy.NFC,
    )
    res_nfc = oracle_nfc.diff(nfc, nfd)
    assert res_nfc.equal is True
    assert res_nfc.drift_kind == co.DriftKind.REPRESENTATIONAL

    oracle_ci = co.CanonicalOracle(
        declared_type="text",
        collation_policy=co.CollationPolicy.CASE_INSENSITIVE,
    )
    res_ci = oracle_ci.diff("HELLO", "hello")
    assert res_ci.equal is True
    assert res_ci.drift_kind == co.DriftKind.REPRESENTATIONAL

    res_cs = oracle_bin.diff("HELLO", "hello")
    assert res_cs.equal is False
    assert res_cs.drift_kind == co.DriftKind.REAL_DRIFT


def test_multiset_and_unordered_key_hashing():
    d1 = {"a": 1, "b": [10, 20], "c": {"x": "foo", "y": "bar"}}
    d2 = {"c": {"y": "bar", "x": "foo"}, "b": [10, 20], "a": 1}

    oracle = co.CanonicalOracle(declared_type="json")
    assert oracle.multiset_hash(d1) == oracle.multiset_hash(d2)

    res = oracle.diff(d1, d2)
    assert res.equal is True
    assert res.drift_kind in (co.DriftKind.NONE, co.DriftKind.REPRESENTATIONAL)

    d3 = {"a": 1, "b": [10, 20], "c": {"x": "foo", "y": "baz"}}
    res_diff = oracle.diff(d1, d3)
    assert res_diff.equal is False
    assert res_diff.drift_kind == co.DriftKind.REAL_DRIFT

    mset1 = ["k1", "k2", "k1", "k3"]
    mset2 = ["k3", "k1", "k2", "k1"]
    mset3 = ["k1", "k2", "k3"]

    h1 = oracle.multiset_hash(mset1)
    h2 = oracle.multiset_hash(mset2)
    h3 = oracle.multiset_hash(mset3)

    assert h1 == h2
    assert h1 != h3


def test_separation_of_display_sort_and_equality():
    oracle = co.CanonicalOracle(declared_type="decimal(10,2)")
    v1 = Decimal("1.0")
    v2 = Decimal("1.00")

    d1 = oracle.display(v1)
    d2 = oracle.display(v2)
    assert d1 != d2 or d1 == "1.0"

    s1 = oracle.sort_key(v1)
    s2 = oracle.sort_key(v2)
    assert s1 != s2
    assert s1[0] == s2[0]

    assert oracle.equals(v1, v2) is True


def test_independent_test_vectors():
    passed, failed = co.verify_test_vectors()
    assert failed == []
    assert len(passed) >= 10


def test_canon_hooks():
    orc = canon.canonical_oracle(engine="postgres", declared="numeric(10,2)")
    assert orc is not None
    assert canon.oracle_equals(Decimal("1.5"), Decimal("01.5")) is True
    res = canon.detect_drift("hello", "hello ")
    assert res.equal is False
    assert res.drift_kind == co.DriftKind.SPACE_DRIFT

    h1 = canon.multiset_hash({"x": 1, "y": 2})
    h2 = canon.multiset_hash({"y": 2, "x": 1})
    assert h1 == h2


def test_total_order_sorting_floats_and_decimals():
    oracle = co.CanonicalOracle(declared_type="float")
    neg_zero = math.copysign(0.0, -1.0)
    pos_zero = 0.0
    items = [float("inf"), 10.0, pos_zero, neg_zero, -5.0, -float("inf"), float("nan")]
    sorted_items = sorted(items, key=lambda x: oracle.sort_key(x))
    assert math.isinf(sorted_items[0]) and sorted_items[0] < 0
    assert sorted_items[1] == -5.0
    assert math.copysign(1.0, sorted_items[2]) < 0.0
    assert math.copysign(1.0, sorted_items[3]) > 0.0
    assert sorted_items[4] == 10.0
    assert math.isinf(sorted_items[5]) and sorted_items[5] > 0
    assert math.isnan(sorted_items[6])

    oracle_dec = co.CanonicalOracle(declared_type="decimal(10,2)")
    d_items = [Decimal("2.0"), Decimal("1.00"), Decimal("1.0"), Decimal("-1.0")]
    sorted_d = sorted(d_items, key=lambda x: oracle_dec.sort_key(x))
    assert sorted_d[0] == Decimal("-1.0")
    assert sorted_d[1] == Decimal("1.0") and sorted_d[1].as_tuple().exponent == -1
    assert sorted_d[2] == Decimal("1.00") and sorted_d[2].as_tuple().exponent == -2
    assert sorted_d[3] == Decimal("2.0")


def test_multiset_hash_no_xor_cancellation():
    h_empty = co.multiset_hash([])
    h_pair = co.multiset_hash(["val", "val"])
    h_single = co.multiset_hash(["val"])
    assert h_pair != h_empty
    assert h_pair != h_single
    assert h_pair != "0" * 64

    h_three = co.multiset_hash(["val", "val", "other"])
    h_other = co.multiset_hash(["other"])
    assert h_three != h_other


def test_detect_real_drift_across_types():
    oracle = co.CanonicalOracle(declared_type="int")
    res_int = oracle.diff(10, 20)
    assert res_int.equal is False
    assert res_int.drift_kind == co.DriftKind.REAL_DRIFT

    res_null = oracle.diff(10, None)
    assert res_null.equal is False
    assert res_null.drift_kind == co.DriftKind.REAL_DRIFT

    oracle_bool = co.CanonicalOracle(declared_type="boolean")
    res_bool = oracle_bool.diff(True, False)
    assert res_bool.equal is False
    assert res_bool.drift_kind == co.DriftKind.REAL_DRIFT

    oracle_txt = co.CanonicalOracle(declared_type="text")
    res_txt = oracle_txt.diff("alpha", "beta")
    assert res_txt.equal is False
    assert res_txt.drift_kind == co.DriftKind.REAL_DRIFT


def test_version_and_engine_awareness():
    orc_my57 = co.CanonicalOracle(engine="mysql", engine_version="5.7", declared_type="varchar(10)")
    orc_my80 = co.CanonicalOracle(engine="mysql", engine_version="8.0", declared_type="varchar(10)")
    assert orc_my57.engine == "mysql"
    assert orc_my57.engine_version == "5.7"
    assert orc_my80.engine_version == "8.0"

    orc_pg = co.CanonicalOracle(engine="postgres", engine_version="16", declared_type="numeric(12,4)")
    assert orc_pg.type_spec.cls == "decimal"
    assert orc_pg.type_spec.precision == 12
    assert orc_pg.type_spec.scale == 4


def test_canonical_oracle_rungs_under_watched():
    with watched():
        # canonical_oracle_rungs:
        # 1. numeric_scale_policy
        o1 = co.decide_canonical_oracle({"numeric_scale": True})
        assert o1.rung.name == "numeric_scale_policy"

        # 2. type_aware_space_policy
        o2 = co.decide_canonical_oracle({"numeric_scale": False, "type_aware_space": True})
        assert o2.rung.name == "type_aware_space_policy"

        # 3. type_aware_collation_policy
        o3 = co.decide_canonical_oracle({"numeric_scale": False, "type_aware_space": False, "type_aware_collation": True})
        assert o3.rung.name == "type_aware_collation_policy"

        # 4. multiset_order_independent_hash
        o4 = co.decide_canonical_oracle({"numeric_scale": False, "type_aware_space": False, "type_aware_collation": False, "multiset_hash": True})
        assert o4.rung.name == "multiset_order_independent_hash"

        # 5. refuse_incompatible_type_coercion
        o5 = co.decide_canonical_oracle({"numeric_scale": False, "type_aware_space": False, "type_aware_collation": False, "multiset_hash": False})
        assert o5.rung.name == "refuse_incompatible_type_coercion"

        # 6. refuse fallback
        o6 = co.decide_canonical_oracle({"numeric_scale": False, "type_aware_space": False, "type_aware_collation": False, "multiset_hash": False, "allow_incompatible_coercion": True})
        assert o6.rung is None

        # canonical_policy_rungs:
        # 1. preserve_exact_scale
        p1 = co.decide_canonical_policy({"exact_scale": True})
        assert p1.rung.name == "preserve_exact_scale"

        # 2. pad_fixed_space
        p2 = co.decide_canonical_policy({"exact_scale": False, "pad_space": True})
        assert p2.rung.name == "pad_fixed_space"

        # 3. normalize_unicode_collation
        p3 = co.decide_canonical_policy({"exact_scale": False, "pad_space": False, "unicode_norm": True})
        assert p3.rung.name == "normalize_unicode_collation"

        # 4. signed_zero_numeric_equal
        p4 = co.decide_canonical_policy({"exact_scale": False, "pad_space": False, "unicode_norm": False, "signed_zero_equal": True})
        assert p4.rung.name == "signed_zero_numeric_equal"

        # 5. refuse_lossy_downcast
        p5 = co.decide_canonical_policy({"exact_scale": False, "pad_space": False, "unicode_norm": False, "signed_zero_equal": False})
        assert p5.rung.name == "refuse_lossy_downcast"

        # 6. refuse fallback
        p6 = co.decide_canonical_policy({"exact_scale": False, "pad_space": False, "unicode_norm": False, "signed_zero_equal": False, "allow_lossy_downcast": True})
        assert p6.rung is None

