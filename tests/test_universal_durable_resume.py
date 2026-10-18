import datetime
import decimal
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from migkit import checkpoint as cp_mod
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("checkpoint", "universal_resume_rungs", "o1_journal_checkpoint_replay", "holds"),
    ("checkpoint", "universal_resume_rungs", "o1_journal_checkpoint_replay", "refuses"),
    ("checkpoint", "universal_resume_rungs", "typed_cursor_preservation", "holds"),
    ("checkpoint", "universal_resume_rungs", "typed_cursor_preservation", "refuses"),
    ("checkpoint", "universal_resume_rungs", "matching_fingerprint_retention", "holds"),
    ("checkpoint", "universal_resume_rungs", "matching_fingerprint_retention", "refuses"),
    ("checkpoint", "universal_resume_rungs", "adaptive_range_density_scaling", "holds"),
    ("checkpoint", "universal_resume_rungs", "adaptive_range_density_scaling", "refuses"),
    ("checkpoint", "universal_resume_rungs", "refuse_corrupted_journal_plan", "holds"),
    ("checkpoint", "universal_resume_rungs", "refuse_corrupted_journal_plan", "refuses"),
    ("checkpoint", "cursor_serialization_rungs", "datetime_timezone_aware", "holds"),
    ("checkpoint", "cursor_serialization_rungs", "datetime_timezone_aware", "refuses"),
    ("checkpoint", "cursor_serialization_rungs", "uuid_canonical_hex", "holds"),
    ("checkpoint", "cursor_serialization_rungs", "uuid_canonical_hex", "refuses"),
    ("checkpoint", "cursor_serialization_rungs", "decimal_fixed_scale", "holds"),
    ("checkpoint", "cursor_serialization_rungs", "decimal_fixed_scale", "refuses"),
    ("checkpoint", "cursor_serialization_rungs", "composite_tuple_cursor", "holds"),
    ("checkpoint", "cursor_serialization_rungs", "composite_tuple_cursor", "refuses"),
    ("checkpoint", "cursor_serialization_rungs", "refuse_untyped_opaque_cursor", "holds"),
    ("checkpoint", "cursor_serialization_rungs", "refuse_untyped_opaque_cursor", "refuses"),
]


def test_cursor_serialization_and_deserialization_preserves_types():
    dt = datetime.datetime(2026, 10, 6, 14, 30, 0, tzinfo=datetime.timezone.utc)
    d = datetime.date(2026, 10, 6)
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dec = decimal.Decimal("123456.7890")
    composite = (101, dt, "part_A")

    s_dt = cp_mod.serialize_cursor(dt)
    assert s_dt["_type"] == "datetime"
    assert cp_mod.deserialize_cursor(s_dt) == dt

    s_d = cp_mod.serialize_cursor(d)
    assert s_d["_type"] == "date"
    assert cp_mod.deserialize_cursor(s_d) == d

    s_u = cp_mod.serialize_cursor(u)
    assert s_u["_type"] == "uuid"
    assert cp_mod.deserialize_cursor(s_u) == u

    s_dec = cp_mod.serialize_cursor(dec)
    assert s_dec["_type"] == "decimal"
    assert cp_mod.deserialize_cursor(s_dec) == dec

    s_comp = cp_mod.serialize_cursor(composite)
    assert s_comp["_type"] == "composite"
    assert cp_mod.deserialize_cursor(s_comp) == composite


def test_where_clause_with_datetime_and_uuid_quotes_safely():
    dt1 = datetime.datetime(2026, 1, 1, 0, 0, 0)
    dt2 = datetime.datetime(2026, 1, 2, 0, 0, 0)
    w = cp_mod.where('"created_at"', dt1, dt2)
    assert w == '"created_at" >= \'2026-01-01T00:00:00\' and "created_at" < \'2026-01-02T00:00:00\''

    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    w_uuid = cp_mod.where('"id"', u, None)
    assert w_uuid == '"id" >= \'12345678-1234-5678-1234-567812345678\''

    w_int = cp_mod.where('"id"', 10, 20)
    assert w_int == '"id" >= 10 and "id" < 20'


def test_plan_ranges_datetime_and_dates():
    start = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2026, 1, 1, 4, 0, 0, tzinfo=datetime.timezone.utc)
    # 3600 seconds per chunk -> 4 chunks
    ranges = cp_mod.plan_ranges(start, end, 3600)
    assert len(ranges) == 5
    assert ranges[0][0] is None
    assert ranges[-1][1] is None
    assert ranges[1][0] == start + datetime.timedelta(hours=1)


def test_plan_ranges_density_proportional_scaling():
    start = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2026, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc)
    # Total rows = 10,000, chunk = 2,500 rows -> 4 chunks across 86400s (step = 21600s = 6 hours)
    ranges = cp_mod.plan_ranges(start, end, 2500, total_rows=10000)
    assert len(ranges) == 5
    assert ranges[0][0] is None
    assert ranges[-1][1] is None
    assert ranges[1][0] == start + datetime.timedelta(hours=6)

    # Date ranges with total_rows scaling
    d_start = datetime.date(2026, 1, 1)
    d_end = datetime.date(2026, 1, 31)
    # 30 days, 3000 rows, chunk = 1000 rows -> 3 chunks (10 days per chunk)
    d_ranges = cp_mod.plan_ranges(d_start, d_end, 1000, total_rows=3000)
    assert len(d_ranges) == 4
    assert d_ranges[0][0] is None
    assert d_ranges[-1][1] is None
    assert d_ranges[1][0] == datetime.date(2026, 1, 11)


def test_sub_unit_range_progress_streaming_and_o1_resume(tmp_path):
    report_dir = tmp_path / "reports" / "shop"
    report_dir.mkdir(parents=True, exist_ok=True)
    cp_path = report_dir / "checkpoint.json"

    cp = cp_mod.Checkpoint(str(cp_path))
    dt_lo = datetime.datetime(2026, 10, 1, 0, 0, 0)
    dt_hi = datetime.datetime(2026, 10, 5, 0, 0, 0)
    ranges = cp_mod.plan_ranges(dt_lo, dt_hi, 86400)
    expr = "md5(order_id||created_at)"

    todo = cp.begin("shop.order_histories", expr, ranges)
    assert len(todo) == len(ranges)

    # Simulate executing 50% of the ranges and crashing
    done_count = len(ranges) // 2
    for rlo, rhi in ranges[:done_count]:
        rows, csum = 1000, 123456789
        cp.record("shop.order_histories", rlo, rhi, rows, csum)
        cp_mod.record_progress_range(report_dir, "hop-prod", "shop", "shop.order_histories",
                                     rlo, rhi, rows, csum)

    # Fresh process resumes
    resumed_cp = cp_mod.Checkpoint(str(cp_path))
    # Test loading from journal as well
    loaded = resumed_cp.load_progress_journal(report_dir / "check-progress.jsonl")
    assert loaded == 0  # checkpoint.json already had them

    # If checkpoint.json was deleted (e.g. disk corruption), journal restores it in O(1)
    recovered_cp = cp_mod.Checkpoint(str(report_dir / "fresh_cp.json"))
    recovered = recovered_cp.load_progress_journal(report_dir / "check-progress.jsonl")
    assert recovered == done_count

    # Verify that begin on recovered_cp retains the journal-loaded completed ranges without discarding them!
    recovered_todo = recovered_cp.begin("shop.order_histories", expr, ranges)
    assert len(recovered_todo) == len(ranges) - done_count
    assert recovered_cp.resumed("shop.order_histories") == done_count

    remaining = resumed_cp.begin("shop.order_histories", expr, ranges)
    assert len(remaining) == len(ranges) - done_count
    assert resumed_cp.resumed("shop.order_histories") == done_count

    # Execute remaining ranges
    for rlo, rhi in remaining:
        resumed_cp.record("shop.order_histories", rlo, rhi, 1000, 123456789)

    total_rows, total_csum = resumed_cp.total("shop.order_histories")
    assert total_rows == len(ranges) * 1000


def test_universal_resume_and_cursor_serialization_decide_rungs_reachability():
    with watched():
        # universal_resume_rungs:
        # 1. o1_journal_checkpoint_replay
        u1 = cp_mod.decide_universal_resume({"journal_replay": True})
        assert u1.rung.name == "o1_journal_checkpoint_replay"

        # 2. typed_cursor_preservation
        u2 = cp_mod.decide_universal_resume({"journal_replay": False, "typed_cursor": True})
        assert u2.rung.name == "typed_cursor_preservation"

        # 3. matching_fingerprint_retention
        u3 = cp_mod.decide_universal_resume({"journal_replay": False, "typed_cursor": False, "matching_fingerprint": True})
        assert u3.rung.name == "matching_fingerprint_retention"

        # 4. adaptive_range_density_scaling
        u4 = cp_mod.decide_universal_resume({"journal_replay": False, "typed_cursor": False, "matching_fingerprint": False, "adaptive_density": True})
        assert u4.rung.name == "adaptive_range_density_scaling"

        # 5. refuse_corrupted_journal_plan
        u5 = cp_mod.decide_universal_resume({"journal_replay": False, "typed_cursor": False, "matching_fingerprint": False, "adaptive_density": False})
        assert u5.rung.name == "refuse_corrupted_journal_plan"

        # 6. refuse fallback
        u6 = cp_mod.decide_universal_resume({"journal_replay": False, "typed_cursor": False, "matching_fingerprint": False, "adaptive_density": False, "allow_incompatible_plan": True})
        assert u6.rung is None

        # cursor_serialization_rungs:
        # 1. datetime_timezone_aware
        c1 = cp_mod.decide_cursor_serialization({"datetime_supported": True})
        assert c1.rung.name == "datetime_timezone_aware"

        # 2. uuid_canonical_hex
        c2 = cp_mod.decide_cursor_serialization({"datetime_supported": False, "uuid_supported": True})
        assert c2.rung.name == "uuid_canonical_hex"

        # 3. decimal_fixed_scale
        c3 = cp_mod.decide_cursor_serialization({"datetime_supported": False, "uuid_supported": False, "decimal_supported": True})
        assert c3.rung.name == "decimal_fixed_scale"

        # 4. composite_tuple_cursor
        c4 = cp_mod.decide_cursor_serialization({"datetime_supported": False, "uuid_supported": False, "decimal_supported": False, "composite_supported": True})
        assert c4.rung.name == "composite_tuple_cursor"

        # 5. refuse_untyped_opaque_cursor
        c5 = cp_mod.decide_cursor_serialization({"datetime_supported": False, "uuid_supported": False, "decimal_supported": False, "composite_supported": False})
        assert c5.rung.name == "refuse_untyped_opaque_cursor"

        # 6. refuse fallback
        c6 = cp_mod.decide_cursor_serialization({"datetime_supported": False, "uuid_supported": False, "decimal_supported": False, "composite_supported": False, "allow_untyped_cursor": True})
        assert c6.rung is None
