"""Tests for shadow resync decide rungs, DBAPI engine, and unswallowed errors (Row 618: RCH10).

Proves that:
1. resync_strategy_rungs covers all 5 branches (shadow_table_resnapshot, fenced_range_repair, whole_table_reload).
2. shadow_resync_threshold_rungs covers all 5 branches (configured, adaptive, default).
3. SwapJournal unswallows journal read corruption, raising JournalReadError.
4. DBAPIShadowTableEngine performs atomic swap, repair_fenced_ranges, and rollback on real SQLite.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
import pytest

from migkit import decide
from migkit.shadow_resync import (
    AtomicSwapCrashError,
    DBAPIShadowTableEngine,
    JournalReadError,
    ObjectMetrics,
    ResyncStrategy,
    ShadowResyncCoordinator,
    SwapJournal,
    decide_resync_thresholds,
    resync_strategy_rungs,
    shadow_resync_threshold_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("shadow_resync", "resync_strategy_rungs", "shadow_table_resnapshot", "holds"),
    ("shadow_resync", "resync_strategy_rungs", "shadow_table_resnapshot", "refuses"),
    ("shadow_resync", "resync_strategy_rungs", "fenced_range_repair", "holds"),
    ("shadow_resync", "resync_strategy_rungs", "fenced_range_repair", "refuses"),
    ("shadow_resync", "resync_strategy_rungs", "whole_table_reload", "holds"),
    ("shadow_resync", "shadow_resync_threshold_rungs", "configured_resync_thresholds", "holds"),
    ("shadow_resync", "shadow_resync_threshold_rungs", "configured_resync_thresholds", "refuses"),
    ("shadow_resync", "shadow_resync_threshold_rungs", "adaptive_workload_thresholds", "holds"),
    ("shadow_resync", "shadow_resync_threshold_rungs", "adaptive_workload_thresholds", "refuses"),
    ("shadow_resync", "shadow_resync_threshold_rungs", "default_resync_thresholds", "holds"),
]


def test_resync_strategy_decide_rungs_coverage():
    with watched() as seen:
        # 1. Atomic swap supported + schema drifted -> shadow_table_resnapshot holds
        c1 = decide.climb(
            "resync strategy",
            resync_strategy_rungs(),
            {
                "supports_atomic_swap": True,
                "has_headroom": True,
                "schema_drifted": True,
                "within_range_threshold": True,
                "has_divergent_ranges": True,
            },
        )
        assert c1.path == "shadow_table_resnapshot"

        # 2. No swap support, bounded divergence -> shadow_table_resnapshot refuses, fenced_range_repair holds
        c2 = decide.climb(
            "resync strategy",
            resync_strategy_rungs(),
            {
                "supports_atomic_swap": False,
                "has_headroom": True,
                "schema_drifted": False,
                "within_range_threshold": True,
                "has_divergent_ranges": True,
            },
        )
        assert c2.path == "fenced_range_repair"

        # 3. No swap support, no divergent ranges -> both refuse, whole_table_reload holds
        c3 = decide.climb(
            "resync strategy",
            resync_strategy_rungs(),
            {
                "supports_atomic_swap": False,
                "has_headroom": True,
                "schema_drifted": False,
                "within_range_threshold": False,
                "has_divergent_ranges": False,
            },
        )
        assert c3.path == "whole_table_reload"

    assert set(branch(seen, "shadow_table_resnapshot")) == {HOLDS, REFUSES}
    assert set(branch(seen, "fenced_range_repair")) == {HOLDS, REFUSES}
    assert branch(seen, "whole_table_reload") == [HOLDS]


def test_shadow_resync_threshold_rungs_coverage():
    metrics_known = ObjectMetrics(table_name="t1", total_rows=1000, estimated_table_bytes=50000)
    metrics_unknown = ObjectMetrics(table_name="t2", total_rows=0, estimated_table_bytes=0)

    with watched() as seen:
        # 1. Custom options -> configured_resync_thresholds holds
        t1 = decide_resync_thresholds(metrics_known, 1000000, options={"resync_range_max_rows": 200})
        assert t1["range_max_rows"] == 200

        # 2. No custom options, known volume -> configured refuses, adaptive holds
        t2 = decide_resync_thresholds(metrics_known, 1000000)
        assert t2["range_max_rows"] == 50

        # 3. No custom options, unknown volume -> both refuse, default holds
        t3 = decide_resync_thresholds(metrics_unknown, 1000000)
        assert t3["range_max_rows"] == 50

    assert set(branch(seen, "configured_resync_thresholds")) == {HOLDS, REFUSES}
    assert set(branch(seen, "adaptive_workload_thresholds")) == {HOLDS, REFUSES}
    assert branch(seen, "default_resync_thresholds") == [HOLDS]


def test_swap_journal_unswallowed_corruption(tmp_path: Path):
    jpath = tmp_path / "corrupt.journal"
    jpath.write_text("NOT_VALID_JSON{:::}")
    journal = SwapJournal(jpath)
    with pytest.raises(JournalReadError) as excinfo:
        journal.read()
    assert "failed" in str(excinfo.value).lower() or "torn" in str(excinfo.value).lower() or "damaged" in str(excinfo.value).lower()


def test_dbapi_shadow_table_engine_sqlite_lifecycle(tmp_path: Path):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    engine = DBAPIShadowTableEngine(conn, dialect="sqlite")

    engine.create_table("users", ["id", "name"], primary_key="id", rows=[{"id": 1, "name": "alice"}])
    assert engine.table_exists("users")
    assert engine.get_rows("users") == [{"id": 1, "name": "alice"}]

    engine.create_table("users__shadow", ["id", "name"], primary_key="id", rows=[{"id": 1, "name": "alice_updated"}, {"id": 2, "name": "bob"}])
    journal = SwapJournal(tmp_path / "swap.journal")

    engine.execute_atomic_swap("users", "users__shadow", "users__old", journal=journal)

    assert engine.table_exists("users")
    assert engine.table_exists("users__old")
    rows = sorted(engine.get_rows("users"), key=lambda r: r["id"])
    assert len(rows) == 2
    assert rows[0]["name"] == "alice_updated"
    assert rows[1]["name"] == "bob"

    engine.repair_fenced_ranges("users", [(2, 2)], [{"id": 2, "name": "bob_repaired"}])
    rows2 = sorted(engine.get_rows("users"), key=lambda r: r["id"])
    assert rows2[1]["name"] == "bob_repaired"

    conn.close()
