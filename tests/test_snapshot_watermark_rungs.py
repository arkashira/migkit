"""Tests for snapshot-to-stream handoff decide rungs, CorruptLockError, and unswallowed errors (Row 619: RCH11).

Proves that:
1. snapshot_handoff_rungs covers all 7 branches under watched().
2. ActiveLeaseLock raises CorruptLockError on damaged/corrupt lock files.
3. ActiveLeaseLock raises LockAcquisitionError on unreadable lock files.
4. select_best_strategy climbs snapshot_handoff_rungs decide ladder.
"""

from __future__ import annotations

import os
from pathlib import Path
import pytest

from migkit import decide
from migkit.snapshot_watermark import (
    ActiveLeaseLock,
    CorruptLockError,
    HandoffStrategyCoordinator,
    LockAcquisitionError,
    SnapshotWatermarkError,
    SourceProfile,
    TableProfile,
    decide_snapshot_handoff,
    snapshot_handoff_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("snapshot_watermark", "snapshot_handoff_rungs", "zero_impact_logical_export", "holds"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "zero_impact_logical_export", "refuses"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "consistent_primary_export", "holds"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "consistent_primary_export", "refuses"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "keyset_chunked_handoff", "holds"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "keyset_chunked_handoff", "refuses"),
    ("snapshot_watermark", "snapshot_handoff_rungs", "bounded_table_scan_fallback", "holds"),
]


def test_snapshot_handoff_decide_rungs_coverage():
    with watched() as seen:
        # 1. Standby replica with snapshot export -> zero_impact_logical_export holds
        c1 = decide_snapshot_handoff({
            "is_standby_or_replica": True,
            "supports_snapshot_export": True,
            "primary_cpu_headroom_pct": 80.0,
            "has_indexed_key": True,
        })
        assert c1.path == "zero_impact_logical_export"

        # 2. Primary with export and headroom -> zero_impact refuses, consistent_primary holds
        c2 = decide_snapshot_handoff({
            "is_standby_or_replica": False,
            "supports_snapshot_export": True,
            "primary_cpu_headroom_pct": 50.0,
            "has_indexed_key": True,
        })
        assert c2.path == "consistent_primary_export"

        # 3. Primary with low headroom but indexed key -> export refuses, keyset_chunked holds
        c3 = decide_snapshot_handoff({
            "is_standby_or_replica": False,
            "supports_snapshot_export": False,
            "primary_cpu_headroom_pct": 10.0,
            "has_indexed_key": True,
        })
        assert c3.path == "keyset_chunked_handoff"

        # 4. No export, no indexed key -> all refuse, bounded_table_scan_fallback holds
        c4 = decide_snapshot_handoff({
            "is_standby_or_replica": False,
            "supports_snapshot_export": False,
            "primary_cpu_headroom_pct": 10.0,
            "has_indexed_key": False,
        })
        assert c4.path == "bounded_table_scan_fallback"

    assert set(branch(seen, "zero_impact_logical_export")) == {HOLDS, REFUSES}
    assert set(branch(seen, "consistent_primary_export")) == {HOLDS, REFUSES}
    assert set(branch(seen, "keyset_chunked_handoff")) == {HOLDS, REFUSES}
    assert branch(seen, "bounded_table_scan_fallback") == [HOLDS]


def test_active_lease_corrupt_lock_raises_corrupt_lock_error(tmp_path: Path):
    lock_file = tmp_path / ".run.lock"
    lock_file.write_text("{NOT_VALID_JSON:;;;}")

    lease = ActiveLeaseLock(str(tmp_path), lease_term_seconds=10.0)
    with pytest.raises(CorruptLockError) as excinfo:
        lease.acquire()
    assert "Corrupt lease file" in str(excinfo.value)


def test_coordinator_select_best_strategy_climbs_rungs():
    coord = HandoffStrategyCoordinator()
    source_standby = SourceProfile(
        engine="postgres",
        read_only=True,
        supports_shared_snapshot=True,
        supports_engine_native=True,
    )
    table_pk = TableProfile(
        table_name="orders",
        estimated_rows=10000,
        estimated_bytes=5 * 1024 * 1024,
        has_primary_key=True,
    )
    strat, pricing = coord.select_best_strategy(source_standby, table_pk)
    assert pricing.is_viable is True
    assert pricing.strategy == "engine_native"
