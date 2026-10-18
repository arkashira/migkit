from types import SimpleNamespace
import pytest
from migkit import verifiers
from migkit.verifiers import (
    CrossoverWatermarkFencer,
    FencingResult,
    crossover_watermark_fence,
    lsn_to_int,
    lsn_compare,
    lsn_gte,
    parse_gtid_set,
    gtid_is_subset,
)
from migkit.engines.postgres import PostgresEngine
from migkit.engines.mysql import MySQLEngine
from migkit.result_scope import PracticalProblem, DatabaseSummary, build_grouped_summary
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("verifiers", "crossover_watermark_rungs", "crossover_position_reached", "holds"),
    ("verifiers", "crossover_watermark_rungs", "crossover_position_reached", "refuses"),
    ("verifiers", "crossover_watermark_rungs", "candidate_recheck_convergence", "holds"),
    ("verifiers", "crossover_watermark_rungs", "candidate_recheck_convergence", "refuses"),
    ("verifiers", "crossover_watermark_rungs", "flush_wait_timeout_envelope", "holds"),
    ("verifiers", "crossover_watermark_rungs", "flush_wait_timeout_envelope", "refuses"),
    ("verifiers", "crossover_watermark_rungs", "benign_inflight_classification", "holds"),
    ("verifiers", "crossover_watermark_rungs", "benign_inflight_classification", "refuses"),
    ("verifiers", "crossover_watermark_rungs", "refuse_unfenced_candidate_drift", "holds"),
    ("verifiers", "crossover_watermark_rungs", "refuse_unfenced_candidate_drift", "refuses"),
]


def test_lsn_comparison_helpers():
    assert lsn_to_int("0/19EB540") == 0x19EB540
    assert lsn_to_int("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert lsn_to_int(12345) == 12345
    assert lsn_to_int("12345") == 12345
    assert lsn_to_int(None) is None

    assert lsn_compare("0/19EB540", "0/19EB660") == -1
    assert lsn_compare("0/19EB660", "0/19EB660") == 0
    assert lsn_compare("0/19EB700", "0/19EB660") == 1

    assert not lsn_gte("0/19EB540", "0/19EB660")
    assert lsn_gte("0/19EB660", "0/19EB660")
    assert lsn_gte("0/19EB700", "0/19EB660")


def test_gtid_comparison_helpers():
    uuid1 = "3E11FA47-71CA-11E1-9E33-C80AA9E29B45"
    uuid2 = "4F22EB58-82DB-22F2-AF44-D91BB0F3AC56"

    sub = f"{uuid1}:1-100"
    sup = f"{uuid1}:1-105"
    assert gtid_is_subset(sub, sup)
    assert not gtid_is_subset(sup, sub)

    sub_multi = f"{uuid1}:1-50:60-100"
    sup_multi = f"{uuid1}:1-120"
    assert gtid_is_subset(sub_multi, sup_multi)

    both_sub = f"{uuid1}:1-50,{uuid2}:1-20"
    both_sup = f"{uuid1}:1-100,{uuid2}:1-30"
    assert gtid_is_subset(both_sub, both_sup)
    assert not gtid_is_subset(both_sup, both_sub)


def test_crossover_watermark_fencing_convergence_postgres():
    l0 = "0/2000"
    candidate_keys = ["k1", "k2", "k3", "k4", "k5", "k6", "k7"]

    class MockPgEngine:
        def __init__(self):
            self.current_position = "0/1500"
            self.hop = SimpleNamespace(options={}, name="test-hop")

        def src_lsn(self, db):
            return l0

        def cdc_apply_position(self, db):
            return self.current_position

        def target_flush_lsn(self, db):
            return self.current_position

        def is_cdc_applied(self, db, lsn):
            return lsn_gte(self.current_position, lsn)

        def wait_cdc_flush(self, db, lsn, timeout=300):
            self.current_position = "0/2000"
            return True

        def fence_wait(self, db, lsn, timeout=300):
            return self.wait_cdc_flush(db, lsn, timeout)

        def _compare_pks(self, db, table, keys):
            if lsn_gte(self.current_position, l0):
                return [], [], []
            return list(keys), [], []

    eng = MockPgEngine()
    fencer = CrossoverWatermarkFencer(eng, "orders_db", "orders")
    captured = fencer.capture_l0()
    assert captured == l0

    res = fencer.fence("orders", candidate_keys, l0=captured)
    assert isinstance(res, FencingResult)
    assert res.status == "ok"
    assert res.defect_class == "in_flight_replication_write"
    assert res.tier == "benign"
    assert sorted(res.converged) == sorted(candidate_keys)
    assert len(res.defects) == 0


def test_crossover_watermark_fencing_persistent_defects_postgres():
    l0 = "0/3000"
    candidate_keys = ["k1", "k2", "k3", "k4", "k5"]

    class MockPgEngine:
        def __init__(self):
            self.pos = "0/3500"
            self.hop = SimpleNamespace(options={}, name="test-hop")

        def src_lsn(self, db):
            return l0

        def target_flush_lsn(self, db):
            return self.pos

        def is_cdc_applied(self, db, lsn):
            return lsn_gte(self.pos, lsn)

        def fence_wait(self, db, lsn, timeout=300):
            return True

        def _compare_pks(self, db, table, keys):
            return ["k4"], ["k5"], []

    eng = MockPgEngine()
    res = crossover_watermark_fence(eng, "inventory_db", "inventory", candidate_keys, l0=l0)
    assert res.status == "diff"
    assert res.defect_class == "application_defect"
    assert sorted(res.converged) == ["k1", "k2", "k3"]
    assert sorted(res.defects) == ["k4", "k5"]
    assert res.classifications["k1"] == "in_flight_replication_write"
    assert res.classifications["k4"] == "application_defect"


def test_crossover_watermark_fencing_mysql_gtid():
    uuid = "3E11FA47-71CA-11E1-9E33-C80AA9E29B45"
    l0 = f"{uuid}:1-100"
    candidate_keys = [f"row_{i}" for i in range(1, 8)]

    class MockMySqlEngine:
        def __init__(self):
            self.current_gtid = f"{uuid}:1-90"
            self.hop = SimpleNamespace(options={}, name="mysql-hop")

        def src_lsn(self, db):
            return l0

        def cdc_apply_position(self, db):
            return self.current_gtid

        def target_flush_gtid(self, db):
            return self.current_gtid

        def is_cdc_applied(self, db, gtid):
            return gtid_is_subset(gtid, self.current_gtid)

        def fence_wait(self, db, gtid, timeout=300):
            self.current_gtid = f"{uuid}:1-110"
            return True

        def _compare_pks(self, db, table, keys):
            if gtid_is_subset(l0, self.current_gtid):
                return [], [], []
            return list(keys), [], []

    eng = MockMySqlEngine()
    fencer = CrossoverWatermarkFencer(eng, "shop", "orders")
    res = fencer.fence("orders", candidate_keys, l0=l0)

    assert res.status == "ok"
    assert res.defect_class == "in_flight_replication_write"
    assert res.tier == "benign"
    assert len(res.converged) == 7
    assert len(res.defects) == 0


def test_postgres_engine_crossover_methods():
    hop = SimpleNamespace(options={}, name="test-hop", workers=1, report_dir=lambda db: None)
    eng = PostgresEngine(hop)
    assert hasattr(eng, "capture_watermark")
    assert hasattr(eng, "cdc_apply_position")
    assert hasattr(eng, "target_flush_lsn")
    assert hasattr(eng, "is_cdc_applied")
    assert hasattr(eng, "crossover_watermark_fence")

    eng.src_lsn = lambda db: "0/3000"
    eng.applied_lsn = lambda db: "0/3000"
    eng.fence_wait = lambda db, lsn, timeout=300: True
    eng._compare_pks = lambda db, tbl, keys: ([], [], [])

    res = eng.crossover_watermark_fence("db1", "tbl1", ["k1", "k2"])
    assert res.status == "ok"
    assert res.converged == ["k1", "k2"]
    assert res.defect_class == "in_flight_replication_write"


def test_mysql_engine_crossover_methods():
    hop = SimpleNamespace(options={}, name="test-hop", workers=1, report_dir=lambda db: None)
    eng = MySQLEngine(hop)
    assert hasattr(eng, "capture_watermark")
    assert hasattr(eng, "cdc_apply_position")
    assert hasattr(eng, "target_flush_gtid")
    assert hasattr(eng, "is_cdc_applied")
    assert hasattr(eng, "crossover_watermark_fence")

    uuid = "3E11FA47-71CA-11E1-9E33-C80AA9E29B45"
    eng.src_lsn = lambda db: f"{uuid}:1-50"
    eng._brands = lambda: [SimpleNamespace(name="mysql"), SimpleNamespace(name="mysql")]
    eng._q = lambda side, sql, args=None: [[1]] if "gtid_subset" in sql else [[f"{uuid}:1-50"]]
    eng.fence_wait = lambda db, gtid, timeout=300: True
    eng._compare_pks = lambda db, tbl, keys: ([], [], [])

    res = eng.crossover_watermark_fence("db1", "tbl1", ["k1"])
    assert res.status == "ok"
    assert res.converged == ["k1"]
    assert res.defect_class == "in_flight_replication_write"


def test_result_scope_benign_classification():
    from migkit.result_scope import classify_care
    r_converged = {
        "check": "data",
        "category": "data",
        "scope": "orders_db.orders",
        "status": "diff",
        "detail": "all diffs proven in_flight_replication_write: watermark fenced to L0",
        "defect_class": "in_flight_replication_write",
    }
    hop = SimpleNamespace(options={}, name="hop1", scope_fingerprint=lambda: "fp")
    care = classify_care(r_converged, hop)
    assert care["tier"] == "benign"
    assert care["tier_badge"] == "BENIGN"
    assert care["care_level"] == "dont_care"
    assert not care["action_required"]

    envelope = build_grouped_summary([r_converged], hop=hop)
    problems = envelope.databases["orders_db"].practical_problems
    assert len(problems) == 1
    p = problems[0]
    assert p.defect_class == "in_flight_replication_write"
    assert p.tier == "benign"
    assert p.tier_badge == "BENIGN"
    assert p.care_level == "dont_care"
    assert not p.action_required


def test_concurrent_load_simulation():
    l0 = "0/50000"
    num_rows = 100
    candidate_keys = [f"txn_{i}" for i in range(num_rows)]

    class ActiveWorkerReplicationEngine:
        def __init__(self):
            self.current_lsn = "0/48000"
            self.step = 0
            self.hop = SimpleNamespace(options={}, name="prod-hop")

        def src_lsn(self, db):
            return l0

        def cdc_apply_position(self, db):
            return self.current_lsn

        def target_flush_lsn(self, db):
            return self.current_lsn

        def is_cdc_applied(self, db, lsn):
            return lsn_gte(self.current_lsn, lsn)

        def fence_wait(self, db, lsn, timeout=300):
            self.current_lsn = "0/52000"
            return True

        def _compare_pks(self, db, table, keys):
            if lsn_gte(self.current_lsn, l0):
                return [], [], []
            return list(keys)[:30], [], []

    eng = ActiveWorkerReplicationEngine()
    res = crossover_watermark_fence(eng, "trading_db", "trades", candidate_keys, l0=l0)
    assert res.status == "ok"
    assert len(res.converged) == num_rows
    assert len(res.defects) == 0
    assert res.defect_class == "in_flight_replication_write"
    assert res.tier == "benign"


def test_crossover_watermark_rungs_hold_and_refuse():
    with watched() as seen:
        c = verifiers.decide_crossover_watermark({"current_position": "0/2000", "l0": "0/1000"})
    assert c.rung.name == "crossover_position_reached"
    assert branch(seen, "crossover_position_reached") == [HOLDS]

    with watched() as seen:
        c = verifiers.decide_crossover_watermark({"current_position": "0/500", "l0": "0/1000", "converged": True, "defects": False})
    assert branch(seen, "crossover_position_reached") == [REFUSES]
    assert c.rung.name == "candidate_recheck_convergence"
    assert branch(seen, "candidate_recheck_convergence") == [HOLDS]

    with watched() as seen:
        c = verifiers.decide_crossover_watermark({
            "current_position": "0/500", "l0": "0/1000", "converged": False,
            "wait_time": 5.0, "timeout": 30.0, "flushed": True
        })
    assert branch(seen, "candidate_recheck_convergence") == [REFUSES]
    assert c.rung.name == "flush_wait_timeout_envelope"
    assert branch(seen, "flush_wait_timeout_envelope") == [HOLDS]

    with watched() as seen:
        c = verifiers.decide_crossover_watermark({
            "current_position": "0/500", "l0": "0/1000", "converged": False,
            "wait_time": 50.0, "timeout": 30.0, "all_candidates_converged": True
        })
    assert branch(seen, "flush_wait_timeout_envelope") == [REFUSES]
    assert c.rung.name == "benign_inflight_classification"
    assert branch(seen, "benign_inflight_classification") == [HOLDS]

    with watched() as seen:
        c = verifiers.decide_crossover_watermark({
            "current_position": "0/500", "l0": "0/1000", "converged": False,
            "wait_time": 50.0, "timeout": 30.0, "all_candidates_converged": False,
            "defects": True
        })
    assert branch(seen, "benign_inflight_classification") == [REFUSES]
    assert c.rung.name == "refuse_unfenced_candidate_drift"
    assert branch(seen, "refuse_unfenced_candidate_drift") == [HOLDS]

    with watched() as seen:
        try:
            verifiers.decide_crossover_watermark({
                "current_position": "0/500", "l0": "0/1000", "converged": False,
                "wait_time": 50.0, "timeout": 30.0, "all_candidates_converged": False,
                "defects": False, "flushed": True
            })
        except Exception:
            pass
    assert branch(seen, "refuse_unfenced_candidate_drift") == [REFUSES]


def test_absent_compare_pks_does_not_false_ok():
    class DummyEngine:
        def src_lsn(self, db):
            return "0/1000"
        def is_cdc_applied(self, db, lsn):
            return True

    eng = DummyEngine()
    fencer = CrossoverWatermarkFencer(eng, "db", "tbl")
    res = fencer.fence("tbl", ["k1", "k2"])
    assert res.status == "diff"
    assert res.defects == ["k1", "k2"]
    assert res.defect_class == "application_defect"


def test_dynamic_fence_timeout_resolution(monkeypatch):
    monkeypatch.setenv("MIGKIT_FENCE_TIMEOUT", "45.0")
    assert verifiers.get_default_fence_timeout() == 45.0

