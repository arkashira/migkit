import pytest

from migkit import ratchet
from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine


class FakePostgresConn:
    def __init__(self, slots=None, sequences=None):
        self.slots = set(slots or [])
        self.sequences = dict(sequences or {})
        self.executed = []

    def cursor(self):
        return FakeCursor(self)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, params=None):
        self.last_query = query
        self.conn.executed.append(query)

    def fetchone(self):
        q = self.last_query.lower()
        if "from pg_replication_slots" in q:
            for s in self.conn.slots:
                if f"'{s}'" in q:
                    return (1,)
            return None
        if "pg_export_snapshot" in q:
            return ("00000003-0000002A-1",)
        return None


class FakePostgresEngine:
    engine = "postgres"

    def __init__(self, slots=None, seq_data=None):
        self.slots = set(slots or [])
        self.seq_data = dict(seq_data or {})
        self.executed = []

    def _repl_name(self):
        return "migkit_slot"

    def _psql(self, side, db, query):
        self.executed.append((side, db, query))
        q = query.lower()
        if "from pg_replication_slots" in q:
            for s in self.slots:
                if f"'{s}'" in q:
                    return "1"
            return ""
        if "from pg_sequences" in q:
            lines = []
            for name, vals in self.seq_data.items():
                val = vals.get(f"{side}_seq", 0)
                lines.append(f"{name}|{val}")
            return "\n".join(lines)
        if "setval(" in q:
            return "setval"
        return ""

    def _seq_owned(self, side, db):
        return {name: ("public", "tbl", "id") for name in self.seq_data}

    def _seq_col_max(self, side, db, owned):
        out = {}
        for name in owned:
            out[name] = self.seq_data.get(name, {}).get(f"{side}_max", 0)
        return out


class FakeMySQLEngine:
    engine = "mysql"

    def __init__(self, counters=None):
        self.counters = counters or ({}, {})
        self.executed = []

    def _counters(self, db):
        return self.counters

    def _q(self, side, query, *args, **kwargs):
        self.executed.append((side, query))
        return []


def test_verify_target_replication_slot_exists():
    eng = FakePostgresEngine(slots=["migkit_slot"])
    assert ratchet.verify_target_replication_slot(eng, "testdb", "migkit_slot") is True
    assert ratchet.verify_target_replication_slot(eng, "testdb") is True


def test_verify_target_replication_slot_missing():
    eng = FakePostgresEngine(slots=["other_slot"])
    assert ratchet.verify_target_replication_slot(eng, "testdb", "migkit_slot") is False


def test_authorize_snapshot_export_success():
    eng = FakePostgresEngine(slots=["migkit_slot"])
    assert ratchet.authorize_snapshot_export(eng, "testdb", "migkit_slot") is True


def test_authorize_snapshot_export_refuses_when_missing():
    eng = FakePostgresEngine(slots=[])
    with pytest.raises(ratchet.SlotMissingError) as exc:
        ratchet.authorize_snapshot_export(eng, "testdb", "migkit_slot")
    assert "missing on target" in str(exc.value)
    assert "refused" in str(exc.value)


def test_postgres_engine_export_snapshot_refuses_when_slot_missing():
    hop = Hop(
        name="test_hop",
        engine="postgres",
        source=Endpoint(host="127.0.0.1", port=5432, user="pg", password="pw"),
        target=Endpoint(host="127.0.0.1", port=5433, user="pg", password="pw"),
        databases=["db1"],
        options={"verify_slot_before_snapshot": True},
    )
    eng = PostgresEngine(hop)
    eng._psql = lambda side, db, q: ""
    with pytest.raises(ratchet.SlotMissingError):
        eng._export_snapshot("src", "db1", slot_name="app_slot", verify_target_slot=True)


def test_postgres_engine_export_snapshot_authorizes_when_slot_present():
    hop = Hop(
        name="test_hop",
        engine="postgres",
        source=Endpoint(host="127.0.0.1", port=5432, user="pg", password="pw"),
        target=Endpoint(host="127.0.0.1", port=5433, user="pg", password="pw"),
        databases=["db1"],
    )
    eng = PostgresEngine(hop)
    fake_conn = FakePostgresConn(slots=["app_slot"])
    eng._psql = lambda side, db, q: "1" if "app_slot" in q else ""
    eng._connect = lambda side, db: fake_conn
    conn, snap_id = eng._export_snapshot("src", "db1", slot_name="app_slot", verify_target_slot=True)
    assert snap_id == "00000003-0000002A-1"


def test_ratchet_source_max_greater():
    item = ratchet.calculate_ratchet(
        name="orders_id_seq",
        source_max=5000,
        target_max=2000,
        current_target_seq=2000,
        headroom=10000,
        engine="postgres",
    )
    assert item.greatest_max == 5000
    assert item.ratchet_target_seq == 15000
    assert item.has_adequate_headroom is False
    assert item.forward_ddl == "SELECT setval('orders_id_seq', 15000, true);"
    assert item.rollback_ddl == "SELECT setval('orders_id_seq', 2000, true);"


def test_ratchet_target_max_greater():
    item = ratchet.calculate_ratchet(
        name="orders_id_seq",
        source_max=3000,
        target_max=7000,
        current_target_seq=7000,
        headroom=10000,
        engine="postgres",
    )
    assert item.greatest_max == 7000
    assert item.ratchet_target_seq == 17000
    assert item.has_adequate_headroom is False
    assert item.forward_ddl == "SELECT setval('orders_id_seq', 17000, true);"
    assert item.rollback_ddl == "SELECT setval('orders_id_seq', 7000, true);"


def test_ratchet_stuck_at_zero():
    item = ratchet.calculate_ratchet(
        name="items_id_seq",
        source_max=25000,
        target_max=25000,
        current_target_seq=0,
        headroom=10000,
        engine="postgres",
    )
    assert item.greatest_max == 25000
    assert item.ratchet_target_seq == 35000
    assert item.has_adequate_headroom is False
    assert item.forward_ddl == "SELECT setval('items_id_seq', 35000, true);"
    assert item.rollback_ddl == "SELECT setval('items_id_seq', 1, false);"


def test_ratchet_already_has_headroom():
    item = ratchet.calculate_ratchet(
        name="orders_id_seq",
        source_max=1000,
        target_max=2000,
        current_target_seq=20000,
        headroom=10000,
        engine="postgres",
    )
    assert item.has_adequate_headroom is True
    assert item.forward_ddl == ""
    assert item.rollback_ddl == ""


def test_pre_syncback_cutover_check_refuses_on_deficit():
    sources = {"public.orders_id_seq": 1000}
    targets = {"public.orders_id_seq": 1000}
    current = {"public.orders_id_seq": 1000}
    plan = ratchet.plan_ratchet(sources, targets, current, engine="postgres", db="app")
    assert plan.refused is True
    assert len(plan.refusal_reasons) == 1
    assert "public.orders_id_seq" in plan.refusal_reasons[0]
    assert plan.items[0].ratchet_target_seq == 11000

    with pytest.raises(ratchet.HeadroomDeficitError):
        eng = FakePostgresEngine(seq_data={"public.orders_id_seq": {"src_seq": 1000, "dst_seq": 1000}})
        ratchet.pre_syncback_cutover_check(eng, "app", auto_ratchet=False, refuse_on_deficit=True)


def test_pre_syncback_cutover_check_auto_ratchet_applies_and_passes():
    eng = FakePostgresEngine(seq_data={"public.orders_id_seq": {"src_seq": 1000, "dst_seq": 1000}})
    plan = ratchet.pre_syncback_cutover_check(eng, "app", auto_ratchet=True)
    assert plan.refused is False
    assert plan.applied is True
    assert plan.items[0].has_adequate_headroom is True
    assert plan.items[0].current_target_seq == 11000
    assert len(eng.executed) > 0


def test_pre_syncback_cutover_check_mysql_auto_increment():
    eng = FakeMySQLEngine(counters=({"orders": 5000}, {"orders": 5000}))
    plan = ratchet.build_headroom_ratchet_plan(eng, "shop")
    assert plan.refused is True
    assert plan.items[0].ratchet_target_seq == 15000
    assert plan.items[0].forward_ddl == "ALTER TABLE `shop`.`orders` AUTO_INCREMENT = 15000;"
    assert plan.items[0].rollback_ddl == "ALTER TABLE `shop`.`orders` AUTO_INCREMENT = 5000;"

    plan.apply_forward(eng)
    assert plan.applied is True
    assert plan.refused is False
    assert any("AUTO_INCREMENT = 15000" in cmd[1] for cmd in eng.executed)


def test_reversible_rollback_ddl_script():
    sources = {"orders": 500, "line_items": 1000}
    targets = {"orders": 500, "line_items": 1000}
    current = {"orders": 500, "line_items": 1000}
    plan = ratchet.plan_ratchet(sources, targets, current, engine="mysql", db="shop")

    fwd_script = plan.generate_forward_ddl_script()
    rev_script = plan.generate_rollback_ddl_script()

    assert "ALTER TABLE `shop`.`line_items` AUTO_INCREMENT = 11000;" in fwd_script
    assert "ALTER TABLE `shop`.`orders` AUTO_INCREMENT = 10500;" in fwd_script

    assert "ALTER TABLE `shop`.`orders` AUTO_INCREMENT = 500;" in rev_script
    assert "ALTER TABLE `shop`.`line_items` AUTO_INCREMENT = 1000;" in rev_script

    rev_lines = [l for l in rev_script.splitlines() if l.startswith("ALTER")]
    assert "orders" in rev_lines[0]
    assert "line_items" in rev_lines[1]


def test_roundtrip_forward_and_rollback_restores_state():
    state = {"public.t1_id_seq": 100, "public.t2_id_seq": 0}

    class MockPgRunner:
        def _psql(self, side, db, sql):
            for line in sql.splitlines():
                line = line.strip()
                if not line or line.startswith("--"):
                    continue
                if "setval('public.t1_id_seq', 10100" in line:
                    state["public.t1_id_seq"] = 10100
                elif "setval('public.t1_id_seq', 100" in line:
                    state["public.t1_id_seq"] = 100
                elif "setval('public.t2_id_seq', 10000" in line:
                    state["public.t2_id_seq"] = 10000
                elif "setval('public.t2_id_seq', 1, false" in line:
                    state["public.t2_id_seq"] = 0

    runner = MockPgRunner()
    sources = {"public.t1_id_seq": 100, "public.t2_id_seq": 0}
    targets = {"public.t1_id_seq": 100, "public.t2_id_seq": 0}
    current = dict(state)
    plan = ratchet.plan_ratchet(sources, targets, current, engine="postgres", db="testdb")

    plan.apply_forward(runner)
    assert state["public.t1_id_seq"] == 10100
    assert state["public.t2_id_seq"] == 10000

    plan.apply_rollback(runner)
    assert state["public.t1_id_seq"] == 100
    assert state["public.t2_id_seq"] == 0


def test_cutover_reverse_rows_flags_unratcheted_headroom():
    from migkit import cutover

    hop = Hop(
        name="rev_hop",
        engine="postgres",
        source=Endpoint(host="127.0.0.1", port=5432, user="pg", password="pw"),
        target=Endpoint(host="127.0.0.1", port=5433, user="pg", password="pw"),
        databases=["db1"],
        options={"reverse": "at_cutover"},
    )
    eng = FakePostgresEngine(seq_data={"public.orders_seq": {"src_seq": 100, "dst_seq": 100}})
    rows = cutover._reverse_rows(hop, eng, "db1", "fail")
    hit = [r for r in rows if "pre-syncback headroom" in r[1]]
    assert hit, rows
    assert hit[0][0] == "fail"
    assert "public.orders_seq" in hit[0][1]


PROVES_RUNGS = [
    ("ratchet", "headroom_ratchet_rungs", "adequate_headroom_verified", "holds"),
    ("ratchet", "headroom_ratchet_rungs", "adequate_headroom_verified", "refuses"),
    ("ratchet", "headroom_ratchet_rungs", "automated_forward_ratchet_applied", "holds"),
    ("ratchet", "headroom_ratchet_rungs", "automated_forward_ratchet_applied", "refuses"),
    ("ratchet", "headroom_ratchet_rungs", "replication_slot_pre_snapshot_verified", "holds"),
    ("ratchet", "headroom_ratchet_rungs", "replication_slot_pre_snapshot_verified", "refuses"),
    ("ratchet", "headroom_ratchet_rungs", "reversible_rollback_script_generation", "holds"),
    ("ratchet", "headroom_ratchet_rungs", "reversible_rollback_script_generation", "refuses"),
    ("ratchet", "headroom_ratchet_rungs", "refuse_unratcheted_cutover_deficit", "holds"),
    ("ratchet", "headroom_ratchet_rungs", "refuse_unratcheted_cutover_deficit", "refuses"),
]

from tests.rung_branches import branch, watched, HOLDS, REFUSES


def test_headroom_ratchet_decide_ladder_all_branches():
    with watched() as seen:
        c = ratchet.decide_headroom_ratchet({
            "current_seq": 25000,
            "greatest_max": 10000,
            "headroom": 10000,
        })
    assert branch(seen, "adequate_headroom_verified") == [HOLDS]
    assert c.rung.name == "adequate_headroom_verified"

    with watched() as seen:
        c = ratchet.decide_headroom_ratchet({
            "deficit_detected": True,
            "forward_applied": True,
        })
    assert branch(seen, "adequate_headroom_verified") == [REFUSES]
    assert branch(seen, "automated_forward_ratchet_applied") == [HOLDS]
    assert c.rung.name == "automated_forward_ratchet_applied"

    with watched() as seen:
        c = ratchet.decide_headroom_ratchet({
            "deficit_detected": True,
            "forward_failed": True,
            "slot_verified": True,
        })
    assert branch(seen, "automated_forward_ratchet_applied") == [REFUSES]
    assert branch(seen, "replication_slot_pre_snapshot_verified") == [HOLDS]
    assert c.rung.name == "replication_slot_pre_snapshot_verified"

    with watched() as seen:
        c = ratchet.decide_headroom_ratchet({
            "deficit_detected": True,
            "forward_failed": True,
            "slot_missing": True,
            "has_rollback_script": True,
        })
    assert branch(seen, "replication_slot_pre_snapshot_verified") == [REFUSES]
    assert branch(seen, "reversible_rollback_script_generation") == [HOLDS]
    assert c.rung.name == "reversible_rollback_script_generation"

    with watched() as seen:
        c = ratchet.decide_headroom_ratchet({
            "deficit_detected": True,
            "forward_failed": True,
            "slot_missing": True,
            "rollback_omitted": True,
            "explicit_deficit_override": True,
        })
    assert branch(seen, "reversible_rollback_script_generation") == [REFUSES]
    assert branch(seen, "refuse_unratcheted_cutover_deficit") == [HOLDS]
    assert c.rung.name == "refuse_unratcheted_cutover_deficit"

    with watched() as seen:
        try:
            ratchet.decide_headroom_ratchet({
                "deficit_detected": True,
                "forward_failed": True,
                "slot_missing": True,
                "rollback_omitted": True,
                "unratcheted_refused": True,
            })
        except Exception:
            pass
    assert branch(seen, "refuse_unratcheted_cutover_deficit") == [REFUSES]


def test_dynamic_headroom_getter(monkeypatch):
    monkeypatch.setenv("MIGKIT_HEADROOM_RATCHET", "25000")
    assert ratchet.get_default_headroom() == 25000


def test_build_headroom_ratchet_plan_refuses_on_exception():
    class FailingEngine:
        engine = "postgres"

        def _psql(self, side, db, q):
            raise RuntimeError("database unreachable")

    plan = ratchet.build_headroom_ratchet_plan(FailingEngine(), "mydb")
    assert plan.refused is True
    assert any("database unreachable" in r for r in plan.refusal_reasons)

