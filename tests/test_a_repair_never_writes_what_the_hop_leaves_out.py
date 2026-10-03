"""A repair never writes into what the hop leaves out.

migkit never writes to the source, and on the target it writes only into
what the hop takes: what it leaves out is the target's own. Every repair
builder read the catalogue or the last check's files its own way, and
several read past the scope:

* the row repair of MySQL, SQL Server and MongoDB, and the neutral one,
  read every drilldown file the last check left; a check run before
  `exclude: [audit_log]` was added left `data-audit_log.extra`, and
  `sync --kind rows --apply` deleted the target's own rows. A file for a
  table the hop no longer takes is now left alone, and the plan says so;
* PostgreSQL's text repair read every text column of the target and,
  with MIGKIT_REPAIR_TEXT, updated tables the hop leaves out;
* the constraint repairs validated (PostgreSQL) and enforced (MySQL) the
  checks of tables the hop leaves out;
* MySQL's event repair made every source event on the target, and its
  grant check and repair replayed grants on tables the hop leaves out;
  PostgreSQL's compared a routine's grants and a sequence's by its own
  name, never by the table it feeds.

Each now reads through the engine's one scope answer (`Engine.in_scope`,
`_drills_in_scope`; backlog P0-SCOPE.6b). Pure: no server is asked.
"""
import json
import types

from migkit.config import Endpoint, Hop
from migkit.engines.base import LEFT_OUT, RepairAction, Result

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.repair", ("postgres", "mysql", "mongodb", "mssql", "redis",
                      "hetero", "generic")),
]


def _hop(engine, tmp_path, **kw):
    hop = Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
              databases=["appdb"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _drills(tmp_path, *names):
    for name in names:
        (tmp_path / f"data-{name}").write_text("")


def _said(plan):
    return [a for a in plan if a.kind == LEFT_OUT]


# ---- the drilldown a check left under another scope ------------------------

def test_a_drilldown_of_a_table_left_out_is_said_and_never_acted_on(
        tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path, exclude=["audit_log"]))
    taken, said = eng._drills_in_scope("appdb", ["public.orders",
                                                 "public.audit_log"])
    assert taken == ["public.orders"]
    assert said.kind == LEFT_OUT and not said.statements and not said.undo
    assert "public.audit_log" in said.note and "no longer takes" in said.note
    # nothing left out, nothing said
    assert eng._drills_in_scope("appdb", ["public.orders"]) == (
        ["public.orders"], None)


def test_postgres_rows(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _drills(tmp_path, "public.audit_log.extra", "public.orders.missing")
    eng = PostgresEngine(_hop("postgres", tmp_path, exclude=["audit_log"]))
    eng._psql = lambda side, db, sql, **_: ""
    eng._all_tables = lambda side, db: ["public.orders", "public.audit_log"]
    plan = eng.repair_plan("appdb", "rows")
    assert [a.statements for a in plan if a.kind == "rows"] == [
        ["resync-rows public.orders"]]
    assert "public.audit_log" in _said(plan)[0].note


def test_mysql_rows(tmp_path, monkeypatch):
    """The scenario itself: a check before `exclude: [audit_log]` left
    `data-audit_log.extra`, and the resync deleted the target's rows."""
    from migkit.engines import mysql
    monkeypatch.setattr(mysql, "which", lambda name: None)
    _drills(tmp_path, "audit_log.extra", "orders.missing")
    eng = mysql.MySQLEngine(_hop("mysql", tmp_path, exclude=["audit_log"]))
    eng._q = lambda side, sql, args=None, fresh=False: []
    plan = eng.repair_plan("appdb", "rows")
    rows = [a for a in plan if a.kind == "rows"]
    assert [a.table for a in rows] == ["orders"]
    assert all("audit_log" not in s for a in rows for s in a.statements)
    assert "audit_log" in _said(plan)[0].note


def test_sql_server_rows(tmp_path):
    from migkit.engines.mssql import MSSQLEngine
    _drills(tmp_path, "dbo.audit_log.extra", "dbo.orders.missing")
    (tmp_path / "data-dbo.orders.missing").write_text("1\n")
    (tmp_path / "data-dbo.audit_log.extra").write_text("7\n")
    eng = MSSQLEngine(_hop("mssql", tmp_path, exclude=["dbo.audit_log"]))
    plan = eng._rows_found_plan("appdb")
    assert [a.table for a in plan if a.kind == "rows"] == ["dbo.orders"]
    assert "dbo.audit_log" in _said(plan)[0].note


def test_mongodb_rows(tmp_path):
    from migkit.engines.mongodb import MongoEngine
    _drills(tmp_path, "audit.extra", "orders.missing")
    eng = MongoEngine(_hop("mongodb", tmp_path, exclude=["audit"]))
    plan = eng.repair_plan("appdb", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    assert "audit" in _said(plan)[0].note


def test_redis_keys(tmp_path):
    """A key pattern excluded since the check is the target's own: its
    keys are neither restored nor deleted, by the plan, the repair or the
    way a repair climbs (`repair_found`)."""
    from migkit.engines.redis import RedisEngine
    (tmp_path / "data-db0.extra").write_text(
        '"session:1"\n"session:2"\n"orders:9"\n')
    (tmp_path / "data-db0.missing").write_text('"orders:1"\n')
    eng = RedisEngine(_hop("redis", tmp_path, exclude=["0.session:*"]))
    plan = eng.repair_plan("0", "rows")
    rows = [a for a in plan if a.kind == "rows"]
    assert rows and all("session" not in s for s in rows[0].statements)
    assert "session:1" in _said(plan)[0].note
    assert eng.repair_found("0", "db0")["extra"] == [("orders:9",)]
    # a hop that leaves nothing out reads every key, as before
    whole = RedisEngine(_hop("redis", tmp_path))
    assert not _said(whole.repair_plan("0", "rows"))
    assert len(whole.repair_found("0", "db0")["extra"]) == 3


def test_the_neutral_rows_plan(tmp_path):
    """The plan of every engine that keeps the neutral drilldown - a pair
    of engines, SQLite, the generic one."""
    from migkit.engines.generic import GenericEngine
    (tmp_path / "data-public.orders.missing").write_text('["1"]\n')
    (tmp_path / "data-public.audit_log.extra").write_text('["7"]\n')
    eng = GenericEngine(_hop("generic", tmp_path, exclude=["audit_log"],
                             options={"tables": ["public.orders"]}))
    plan = eng._rows_plan("appdb", "from the source to the target")
    assert [a.table for a in plan if a.kind == "rows"] == ["public.orders"]
    assert "public.audit_log" in _said(plan)[0].note


def test_a_pairs_drilldown_is_filed_by_its_leaf_and_read_as_the_source_names_it(  # noqa: E501
        tmp_path):
    """A pair files a drilldown under the table's own name (`orders`),
    where the hop names it as its source does (`appdb.public.orders`):
    read as itself it was a table the hop does not take, and its repair
    would have been left out."""
    from migkit.engines.hetero import HeteroEngine
    (tmp_path / "data-orders.missing").write_text('["1"]\n')
    (tmp_path / "data-audit_log.extra").write_text('["7"]\n')
    eng = HeteroEngine(_hop("hetero", tmp_path,
                            include=["appdb.public.orders"],
                            options={"source_engine": "postgres",
                                     "target_engine": "mysql"}))
    eng.src_engine.neutral_tables = lambda side, db: ["public.orders"]
    plan = eng._rows_plan("appdb", "from the source to the target")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    assert "audit_log" in _said(plan)[0].note


def test_a_hop_that_takes_everything_plans_every_drilldown_as_before(
        tmp_path, monkeypatch):
    from migkit.engines import mysql
    monkeypatch.setattr(mysql, "which", lambda name: None)
    _drills(tmp_path, "audit_log.extra", "orders.missing")
    eng = mysql.MySQLEngine(_hop("mysql", tmp_path))
    eng._q = lambda side, sql, args=None, fresh=False: []
    plan = eng.repair_plan("appdb", "rows")
    assert not _said(plan)
    assert sorted(a.table for a in plan) == ["audit_log", "orders"]


def test_sync_prints_what_it_leaves_out_and_applies_nothing_of_it(tmp_path):
    from migkit import cli

    class Eng:
        def apply(self, db, action):
            raise AssertionError("a left-out table was acted on")
    said = RepairAction("appdb", LEFT_OUT, [], [], "audit_log left out")
    hop = _hop("mysql", tmp_path)
    cli._apply_actions(hop, Eng(), "appdb", [said], True, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_a_seed_catching_up_says_it_and_applies_nothing_of_it(monkeypatch):
    from migkit import approvals, ship
    monkeypatch.setattr(approvals, "require", lambda *a, **kw: None)
    checks = iter([[Result("data", "appdb.orders", "diff")], []])
    log = []

    class Eng:
        def check_data(self, db):
            return next(checks)

        def repair_plan(self, db, kind):
            return [RepairAction(db, LEFT_OUT, [], [], "audit_log left out")]

        def apply(self, db, action):
            raise AssertionError("a left-out table was acted on")
    w = types.SimpleNamespace(eng=Eng(), db="appdb", hop=None, log=log.append)
    ship._delta_by_the_leaves(w)
    assert any("audit_log left out" in line for line in log), log


# ---- PostgreSQL ------------------------------------------------------------

def test_postgres_validates_only_the_constraints_of_tables_taken(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path, exclude=["event_log"]))

    def psql(side, db, sql, **_):
        if "convalidated" in sql and side == "dst":
            return ("public.orders\x1forders\x1fck_a\x1fCHECK (a > 0) NOT VALID"
                    "\npublic.event_log\x1fevent_log\x1fck_b\x1f"
                    "CHECK (b > 0) NOT VALID")
        return ""
    eng._psql = psql
    act = eng._constraint_repair("appdb")
    assert act.statements == ['ALTER TABLE orders VALIDATE CONSTRAINT "ck_a";']
    assert all("event_log" not in s for s in act.undo)


def test_postgres_the_text_repair_updates_only_tables_taken(tmp_path,
                                                            monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setenv("MIGKIT_REPAIR_TEXT", "1")
    eng = PostgresEngine(_hop("postgres", tmp_path, exclude=["event_log"]))
    broken = "cafÃ©"

    def psql(side, db, sql, **_):
        if sql == PostgresEngine.TEXT_COLUMNS:
            return "public.orders\tnote\npublic.event_log\tbody"
        if "octet_length" in sql and "orders" in sql:
            return json.dumps({"id": 1, "note": broken})
        if "octet_length" in sql and "event_log" in sql:
            return json.dumps({"id": 1, "body": broken})
        return ""
    eng._psql = psql
    eng._pk_cols_of = lambda db, table, side="src": ["id"]
    act = eng._mojibake_repair("appdb")
    assert act.statements, act.note
    assert all("event_log" not in s for s in act.statements + act.undo)
    assert any("orders" in s for s in act.statements)


def test_postgres_grants_of_what_is_left_out_are_not_replayed(tmp_path):
    """A table's, its column's, its sequence's and a routine's grant go
    with what they are on; the schema's and the database's stay."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path,
                              include=["appdb.public.orders"]))

    def grant(kind, obj, sch, name, col=""):
        return "\x1f".join(("app", kind, obj, col, "SELECT", "false", sch,
                            name))
    src = "\n".join([
        grant("TABLE", "public.orders", "public", "orders"),
        grant("TABLE", "public.event_log", "public", "event_log"),
        grant("COLUMN", "public.event_log", "public", "event_log", "body"),
        grant("SEQUENCE", "public.event_log_id_seq", "public",
              "event_log_id_seq"),
        grant("FUNCTION", "public.f_log()", "public", "f_log"),
        grant("SCHEMA", "public", "public", "")])

    def psql(side, db, sql, **_):
        if sql == "grants":
            return src if side == "src" else ""
        if "pg_roles" in sql:
            return "app"
        if sql == PostgresEngine.SEQ_OWNED_Q:
            return "public.event_log_id_seq|public|event_log|id"
        return ""
    eng._psql = psql
    eng._grants_sql = lambda side, db: "grants"
    miss, extra, smiss, seen, sseen = eng._grant_gaps("appdb")
    said = " ".join(miss + smiss)
    assert "public.orders" in said and "SCHEMA" in said
    for name in ("event_log", "f_log"):
        assert name not in said, (name, said)


# ---- MySQL -----------------------------------------------------------------

def _my(tmp_path, answer, **kw):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path, **kw))
    eng._q = lambda side, sql, args=None, fresh=False: answer(side, sql)
    return eng


def test_mysql_enforces_only_the_checks_of_tables_taken(tmp_path):
    def answer(side, sql):
        if "constraint_type='CHECK'" in sql:
            state = "YES" if side == "src" else "NO"
            return [("orders", "ck_a", state), ("audit_log", "ck_b", state)]
        return []
    act = _my(tmp_path, answer, exclude=["audit_log"])._constraint_repair(
        "appdb")
    assert act.statements == ["ALTER TABLE `orders` ALTER CHECK `ck_a`"
                              " ENFORCED;"]


def test_mysql_makes_only_the_events_the_hop_takes(tmp_path):
    def answer(side, sql):
        if side == "dst":
            return []
        if "information_schema.events" in sql:
            return [("ev_orders",), ("ev_purge_audit",)]
        if sql.startswith("show create event"):
            name = sql.rsplit(".", 1)[-1].strip("`")
            return [(name, "STRICT_TRANS_TABLES", "UTC",
                     f"CREATE EVENT `{name}` ON SCHEDULE EVERY 1 DAY"
                     " ENABLE DO select 1")]
        return []
    act = _my(tmp_path, answer, exclude=["ev_purge_audit"])._event_repair(
        "appdb")
    said = " ".join(act.statements + act.undo)
    assert "ev_orders" in said and "ev_purge_audit" not in said, said


def test_mysql_grants_on_tables_left_out_are_neither_compared_nor_replayed(
        tmp_path):
    eng = _my(tmp_path, lambda side, sql: [], exclude=["audit_log"])
    got = eng._grants_taken("appdb", {
        "app@%": {"GRANT SELECT ON <db>.* TO app@%",
                  "GRANT SELECT ON <db>.audit_log TO app@%",
                  "GRANT SELECT (a) ON <db>.orders TO app@%"},
        "auditor@%": {"GRANT INSERT ON <db>.audit_log TO auditor@%"}})
    # the auditor holds nothing the hop takes, and is not called missing
    assert got == {"app@%": {"GRANT SELECT ON <db>.* TO app@%",
                             "GRANT SELECT (a) ON <db>.orders TO app@%"}}

    held = {"src": {"app@%": {"GRANT SELECT ON <db>.orders TO app@%",
                              "GRANT SELECT ON <db>.audit_log TO app@%"}},
            "dst": {}}
    raw = {("app@%", g): g.replace("<db>", "`appdb`") + ";"
           for g in held["src"]["app@%"]}

    def grants_for_db(side, dbn):
        eng._grant_raw = {"src": raw}
        eng._grant_users = {"dst": {"app@%"}}
        return held[side], ""
    eng._grants_for_db = grants_for_db
    act = eng._grant_repair("appdb")
    assert len(act.statements) == 1 and "orders" in act.statements[0]
    check = eng._check_grants("appdb")
    assert "audit_log" not in check[0].detail, check[0].detail
