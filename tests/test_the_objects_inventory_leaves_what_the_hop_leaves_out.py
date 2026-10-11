"""The objects inventory holds only what the hop takes.

`objects.json` compared every object of both databases. On MySQL only an
excluded table's indexes were left out: with `exclude: [audit_log]` and
`audit_log` on the target alone, every check said "table extra:
audit_log", with its trigger beside it, and the views, routines and
events were never scoped at all. On PostgreSQL a table the hop leaves out
was left out with its indexes, constraints and triggers, but the views,
materialized views, sequences and functions of what it leaves out were
compared - and the schema fix, told to leave out only tables, made them
on the target. On MongoDB the indexes of an excluded collection were.

Now each object is read the way the hop names it: a table, view,
routine or event by its own name (an object named in `include` or
`exclude`), a trigger or an index by its table, a sequence by the table
whose column it feeds (backlog P0-SCOPE.6b).

Pure: each engine's catalogue reads are answered in-process.
"""
import json

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.objects", ("postgres", "mysql")),
    ("verify.structure.objects", ("postgres", "mysql", "mongodb")),
]


def _hop(engine, tmp_path, **kw):
    hop = Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
              databases=["appdb"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


# ---- PostgreSQL ------------------------------------------------------------

PG_HELD = ["table|public.orders", "table|public.event_log",
           "view|public.orders_v", "view|public.event_v",
           "matview|public.event_mv",
           "sequence|public.orders_id_seq", "sequence|public.event_log_id_seq",
           "function|public.f_orders(integer)", "function|public.f_log()",
           "trigger|public.event_log.trg_log",
           "pk|public.event_log.event_log_pkey",
           "index|public.event_log_pkey", "pk|public.orders.orders_pkey",
           "index|public.orders_pkey", "extension|plpgsql"]

OWNED = ("public.orders_id_seq|public|orders|id\n"
         "public.event_log_id_seq|public|event_log|id")


def _pg(tmp_path, held, **kw):
    """`held` {side: inventory lines}: the target is the source, less the
    lines it does not have."""
    from migkit.engines.postgres import INVENTORY_SQL, PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path, **kw))

    def psql(side, db, sql, **_):
        if sql == INVENTORY_SQL:
            return "\n".join(held[side])
        if sql == PostgresEngine.SEQ_OWNED_Q:
            return OWNED
        if "indexrelid" in sql and " in (" in sql:
            return "\n".join(n.split("|")[1] for n in held[side]
                             if n.startswith("index|public.event_log"))
        return ""
    eng._psql = psql
    eng._all_tables = lambda side, db: [
        n.split("|")[1] for n in held[side] if n.startswith("table|")]
    return eng


TAKEN = ["appdb.public.orders", "appdb.public.orders_v",
         "appdb.public.f_orders"]


def test_postgres_what_hangs_off_a_table_left_out_is_not_compared(tmp_path):
    """The source holds the event log, its view, materialized view,
    sequence and function; the target holds none of it. A hop that takes
    the orders and their own view and function has nothing missing."""
    held = {"src": PG_HELD,
            "dst": [n for n in PG_HELD if "event" not in n
                    and "f_log" not in n]}
    got = _pg(tmp_path, held, include=TAKEN).check_objects("appdb")
    assert got.status == "ok", got.detail
    inv = json.loads((tmp_path / "objects.json").read_text())
    said = json.dumps(inv)
    for name in ("event_v", "event_mv", "event_log_id_seq", "f_log"):
        assert name not in said, (name, said)
    # what the hop takes is still held to the source
    assert inv["view"]["src"] == 1 and inv["sequence"]["src"] == 1
    assert inv["function"]["src"] == 1


def test_postgres_a_view_the_hop_takes_that_is_missing_is_still_missing(
        tmp_path):
    held = {"src": PG_HELD,
            "dst": [n for n in PG_HELD if n != "view|public.orders_v"]}
    got = _pg(tmp_path, held, include=TAKEN).check_objects("appdb")
    assert got.status == "diff" and "orders_v" in got.detail, got.detail


def test_postgres_a_hop_that_takes_everything_compares_every_object(
        tmp_path):
    held = {"src": PG_HELD,
            "dst": [n for n in PG_HELD if n != "view|public.event_v"]}
    got = _pg(tmp_path, held).check_objects("appdb")
    assert got.status == "diff" and "event_v" in got.detail, got.detail


def test_postgres_the_schema_fix_leaves_out_the_objects_the_hop_does(
        tmp_path):
    """The DDL a schema repair applies is the schema comparison's; told to
    leave out only tables, it made the views and functions of what the
    hop leaves out on the target."""
    held = {"src": PG_HELD, "dst": PG_HELD}
    eng = _pg(tmp_path, held, include=TAKEN)
    left = eng._schema_excludes("appdb")
    for name in ("public.event_log", "public.event_v", "public.event_mv",
                 "public.event_log_id_seq", "public.f_log"):
        assert name in left, (name, left)
    for name in ("public.orders", "public.orders_v", "public.orders_id_seq",
                 "public.f_orders"):
        assert name not in left, (name, left)
    # and the structural differ and the dump leave the same out
    assert ("public", "event_v") in eng._objects_not_taken("src", "appdb")


def test_postgres_a_hop_that_takes_everything_asks_nothing_more(tmp_path):
    asked = []
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path))
    eng._psql = lambda side, db, sql, **_: asked.append(sql) or ""
    assert eng._objects_not_taken("src", "appdb") == set()
    assert asked == []


# ---- MySQL -----------------------------------------------------------------

MY_HELD = {
    "table": [("orders", "orders"), ("audit_log", "audit_log")],
    "view": [("v_orders", "v_orders"), ("v_audit", "v_audit")],
    "function": [("f_audit", "f_audit")],
    "procedure": [("p_orders", "p_orders")],
    "trigger": [("trg_orders", "orders"), ("trg_audit", "audit_log")],
    "event": [("ev_purge_audit", "ev_purge_audit")],
    "index": [("orders.PRIMARY", "orders"),
              ("audit_log.ix_owner", "audit_log")],
}


def _my(tmp_path, only_on_target, **kw):
    """The target holds everything; the source lacks `only_on_target`."""
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path, **kw))
    queries = {q: k for k, q in MySQLEngine.OBJECT_QUERIES.items()}

    def q(side, sql, args=None, fresh=False):
        kind = queries.get(sql)
        if kind is None:
            return []
        return [r for r in MY_HELD[kind] if side == "dst"
                or not any(o in (r[0], r[1]) for o in only_on_target)]
    eng._q = q
    eng._all_tables = lambda side, db: [
        t for t, _ in MY_HELD["table"]
        if side == "dst" or t not in only_on_target]
    return eng


AUDIT = ("audit_log", "v_audit", "f_audit", "ev_purge_audit")


def test_mysql_a_table_the_hop_excludes_on_the_target_alone_is_not_extra(
        tmp_path):
    """The audit log is the target's own, with its trigger, its view, its
    function and its purge event. Named in `exclude`, none is extra."""
    eng = _my(tmp_path, AUDIT, exclude=list(AUDIT))
    got = eng.check_objects("appdb")
    assert got.status == "ok", got.detail
    said = (tmp_path / "objects.json").read_text()
    for name in ("audit_log", "trg_audit", "v_audit", "f_audit",
                 "ev_purge_audit", "ix_owner"):
        assert name not in said, (name, said)


def test_mysql_the_same_target_without_exclude_is_different(tmp_path):
    got = _my(tmp_path, AUDIT).check_objects("appdb")
    assert got.status == "diff", got.detail
    for name in ("audit_log", "v_audit", "ev_purge_audit", "trg_audit"):
        assert name in got.detail, (name, got.detail)


def test_mysql_a_trigger_goes_with_its_table(tmp_path):
    """A hop that takes the orders and their view takes the orders'
    trigger with them, whatever the trigger is called."""
    eng = _my(tmp_path, ("trg_orders",),
              include=["appdb.orders", "appdb.v_orders", "appdb.p_orders"])
    got = eng.check_objects("appdb")
    assert got.status == "diff" and "trg_orders" in got.detail, got.detail
    assert "audit" not in got.detail, got.detail


def test_mysql_the_schema_fix_leaves_out_the_objects_the_hop_does(tmp_path):
    eng = _my(tmp_path, (), include=["appdb.orders", "appdb.v_orders",
                                     "appdb.p_orders"])
    left = eng._schema_excludes("appdb")
    for name in ("audit_log", "v_audit", "f_audit", "ev_purge_audit"):
        assert name in left, (name, left)
    for name in ("orders", "v_orders", "p_orders"):
        assert name not in left, (name, left)
    assert _my(tmp_path, ())._objects_not_taken("dst", "appdb") == set()


# ---- MongoDB ---------------------------------------------------------------

class _Coll:
    def __init__(self, indexes):
        self.indexes = indexes

    def list_indexes(self):
        return [{"name": n, "key": {k: 1}, "v": 2} for n, k in self.indexes]


class _Db:
    def __init__(self, colls):
        self.colls = colls

    def list_collection_names(self):
        return list(self.colls)

    def __getitem__(self, name):
        return _Coll(self.colls[name])

    def command(self, *a, **kw):
        return {"cursor": {"firstBatch": [{"options": {},
                                           "type": "collection"}]}}


def test_mongodb_an_excluded_collection_and_its_indexes_are_not_compared(
        tmp_path):
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb", tmp_path, exclude=["audit"]))
    held = {"src": {"orders": [("_id_", "_id")]},
            "dst": {"orders": [("_id_", "_id")],
                    "audit": [("_id_", "_id"), ("by_owner", "owner")]}}
    eng._client = lambda side: {"appdb": _Db(held[side])}
    got = eng.check_schema("appdb")
    assert [r.status for r in got] == ["ok", "ok"], [r.detail for r in got]
    assert "audit" not in (tmp_path / "objects.json").read_text()


# ---- every engine's kinds, read one way (backlog P0-SCOPE 7, item 3) -------

def test_a_catalogues_own_words_name_the_hops_kinds():
    from migkit.config import object_kind, object_kinds
    assert object_kinds("SQL_STORED_PROCEDURE") == ("procedures",)
    assert object_kinds("SQL_INLINE_TABLE_VALUED_FUNCTION") == ("functions",)
    assert object_kinds("SEQUENCE_OBJECT") == ("sequences",)
    assert object_kinds("MATERIALIZED VIEW") == ("views",)
    assert object_kinds("dictionary") == ("dictionaries",)
    assert object_kinds("routine") == ("functions", "procedures")
    assert object_kinds("PACKAGE BODY") == ("functions", "procedures")
    assert object_kinds("table") == () and object_kinds("SYNONYM") == ()
    assert object_kind("routine") is None and object_kind("VIEW") == "views"


def test_a_word_for_two_kinds_is_taken_where_either_is(tmp_path):
    hop = _hop("postgres", tmp_path, objects={"functions": False})
    assert hop.takes_object("routine", "appdb", "public", "f")
    assert not hop.takes_object("function", "appdb", "public", "f")
    hop = _hop("postgres", tmp_path, objects={"routines": False})
    assert not hop.takes_object("routine", "appdb", "public", "f")
    assert not hop.takes_object("SQL_SCALAR_FUNCTION", "appdb", "dbo", "f")


def test_sql_server_lists_only_the_objects_the_hop_carries(tmp_path):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql", tmp_path, objects={
        "procedures": False, "views": ["dbo.v_keep"]}))
    rows = [("dbo.v_keep", "VIEW", "h1", "dbo", "v_keep"),
            ("dbo.v_other", "VIEW", "h2", "dbo", "v_other"),
            ("dbo.p", "SQL_STORED_PROCEDURE", "h3", "dbo", "p"),
            ("dbo.f", "SQL_SCALAR_FUNCTION", "h4", "dbo", "f"),
            ("dbo.s", "SEQUENCE_OBJECT", "h5", "dbo", "s")]
    eng._cmd = lambda side, db, sql: rows
    got = eng._objects("src", "appdb")
    assert set(got) == {("dbo.v_keep", "VIEW"),
                        ("dbo.f", "SQL_SCALAR_FUNCTION"),
                        ("dbo.s", "SEQUENCE_OBJECT")}, got


def test_oracle_lists_only_the_objects_the_hop_carries(tmp_path):
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle", tmp_path, objects={
        "views": False, "triggers": False}))
    eng._owner = lambda side, db: "APP"

    def rows(side, db, sql, args=()):
        if "all_views" in sql:
            return [("V1", "select 1 from dual")]
        if "all_source" in sql and "TRIGGER" in sql:
            return [("F1", "FUNCTION", "function f1 return number is"
                     " begin return 1; end;"),
                    ("T1", "TRIGGER", "trigger t1 before insert on orders"
                     " begin null; end;")]
        if "all_triggers" in sql:
            return [("T1", "ORDERS", "BEFORE EACH ROW", "INSERT")]
        return []
    eng._rows = rows
    assert eng.neutral_views("src", "appdb") == []
    got = [r.kind for r in eng.code_routines("src", "appdb")]
    assert got == ["function"], got


def test_oracles_own_pull_is_not_taken_where_it_would_bring_triggers():
    """Its table mode brings each table's triggers: a hop whose `objects`
    leaves some out goes by the table copier, which makes none."""
    from migkit.decide import Facts, climb
    from migkit.engines import oracle_pump
    got = climb("appdb", oracle_pump.ways(),
                Facts({"triggers": False}, **{"why not": lambda: None}))
    assert got.path == "copier", got
    got = climb("appdb", oracle_pump.ways(),
                Facts({"triggers": True}, **{"why not": lambda: None}))
    assert got.path == "pump", got


def _lite(tmp_path, **kw):
    import sqlite3
    from migkit.config import Endpoint, Hop
    from migkit.engines.sqlite import SQLiteEngine
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.executescript(
            "create table t (id integer primary key, v text);"
            " create table log (id integer primary key, at text);"
            " create view v_t as select id from t;")
        if name == "a.db":
            con.executescript(
                "create trigger trg after insert on t begin insert into log"
                " (at) values ('x'); end;"
                " create view v_more as select v from t;")
        con.close()
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db")),
              target=Endpoint(host=str(tmp_path / "b.db")),
              databases=["main"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def test_sqlite_compares_only_the_objects_the_hop_carries(tmp_path):
    got = _lite(tmp_path).check_schema("main")
    assert got[0].status == "diff", got
    got = _lite(tmp_path, objects={"triggers": False,
                                   "views": ["v_t"]}).check_schema("main")
    assert got[0].status == "ok", [(r.status, r.detail) for r in got]


def test_sqlite_makes_no_trigger_the_hop_leaves_out(tmp_path):
    import sqlite3
    eng = _lite(tmp_path, objects={"triggers": False})
    (tmp_path / "b.db").unlink()
    sqlite3.connect(tmp_path / "b.db").close()
    eng.native_bulk("main", ["t", "log"], True, lambda m: None)
    con = sqlite3.connect(tmp_path / "b.db")
    kinds = {r[0] for r in con.execute("select type from sqlite_master")}
    con.close()
    assert "trigger" not in kinds and "table" in kinds, kinds


def test_duckdb_reads_and_makes_only_the_sequences_the_hop_carries(tmp_path):
    import duckdb
    from migkit.config import Endpoint, Hop
    from migkit.engines.duckdb import DuckDBEngine
    con = duckdb.connect(str(tmp_path / "a.duckdb"))
    con.execute("create sequence free_seq start 5; create sequence id_seq;"
                " create table t (id integer default nextval('id_seq'),"
                " v text); insert into t (v) values ('x');"
                " create view v_t as select v from t;")
    con.close()
    duckdb.connect(str(tmp_path / "b.duckdb")).close()
    hop = Hop(name="duck", engine="duckdb",
              source=Endpoint(host=str(tmp_path / "a.duckdb")),
              target=Endpoint(host=str(tmp_path / "b.duckdb")),
              databases=["main"], objects={"sequences": False,
                                           "views": False})
    hop.report_dir = lambda db=None: tmp_path
    eng = DuckDBEngine(hop)
    assert eng._sequences("src", "main") == {}
    assert eng._objects("src", "main")["view"] == {}
    said = []
    eng.native_bulk("main", ["t"], True, said.append)
    con = duckdb.connect(str(tmp_path / "b.duckdb"), read_only=True)
    got = {r[0] for r in con.execute(
        "select sequence_name from duckdb_sequences()").fetchall()}
    con.close()
    # the one a carried table draws on is made all the same, and said
    assert got == {"id_seq"}, got
    assert any("id_seq" in m for m in said), said


def test_postgres_the_schema_fix_makes_no_trigger_the_hop_leaves_out(
        tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", tmp_path,
                              objects={"triggers": False}))
    ddl = ('CREATE TABLE "public"."t" ("id" integer);\n'
           'CREATE TRIGGER "trg" AFTER INSERT ON "public"."orders" FOR EACH'
           ' ROW EXECUTE FUNCTION "public"."g"();\n'
           'CREATE INDEX "i" ON "public"."t" ("id");')
    got, left = eng._without_triggers_not_taken("appdb", ddl)
    assert "TRIGGER" not in got and "CREATE INDEX" in got, got
    assert left == ["public.orders.trg"]
    every = PostgresEngine(_hop("postgres", tmp_path))
    assert every._without_triggers_not_taken("appdb", ddl) == (ddl, [])
    # one it would have to cut out of a longer statement is refused
    import pytest
    with pytest.raises(SystemExit, match="not one line"):
        eng._without_triggers_not_taken(
            "appdb", 'CREATE TRIGGER "trg" AFTER INSERT ON "public"."o"\n'
                     "FOR EACH ROW EXECUTE FUNCTION g();")


def test_postgres_the_inventory_leaves_a_trigger_by_its_kind(tmp_path):
    held = {"src": ["table|public.orders", "trigger|public.orders.trg"],
            "dst": ["table|public.orders"]}
    got = _pg(tmp_path, held, objects={"triggers": False}).check_objects(
        "appdb")
    assert got.status == "ok", got.detail
    got = _pg(tmp_path, held).check_objects("appdb")
    assert got.status == "diff", got.detail


#: a dump's list as PostgreSQL 16's restore writes it (measured)
LISTING = """221; 1255 16413 FUNCTION public f(integer, text) postgres
3439; 0 0 COMMENT public FUNCTION f(a integer, b text) postgres
222; 1255 16414 FUNCTION public g() postgres
3288; 2620 16415 TRIGGER public orders trg postgres
3442; 0 0 COMMENT public TRIGGER trg ON orders postgres
3287; 2606 16400 FK CONSTRAINT public orders fk_cust postgres
3443; 0 0 COMMENT public CONSTRAINT fk_cust ON orders postgres
3434; 2618 16483 RULE public people rl postgres
3448; 0 0 COMMENT public RULE rl ON people postgres
"""


def test_a_comment_goes_with_the_object_left_out_of_the_schema():
    """Measured on 16: the comment on a function left out was restored
    and refused (`function public.f(integer, text) does not exist`)."""
    from migkit import movers
    text, dropped = movers._toc_without(
        LISTING, {"1255 16413": {}, "2620 16415": {}, "2606 16400": {}},
        ["public FUNCTION f(a integer, b text)"])
    out = [line for line in text.splitlines() if line.startswith(";")]
    assert [line.split(";")[1] for line in out] == [
        "221", "3439", "3288", "3442", "3287", "3443"], out
    assert dropped == ["1255 16413", "2620 16415", "2606 16400"]
    # what is kept keeps its comment
    assert "3448; 0 0 COMMENT public RULE rl ON people postgres" in text


def test_mysql_compares_only_the_code_the_hop_carries(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path, objects={
        "procedures": ["keep_p"], "triggers": False}))
    text = "\n".join([
        "CREATE TABLE `t` (`id` int);",
        "DELIMITER ;;", "CREATE PROCEDURE `keep_p`() BEGIN END ;;",
        "DELIMITER ;",
        "DELIMITER ;;", "CREATE PROCEDURE `drop_p`() BEGIN END ;;",
        "DELIMITER ;",
        "DELIMITER ;;", "/*!50003 CREATE TRIGGER `trg` BEFORE INSERT ON `t`"
        " FOR EACH ROW SET NEW.id = 1 */;;", "DELIMITER ;"])
    got = eng._without_objects_not_taken("appdb", text)
    assert "keep_p" in got and "drop_p" not in got and "trg" not in got, got
    assert "CREATE TABLE" in got
    every = MySQLEngine(_hop("mysql", tmp_path))
    assert every._without_objects_not_taken("appdb", text) == text


def test_mysql_a_grant_on_a_routine_goes_with_the_routine(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path,
                           objects={"procedures": False}))
    got = eng._grants_taken("appdb", {"app@%": {
        "GRANT EXECUTE ON PROCEDURE <db>.`p` TO `app`@`%`",
        "GRANT EXECUTE ON FUNCTION <db>.`f` TO `app`@`%`",
        "GRANT SELECT ON <db>.* TO `app`@`%`"}})
    assert got == {"app@%": {
        "GRANT EXECUTE ON FUNCTION <db>.`f` TO `app`@`%`",
        "GRANT SELECT ON <db>.* TO `app`@`%`"}}, got


def test_the_stored_code_proof_reads_only_what_the_hop_carries(tmp_path):
    from migkit.storedcode import prove
    from migkit.storedcode.routines import Routine

    class Src:
        def __init__(self, hop):
            self.hop = hop

        def takes(self, db, kind, name, by=None):
            from migkit.engines.base import Engine
            return Engine.takes(self, db, kind, name, by)

        def in_scope(self, db, table):
            return True

        CANON_ENGINE = "postgres"

        def code_routines(self, side, db):
            return [Routine(kind="function", name="public.f"),
                    Routine(kind="procedure", name="public.p"),
                    Routine(kind="trigger", name="trg", table="public.t")]

        def neutral_functions(self, side, db):
            return [("public.f", [], "int", "1"), ("public.g", [], "int",
                                                   "2")]
    src = Src(_hop("postgres", tmp_path, objects={
        "functions": ["public.f"], "triggers": False}))
    got = [(r.kind, r.name) for r in prove.routines_of(src, "src", "appdb")]
    assert got == [("function", "public.f"), ("procedure", "public.p")], got
    # the target's are what it holds
    assert len(prove.routines_of(src, "dst", "appdb")) == 3
    assert prove.expressions(src, "src", "appdb") == {"public.f": "1"}
