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


def _my(tmp_path, only_on_target, bodies=None, **kw):
    """The target holds everything; the source lacks `only_on_target`.
    Each view's, routine's, trigger's and event's definition is `body`,
    or what `bodies` gives it on a side ({(side, name): text})."""
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", tmp_path, **kw))
    queries = {q: k for k, q in MySQLEngine.OBJECT_QUERIES.items()}
    defined = {q: k for k, q in MySQLEngine.OBJECT_DEFINITIONS.items()}

    def held(side, kind):
        return [r for r in MY_HELD[kind] if side == "dst"
                or not any(o in (r[0], r[1]) for o in only_on_target)]

    def q(side, sql, args=None, fresh=False):
        if sql in defined:
            return [(r[0], (bodies or {}).get((side, r[0]), "body"))
                    for r in held(side, defined[sql])]
        kind = queries.get(sql)
        if kind is None:
            return []
        return held(side, kind)
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


def test_mysql_a_body_that_changed_is_different(tmp_path):
    """Present on both sides by name, defined differently: a routine's, a
    trigger's and a view's body were compared by name only, and the
    schema-aware comparison that demoted the text never read one (audit
    2026-10-01, SC16)."""
    same = _my(tmp_path, ()).check_objects("appdb")
    assert same.status == "ok" and "defined alike" in same.detail, \
        same.detail
    for name in ("p_orders", "trg_orders", "v_orders", "ev_purge_audit"):
        got = _my(tmp_path, (), bodies={("dst", name): "other body"}) \
            .check_objects("appdb")
        assert got.status == "diff", (name, got.detail)
        assert "defined differently" in got.detail and name in got.detail, \
            (name, got.detail)


def test_mysql_a_body_the_account_cannot_read_is_said_not_same(tmp_path):
    """A routine's definition comes back NULL to an account that may not
    read it: never read as the same as the other side's."""
    got = _my(tmp_path, (), bodies={("src", "f_audit"): None}) \
        .check_objects("appdb")
    assert got.status == "warn", got.detail
    assert "function f_audit" in got.detail, got.detail


def test_mysql_the_databases_own_name_in_a_view_is_not_a_difference(
        tmp_path):
    """A view keeps its tables qualified by the database; a hop mapping
    the database to another name on the target compares it alike."""
    eng = _my(tmp_path, (), db_map={"appdb": "appdb2"}, bodies={
        ("src", "v_orders"): "select `appdb`.`orders`.`id` from"
                             " `appdb`.`orders`",
        ("dst", "v_orders"): "select `appdb2`.`orders`.`id` from"
                             " `appdb2`.`orders`"})
    got = eng.check_objects("appdb")
    assert got.status == "ok", got.detail


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
