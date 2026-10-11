"""The change tail of an Oracle source follows its tables as they come and
go (P0-PARITY.tail), without a server: the mark a table's copy is held to,
the redo read again for a table that returns, and a schema change the redo
carries as text.

* a table added to a running tail joins at the SCN the copy began at: a
  change that committed at or below it is in what the copy read, one above
  it is applied (`snapshot_mark`, `mark_covers`), in the database the mark
  was taken in and no other;
* a table that left comes back with only its changes since, read again
  from where it left, when the redo still holds the place and the table's
  shape is as it was - and is copied whole when it does not;
* a DDL statement of the redo becomes the change that carries it
  (`logddl.oracle`, `canon.ddl`), at its place among the rows, for each
  table the hop takes; a rename is told apart from a drop and a create.

The first two are named in `PROVES`: their statements are ones the reader
already sent. The statements that carry a schema change are read from the
text LogMiner hands back, which no server has yet handed this reader
(`capabilities.UNPROVED`).
"""
import pytest

from migkit import canon, decide, logddl, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines import oracle_logminer as miner
from migkit.engines.hetero import HeteroEngine
from migkit.engines.oracle import OracleEngine
from tests.oracle_catalogue import cat  # noqa: F401

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.added", "oracle"), ("tail.readded", "oracle")]

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")
IDENTITY = [("1234", "1", "APP_DB", "FREEPDB1")]


def _eng(**kw):
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"], **kw)
    return OracleEngine(hop)


# ---- the mark a copy is held to ----------------------------------------------

def _marked(cat, scn=1000):
    cat.on("from v$database", lambda side, sql: [(1234, 1, "APP_DB")])
    cat.on("con_name", [("FREEPDB1",)])
    cat.on("get_system_change_number", [(scn,)])
    return _eng()


def test_the_mark_is_the_scn_now_in_the_database_it_was_read_in(cat):
    eng = _marked(cat)
    assert eng.snapshot_mark("src", "app") == "1234/1/FREEPDB1|1000"


def test_there_is_no_mark_where_the_database_cannot_be_told_from_another(cat):
    cat.on("from v$database", RuntimeError("ORA-00942"))
    cat.on("get_system_change_number", [(1000,)])
    assert _eng().snapshot_mark("src", "app") is None


def test_a_change_is_covered_at_or_below_the_marks_scn(cat):
    eng = _marked(cat)
    mark = eng.snapshot_mark("src", "app")

    def change(scn):
        return canon.change("update", "t", {"id": 1}, {"id": 1}, txn=scn)
    assert eng.mark_covers("src", "app", mark, change(999)) is True
    assert eng.mark_covers("src", "app", mark, change(1000)) is True
    assert eng.mark_covers("src", "app", mark, change(1001)) is False
    # another database's mark, no mark, a change that names no commit: not
    # known, and only True lets the tail leave a change out
    assert eng.mark_covers("src", "app", "9999/1/FREEPDB1|1000",
                           change(999)) is None
    assert eng.mark_covers("src", "app", None, change(999)) is None
    assert eng.mark_covers("src", "app", mark, canon.change(
        "delete", "t", {"id": 1})) is None


def _read(shapes, rows, ddl=None):
    """Rows of one transaction as the miner turns them into changes."""
    eng = _eng()
    m = miner.Miner.__new__(miner.Miner)
    m.eng, m.side, m.db, m.shapes, m.ddl = eng, "src", "app", shapes, ddl
    return m, m._changes(rows, 5000)


def test_every_change_names_the_scn_its_transaction_committed_at():
    shapes = {"orders": ({"id": "NUMBER", "v": "VARCHAR2"}, ["id"])}
    rows = [(4990, 5000, "1.2.3", miner.INSERT, "ORDERS", 0,
             "insert into \"APP\".\"ORDERS\"(\"ID\", \"V\") values ('1',"
             " 'x');", None, 1)]
    _, got = _read(shapes, rows)
    assert [(c["op"], c["txn"]) for c in got] == [("insert", 5000)]


# ---- a table that returns ----------------------------------------------------

def test_a_read_again_says_how_far_it_has_got():
    assert OracleEngine.REREADS_FROM_A_POSITION is True
    reached = OracleEngine.position_reached
    assert reached({"through": 50}, 50) is True
    assert reached({"through": 60}, 50) is True
    assert reached({"through": 49}, 50) is False
    # where it cannot say, the table is copied instead
    assert reached(None, 50) is None
    assert reached({"through": 50}, None) is None
    assert reached({}, 50) is None


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, eng):
        self.hop, self.src_engine = hop, eng

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _tables(tmp_path, eng):
    hop = Hop(name="tt", engine="oracle", source=EP, target=EP,
              databases=["d"], options={"ddl": "park"})
    hop.report_dir = lambda db=None: tmp_path
    got = tailtables.TableSet(_Pair(hop, eng), "d",
                              tmp_path / "tail-token.json", lambda m: None)
    got.file.said = None
    got._settle = lambda: None
    return got


def _source(columns):
    eng = _eng()
    eng.lost = None
    eng.columns = columns
    eng.neutral_columns = lambda side, db, table: list(eng.columns)
    eng.log_position = lambda side, db: 500
    eng.position_lost = lambda side, db, at: eng.lost
    return eng


LEFT = {"state": "removed", "name": "a", "at": {"through": 400},
        "digest": [3, "9"], "shape": [["id", "integer"], ["v", "text"]],
        "columns": [["id", "integer"]]}
COLUMNS = [("id", "integer"), ("v", "text")]


def test_a_table_that_left_comes_back_with_only_its_changes_since(tmp_path):
    t = _tables(tmp_path, _source(list(COLUMNS)))
    states = {"a": dict(LEFT)}
    t._add(states, "a", "a", {"through": 500}, "put back")
    assert states["a"]["way"] == "from-removal"
    assert states["a"]["at"] == {"through": 400}
    assert states["a"]["upto"] == 500


def test_a_table_is_copied_when_the_read_again_cannot_be_exact(tmp_path):
    eng = _source(list(COLUMNS))
    t = _tables(tmp_path, eng)
    # its shape changed while it was out
    eng.columns = [("id", "integer")]
    states = {"a": dict(LEFT)}
    t._add(states, "a", "a", {"through": 500}, "put back")
    assert states["a"]["way"] == "copy"
    eng.columns = list(COLUMNS)
    # the redo no longer holds the place it left
    eng.lost = "the archived log was deleted"
    states = {"a": dict(LEFT)}
    t._add(states, "a", "a", {"through": 500}, "put back")
    assert states["a"]["way"] == "copy"
    got = decide.climb("a", tailtables._add_ways(), decide.Facts(
        {"at": LEFT["at"], "digest": True, "rereads": True,
         "same_shape": True, "lost": eng.lost}))
    assert got.path == "copy" and "deleted" in got.reason
    eng.lost = None
    # it left with no digest of its target to hold it to
    states = {"a": dict(LEFT, digest=None)}
    t._add(states, "a", "a", {"through": 500}, "put back")
    assert states["a"]["way"] == "copy"


def test_a_read_again_is_a_reader_of_its_own_that_is_let_go():
    eng = _eng()
    closed = []

    class Reader:
        def close(self):
            closed.append(1)
    eng.__dict__["_readers"] = {("src", "app"): Reader()}
    eng.release_changes()
    assert closed == [1] and eng.__dict__["_readers"] == {}


# ---- a schema change the redo carries as text --------------------------------

@pytest.mark.parametrize("sql,want", [
    ('truncate table "APP"."ORDERS"', [("APP", "ORDERS", "truncate", None)]),
    ("TRUNCATE TABLE orders PRESERVE MATERIALIZED VIEW LOG;",
     [("APP", "ORDERS", "truncate", None)]),
    ('drop table "APP"."ORDERS" cascade constraints purge',
     [("APP", "ORDERS", "drop", None)]),
    ('alter table "APP"."ORDERS" add (note varchar2(10))',
     [("APP", "ORDERS", "alter", None)]),
    ('alter table "APP"."ORDERS" rename column a to b',
     [("APP", "ORDERS", "alter", None)]),
    ('alter table "APP"."ORDERS" rename to "ORDERS_OLD"',
     [("APP", "ORDERS", "rename", ("APP", "ORDERS_OLD"))]),
    ("rename orders to orders_old",
     [("APP", "ORDERS", "rename", ("APP", "ORDERS_OLD"))]),
    ('alter table "APP"."ORDERS" truncate partition p1',
     [("APP", "ORDERS", "reload", None)]),
    ('alter table "APP"."ORDERS" exchange partition p1 with table "APP".stage',
     [("APP", "ORDERS", "reload", None), ("APP", "STAGE", "reload", None)]),
    ('alter table "APP"."ORDERS" split partition p1 at (100) into'
     ' (partition p0, partition p1)', [("APP", "ORDERS", "alter", None)]),
    ('flashback table "APP"."ORDERS" to before drop',
     [("APP", "ORDERS", "reload", None)]),
    ('create table "APP"."NEWT" (id number)',
     [("APP", "NEWT", "create", None)]),
    ('create unique index ix on "APP"."ORDERS" (id)',
     [("APP", "ORDERS", "alter", None)]),
    ("/* a comment */ drop table \"Mixed\"",
     [("APP", "Mixed", "drop", None)]),
    ("create global temporary table tmp (x number)", []),
    ("grant select on orders to bob", []),
    ("drop index ix", []),
])
def test_a_statement_of_the_redo_is_read_for_the_tables_it_touches(sql, want):
    assert logddl.oracle(sql, "APP") == want


def test_a_statement_becomes_the_change_that_carries_it():
    eng = _eng(exclude=["audit_log"])
    got = eng._ddl_of("app", 'alter table "APP"."ORDERS" rename to'
                             ' "ORDERS_OLD"', 5000)
    assert got == [{"op": "ddl", "table": "orders", "kind": "rename",
                    "sql": 'alter table "APP"."ORDERS" rename to'
                           ' "ORDERS_OLD"', "to": "orders_old",
                    "txn": 5000}]
    # a table the hop leaves out, another schema's, a statement of none
    assert eng._ddl_of("app", 'drop table "APP"."AUDIT_LOG"', 1) == []
    assert eng._ddl_of("app", 'drop table "OTHER"."ORDERS"', 1) == []
    assert eng._ddl_of("app", "grant select on orders to bob", 1) == []
    # renamed into the hop's scope from outside it: the table is made
    assert [(c["table"], c["kind"]) for c in eng._ddl_of(
        "app", 'alter table "APP"."AUDIT_LOG" rename to "ARCHIVE"', 1)] == [
        ("audit_log", "rename")]


def test_a_schema_change_is_at_its_place_among_the_rows_and_the_shape_is_read_again():
    eng = _eng()
    shapes = {"orders": ({"id": "NUMBER"}, ["id"])}
    rows = [
        (4990, 5000, "1.2.3", miner.INSERT, "ORDERS", 0,
         "insert into \"APP\".\"ORDERS\"(\"ID\") values ('1');", None, 1),
        (4991, 5000, "1.2.3", miner.DDL, "ORDERS", 0,
         "alter table \"APP\".\"ORDERS\" add (note varchar2(10));", None, 2)]
    m, got = _read(shapes, rows, eng._ddl_of)
    assert [(c["op"], c.get("kind")) for c in got] == [
        ("insert", None), ("ddl", "alter")]
    assert got[1]["txn"] == 5000
    # the cached shape of the table is dropped, to be asked of the
    # catalogue at its next row
    assert "orders" not in m.shapes


def test_a_reader_that_is_given_no_translator_leaves_a_schema_change_as_it_did():
    shapes = {}
    rows = [(4991, 5000, "1.2.3", miner.DDL, "ORDERS", 0,
             "drop table \"APP\".\"ORDERS\";", None, 2)]
    _, got = _read(shapes, rows, None)
    assert got == []
