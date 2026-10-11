"""A table swapped in under a name on a SQL Server source is told apart by
the id it keeps through a rename and no other table has (P0-PARITY.tail,
`table_oids`): Change Tracking carries no DDL and a shape is keyed by
name, so two renames that put another table under a name leave the name
and the columns as they were.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue read, and the tail's own table states (`tailtables.TableSet`)
are driven by it. The statement is new and has not run on a server:
`tail.renames` waits in `capabilities.UNPROVED`.
"""
import pytest

from migkit import tailtables
from migkit.engines.hetero import HeteroEngine
from tests.mssql_stub import Server, engine


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, src):
        self.hop, self.src_engine = hop, src

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _source(monkeypatch, tmp_path, oids, **kw):
    """The SQL Server engine over a server whose tables hold `oids`, as
    the tail's source."""
    server = Server().on("sys.tables t", lambda side, sql: [
        (name, oid) for name, oid in oids.items()])
    eng = engine(monkeypatch, tmp_path, server, **kw)
    eng.snapshot_mark = lambda side, db: None
    eng.mark_covers = lambda side, db, mark, change: None
    eng.position_lost = lambda side, db, at: None
    eng.neutral_columns = lambda side, db, table: [("id", "int")]
    eng.log_position = lambda side, db: 10
    eng.neutral_tables = lambda side, db: list(oids)
    return eng


def _tables(tmp_path, src):
    got = tailtables.TableSet(_Pair(src.hop, src), "appdb",
                              tmp_path / "tail-token.json", lambda m: None)
    got.file.said = None
    got._settle = lambda: None
    return got


def test_the_server_answers_each_table_by_the_id_it_keeps(monkeypatch,
                                                           tmp_path):
    src = _source(monkeypatch, tmp_path,
                  {"dbo.a": 101, "dbo.b": 102, "sales.a": 103})
    assert src.table_oids("src", "appdb") == {
        "dbo.a": "101", "dbo.b": "102", "sales.a": "103"}


def test_a_table_swapped_in_under_its_name_is_seen_by_its_id(monkeypatch,
                                                              tmp_path):
    oids = {"dbo.a": 101, "dbo.b": 102}
    src = _source(monkeypatch, tmp_path, oids, options={"ddl": "park"})
    t = _tables(tmp_path, src)
    t.boundary(10, None, force=True)
    assert t.tables["dbo.a"]["oid"] == "101"
    # two renames: a new table now holds the name, and its columns match
    oids["dbo.a"] = 999
    t.scope_at = 0
    t.boundary(11, None, force=True)
    assert t.tables["dbo.a"]["state"] == "copying"
    assert "replaced on the source" in t.tables["dbo.a"]["why"]
    assert t.tables["dbo.a"]["oid"] == "999"
    assert t.tables["dbo.b"]["state"] == "live"


def test_under_the_default_ddl_a_swap_stops_the_tail_before_the_batch(
        monkeypatch, tmp_path):
    oids = {"dbo.a": 101, "dbo.b": 102}
    src = _source(monkeypatch, tmp_path / "stop", oids)
    t = _tables(tmp_path / "stop", src)
    t.boundary(10, None, force=True)
    oids["dbo.b"] = 7
    with pytest.raises(SystemExit) as e:
        t.boundary(11, None, force=True)
    assert "dbo.b was replaced on the source" in str(e.value)
