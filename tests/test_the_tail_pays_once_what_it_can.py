"""The tail pays once what it can, not once a change (audit 2026-10-01,
SP3, SP7, SP8, SP12).

* Holding a change to what the copy read: each entry's plan indexed once
  and a mark asked once a batch about a transaction; an entry the tail is
  past every mark of retired - and never where counters add.
* The kept proof: a table copied or parked beside the tail flagged, not
  every leaf of it rewritten each batch.
* The source's shape: a digest of its catalogue asked of the server
  first, and the whole shape read only where it moved - on every source
  whose tail asks its shape (PostgreSQL, SQL Server, Oracle, MySQL and
  TiDB, SQLite), each digest over exactly the rows its whole read reads;
  held to the last by its fingerprint, compared and written only where it
  moved.
* The row filter's translation and a key's classes: once a table, again
  after the shape changes.
"""
import json

import pytest

from migkit import kept, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine, _CopiedRanges
from tests import servers
from tests.conftest import needs_docker, psql

ms = servers.fixture("sqlserver")
ora = servers.fixture("oracle")
my = servers.fixture("mysql")


class _Src:
    def __init__(self):
        self.asked = 0

    def mark_covers(self, side, db, mark, change):
        self.asked += 1
        return change["txn"] < 100

    def snapshot_mark(self, side, db):
        return None


class _Eng:
    def __init__(self, options=None):
        self.hop = Hop(name="sp", engine="postgres", source=Endpoint(),
                       target=Endpoint(), options=dict(options or {}))
        self.src_engine = _Src()

    @staticmethod
    def _leaf(t):
        return str(t).split(".")[-1]


def _copied(where, n=50, done=True):
    rs = [[i * 10, (i + 1) * 10] for i in range(n)]
    (where / "move.json").write_text(json.dumps({"db..t": {
        "table": "public.t", "key": "id", "ranges": rs,
        "ranges_done": [r[0] for r in rs],
        "ranges_seen": {str(r[0]): f"m{r[0]}" for r in rs}, "done": done}}))


def _change(k, txn):
    return {"op": "update", "table": "public.t", "key": {"id": k},
            "values": {"id": k}, "txn": txn}


def test_a_mark_is_asked_once_a_batch_about_a_transaction(tmp_path):
    _copied(tmp_path)
    copied = _CopiedRanges(_Eng(), "db", tmp_path)
    eng = copied.eng
    # forty changes of one transaction in one range
    batch = [_change(5, 7) for _ in range(40)]
    out, left = copied.leave_out(batch)
    assert out == [] and left == {"public.t": 40}
    # its range's mark and the newest, once each
    assert eng.src_engine.asked == 2, eng.src_engine.asked


def test_an_entry_the_tail_is_past_retires(tmp_path):
    _copied(tmp_path)
    copied = _CopiedRanges(_Eng(), "db", tmp_path)
    copied.RETIRE_AFTER = 5
    # covered by the copy's marks: left out, the entry stays
    assert copied.leave_out([_change(1, 3)])[1] == {"public.t": 1}
    # past every mark: applied - and after five in a row, retired
    for i in range(5):
        out, left = copied.leave_out([_change(1, 200 + i)])
        assert len(out) == 1 and not left
    assert len(copied.retired) == 1
    copied.eng.src_engine.asked = 0
    out, _ = copied.leave_out([_change(1, 300), _change(2, 3)])
    # retired: nothing asked, everything applied - a change the copy read
    # applied again is superseded by the changes after it
    assert copied.eng.src_engine.asked == 0 and len(out) == 2
    # a copy not finished never retires: new ranges and marks may come
    _copied(tmp_path, done=False)
    still = _CopiedRanges(_Eng(), "db", tmp_path)
    still.RETIRE_AFTER = 5
    for i in range(10):
        still.leave_out([_change(1, 200 + i)])
    assert not still.retired


def test_where_counters_add_an_entry_never_retires(tmp_path):
    _copied(tmp_path)
    eng = _Eng({"two_way": {"delta": ["n"]}})
    copied = _CopiedRanges(eng, "db", tmp_path)
    assert copied.keep_all
    copied.RETIRE_AFTER = 2
    for i in range(10):
        copied.leave_out([_change(1, 200 + i)])
    assert not copied.retired
    assert copied.leave_out([_change(1, 3)])[1] == {"public.t": 1}


def test_a_table_beside_the_tail_is_flagged_not_rewritten(tmp_path,
                                                          monkeypatch):
    st = kept.Store(tmp_path / kept.FILE)
    assert st.conn.execute("pragma user_version").fetchone()[0] == \
        kept.Store.VERSION
    st.begin()
    st.write("t", {b: kept._new() for b in range(64)})
    st.commit()
    writes = []
    real = kept.Store.write
    monkeypatch.setattr(kept.Store, "write",
                        lambda self, name, got: writes.append(
                            (name, len(got))) or real(self, name, got))
    keeper = kept.Keeper.__new__(kept.Keeper)
    keeper.store, keeper.unplaced = st, {"t"}
    keeper._tables = {}
    for _ in range(3):
        keeper._commit({}, set(), 0, "0/2", "0/1")
    # no leaf written: the table flagged once
    assert writes == [] and st.stale() == {"t": True}

    class T:
        name = "t"
    why = kept._due(st, T(), 0, 10 ** 9)
    assert why["marked"] == set(range(64))


def test_the_shape_is_compared_and_written_only_where_it_moved(
        tmp_path, monkeypatch):
    from migkit import drift
    shapes = iter([{"public.t": [["id", "int"]]}] * 3
                  + [{"public.t": [["id", "int"], ["w", "int"]]}])
    monkeypatch.setattr(drift, "reader", lambda eng: object())
    monkeypatch.setattr(drift, "shape", lambda eng, side, db: next(shapes))
    compared = []
    real = drift.changes
    monkeypatch.setattr(drift, "changes",
                        lambda a, b: compared.append(1) or real(a, b))
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="sh", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    pair.src_engine = object()
    monkeypatch.setattr(HeteroEngine, "_lacking",
                        lambda self, db, a, b: {})
    path = tmp_path / "tail-token.json"
    for _ in range(3):
        got, behind = pair._shape_now("d", path, "0/1")
        assert got == {"public.t": [["id", "int"]]} and behind == {}
    store = tailtables.Store.of(tmp_path)
    assert store.get("shape") == {"public.t": [["id", "int"]]}
    # read three times, compared and written once: the first
    assert compared == []
    pair._shape_now("d", path, "0/2", stop=False)
    assert compared == [1]
    assert store.get("shape")["public.t"] == [["id", "int"], ["w", "int"]]


def _shaping(tmp_path, monkeypatch, src):
    from migkit import drift
    now = {"shape": {"public.t": [["id", "int"]]}}
    reads = []
    monkeypatch.setattr(drift, "reader", lambda eng: src)
    monkeypatch.setattr(drift, "shape", lambda eng, side, db: reads.append(
        1) or json.loads(json.dumps(now["shape"])))
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="sm", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    pair.src_engine = src
    monkeypatch.setattr(HeteroEngine, "_lacking",
                        lambda self, db, a, b: {})
    return pair, now, reads, tmp_path / "tail-token.json"


def test_a_source_that_digests_its_catalogue_is_read_whole_once_it_moved(
        tmp_path, monkeypatch):
    class Src:
        mark = "a"

        def shape_mark(self, side, db):
            return self.mark
    src = Src()
    pair, now, reads, path = _shaping(tmp_path, monkeypatch, src)
    for _ in range(4):
        got, _ = pair._shape_now("d", path, "0/1")
        assert got == {"public.t": [["id", "int"]]}
    # one whole read: every batch after it asked the digest alone
    assert reads == [1]
    src.mark = "b"
    now["shape"] = {"public.t": [["id", "int"], ["w", "int"]]}
    got, _ = pair._shape_now("d", path, "0/2", stop=False)
    assert len(reads) == 2 and got["public.t"][1] == ["w", "int"]
    assert tailtables.Store.of(tmp_path).get("shape") == now["shape"]
    # a digest that cannot be asked: the whole shape, every batch
    src.shape_mark = lambda side, db: (_ for _ in ()).throw(
        RuntimeError("gone"))
    pair._shape_now("d", path, "0/3", stop=False)
    pair._shape_now("d", path, "0/3", stop=False)
    assert len(reads) == 4


@needs_docker
def test_postgresql_digests_its_catalogue_and_a_change_moves_it(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["src"]
    eng = PostgresEngine(Hop(
        name="sm", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"]))
    got = psql(port, "drop table if exists public.sm; create table"
                     " public.sm (id int primary key, v text)")
    assert got.returncode == 0, got.stderr
    was = eng.shape_mark("src", "postgres")
    assert len(was) == 32 and eng.shape_mark("src", "postgres") == was
    # what drift compares beyond a column's name and type moves it too
    for ddl in ("alter table public.sm alter column v type varchar(9)",
                "alter table public.sm alter column v set default 'x'",
                "create index sm_v on public.sm (v)",
                "alter table public.sm add constraint sm_c check (id > 0)"):
        got = psql(port, ddl)
        assert got.returncode == 0, got.stderr
        now = eng.shape_mark("src", "postgres")
        assert now != was, ddl
        was = now


def _engine(cls, engine, ep=None, db="app"):
    ep = ep or Endpoint(host="10.0.0.5", port=1, user="u",
                        password="CHANGE_ME")
    return cls(Hop(name="sm", engine=engine, source=ep, target=ep,
                   databases=[db]))


def _aliases(sql):
    """The column names of a statement's first select."""
    import re
    head = sql.split(" union all ")[0]
    return sorted(int(n) for n in re.findall(r" c(\d+)(?=[ ,])", head))


def test_sql_server_digests_every_row_its_whole_read_reads(monkeypatch):
    """A column the digest left out would be a change the tail never
    asked the whole shape for: every statement `shape_catalog` reads is
    the one `shape_mark` digests, every column of it."""
    from migkit.engines.mssql import MSSQLEngine
    eng = _engine(MSSQLEngine, "mssql")
    reads = eng._shape_reads()
    asked = []
    rows = [[["dbo.t", "id", "int", False, None, 1, 1, None, None, None,
              False, False, None, 1]],
            [["dbo.t", 1, "id", False, False, 1]],
            [["dbo.t", 1, "PK_t", 1, "clustered", True, False, True,
              None]], [], [], []]
    monkeypatch.setattr(MSSQLEngine, "_kept_rows",
                        lambda self, side, db, sql, args=None:
                        asked.append(sql) or rows[len(asked) - 1])
    cat = eng.shape_catalog("src", "app")
    assert cat.keys["dbo.t"] == ("id",)
    assert cat.columns["dbo.t"][0].extra == "identity(1, 1)"
    assert len(asked) == len(reads) == 6
    for (sql, order, width), ran in zip(reads, asked):
        assert ran == sql + (f" order by {order}" if order else "")
        # every column named, and the width the digest takes them by
        assert _aliases(sql) == list(range(width)), sql[:60]
    asked.clear()
    monkeypatch.setattr(MSSQLEngine, "_kept_rows",
                        lambda self, side, db, sql, args=None:
                        asked.append(sql) or [["3:9"] * 6])
    assert eng.shape_mark("src", "app") == "/".join(["3:9"] * 6)
    mark, = asked
    start = 0
    for sql, _, width in reads:
        # each statement's own part, in order: its rows hashed over every
        # column it names, and no more
        end = mark.index(f"from ({sql}) q)", start)
        part = mark[start:end]
        for n in range(width):
            assert f"q.c{n})" in part, (n, sql[:60])
        assert f"q.c{width})" not in part
        start = end + len(sql)


def test_oracle_digests_every_row_its_whole_read_reads(monkeypatch):
    from migkit.engines.oracle import OracleEngine
    eng = _engine(OracleEngine, "oracle")
    sql, binds = eng._catalog_reads("src", "app")
    asked = []
    monkeypatch.setattr(OracleEngine, "_rows",
                        lambda self, side, db, q, args=None:
                        asked.append((q, args)) or [("T", "ID",
                                                     "NUMBER(38,0)", 1)])
    assert eng.column_catalog("src", "app") == {"t": [("id",
                                                       "NUMBER(38,0)")]}
    assert asked[-1] == (sql + " order by 1, column_id", binds)
    monkeypatch.setattr(OracleEngine, "_rows",
                        lambda self, side, db, q, args=None:
                        asked.append((q, args)) or [(4, 10, 20)])
    assert eng.shape_mark("src", "app") == "4:10:20"
    mark, args = asked[-1]
    assert f"from ({sql}) q" in mark and args == binds
    for column in ("q.ct", "q.cn", "q.cd", "to_char(q.cp)"):
        assert f"nvl2({column}," in mark, column


#: one answer for each of the MySQL catalogue's reads, by what it reads
_MY_SHAPE = {
    "information_schema.tables": [("t", "BASE TABLE")],
    "information_schema.columns": [("t", "id", "int", "NO", "int", None,
                                    "", None, None, 1),
                                   ("t", "v", "varchar(9)", "YES",
                                    "varchar", "x", "",
                                    "utf8mb4_bin", None, 2)],
    "information_schema.statistics": [("t", "PRIMARY", 1, "id", 0, None,
                                       "A", "BTREE", None, "YES"),
                                      ("t", "t_v", 1, "v", 1, 4, "D",
                                       "BTREE", None, "NO")],
    "information_schema.key_column_usage": [],
    "check_constraints": [("t", "(`id` > 0)", "NO")],
    "information_schema.triggers": [("t", "t_bi", "BEFORE", "INSERT", 1,
                                     "set new.v = 'y'")],
}


def _my_shape_engine(monkeypatch, refuse=()):
    """A MySQL engine whose server answers `_MY_SHAPE`, refusing as an
    unknown column a statement holding any of `refuse`."""
    import types

    import pymysql

    from migkit.engines.mysql import MySQLEngine
    eng = _engine(MySQLEngine, "mysql")
    monkeypatch.setattr(MySQLEngine, "_side_brand",
                        lambda self, side: types.SimpleNamespace(
                            name="mysql"))
    asked = []

    def q(self, side, sql, args=None, fresh=False):
        asked.append((sql, args))
        if any(r in sql for r in refuse):
            raise pymysql.err.OperationalError(1054, "Unknown column")
        return next(rows for where, rows in _MY_SHAPE.items()
                    if where in sql)
    monkeypatch.setattr(MySQLEngine, "_q", q)
    return eng, asked


def test_mysql_and_tidb_digest_every_row_their_whole_read_reads(
        monkeypatch):
    """Every statement the whole shape is read in is the one the digest
    asks over, every column of it, the way this server answers it. The
    digest was over a column's table, name, type and place alone, while
    the shape a tail compares holds its null, default and collation and
    every index, foreign key, check and trigger: a change to any of them
    kept the mark, and the tail the shape it read before."""
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.tidb import TiDBEngine
    # TiDB's feed hands its DDL to the shape check: the same digest asks
    assert TiDBEngine.shape_mark is MySQLEngine.shape_mark
    assert TiDBEngine.column_catalog is MySQLEngine.column_catalog
    for _, _, width, ways in MySQLEngine._SHAPE_READS:
        for way in ways:
            # every column named, and the width the digest takes them by
            assert _aliases(way) == list(range(width)), way[:60]
            assert way.count("%s") == 1, way[:60]
    # a server without an index's expression and visibility (MySQL 5.7)
    eng, asked = _my_shape_engine(monkeypatch, refuse=("expression c8",))
    cat = eng.catalog("src", "app")
    assert [c.name for c in cat.columns["t"]] == ["id", "v"]
    assert cat.keys["t"] == ("id",)
    assert cat.indexes["t"][0].columns == ("v(4) desc",)
    assert cat.others["t"][0] == "check (`id` > 0) not enforced"
    assert cat.others["t"][1].startswith("trigger t_bi before insert"
                                         " order 1 ")
    reads = eng._shape_reads("src")
    ran = [sql for sql, _ in asked if "expression c8" not in sql]
    assert len(ran) == len(reads) == 6
    for (_, sql, order, _), got in zip(reads, ran):
        assert got == f"{sql} order by {order}"
    # the way it answered is kept: the next read asks it first
    assert "expression c8" not in reads[2][1]
    assert "null c8, case when ignored" in reads[2][1]
    asked.clear()
    monkeypatch.setattr(MySQLEngine, "_q",
                        lambda self, side, q, args=None, fresh=False:
                        asked.append((q, args)) or [tuple(["3:9"] * 6)])
    assert eng.shape_mark("src", "app") == "/".join(["3:9"] * 6)
    (mark, args), = asked
    assert args == ("app",) * 6
    start = 0
    for _, sql, _, width in reads:
        # each statement's own part, in order: its rows hashed over every
        # column it names, a missing value apart, and no more
        end = mark.index(f"from ({sql}) q)", start)
        part = mark[start:end]
        for n in range(width):
            assert f"coalesce(concat(char_length(q.c{n}), ':', q.c{n})," \
                   " '-')" in part, (n, sql[:60])
        assert f"q.c{width})" not in part
        start = end + len(sql)
    # a sum of each row's hash: no string of every row a length cuts
    assert "group_concat" not in mark


def test_mysql_a_read_no_way_answers_is_never_read_as_empty(monkeypatch):
    import pymysql
    eng, _ = _my_shape_engine(monkeypatch, refuse=("information_schema"
                                                   ".triggers",))
    with pytest.raises(pymysql.err.OperationalError):
        eng.catalog("src", "app")
    # what is not a missing column or table is the server's refusal,
    # not a way to try next
    from migkit.engines.mysql import MySQLEngine

    def denied(self, side, sql, args=None, fresh=False):
        raise pymysql.err.OperationalError(1142, "SELECT command denied")
    monkeypatch.setattr(MySQLEngine, "_q", denied)
    eng.forget_shapes()
    with pytest.raises(pymysql.err.OperationalError):
        eng.catalog("src", "app")
    assert eng._shape_reads("src")[0][1] == \
        MySQLEngine._SHAPE_READS[0][3][0]


def test_sqlite_digests_its_schema_and_any_change_moves_it(tmp_path):
    import sqlite3

    from migkit.engines.sqlite import SQLiteEngine
    path = tmp_path / "a.db"
    con = sqlite3.connect(path)
    con.execute("create table t (id integer primary key, v text)")
    con.commit()
    eng = _engine(SQLiteEngine, "sqlite",
                  Endpoint(host=str(path), port=0, user="", password=""),
                  db="main")
    was = eng.shape_mark("src", "main")
    assert eng.shape_mark("src", "main") == was
    for ddl in ("alter table t add column w integer",
                "create index t_v on t (v)",
                "create table u (id integer primary key)",
                "drop index t_v"):
        con.execute(ddl)
        con.commit()
        now = eng.shape_mark("src", "main")
        assert now != was, ddl
        was = now
    con.close()
    # two files that counted as far apart by their definitions: a file put
    # in the place of another is not read as the same shape
    marks = []
    for name, ddl in (("b.db", "create table t (id integer primary key,"
                                " v text)"),
                      ("c.db", "create table t (id integer primary key,"
                               " v integer)")):
        con = sqlite3.connect(tmp_path / name)
        con.execute(ddl)
        con.commit()
        con.close()
        marks.append(_engine(SQLiteEngine, "sqlite", Endpoint(
            host=str(tmp_path / name), port=0, user="", password=""),
            db="main").shape_mark("src", "main"))
    assert marks[0].split(":")[0] == marks[1].split(":")[0]
    assert marks[0] != marks[1]


def test_every_source_whose_tail_asks_its_shape_digests_it():
    """A source whose catalogue is read whole (`drift.reader`) and has no
    digest has its tail read all of it every batch."""
    from migkit.engines import engines_with
    reads = set(engines_with("shape_catalog")) | set(
        engines_with("column_catalog"))
    assert {"postgres", "mssql", "oracle", "mysql", "tidb",
            "sqlite"} <= reads, reads
    assert reads - set(engines_with("shape_mark")) == set()


def _moves(mark, run, ddls):
    was = mark()
    assert mark() == was
    for ddl in ddls:
        run(ddl)
        now = mark()
        assert now is not None and now != was, ddl
        was = now


@needs_docker
def test_sql_server_digests_its_catalogue_and_a_change_moves_it(ms):
    from migkit.engines.mssql import MSSQLEngine
    db = ms.name
    ms.one.sql("create table dbo.sm (id int primary key, v nvarchar(10))",
               db)
    eng = _engine(MSSQLEngine, "mssql", ms.one.endpoint(), db)
    _moves(lambda: eng.shape_mark("src", db),
           lambda ddl: ms.one.sql(ddl, db),
           ("alter table dbo.sm alter column v nvarchar(20)",
            "alter table dbo.sm add constraint sm_d default 'x' for v",
            "create index sm_v on dbo.sm (v)",
            "alter table dbo.sm add constraint sm_c check (id > 0)",
            "alter table dbo.sm add w int"))
    # and the whole shape it stands for reads as before
    assert [c.name for c in eng.shape_catalog("src", db).columns[
        "dbo.sm"]] == ["id", "v", "w"]


@needs_docker
def test_oracle_digests_its_catalogue_and_a_change_moves_it(ora):
    from migkit.engines.oracle import OracleEngine
    db = ora.name
    owner = db.upper()
    ora.one.sql(f'create table "{owner}".SM (id number primary key,'
                " v varchar2(10))")
    eng = _engine(OracleEngine, "oracle", ora.one.endpoint(), db)
    _moves(lambda: eng.shape_mark("src", db), ora.one.sql,
           (f'alter table "{owner}".SM modify (v varchar2(20))',
            f'alter table "{owner}".SM add (w number(10,2))',
            f'create table "{owner}".SN (id number)'))
    assert eng.column_catalog("src", db)["sm"] == sorted(
        [("id", "NUMBER"), ("v", "VARCHAR2(20)"), ("w", "NUMBER(10,2)")])


@needs_docker
def test_mysql_digests_its_catalogue_and_a_change_moves_it(my):
    from migkit.engines.mysql import MySQLEngine
    db = my.name
    my.src.sql("create table sm (id int primary key, v varchar(10))", db)
    eng = _engine(MySQLEngine, "mysql", my.src.endpoint(), db)
    _moves(lambda: eng.shape_mark("src", db),
           lambda ddl: my.src.sql(ddl, db),
           ("alter table sm modify v varchar(20)",
            "alter table sm add w int",
            "create table sn (id int)",
            # a column's place, its definition the same
            "alter table sm modify w int after id",
            # what the shape compares beyond a column's name and type
            "alter table sm modify v varchar(20) not null",
            "alter table sm alter v set default 'x'",
            "alter table sm modify v varchar(20) collate utf8mb4_bin not"
            " null default 'x'",
            "create index sm_v on sm (v)",
            "alter table sm alter index sm_v invisible",
            "alter table sn add primary key (id)",
            "alter table sm add constraint sm_f foreign key (w)"
            " references sn (id)",
            "alter table sm add constraint sm_c check (id > 0)",
            "alter table sm alter check sm_c not enforced",
            "create trigger sm_t before insert on sm for each row"
            " set new.w = 1"))
    # and the whole shape it stands for reads as before
    assert [c.name for c in eng.shape_catalog("src", db).columns[
        "sm"]] == ["id", "w", "v"]


def test_the_row_filters_translation_is_made_once_a_table(monkeypatch):
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="rf", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    monkeypatch.setattr(type(pair.hop), "row_filter",
                        lambda self, db, *parts: "id > 5", raising=False)
    made = []
    monkeypatch.setattr(HeteroEngine, "_row_scope_of",
                        lambda self, db, t, pred: made.append(t)
                        or (pred, f"{pred} /* {t} */"))
    for _ in range(4):
        assert pair._row_scope("d", "public.a") == \
            ("id > 5", "id > 5 /* public.a */")
    pair._row_scope("d", "public.b")
    assert made == ["public.a", "public.b"]
    # a shape that moved: made again
    pair.forget_shapes()
    pair._row_scope("d", "public.a")
    assert made == ["public.a", "public.b", "public.a"]
    # one that cannot be translated is said every time, never kept
    monkeypatch.setattr(HeteroEngine, "_row_scope_of",
                        lambda self, db, t, pred: (_ for _ in ()).throw(
                            SystemExit("no translation")))
    for _ in range(2):
        with pytest.raises(SystemExit):
            pair._row_scope("d", "public.c")
