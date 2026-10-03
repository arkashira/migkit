"""`schema --convert --apply` (audit SC7, SC21): on a PostgreSQL target
in one transaction on one connection, every statement that fails named
and none of the others kept; and the tables it made recorded as the move
records those it builds, so their indexes are added once the rows are in.

Before: each statement on a connection of its own, half a schema left
where one failed; and a table made here was no longer missing when the
move came, so the move built none of its indexes - lost for good.
"""
import json

from migkit import cli


class _Cursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql):
        self.conn.ran.append(sql)
        if "bad" in sql and not sql.startswith(("savepoint", "rollback")):
            raise RuntimeError("relation \"nope\" does not exist\nLINE 1")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self):
        self.ran, self.ended = [], []

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.ended.append("commit")

    def rollback(self):
        self.ended.append("rollback")

    def close(self):
        self.ended.append("close")


def _hop(engine):
    from migkit.config import Endpoint, Hop
    return Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint())


def _pg():
    """A PostgreSQL target whose connections are `_Conn`s."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres"))
    eng.conns = []

    def connect(side, db, purpose="read", **extra):
        eng.conns.append((side, db))
        eng.last = _Conn()
        return eng.last
    eng._connect = connect
    return eng


class _Pair:
    def __init__(self, target=None):
        self.dst_engine = target or _pg()


def test_one_transaction_whole_or_not_at_all():
    pair = _Pair()
    made, failed, whole = cli._create_converted(
        pair, "app", [("a", "create table a (id int)"),
                      ("v", "create view v as select * from bad"),
                      ("b", "create table b (id int)")])
    assert whole and made == [] and len(failed) == 1, (made, failed)
    assert failed[0].startswith("v: relation"), failed
    assert pair.dst_engine.conns == [("dst", "app")]      # one connection
    assert pair.dst_engine.last.ended == ["rollback", "close"]
    # each under its own savepoint, so every failure is named
    ran = pair.dst_engine.last.ran
    assert "rollback to savepoint migkit_convert_1" in ran, ran
    assert "create table b (id int)" in ran, ran


def test_all_made_is_committed_once():
    pair = _Pair()
    made, failed, whole = cli._create_converted(
        pair, "app", [("a", "create table a (id int)"),
                      ("b", "create table b (id int)")])
    assert (made, failed, whole) == (["a", "b"], [], True)
    assert pair.dst_engine.last.ended == ["commit", "close"]


def test_a_mysql_target_takes_them_on_one_connection():
    """Its DDL commits as it runs, so not one transaction - but one
    connection, signed in once, where each was a connection of its own
    (audit 2026-10-01, SC21); a statement that fails is named and the rest
    are made."""
    import pymysql

    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql"))
    opened = []

    class Cur(_Cursor):
        def execute(self, sql):
            self.conn.ran.append(sql)
            if "bad" in sql:
                raise pymysql.err.ProgrammingError(
                    1146, "Table 'app.bad' doesn't exist")

    class Conn(_Conn):
        def cursor(self):
            return Cur(self)

    def conn(side, retry=True):
        opened.append(side)
        opened.append(Conn())
        return opened[-1]
    eng._conn = conn
    got, failed, whole = cli._create_converted(
        _Pair(eng), "app", [("a", "create table a (id int)"),
                            ("v", "create view v as select * from bad"),
                            ("b", "create table b (id int)")])
    assert got == ["a", "b"] and not whole, (got, failed)
    assert len(failed) == 1 and failed[0].startswith("v: "), failed
    assert opened[0] == "dst" and len(opened) == 2, opened
    assert opened[1].ran[0] == "use `app`", opened[1].ran
    assert opened[1].ended == ["commit", "close"]


def test_any_other_target_takes_them_one_by_one():
    from migkit.engines.base import Engine
    made = []

    class Other(Engine):
        def neutral_create_code(self, side, db, stmt):
            if "bad" in stmt:
                raise RuntimeError("Table 'bad' doesn't exist")
            made.append(stmt)
    got, failed, whole = cli._create_converted(
        _Pair(Other(_hop("sqlite"))), "app",
        [("a", "create table a (id int)"),
         ("v", "create view v as select * from bad")])
    assert got == ["a"] and len(failed) == 1 and not whole
    assert made == ["create table a (id int)"]


def test_the_tables_it_made_get_their_indexes_from_the_move(tmp_path):
    class Hop:
        def report_dir(self, db=None):
            return tmp_path

    class Src:
        def neutral_tables(self, side, db):
            return ["public.orders", "public.people"]

    class Pair:
        src_engine = Src()

        @staticmethod
        def _leaf(t):
            return t.split(".")[-1]
    (tmp_path / "built-tables.json").write_text(json.dumps(
        {"items": "public.items"}))
    cli._record_built(Hop(), Pair(), "app", ["orders", "a_view"])
    assert json.loads((tmp_path / "built-tables.json").read_text()) == {
        "items": "public.items", "orders": "public.orders"}
