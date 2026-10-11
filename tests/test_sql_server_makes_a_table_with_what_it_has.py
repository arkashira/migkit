"""A table SQL Server makes on a target is made with what the source had
(P0-PARITY.all): its indexes read and made, its columns' defaults,
nullability and identity carried, the tables the target lacks made before
any is filled and their indexes built once the rows are in.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads. Every statement here is new and has not run on a server:
none of these items is in `PROVES`; each waits in
`capabilities.UNPROVED` for the test that runs it on one.
"""
import pytest

from tests.mssql_stub import Server, engine


def _index(name, unique, column, key, filtered=0, kind=2):
    return (name, unique, column, key, filtered, kind)


def test_indexes_are_read_by_their_key_columns_in_order(monkeypatch,
                                                         tmp_path):
    s = Server().on("sys.indexes i join sys.index_columns", [
        _index("ix_a", 0, "region", 1), _index("ix_a", 0, "placed", 2),
        # an included column is no part of the key
        _index("ix_a", 0, "note", 0),
        _index("ux_code", 1, "code", 1),
        _index("ix_open", 0, "status", 1, filtered=1),
        _index("cs_all", 0, "amount", 0, kind=6),
    ])
    eng = engine(monkeypatch, tmp_path, s)
    got = eng.neutral_indexes("src", "appdb", "dbo.orders")
    assert got == [("cs_all", False, ["amount"], False),
                   ("ix_a", False, ["region", "placed"], True),
                   ("ix_open", False, ["status"], False),
                   ("ux_code", True, ["code"], True)]


def test_an_index_is_made_over_its_columns_under_its_name(monkeypatch,
                                                           tmp_path):
    eng = engine(monkeypatch, tmp_path, Server())
    assert eng.neutral_create_index_sql(
        "dst", "appdb", "dbo.orders", "ix_a", False, ["region", "placed"]) \
        == "create index [ix_a] on [dbo].[orders] ([region], [placed])"
    assert eng.neutral_create_index_sql(
        "dst", "appdb", "dbo.orders", "ux]code", True, ["code"]) \
        == "create unique index [ux]]code] on [dbo].[orders] ([code])"


def test_a_schema_change_is_run_on_the_target_and_never_on_a_source(
        monkeypatch, tmp_path):
    s = Server()
    eng = engine(monkeypatch, tmp_path, s)
    eng.execute_ddl("dst", "appdb", "create index [i] on [dbo].[t] ([a])")
    assert s.sent("create index", "dst")
    with pytest.raises(SystemExit) as e:
        eng.execute_ddl("src", "appdb", "create index [i] on [dbo].[t] ([a])")
    assert "does not change a schema on a source" in str(e.value)


def test_a_default_is_read_without_the_parentheses_the_catalogue_adds(
        monkeypatch, tmp_path):
    from migkit.engines.mssql import MSSQLEngine as E
    assert E._unwrapped("((0))") == "0"
    assert E._unwrapped("('new')") == "'new'"
    assert E._unwrapped("(getdate())") == "getdate()"
    assert E._unwrapped("(([a])+([b]))") == "([a])+([b])"
    assert E._unwrapped("(dateadd(day,(1),getdate()))") == \
        "dateadd(day,(1),getdate())"
    # parentheses inside a literal are the literal's
    assert E._unwrapped("('(x')") == "'(x'"
    assert E._unwrapped("(1)+(2)") == "(1)+(2)"


def test_nullability_defaults_and_identity_are_carried(monkeypatch,
                                                        tmp_path):
    s = Server().on("sys.columns c where", [
        ("id", 0, 1, None, 0), ("status", 0, 0, "('new')", 0),
        ("score", 1, 0, "((0))", 0), ("total", 1, 0, None, 1),
        ("note", 1, 0, None, 0)])
    eng = engine(monkeypatch, tmp_path, s)
    got = eng.neutral_column_rules("src", "appdb", "dbo.orders")
    assert got == {
        "id": {"null": False, "default": None, "identity": True},
        "status": {"null": False, "default": "'new'", "identity": False},
        "score": {"null": True, "default": "0", "identity": False},
        "note": {"null": True, "default": None, "identity": False}}
    # a computed column is the server's to compute: it carries no rule
    assert "total" not in got


def test_a_default_is_asked_of_the_server_before_it_is_trusted(monkeypatch,
                                                                tmp_path):
    def answer(side, sql):
        if "bogus" in sql:
            raise RuntimeError("the target refused a statement: Invalid"
                               " column name 'bogus'")
        return [(1,)]
    s = Server().on("select \\(", answer)
    eng = engine(monkeypatch, tmp_path, s)
    assert eng.default_works("dst", "appdb", "getdate()") is True
    assert eng.default_works("dst", "appdb", "bogus + 1") is False


def test_a_table_the_target_lacks_is_made_before_any_is_filled(
        monkeypatch, tmp_path):
    made, finished = [], []

    class Pair:
        def create_missing(self, db, log=None):
            made.append(db)

        def finish_created(self, db, log=None):
            finished.append(db)
    eng = engine(monkeypatch, tmp_path, Server())
    eng._as_pair = lambda *a, **k: Pair()
    eng.create_missing("appdb")
    eng.finish_created("appdb")
    assert made == ["appdb"] and finished == ["appdb"]


def test_the_indexes_of_a_built_table_are_made_once_its_rows_are_in(
        monkeypatch, tmp_path):
    """The pair's `finish_created` reads the source's indexes through
    `neutral_indexes` and builds them through the target's
    `neutral_create_index_sql`: the two this engine now has."""
    from migkit import durable
    s = Server().on("sys.indexes i join sys.index_columns", [
        _index("ix_a", 0, "region", 1), _index("ux_code", 1, "code", 1),
        _index("ix_open", 0, "status", 1, filtered=1)])
    eng = engine(monkeypatch, tmp_path, s)
    monkeypatch.setattr(type(eng), "neutral_columns",
                        lambda self, side, db, t: [
                            ("region", "varchar(10)"), ("code", "varchar(5)"),
                            ("status", "varchar(5)")])
    durable.write_json(tmp_path / "built-tables.json",
                       {"orders": "dbo.orders"})
    said = []
    pair = eng._as_pair()
    # the table's columns map to themselves
    pair._mapped_types = lambda db, t, cols: (None, {n: n for n in cols})
    pair.finish_created("appdb", said.append)
    made = s.sent("create (unique )?index", "dst")
    assert made == [
        "create index [ix_a] on [dbo].[orders] ([region])",
        "create unique index [ux_code] on [dbo].[orders] ([code])"]
    assert any("index ix_open not carried" in m for m in said)
