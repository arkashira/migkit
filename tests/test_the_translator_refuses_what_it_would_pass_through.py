"""What the query translator would carry with another meaning, refused or
rewritten (backlog R11, research round 2026-09-28 F3).

Measured on the installed translator (30.18): a hierarchical query
(`CONNECT BY`) is written back as it came for every target, with no
warning, so a PostgreSQL view creation fails and a MySQL one means
something else; Oracle's `(+)` outer join is refused although the
rewrite into an ANSI join ships beside it; `TRY_CAST` becomes a plain
`CAST`, which raises where the source answered NULL; and a body of
statements comes back as text it did not read, generated back verbatim.
"""
import pytest

from migkit.config import Endpoint, Hop


def _pair(src, monkeypatch, views=(), functions=()):
    from migkit.engines import engine_named
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="tr", engine="hetero",
              options={"source_engine": src, "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              databases=["cx"])
    cls = type(engine_named(src, hop))
    monkeypatch.setattr(cls, "neutral_views",
                        lambda self, side, db: list(views))
    monkeypatch.setattr(cls, "neutral_tables",
                        lambda self, side, db: ["emp", "dept"])
    monkeypatch.setattr(cls, "neutral_functions",
                        lambda self, side, db: list(functions))
    return HeteroEngine(hop)


def test_a_hierarchical_query_is_refused_not_passed_through(monkeypatch):
    eng = _pair("oracle", monkeypatch, views=[
        ("tree", "select id, mgr, level as depth from emp start with mgr"
                 " is null connect by prior id = mgr")])
    got = {n: s for n, s, _ in eng.converted_code("cx", propose=False)}
    assert got["tree"].startswith("-- view tree not converted: it is a"
                                  " hierarchical query (CONNECT BY)"), got
    # nothing of it is left to run: every line is a comment
    assert all(ln.startswith("--") for ln in got["tree"].splitlines()), got


def test_an_oracle_outer_join_becomes_an_ansi_one(monkeypatch):
    eng = _pair("oracle", monkeypatch, views=[
        ("staff", "select e.id, d.name from emp e, dept d where e.dept ="
                  " d.id(+) and d.open(+) = 1")])
    got = {n: s for n, s, _ in eng.converted_code("cx", propose=False)}
    # the constant's condition moves into the join, so an employee of a
    # closed department keeps its row with no name - as Oracle answers
    assert got["staff"] == ("create view staff as SELECT e.id, d.name FROM"
                            " emp AS e LEFT JOIN dept AS d ON e.dept = d.id"
                            " AND d.open = 1;"), got


def test_try_cast_answers_null_where_the_value_does_not_convert(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine

    def rows(self, side, db, sql, args=None):
        if "from sys.tables" in sql:
            return [["dbo.emp"]]
        if "from sys.views" in sql:
            return [["dbo.codes", "CREATE VIEW dbo.codes AS SELECT"
                                  " TRY_CAST(code AS int) AS n FROM dbo.emp"]]
        if "from sys.objects" in sql:
            return []
        raise AssertionError(sql)
    monkeypatch.setattr(MSSQLEngine, "_rows", rows)
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(Hop(
        name="ms", engine="hetero",
        options={"source_engine": "mssql", "target_engine": "postgres"},
        source=Endpoint(host="127.0.0.1", port=1, user="sa",
                        password="CHANGE_ME"),
        target=Endpoint(host="127.0.0.1", port=1, user="u",
                        password="CHANGE_ME"), databases=["cx"]))
    got = {n: s for n, s, _ in eng.converted_code("cx", propose=False)}
    assert got["codes"] == (
        "create view codes as SELECT CASE WHEN PG_INPUT_IS_VALID(CAST(code"
        " AS TEXT), 'INT') THEN CAST(CAST(code AS TEXT) AS INT) END AS n"
        " FROM emp;"), got


@pytest.mark.parametrize("dst,expect", [
    ("oracle", "SELECT CAST(x AS INT DEFAULT NULL ON CONVERSION ERROR)"),
    ("clickhouse", "SELECT accurateCastOrNull(x, 'Int32')"),
    ("tsql", "SELECT TRY_CAST(x AS INTEGER)")])
def test_try_cast_on_every_target_that_has_a_form_for_it(dst, expect):
    from migkit.storedcode.translate import translate
    assert translate("select try_cast(x as int)", "tsql", dst) == expect


def test_try_cast_is_refused_where_the_target_has_no_form_for_it():
    from migkit.storedcode.translate import Refused, translate
    with pytest.raises(Refused, match="has no cast that does that"):
        translate("select try_cast(x as int)", "tsql", "mysql")


@pytest.mark.parametrize("src,text", [
    ("mysql", "IF x > 1 THEN SELECT 1; END IF"),
    ("mysql", "CREATE TRIGGER t BEFORE INSERT ON a FOR EACH ROW SET"
              " NEW.x = 1"),
    ("tsql", "BEGIN TRY SELECT 1 END TRY BEGIN CATCH SELECT 2 END CATCH"),
    ("mysql", "REPLACE INTO t (a) VALUES (1)")])
def test_a_body_of_statements_is_refused_not_kept_as_text(src, text):
    """Each of these the translator reads as text it did not parse and
    writes back verbatim - an IF block even as `... END AS IF`."""
    from migkit.storedcode.translate import Refused, translate
    with pytest.raises(Refused):
        translate(text, src, "postgres")


def test_a_function_reading_a_table_is_not_taken_for_its_select_list(
        monkeypatch):
    """PostgreSQL's one-expression reading named the select's FROM by the
    translator's old key: `select max(amount) from orders` was taken for
    `max(amount)`, and carried to the other engine without its table."""
    from migkit.engines.postgres import PostgresEngine
    rows = "\n".join("\x1f".join(r) for r in [
        ("top", "f", "sql", "c character varying", "numeric",
         "select max(amount) from orders"),
        ("twice", "f", "sql", "x integer", "integer", "select x * 2")])
    monkeypatch.setattr(PostgresEngine, "_psql",
                        lambda self, side, db, sql, *a, **k: rows)
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"), databases=["cx"])
    got = {n: one for n, _, _, one in
           PostgresEngine(hop).neutral_functions("src", "cx")}
    assert got == {"top": None, "twice": "x * 2"}, got
