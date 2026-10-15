"""PostgreSQL's and SQL Server's deep value checks read a table once a
side through the shared scan plan (backlog P0-VERIFY-EXECUTION,
`scanplan`): every column's NULL/empty, float and replacement-character
aggregates in one read, not one read a column a check; a column the read
fails on is said as unknown, never as clean or as zero; no column limit.

Pure: the servers' answers are given in-process.
"""
import re
from types import SimpleNamespace

import pytest

from migkit import sizing
from migkit.engines.mssql import MSSQLEngine
from migkit.engines.postgres import PostgresEngine

PROVES = [("verify.value.one-plan", ("postgres", "mssql"))]

ROWS = {"src": [(None, 1.5), ("", 2.5), ("x�", None)],
        "dst": [(None, 1.5), (None, 2.5), ("x��", None)]}


def _split(exprs):
    """The top-level comma-separated expressions of a select list."""
    out, depth, cur = [], 0, ""
    for ch in exprs:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    return out + [cur.strip()]


def _value(e, rows):
    texts = [r[0] for r in rows]
    floats = [r[1] for r in rows if r[1] is not None]
    if "is null" in e:
        return sum(t is None for t in texts)
    if "= ''" in e:
        return sum(t == "" for t in texts)
    if "FFFD" in e:
        return sum((t or "").count("�") for t in texts)
    if "65533" in e:
        return sum("�" in (t or "") for t in texts)
    if "max(abs" in e:
        return max(floats, default=0)
    if "sum(cast" in e:
        return sum(floats)
    if e.startswith("count("):
        return len(floats)
    raise AssertionError(e)


def _answer(asked, side, sql, fail=None):
    asked.append((side, sql))
    head, rest = re.match(r"select (count(?:_big)?\(\*\)), (.*) from ",
                          sql).groups()
    exprs = _split(rest)
    if fail and any(fail in e for e in exprs):
        raise RuntimeError("Lost connection while reading")
    rows = ROWS[side]
    return [len(rows)] + [_value(e, rows) for e in exprs], head


@pytest.fixture(autouse=True)
def _plan(monkeypatch):
    monkeypatch.setattr(sizing, "estimate",
                        lambda *a, **k: sizing.Plan(2, 4, {}))
    monkeypatch.setattr(sizing, "resources", lambda *a, **k: {})


def _hop(tmp_path):
    return SimpleNamespace(slice=10, report_dir=lambda db: tmp_path)


def _mssql(tmp_path, asked, fail=None):
    eng = object.__new__(MSSQLEngine)
    eng.hop = _hop(tmp_path)
    named = {MSSQLEngine.TEXT_COLUMNS: [("dbo.t", "c")],
             MSSQLEngine.FLOAT_COLUMNS: [("dbo.t", "f")]}
    eng._columns_named = lambda db, kinds: named[kinds]
    eng._qualified = lambda side, db, t: t
    heads = []

    def cmd(side, db, sql):
        row, head = _answer(asked, side, sql, fail)
        heads.append(head)
        return [[str(v) for v in row]]
    eng._cmd = cmd
    return eng, heads


def test_sql_server_reads_null_empty_and_float_in_one_read_a_side(tmp_path):
    asked = []
    eng, heads = _mssql(tmp_path, asked)
    scans = eng._deep_scans("app")
    assert len(asked) == 2 and {s for s, _ in asked} == {"src", "dst"}
    # a table past two billion rows still counts
    assert set(heads) == {"count_big(*)"}
    null = eng._deep_nullempty("app", scans)
    assert null.status == "diff" and "src null/empty=1|1 dst=2|0" in null.detail
    assert eng._deep_float("app", scans).status == "ok"
    assert len(asked) == 2


def test_sql_server_says_a_column_it_could_not_read_as_unknown(tmp_path):
    asked = []
    eng, _ = _mssql(tmp_path, asked, fail="max(abs")
    scans = eng._deep_scans("app")
    flt = eng._deep_float("app", scans)
    assert flt.status == "error" and "dbo.t.f" in flt.detail
    assert "Lost connection" in flt.detail
    # the other column still counts, read apart
    assert eng._deep_nullempty("app", scans).status == "diff"


def _postgres(tmp_path, asked, fail=None, columns=1):
    eng = object.__new__(PostgresEngine)
    eng.hop = _hop(tmp_path)
    eng._change_marker = lambda side, db, t: None
    eng._held_for_cutover = lambda db: ""
    eng._scoped = lambda db, text, sep="|": text.splitlines()

    def psql(side, db, sql):
        if "pg_attribute" in sql:
            return "\n".join(f"public.t|c{i}" for i in range(columns))
        row, _ = _answer(asked, side, sql, fail)
        return "|".join(str(v) for v in row)
    eng._psql = psql
    return eng


def test_postgres_reads_every_text_column_of_a_table_once_a_side(tmp_path):
    asked = []
    eng = _postgres(tmp_path, asked, columns=40)
    scans, unread = eng._deep_scans("app", "select pg_attribute")
    assert not unread and len(asked) == 2
    # no thirty-column limit: every column is read, in the one read
    assert len(scans.of("nullempty")) == 40
    agg = scans.of("nullempty")[39]
    assert scans.value("src", agg) == (1, 1)
    assert scans.value("dst", agg) == (2, 0)
    fffd = scans.of("charset")[0]
    assert scans.value("src", fffd) == (1,) and scans.value("dst", fffd) == (2,)


def test_postgres_never_reads_a_failed_column_as_zero(tmp_path):
    asked = []
    eng = _postgres(tmp_path, asked, fail="FFFD")
    scans, _ = eng._deep_scans("app", "select pg_attribute")
    fffd = scans.of("charset")[0]
    assert scans.value("src", fffd) is None
    assert set(scans.failures("charset")) == {"public.t.c0"}
    assert scans.value("dst", scans.of("nullempty")[0]) == (2, 0)


def test_postgres_says_why_where_the_columns_cannot_be_listed(tmp_path):
    eng = _postgres(tmp_path, [])

    def broken(side, db, sql):
        raise RuntimeError("permission denied for table pg_attribute")
    eng._psql = broken
    scans, unread = eng._deep_scans("app", "select pg_attribute")
    assert scans is None and "permission denied" in unread["the text columns"]
