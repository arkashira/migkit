"""The batches that are not a keyed read's page (READ.batch-literals) - a
list asked by key, rows written at once, a cursor's batch - are sized by
one rung (`decide.batch_rows`), asked once as the list, the write or the
cursor opens: where they were `subset.BATCH`, `repairlog.SNAPSHOT_BATCH`,
`leaves.IN_LIST`, `leaves.confirm`'s 5000 and a cursor's 1000. And the
dump programs' option lines for one intent are said once
(`movers.pg_schema_args`, DUMP.options). Fakes only, no servers."""
import ast
import json
import pathlib
from types import SimpleNamespace

import pytest

from migkit import decide, leaves, movers, pieces, repairlog, subset
from migkit.engines.base import Engine
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("batch_rows", "the statement's limit", "holds"),
    ("batch_rows", "the statement's limit", "refuses"),
    ("batch_rows", "measured writes", "holds"),
    ("batch_rows", "measured writes", "refuses"),
    ("batch_rows", "the read's page", "holds"),
    ("batch_rows", "the read's page", "refuses"),
    ("batch_rows", "unmeasured", "holds"),
]
HOME = pathlib.Path(movers.__file__).resolve().parent


def test_a_list_by_key_is_held_to_the_statements_limit_per_row():
    with watched() as seen:
        rows, c = decide.batch_rows({"kind": decide.BY_KEY, "limit": 2100,
                                     "params": 3, "page": 5000})
    assert (rows, c.path) == (700, "the statement's limit")
    assert branch(seen, "the statement's limit") == [HOLDS]
    with watched() as seen:
        rows, c = decide.batch_rows({"kind": decide.BY_KEY, "params": 3})
    assert (rows, c.path) == (decide.PAGE_UNMEASURED, "unmeasured")
    assert branch(seen, "the statement's limit") == [REFUSES]
    assert "no limit" in why(seen, "the statement's limit")
    assert branch(seen, "unmeasured") == [HOLDS]
    # a limit smaller than the page still holds any other rung's answer
    rows, c = decide.batch_rows({"kind": decide.WRITE, "limit": 900,
                                 "params": 9, "page": 5000})
    assert (rows, c.path) == (100, "the read's page")


def test_a_write_follows_its_measured_rate_and_the_memory_its_width_takes():
    with watched() as seen:
        rows, c = decide.batch_rows({"kind": decide.WRITE, "rate": 4000,
                                     "width": 10, "seconds": 2})
    assert (rows, c.path) == (8000, "measured writes")
    assert branch(seen, "measured writes") == [HOLDS]
    wide, _ = decide.batch_rows({"kind": decide.WRITE, "rate": 4000,
                                 "width": 10 ** 6, "free": 64 * 10 ** 6})
    assert wide == 4                      # 64 MB / 16 shares / 1 MB a row
    with watched() as seen:
        rows, c = decide.batch_rows({"kind": decide.WRITE, "width": 10,
                                     "page": 300})
    assert (rows, c.path) == (300, "the read's page")
    assert branch(seen, "measured writes") == [REFUSES]
    assert branch(seen, "the read's page") == [HOLDS]
    assert "has been timed" in why(seen, "measured writes")
    with watched() as seen:
        assert decide.cursor_rows(None) == decide.PAGE_UNMEASURED
    assert branch(seen, "the read's page") == [REFUSES]
    assert decide.cursor_rows(16) == 16


class Asked:
    """`decide.batch_rows` counted: the facts of every ask."""

    def __init__(self, monkeypatch, rows=None):
        self.facts, real = [], decide.batch_rows

        def ask(facts):
            self.facts.append(dict(facts))
            got = real(facts)
            return (rows, got[1]) if rows else got
        monkeypatch.setattr(decide, "batch_rows", ask)


class Sink(Engine):
    STATEMENT_VALUES = 3000

    def __init__(self):
        super().__init__(None)
        self.writes, self.lists = [], []

    def neutral_write(self, side, db, table, columns, rows):
        self.writes.append(len(rows))
        return len(rows)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        self.lists.append(len(keys))
        return {str(k): k for k in keys}


def test_a_subset_writes_each_table_by_one_ask_and_the_next_by_its_rate(
        monkeypatch):
    asked = Asked(monkeypatch, rows=300)
    eng = Sink()
    cols = [("id", "integer"), ("v", "text")]
    pair = SimpleNamespace(dst_engine=eng,
                           _move_columns=lambda s, d, db: (cols, cols, []))
    p = subset.Plan("d", columns={"t": cols},
                    rows={"t": {str(i): [i, "x"] for i in range(1000)}})
    assert subset._write(pair, "d", "t", "t", p, {}, lambda m: None) == 1000
    assert eng.writes == [300, 300, 300, 100]
    assert len(asked.facts) == 1
    assert asked.facts[0]["kind"] == decide.WRITE
    assert asked.facts[0]["limit"] == 3000 and asked.facts[0]["params"] == 2
    assert asked.facts[0]["rate"] is None and asked.facts[0]["width"]
    assert p.write_rate and p.write_rate > 0
    first = p.write_rate
    subset._write(pair, "d", "t", "t", p, {}, lambda m: None)
    assert len(asked.facts) == 2 and asked.facts[1]["rate"] == first


def test_a_snapshot_is_put_back_in_writes_the_decision_sized_once(
        monkeypatch, tmp_path):
    asked = Asked(monkeypatch, rows=40)
    eng = Sink()
    eng.neutral_empty = lambda *a: None
    cols = [("id", "integer"), ("v", "text")]
    (tmp_path / "t.snapshot.jsonl").write_text(
        "".join(json.dumps([str(i), "x"]) + "\n" for i in range(100)))
    monkeypatch.setattr(repairlog, "table_digest",
                        lambda *a: [100, "abc"])
    io = repairlog.TableIO(eng, "d", "t", cols, ["id"], tmp_path)
    io.put(("t",), {"snapshot": "t.snapshot.jsonl", "digest": [100, "abc"]})
    assert eng.writes == [40, 40, 20]
    assert len(asked.facts) == 1 and asked.facts[0]["kind"] == decide.WRITE
    assert not hasattr(repairlog, "SNAPSHOT_BATCH")


def test_the_leaves_lists_and_the_confirm_read_ask_the_engines_limit():
    eng = Sink()
    eng.STATEMENT_VALUES = 600
    assert leaves.in_list(eng) == 600 and leaves.in_list(eng, 2) == 300
    assert leaves.in_list(SimpleNamespace()) == decide.PAGE_UNMEASURED
    assert not hasattr(leaves, "IN_LIST") and not hasattr(subset, "BATCH")
    other = Sink()
    other.STATEMENT_VALUES = 400
    side = dict(side="src", db="d", table="t", where=None,
                cols=[("id", "integer"), ("v", "text")], key=["id"])
    a = SimpleNamespace(eng=eng, **side)
    b = SimpleNamespace(eng=other, **side)
    keys = [(str(i),) for i in range(1000)]
    found = SimpleNamespace(changed=keys, raw={k: k for k in keys})
    leaves.confirm(a, b, found)
    # one list a side, as long as the side that carries fewer takes
    assert eng.lists == [400] and other.lists == [400]
    leaves.confirm(a, b, found, most=7)
    assert eng.lists[-1] == 7


def test_the_schema_dump_options_for_one_intent_are_the_same_list():
    assert movers.pg_schema_args("pg_dump") == [
        "--schema-only", "--no-owner", "--no-privileges"]
    # an archive keeps the owners for its restore, which drops them
    assert movers.pg_schema_args("pg_dump", True, "pre-data") == [
        "--schema-only", "-Fc", "--section=pre-data"]
    assert movers.pg_schema_args("pg_restore", section="post-data") == [
        "--section=post-data", "--no-owner", "--no-privileges"]
    assert movers.pg_schema_args("pg_restore") == list(movers.PG_UNOWNED)


def test_the_engine_builds_its_schema_reads_from_the_one_owner():
    from migkit.engines.postgres import PostgresEngine
    got = []
    eng = PostgresEngine.__new__(PostgresEngine)
    eng._client = lambda side, db, program, *args, **k: (
        got.append((program, list(args))) or ([program], {}))
    import migkit.engines.postgres as pg
    real = pg.run
    pg.run = lambda argv, env=None: SimpleNamespace(stdout="")
    try:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            eng._psql = lambda *a, **k: ""
            eng._sequence_snapshot = lambda *a, **k: ""
            try:
                eng.snapshot_state("d", pathlib.Path(d), kind="schema")
            except Exception:  # noqa: BLE001 - what ran first is the dump
                pass
    finally:
        pg.run = real
    dumps = [args for program, args in got if program == "pg_dump"]
    assert dumps and dumps[-1] == movers.pg_schema_args("pg_dump")


def test_no_schema_option_line_is_written_out_beside_its_owner():
    """A dump or restore built in movers.py or the PostgreSQL engine names
    no owner-or-section option of its own: each asks `pg_schema_args`."""
    said = {"--schema-only", "--no-owner", "--no-privileges", "-Fc"}
    for rel in ("movers.py", "engines/postgres.py"):
        tree = ast.parse((HOME / rel).read_text())
        for node in ast.walk(tree):
            items = None
            if isinstance(node, ast.List) and node.elts and isinstance(
                    node.elts[0], ast.Constant) and node.elts[0].value in (
                    "pg_dump", "pg_restore"):
                items = node.elts
            elif isinstance(node, ast.Call) and getattr(
                    node.func, "attr", None) == "_client":
                items = node.args
            for e in items or ():
                assert not (isinstance(e, ast.Constant)
                            and e.value in said), (rel, node.lineno, e.value)


# --- the numbers callers passed a cursor, the chunk loops and the engines'
# statement limits are the decision's (READ.cursor-literals) ---------------

#: the engines whose server says a limit to one statement's values, and it
STATED = [("postgres", "PostgresEngine", 65535),
          ("pgwire", "PgWireEngine", 65535),
          ("cockroachdb", "CockroachEngine", 65535),
          ("mysql", "MySQLEngine", 65535),
          ("tidb", "TiDBEngine", 65535),
          ("mssql", "MSSQLEngine", 2098),
          ("oracle", "OracleEngine", 1000),
          ("db2", "Db2Engine", 32767),
          ("sqlite", "SQLiteEngine", None)]


@pytest.mark.parametrize("module,name,limit", STATED)
def test_an_engine_with_a_statement_limit_says_it_and_its_lists_keep_it(
        module, name, limit):
    import importlib
    import sqlite3
    cls = getattr(importlib.import_module(f"migkit.engines.{module}"), name)
    if limit is None:   # the library's own, as its version says
        limit = 32766 if sqlite3.sqlite_version_info >= (3, 32) else 999
    assert cls.STATEMENT_VALUES == limit
    eng = cls.__new__(cls)
    with watched() as seen:
        assert eng.batch_size(decide.BY_KEY, 3) == limit // 3
    assert branch(seen, "the statement's limit") == [HOLDS]
    # a key wider than the statement still goes, one row a statement
    assert eng.batch_size(decide.BY_KEY, 2 * limit) == 1


def test_a_cursor_a_caller_opens_is_the_decisions_and_follows_its_facts(
        monkeypatch):
    sizes = []

    class Eng:
        RESUMES_BY_KEY = False

        def neutral_batches(self, side, db, table, columns, size=None,
                            where=None):
            sizes.append(size)
            yield [(1, "x")] * 3
    cols = [("id", "integer"), ("v", "text")]
    pieces.sample_sizes(Eng(), "src", "d", "t", cols, 50)
    pieces.sample_sizes(Eng(), "src", "d", "t", cols, 9)
    assert sizes == [50, 9]             # the page the sample was given
    asked = Asked(monkeypatch, rows=7)
    pieces.sample_sizes(Eng(), "src", "d", "t", cols, 50)
    assert sizes[-1] == 7 and asked.facts[-1]["kind"] == decide.CURSOR


class Cur:
    """A cursor that counts the values of every statement."""

    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, args=None):
        self.calls.append(len(args or ()))


def test_a_write_loop_cuts_by_the_engines_limit_and_the_rows_values(
        monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine.__new__(PostgresEngine)
    eng._piping = lambda: None
    cur = Cur()
    conn = SimpleNamespace(cursor=lambda: cur)
    values = [(i, "x") for i in range(10)]
    eng.STATEMENT_VALUES = 8            # 4 rows of 2 values a statement
    eng._in_statements(conn, "insert into t values ", "", values)
    assert cur.calls == [8, 8, 4]
    cur.calls.clear()
    eng.STATEMENT_VALUES = PostgresEngine.STATEMENT_VALUES
    eng._in_statements(conn, "insert into t values ", "", values)
    assert cur.calls == [20]
    cur.calls.clear()
    monkeypatch.setattr(pieces, "BATCH_BYTES", 3 * 10 ** 6)
    wide = [(i, "x" * 10 ** 6) for i in range(10)]
    eng._in_statements(conn, "insert into t values ", "", wide)
    # the rows' width holds a batch to the bytes one batch may take
    assert cur.calls == [4, 4, 4, 4, 4]


def test_a_delete_by_key_loop_cuts_by_the_engines_limit_per_key():
    from contextlib import contextmanager
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine.__new__(MySQLEngine)
    eng.local_table = lambda t: t
    eng._d = lambda side, db: db
    cur = Cur()

    @contextmanager
    def writer(side, db):
        yield SimpleNamespace(cursor=lambda: cur)
    eng._writer = writer
    keys = [{"id": i, "k": i} for i in range(9)]
    eng.STATEMENT_VALUES = 6            # 3 keys of 2 columns a statement
    eng._apply_deletes("dst", "d", "t", keys)
    assert cur.calls == [6, 6, 6]
    cur.calls.clear()
    eng.STATEMENT_VALUES = 9
    eng._apply_deletes("dst", "d", "t", keys)
    assert cur.calls == [8, 8, 2]
