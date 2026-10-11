"""The MySQL deep checks read every column they plan and say what they
could not read; `watch` reads what is there now and which tables are
still written; the move's guard and every list leave out migkit's own
tables by their exact names and nothing more (audit 2026-10-01, LG2,
V14, DU26).

The character set check read the first 30 text columns, the generated
check passed over a failed read, the NULL/empty and float checks dropped
a failed column with `continue`, the large-value check the same - and
each still said `ok`. `watch` read the catalogue's estimates as cached
up to a day and had no way to say a table was still being written.
`like 'migkit%'` hid `migkitchen`, and `moved_nothing` asked every table.

Pure: the server's answers are given in-process.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine

TYPES = {"text": [("orders", "note"), ("lines", "body")],
         "float": [("orders", "price"), ("lines", "weight")],
         "blob": [("orders", "note"), ("lines", "body")],
         "datetime": [("orders", "at"), ("lines", "at")]}


def _engine(fail=None, rows=None, **kw):
    """`fail`: the table whose reads fail; `rows(sql)` answers a scan."""
    eng = MySQLEngine(Hop(name="d", engine="mysql", source=Endpoint(),
                          target=Endpoint(), databases=["app"], **kw))
    eng.hop.report_dir = lambda db=None: __import__("pathlib").Path("/tmp")
    asked = []

    def q(side, sql, args=None, fresh=False):
        asked.append((side, sql, fresh))
        if "information_schema.columns" in sql and "data_type in" in sql:
            kinds = set(args[1:])
            for kind in ("text", "float", "blob", "datetime"):
                if kind in kinds:
                    return TYPES[kind]
            return []
        if "information_schema.tables" in sql:
            return [("orders",), ("lines",)]
        if "max_allowed_packet" in sql:
            return [(64 << 20,)]
        if "constraint_name='PRIMARY'" in sql:
            return [("id",)]
        if "character_set_name is not null" in sql:
            return []
        if fail and f"`{fail}`" in sql:
            raise RuntimeError("Lost connection to MySQL server")
        return (rows or (lambda s, side: [(0,) * 30]))(sql, side)
    eng._q = q
    eng.asked = asked
    return eng


CHECKS = {
    "nullempty": lambda e: e._deep_nullempty("app", "app")[0],
    "float": lambda e: e._deep_float("app", "app")[0],
    "charset": lambda e: e._deep_charset("app", "app"),
    "lobs": lambda e: e._lob_check("app"),
    "time shift": lambda e: e._deep_timeshift("app"),
}


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_a_column_not_read_is_said_never_ok(name):
    whole = CHECKS[name](_engine())
    assert whole.status == "ok", (name, whole.detail)
    part = CHECKS[name](_engine(fail="lines"))
    assert part.status == "warn", (name, part.detail)
    assert "not read: lines." in part.detail, (name, part.detail)
    assert "compared 1 of 2" in part.detail, (name, part.detail)


def test_a_difference_found_stays_a_difference_beside_what_was_not_read():
    def rows(sql, side):
        # the target holds one NULL where the source holds ''
        if "`orders`" in sql:
            return [(0, 1)] if side == "src" else [(1, 0)]
        return [(0,) * 30]
    got = _engine(fail="lines", rows=rows)._deep_nullempty("app", "app")[0]
    assert got.status == "diff" and "orders.note" in got.detail, got.detail
    assert "not read: lines.body" in got.detail, got.detail


def test_every_planned_column_is_read_one_scan_a_table():
    eng = _engine()
    eng._deep_nullempty("app", "app")
    scans = [sql for _, sql, _ in eng.asked if "is null" in sql]
    # two tables, two sides: four scans, whatever the columns
    assert len(scans) == 4, scans


def test_a_stored_generated_column_not_read_is_said():
    eng = MySQLEngine(Hop(name="d", engine="mysql", source=Endpoint(),
                          target=Endpoint(), databases=["app"]))

    def q(side, sql, args=None, fresh=False):
        if "generation_expression <> ''" in sql:
            return [("orders", "total", "STORED GENERATED", "`a` * 2"),
                    ("lines", "sum", "STORED GENERATED", "`b` + 1")]
        if "<=>" in sql and "`lines`" in sql:
            raise RuntimeError("Lost connection to MySQL server")
        return [(0,)]
    eng._q = q
    got = eng._deep_generated("app", "app")
    assert got.status == "warn" and "not read: lines.sum" in got.detail, \
        got.detail


def test_watch_reads_what_is_there_now_and_what_is_still_written():
    eng = MySQLEngine(Hop(name="w", engine="mysql", source=Endpoint(),
                          target=Endpoint(), databases=["app"],
                          exclude=["audit"]))
    seen = []

    def q(side, sql, args=None, fresh=False):
        seen.append(fresh)
        if "table_rows" in sql:
            return [("orders", 5), ("lines", 3), ("audit", 7)]
        if "table_io_waits_summary_by_table" in sql:
            return [("orders", 40), ("lines", 2), ("audit", 9)]
        return []
    eng._q = q
    got = eng.watch_sample("app")
    assert got["src_rows"] == 8 and got["dst_rows"] == 8, got
    assert got["src_tables"] == {"orders": 40, "lines": 2}, got
    assert seen[:2] == [True, True], seen


def test_watch_without_a_count_of_writes_says_nothing_of_them():
    eng = MySQLEngine(Hop(name="w", engine="mysql", source=Endpoint(),
                          target=Endpoint(), databases=["app"]))

    def q(side, sql, args=None, fresh=False):
        if "table_io_waits_summary_by_table" in sql:
            raise RuntimeError("performance_schema is off")
        return [("orders", 5)]
    eng._q = q
    assert "src_tables" not in eng.watch_sample("app")


def test_migkits_own_tables_are_left_out_by_their_names_alone():
    eng = _engine()
    eng._all_tables("src", "app")
    sql = [s for _, s, _ in eng.asked if "information_schema.tables" in s][0]
    assert "not in ('migkit_origin', 'migkit_cutover_mark')" in sql, sql
    assert "migkit%" not in sql


def test_the_moves_guard_asks_only_what_the_move_carries():
    eng = _engine(exclude=["lines"])
    eng.moved_nothing("app")
    asked = [s for _, s, _ in eng.asked if s.startswith("select 1 from")]
    assert asked and all("lines" not in s for s in asked), asked
