"""A partition that arrived empty is found by the deep check on SQL Server,
Oracle and TiDB (P0-PARITY.part), without a server: each engine's own
partition reader over the answers a catalogue gives, and the one verdict
every engine's partitions are held to (`Engine._partitions_verdict`).

Measured on a DTS leg: the current month's partitions arrived empty while
the table's total looked plausible. So each partition of each partitioned
table is held to its own rows on both sides; a table partitioned on one
side only, or by another method or column, is named; and a catalogue that
cannot be read is an error and never "no partitioned tables".

These are not named in `PROVES`: the catalogue statements are new and have
not run against a server of each kind (`UNPROVED`).
"""
import re
import tempfile
from pathlib import Path

import pytest

from migkit.config import Endpoint, Hop

EP = Endpoint(host="x", port=1, user="x", password="x")

#: {side: {table: [(partition, bound, rows)]}} - the same two partitions on
#: both sides, the target's second empty
SRC = {"sales": [("p1", "<= 100", 5), ("p2", "MAXVALUE", 7)]}
DST_EMPTY = {"sales": [("p1", "<= 100", 5), ("p2", "MAXVALUE", 0)]}


def _hop(engine):
    hop = Hop(name="t", engine=engine, source=EP, target=EP,
              databases=["d"])
    where = Path(tempfile.mkdtemp())
    hop.report_dir = lambda db=None: where
    return hop


# ---- SQL Server ------------------------------------------------------------

def _mssql(src, dst, fail=False):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))
    data = {"src": src, "dst": dst}

    def cmd(side, db, sql, *a, **k):
        if fail:
            raise RuntimeError("the account cannot read sys.partitions")
        tables = data[side]
        if "from sys.partitions p" in sql:
            return [[f"dbo.{t}", "pf_sales", "RANGE", "0", "sale_date",
                     str(i + 1), str(len(ps))]
                    for t, ps in tables.items()
                    for i, (_, _, _) in enumerate(ps)]
        if "sys.partition_range_values" in sql:
            return [["pf_sales", "1", "100"]]
        if "group by $PARTITION" in sql:
            t = re.search(r"\[dbo\]\.\[(\w+)\]", sql)
            t = t.group(1) if t else next(iter(tables))
            return [[str(i + 1), str(rows)]
                    for i, (_, _, rows) in enumerate(tables[t]) if rows]
        if "from sys.tables t join sys.schemas s" in sql:
            return [[f"dbo.{t}"] for t in set(tables) | {"plain"}]
        raise AssertionError(sql)
    eng._cmd = cmd
    eng._qualified = lambda side, db, t: "[" + t.split(".")[0] + "].[" + \
        t.split(".")[1] + "]"
    return eng


def test_sql_server_names_a_partition_that_arrived_empty():
    got = _mssql({"sales": SRC["sales"]}, {"sales": DST_EMPTY["sales"]}
                 )._deep_partitions("d")
    assert got.status == "diff", got
    assert "partition p2: empty on target, the source's holds 7 rows" \
        in got.detail


def test_sql_server_passes_partitions_that_hold_the_same_rows_by_count():
    got = _mssql(SRC, SRC)._deep_partitions("d")
    assert got.status == "ok", got
    assert "1 partitioned tables, 2 partitions" in got.detail
    assert "compared by count only" in got.detail


def test_sql_server_says_a_table_partitioned_on_one_side_only():
    eng = _mssql(SRC, {})
    got = eng._deep_partitions("d")
    assert got.status == "diff"
    assert "dbo.sales: missing on target" in got.detail \
        or "partitioned on source, not on target" in got.detail


def test_sql_server_that_cannot_read_its_partitions_does_not_pass():
    got = _mssql(SRC, SRC, fail=True)._deep_partitions("d")
    assert got.status == "error"
    assert "could not read the partitions" in got.detail


# ---- Oracle ------------------------------------------------------------------

def _oracle(src, dst, fail=False):
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle"))
    data = {"src": src, "dst": dst}
    eng._owner = lambda side, db: "APP"
    eng._qualified = lambda side, db, t: f'"APP"."{t}"'

    def rows(side, db, sql, args=None):
        if fail:
            raise RuntimeError("ORA-00942: table or view does not exist")
        tables = data[side]
        if "all_part_key_columns" in sql:
            return [(t.upper(), "SALE_DATE") for t in tables]
        if "all_part_tables" in sql:
            return [(t.upper(), "RANGE") for t in tables]
        if "all_tab_partitions" in sql:
            return [(t.upper(), p.upper(), b) for t, ps in tables.items()
                    for p, b, _ in ps]
        if "partition (" in sql:
            t = re.search(r'"APP"\."(\w+)"', sql).group(1).lower()
            names = re.findall(r'select (\d+), count\(\*\) from "APP"\."\w+"'
                               r' partition \("(\w+)"\)', sql)
            found = {p.upper(): n for p, _, n in tables[t]}
            return [(int(i), found[name]) for i, name in names]
        if "from all_tables" in sql:
            return [(t.upper(),) for t in set(tables) | {"plain"}]
        raise AssertionError(sql)
    eng._rows = rows
    return eng


def test_oracle_names_a_partition_that_arrived_empty():
    eng = _oracle({"sales": SRC["sales"]}, {"sales": DST_EMPTY["sales"]})
    got = eng._deep_partitions("d")
    assert got.status == "diff", got
    assert "P2: empty on target" in got.detail \
        or "partition P2" in got.detail


def test_oracle_passes_partitions_that_hold_the_same_rows_by_count():
    got = _oracle(SRC, SRC)._deep_partitions("d")
    assert got.status == "ok", got
    assert "compared by count only" in got.detail


def test_oracle_says_a_table_that_is_partitioned_by_another_method():
    eng = _oracle(SRC, SRC)
    real = eng._rows

    def other(side, db, sql, args=None):
        if side == "dst" and "all_part_tables" in sql:
            return [("SALES", "LIST")]
        return real(side, db, sql, args)
    eng._rows = other
    got = eng._deep_partitions("d")
    assert got.status == "diff"
    assert "partition scheme differs" in got.detail


def test_oracle_that_cannot_read_its_partitions_does_not_pass():
    got = _oracle(SRC, SRC, fail=True)._deep_partitions("d")
    assert got.status == "error"
    assert "could not read the partitions" in got.detail


# ---- TiDB ----------------------------------------------------------------------

@pytest.fixture
def tidb(monkeypatch):
    from migkit.engines import mysql
    from migkit.engines.tidb import TiDBEngine

    def make(src, dst, fail=False):
        data = {"src": src, "dst": dst}

        def q(self, side, sql, args=None, fresh=False):
            if fail:
                raise RuntimeError("information_schema.partitions: denied")
            tables = data[side]
            if "information_schema.partitions" in sql:
                return [(t, "RANGE", "sale_date", p, b)
                        for t, ps in tables.items() for p, b, _ in ps]
            if "partition (" in sql:
                t = re.search(r"`(\w+)` partition", sql).group(1)
                reads = re.findall(r"select (\d+), count\(\*\)", sql)
                return [(int(i), tables[t][int(i)][2], "0") for i in reads]
            return []
        monkeypatch.setattr(mysql.MySQLEngine, "_q", q)
        monkeypatch.setattr(mysql.MySQLEngine, "_all_tables",
                            lambda self, side, db: list(data[side]))
        monkeypatch.setattr(mysql.MySQLEngine, "_row_expr",
                            lambda self, db, t: "`id`")
        monkeypatch.setattr(TiDBEngine, "_d", lambda self, side, db: db)
        return TiDBEngine(_hop("tidb"))
    return make


def test_tidb_names_a_partition_that_arrived_empty(tidb):
    eng = tidb({"sales": SRC["sales"]}, {"sales": DST_EMPTY["sales"]})
    got = eng._deep_partitions("d", "d")
    assert got.status == "diff", got
    assert "partition p2: empty on target, the source's holds 7 rows" \
        in got.detail


def test_tidb_passes_partitions_that_hold_the_same_rows(tidb):
    got = tidb(SRC, SRC)._deep_partitions("d", "d")
    assert got.status == "ok", got
    assert "1 partitioned tables, 2 partitions" in got.detail


def test_tidb_that_cannot_read_its_partitions_does_not_pass(tidb):
    got = tidb(SRC, SRC, fail=True)._deep_partitions("d", "d")
    assert got.status == "error"
    assert "could not read the partitions" in got.detail
