"""The hop's row filter reaches the engine's own readers (P0-PARITY.scope,
`scope.row-filter`), without a server: the rows a copy reads, the digest a
check folds, and the rows a repair empties are the filter's alone.

* DuckDB, on real files: the read, the digest and the delete each take
  only the rows the filter keeps, and a delete leaves the others;
* Redshift, BigQuery, CockroachDB and Aurora DSQL: the filter is one
  parenthesised condition beside the resume condition, in the engine's own
  quoting and its own limit - never beside it as a second reading of the
  hop's `where`.
"""
import pathlib
import tempfile

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import _class_for, pgwire

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.row-filter", ("duckdb", "redshift", "bigquery",
                                "cockroachdb", "dsql"))]

KEEP = "region = 'apac'"
COLUMNS = [("id", "integer"), ("region", "text")]
EP = Endpoint(host="x", port=1, user="x", password="x")


def _files():
    import duckdb
    where = pathlib.Path(tempfile.mkdtemp())
    for name in ("s.duckdb", "t.duckdb"):
        con = duckdb.connect(str(where / name))
        con.execute("create table orders (id integer primary key,"
                    " region varchar)")
        con.execute("insert into orders values (1, 'apac'), (2, 'emea'),"
                    " (3, 'apac')")
        con.close()
    hop = Hop(name="t", engine="duckdb",
              source=Endpoint(host=str(where / "s.duckdb"), port=0),
              target=Endpoint(host=str(where / "t.duckdb"), port=0),
              databases=["main"])
    return _class_for("duckdb")(hop)


def test_duckdb_reads_only_the_rows_the_filter_keeps():
    eng = _files()
    rows, last = eng.neutral_read("src", "main", "orders", COLUMNS, None,
                                  100, KEEP)
    assert [list(r) for r in rows] == [[1, "apac"], [3, "apac"]]
    assert last == (3,)
    # resumed after a row, still through the filter
    rows, _ = eng.neutral_read("src", "main", "orders", COLUMNS, (1,), 100,
                               KEEP)
    assert [list(r) for r in rows] == [[3, "apac"]]


def test_duckdb_folds_the_digest_of_the_kept_rows_alone():
    eng = _files()
    kept = eng.neutral_digest("src", "main", "orders", COLUMNS, where=KEEP)
    whole = eng.neutral_digest("src", "main", "orders", COLUMNS)
    assert kept[0] == 2 and whole[0] == 3 and kept != whole


def test_duckdb_empties_only_the_kept_rows_of_the_target():
    eng = _files()
    assert eng.neutral_empty("dst", "main", "orders", where=KEEP) == 2
    rows, _ = eng.neutral_read("dst", "main", "orders", COLUMNS)
    assert [list(r) for r in rows] == [[2, "emea"]]


@pytest.mark.parametrize("name,quoted,limit", [
    ("redshift", '"id" >', "limit 100"),
    ("bigquery", "`id` >", "limit 100"),
    ("cockroachdb", '"id" >', "limit 100"),
    ("dsql", '"id" >', "limit 100"),
])
def test_the_filter_is_one_condition_beside_the_resume(
        monkeypatch, name, quoted, limit):
    # the columns of the table are not this test's: nothing is asked
    monkeypatch.setattr(pgwire.PgWireEngine, "_rows",
                        lambda self, side, db, sql, args=None: [])
    eng = _class_for(name)(Hop(name="t", engine=name, source=EP, target=EP,
                               databases=["d"]))
    eng.neutral_key = lambda side, db, table: ["id"]
    sql, args, key, names = eng._read_query(
        "src", "d", "shop.t", COLUMNS, (5,), 100, KEEP)
    sql = " ".join(sql.split())
    assert sql.count(KEEP) == 1, sql
    assert f"and ({KEEP})" in sql and quoted in sql, sql
    assert sql.endswith(limit), sql
    assert args == [5] and key == ["id"] and names == ["id", "region"]
    # with no filter the condition is the resume alone
    sql, _, _, _ = eng._read_query("src", "d", "shop.t", COLUMNS, (5,), 100,
                                   None)
    assert KEEP not in sql
