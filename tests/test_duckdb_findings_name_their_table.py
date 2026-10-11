"""A DuckDB finding names its table as the source names it
(P0-PARITY.verify, `verify.table`), on two real files: the table of a
difference is a field of the finding (`Result.table`), so a report, a
repair and a hop's history ask it of the finding and do not read it out of
its text.
"""
import pathlib
import tempfile

from migkit.config import Endpoint, Hop
from migkit.engines.duckdb import DuckDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.table", "duckdb")]


def _hop(source_rows, target_rows):
    import duckdb
    where = pathlib.Path(tempfile.mkdtemp())
    for name, rows in (("s.duckdb", source_rows), ("t.duckdb", target_rows)):
        con = duckdb.connect(str(where / name))
        con.execute("create table orders (id integer primary key,"
                    " v varchar)")
        if rows:
            con.execute(f"insert into orders values {rows}")
        con.close()
    hop = Hop(name="t", engine="duckdb",
              source=Endpoint(host=str(where / "s.duckdb"), port=0),
              target=Endpoint(host=str(where / "t.duckdb"), port=0),
              databases=["main"])
    hop.report_dir = lambda db=None: where
    return DuckDBEngine(hop)


def test_a_difference_names_the_table_it_was_found_in():
    eng = _hop("(1,'a'),(2,'b'),(3,'c')", "(1,'a'),(2,'X')")
    for check in (eng.check_counts, eng.check_data):
        found = [r for r in check("main") if r.status == "diff"]
        assert len(found) == 1, check.__name__
        assert found[0].scope == "main.orders"
        assert found[0].table == "orders", found[0]


def test_a_table_that_agrees_has_no_difference_to_name():
    eng = _hop("(1,'a')", "(1,'a')")
    for check in (eng.check_counts, eng.check_data):
        got = check("main")
        assert [r.status for r in got] == ["ok"], check.__name__
