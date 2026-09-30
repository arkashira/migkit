"""SQL Server's data check says its own answer for every table it read.

A table whose two row-hash sums differed while it was read, and whose
drilldown then found no row that differs by key, was left out of the
answer: nothing said it was equal, and the report used to turn a
`missing=0 extra=0 changed=0` into `ok` for it - a rewrite the one verdict
(`verdict.decide`) no longer makes. It is now the check's own `ok`,
named, when its sums agree read again; still different, a `diff` that
says it could not be placed. Held without a server: the engine's reads
are stood in for.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine


def _engine(tmp_path, sums, drill):
    ep = Endpoint(host="10.0.0.1", port=1433, user="u", password="p")
    eng = MSSQLEngine(Hop(name="ms", engine="mssql", source=ep, target=ep,
                          databases=["shop"]))
    eng.hop.report_dir = lambda db=None: tmp_path
    reads = iter(sums)

    def cmd(side, db, sql):
        if "from sys.tables" in sql:
            return [["dbo.t"]]
        return [next(reads)]
    eng._cmd = cmd
    eng._row_json = lambda db, t: ("N'{}'", [])
    eng._drilldown = lambda db, t: drill
    eng._health = lambda side, db=None: None
    eng.digest_salt = lambda: "s"
    return eng


def test_a_table_that_settled_is_the_check_s_own_ok(tmp_path):
    # the first two reads differ, a write between them; read again, equal
    eng = _engine(tmp_path, [["2", "10"], ["3", "12"], ["3", "12"],
                             ["3", "12"]], (0, 0, 0))
    (got,) = [r for r in eng.check_data("shop") if r.check == "data"]
    assert got.status == "ok", got
    assert "equal when read again: dbo.t" in got.detail, got.detail


def test_one_that_still_differs_and_cannot_be_placed_is_a_diff(tmp_path):
    eng = _engine(tmp_path, [["2", "10"], ["2", "12"], ["2", "10"],
                             ["2", "12"]], (0, 0, 0))
    (got,) = [r for r in eng.check_data("shop") if r.check == "data"]
    assert got.status == "diff", got
    assert "not localized" in got.detail, got.detail


def test_the_settled_ok_reads_as_same_in_the_one_verdict(tmp_path):
    from migkit import verdict
    eng = _engine(tmp_path, [["2", "10"], ["3", "12"], ["3", "12"],
                             ["3", "12"]], (0, 0, 0))
    rows = [r.__dict__ for r in eng.check_data("shop")]
    assert verdict.decide(rows).status == "same", rows
