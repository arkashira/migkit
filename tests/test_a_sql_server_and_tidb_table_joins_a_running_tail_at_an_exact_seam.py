"""The seam a table added to a running tail joins at (backlog P0-PARITY,
`tail.added`) on SQL Server and TiDB, without a server: the mark taken when
the copy begins, and a change left out of the tail only when the mark shows
it committed.

* SQL Server: the mark is the database's change tracking version after the
  database it was read in, and none where the database tracks no changes -
  nothing is then left out; a version at or below it is covered, one above
  it is not, and a mark made in another database says nothing;
* TiDB: the mark is the timestamp oracle's now, one clock for the copy and
  the feed, and a change at or below it is covered.
"""
from migkit import canon
from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.added", "mssql"), ("tail.added", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _mssql(answers):
    """A SQL Server engine whose queries are answered by `answers`, keyed by
    a word of the statement; an Exception is raised."""
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(Hop(name="s", engine="mssql", source=EP, target=EP,
                          databases=["d"]))

    def run(side, db, sql, *a, **k):
        for word, got in answers.items():
            if word in sql:
                if isinstance(got, Exception):
                    raise got
                return got
        raise RuntimeError(f"unexpected: {sql}")
    eng.run_rule = run
    return eng


IDENTITY = [("srv", 5, "2026-01-01T00:00:00")]


def test_the_sql_server_mark_is_its_version_in_the_database_it_was_read_in():
    eng = _mssql({"@@servername": IDENTITY,
                  "change_tracking_current_version": [(40,)]})
    assert eng.snapshot_mark("src", "d") == "srv/5/2026-01-01T00:00:00|40"


def test_there_is_no_sql_server_mark_where_nothing_is_tracked():
    for version in ([(None,)], [], RuntimeError("not enabled")):
        eng = _mssql({"@@servername": IDENTITY,
                      "change_tracking_current_version": version})
        assert eng.snapshot_mark("src", "d") is None
    # nor where the database cannot be told from another
    eng = _mssql({"@@servername": RuntimeError("denied"),
                  "change_tracking_current_version": [(40,)]})
    assert eng.snapshot_mark("src", "d") is None


def test_a_sql_server_change_is_covered_at_or_below_the_mark_s_version():
    eng = _mssql({"@@servername": IDENTITY,
                  "change_tracking_current_version": [(40,)]})
    mark = eng.snapshot_mark("src", "d")

    def change(v):
        return canon.change("update", "dbo.t", {"id": 1}, {"id": 1}, txn=v)
    assert eng.mark_covers("src", "d", mark, change(39)) is True
    assert eng.mark_covers("src", "d", mark, change(40)) is True
    assert eng.mark_covers("src", "d", mark, change(41)) is False
    # a mark made in another database, or no version: not known
    assert eng.mark_covers("src", "d", "srv/6/2026-01-01T00:00:00|40",
                           change(39)) is None
    assert eng.mark_covers("src", "d", mark, canon.change(
        "delete", "dbo.t", {"id": 1})) is None
    assert eng.mark_covers("src", "d", None, change(39)) is None


def _tidb(now):
    from migkit.engines.tidb import TiDBEngine
    eng = TiDBEngine(Hop(name="t", engine="tidb", source=EP, target=EP,
                         databases=["d"]))
    eng._tso = lambda side: now
    return eng


def test_the_tidb_mark_is_the_timestamp_oracle_s_now():
    assert _tidb(450000000000000001).snapshot_mark("src", "d") \
        == "450000000000000001"


def test_a_tidb_change_is_covered_at_or_below_the_mark():
    eng = _tidb(500)
    mark = eng.snapshot_mark("src", "d")
    assert eng.mark_covers("src", "d", mark, {"txn": 499}) is True
    assert eng.mark_covers("src", "d", mark, {"txn": 500}) is True
    assert eng.mark_covers("src", "d", mark, {"txn": 501}) is False
    # no mark, or a change with no timestamp: not known
    assert eng.mark_covers("src", "d", None, {"txn": 499}) is None
    assert eng.mark_covers("src", "d", mark, {}) is None
    assert eng.mark_covers("src", "d", mark, {"txn": "499"}) is None
