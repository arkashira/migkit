"""MySQL's data check says how each table it compared came out
(`Result.tables`), and its schema comparison is a finding of the category
structure.schema-diff (P0-PARITY.verify).

What a service's own check, and a hop that keeps some changes off the
target, are held to is read from the finding and not back out of its
words: the per-table finding names its table and how it came out, and the
database's count answer names every table it counted - the ones a side
lacks included.

Pure: the catalogue reads and the table comparison are answered in-process.
"""
from types import SimpleNamespace

from migkit.config import Endpoint, Hop
from migkit.engines.base import Result

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.tables", "mysql"),
          ("verify.structure.schema-diff", "mysql")]


def _engine(tmp_path, **kw):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint(),
              databases=["appdb"], workers=1, **kw)
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _tables(held):
    def ask(side, sql, args=None, fresh=False):
        if "information_schema.tables" in sql:
            return [(t,) for t in held[side]]
        return []
    return ask


def test_each_table_a_finding_compared_is_said_with_how_it_came_out(
        tmp_path):
    eng = _engine(tmp_path)
    eng._q = _tables({"src": ["lines", "orders", "only_src"],
                      "dst": ["lines", "orders", "only_dst"]})
    eng._health = lambda side: None
    seen = {"orders": (Result("data", "appdb.orders", "diff",
                              "missing=1 extra=0 changed=0"), 3, 2),
            "lines": (Result("data", "appdb.lines", "ok", "equal"), 5, 5)}
    eng._diff_table = lambda db, t: seen[t]
    eng._resolve_inflight = lambda db, bad, stream=None: (bad, [], [])
    eng._second_notes = lambda db, t: ""
    eng._second_rows = lambda db, t: None
    got = {r.scope: r for r in eng._own_check_data("appdb",
                                                   with_counts=True)}
    assert got["appdb.orders"].tables == {"orders": "diff"}
    assert got["appdb.lines"].tables == {"lines": "ok"}
    # the database's count answer names every table it counted: the two
    # sides' own, and the ones only a side has
    counts = got["appdb"]
    assert counts.check == "counts" and counts.status == "diff"
    assert counts.tables == {"orders": "diff", "lines": "ok",
                             "only_src": "diff", "only_dst": "diff"}


def test_a_count_that_agrees_says_every_table_ok(tmp_path):
    eng = _engine(tmp_path)
    eng._q = _tables({"src": ["a", "b"], "dst": ["a", "b"]})
    eng._health = lambda side: None
    eng._diff_table = lambda db, t: (Result("data", f"appdb.{t}", "ok", ""),
                                     4, 4)
    got = [r for r in eng._own_check_data("appdb", with_counts=True)
           if r.check == "counts"]
    assert [r.status for r in got] == ["ok"]
    assert got[0].tables == {"a": "ok", "b": "ok"}


# ---- the schema comparison -------------------------------------------------

def _compare(tmp_path, monkeypatch, out, code=0):
    """The schema-aware comparison answering `out`."""
    from migkit import movers
    eng = _engine(tmp_path)
    eng._schema_urls = lambda db: ("mysql://src", "mysql://dst")
    eng._schema_excludes = lambda db: []
    monkeypatch.setattr(movers, "schema_diff", lambda a, b, ex: SimpleNamespace(
        returncode=code, stdout=out, stderr=""))
    return eng


def test_a_schema_that_differs_is_a_schema_diff_finding(tmp_path,
                                                        monkeypatch):
    eng = _compare(tmp_path, monkeypatch,
                   "ALTER TABLE `t` ADD COLUMN `note` text;")
    got = eng._atlas("appdb")
    assert got.status == "diff"
    assert got.category == "structure.schema-diff"
    assert (tmp_path / "schema-fix.sql").read_text().startswith("ALTER TABLE")


def test_schemas_that_match_are_a_clean_schema_diff_finding(tmp_path,
                                                            monkeypatch):
    eng = _compare(tmp_path, monkeypatch, "Schemas are synced, no changes")
    (tmp_path / "schema-fix.sql").write_text("stale")
    got = eng._atlas("appdb")
    assert got.status == "ok"
    assert got.category == "structure.schema-diff"
    assert not (tmp_path / "schema-fix.sql").exists()


def test_the_schema_check_carries_it(tmp_path, monkeypatch):
    """`check_schema` makes the comparison where the program that reads
    schemas is installed, and its answer is the one that decides the
    textual difference is cosmetic."""
    from migkit.engines import mysql
    eng = _compare(tmp_path, monkeypatch, "Schemas are synced, no changes")
    eng._dump_schema = lambda side, db, physical=None: "create table t;\n"
    eng.check_objects = lambda db: Result("schema", f"{db} objects", "ok")
    eng.check_app_versions = lambda db: []
    eng._mapped_schema = lambda db: []
    monkeypatch.setattr(mysql, "which", lambda name: "/usr/bin/" + name)
    got = eng.check_schema("appdb")
    assert [r.category for r in got
            if r.category == "structure.schema-diff"] == [
        "structure.schema-diff"], [(r.scope, r.category) for r in got]
