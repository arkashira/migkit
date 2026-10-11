"""A TiDB side is named as TiDB, not counted as the MySQL it speaks for
(P0-PARITY.assess, `assess.brands`), without a server: the engine's own
probe over the answers a server gives to `version()` and
`@@version_comment`, through the same identification the MySQL engine
uses.

* TiDB puts its name in the version string, MySQL only in the comment;
* a pairing of the two is said to be a different software behind one
  protocol, so that equal version numbers are not read as agreement;
* a side that does not answer is unidentified - a warning, never TiDB's
  or MySQL's by default.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.brands", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")

TIDB = [("8.0.11-TiDB-v8.1.0", "TiDB Server (Apache License 2.0)"
                               " Community Edition, MySQL 8.0 compatible")]
MYSQL = [("8.0.36", "MySQL Community Server - GPL")]


def _engine(src, dst):
    eng = TiDBEngine(Hop(name="b", engine="tidb", source=EP, target=EP,
                         databases=["d"]))
    answers = {"src": src, "dst": dst}

    def q(side, sql, *a, **k):
        got = answers[side]
        if isinstance(got, Exception):
            raise got
        return got
    eng._q = q
    return eng


def test_a_tidb_side_is_named_by_its_version_string():
    src, dst = _engine(TIDB, TIDB)._brands()
    assert (src.name, dst.name) == ("tidb", "tidb")
    assert src.family == "mysql"
    assert src.version == "8.0.11-TiDB-v8.1.0"
    assert "TiDB" in src.evidence


def test_a_mysql_side_beside_it_is_not_taken_for_tidb():
    src, dst = _engine(TIDB, MYSQL)._brands()
    assert (src.name, dst.name) == ("tidb", "mysql")
    rows = {r["item"]: r for r in _engine(TIDB, MYSQL)._brand_rows()}
    assert "different software behind the same protocol" \
        in rows["brand pairing"]["detail"]
    assert rows["brand pairing"]["level"] == "warn"


def test_a_side_that_does_not_answer_is_unidentified_not_assumed():
    eng = _engine(TIDB, RuntimeError("refused"))
    src, dst = eng._brands()
    assert src.name == "tidb"
    assert dst.name != "mysql" and dst.name != "tidb"
    rows = [r for r in eng._brand_rows() if r["item"] == "target brand"]
    assert rows and rows[0]["level"] == "warn"
