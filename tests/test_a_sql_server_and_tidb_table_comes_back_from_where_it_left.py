"""A table that left a running tail and comes back (backlog P0-PARITY,
`tail.readded`) on SQL Server and TiDB, without a server: the source is
read again from where the table left, so only its changes since are
carried - when the source's log still holds that place and the table's
shape is as it was - and the table is copied whole when it does not.

* the tail knows how far a read again has got (`position_reached`): SQL
  Server by its change tracking version, TiDB by its fence, and not at all
  where it cannot say, so that the table is copied instead;
* the ladder (`tailtables._add`) chooses the read again for a table that
  left with its digest, and the copy for one whose shape changed, whose
  place the log no longer holds, or which left with no digest.
"""
import pytest

from migkit import decide, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.readded", "mssql"), ("tail.readded", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _engine(name):
    if name == "mssql":
        from migkit.engines.mssql import MSSQLEngine as Engine
        here, now = 40, 50
    else:
        from migkit.engines.tidb import TiDBEngine as Engine
        here, now = {"fence": 400}, {"fence": 500}
    eng = Engine(Hop(name="x", engine=name, source=EP, target=EP,
                     databases=["d"]))
    eng.lost = None
    eng.columns = [("id", "integer"), ("v", "text")]
    eng.neutral_columns = lambda side, db, table: list(eng.columns)
    eng.log_position = lambda side, db: now
    eng.position_lost = lambda side, db, at: eng.lost
    return eng, here, now


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, eng):
        self.hop, self.src_engine = hop, eng

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _tables(tmp_path, eng):
    hop = Hop(name="tt", engine="mssql", source=EP, target=EP,
              databases=["d"], options={"ddl": "park"})
    hop.report_dir = lambda db=None: tmp_path
    got = tailtables.TableSet(_Pair(hop, eng), "d",
                              tmp_path / "tail-token.json", lambda m: None)
    got.file.said = None
    got._settle = lambda: None
    return got


def test_a_sql_server_read_again_says_how_far_it_has_got():
    from migkit.engines.mssql import MSSQLEngine
    assert MSSQLEngine.REREADS_FROM_A_POSITION is True
    reached = MSSQLEngine.position_reached
    assert reached(50, 50) is True
    assert reached(60, 50) is True
    assert reached(49, 50) is False
    # where it cannot say, the table is copied instead
    assert reached(None, 50) is None
    assert reached(50, None) is None


def test_a_tidb_read_again_says_how_far_it_has_got():
    from migkit.engines.tidb import TiDBEngine
    assert TiDBEngine.REREADS_FROM_A_POSITION is True
    reached = TiDBEngine.position_reached
    assert reached({"fence": 500}, {"fence": 500}) is True
    assert reached({"fence": 501}, {"fence": 500}) is True
    assert reached({"fence": 499}, {"fence": 500}) is False
    assert reached({"fence": None}, {"fence": 500}) is False
    # where it cannot say, the table is copied instead
    assert reached({}, {"fence": 500}) is None
    assert reached(None, {"fence": 500}) is None
    assert reached({"fence": 500}, None) is None


@pytest.mark.parametrize("name", ["mssql", "tidb"])
def test_a_table_that_left_comes_back_with_only_its_changes_since(
        tmp_path, name):
    eng, here, now = _engine(name)
    t = _tables(tmp_path, eng)
    left = {"state": "removed", "name": "a", "at": here,
            "digest": [3, "9"], "shape": [["id", "integer"], ["v", "text"]],
            "columns": [["id", "integer"]]}
    states = {"a": dict(left)}
    t._add(states, "a", "a", now, "put back")
    assert states["a"]["way"] == "from-removal"
    assert states["a"]["at"] == here and states["a"]["upto"] == now
    assert states["a"]["digest"] == [3, "9"]


@pytest.mark.parametrize("name", ["mssql", "tidb"])
def test_a_table_is_copied_when_the_read_again_cannot_be_exact(
        tmp_path, name):
    eng, here, now = _engine(name)
    t = _tables(tmp_path, eng)
    left = {"state": "removed", "name": "a", "at": here,
            "digest": [3, "9"], "shape": [["id", "integer"], ["v", "text"]]}
    # its shape changed while it was out
    eng.columns = [("id", "integer")]
    states = {"a": dict(left)}
    t._add(states, "a", "a", now, "put back")
    assert states["a"]["way"] == "copy"
    eng.columns = [("id", "integer"), ("v", "text")]
    # the log no longer holds the place it left
    eng.lost = "its tracking was cleaned up"
    states = {"a": dict(left)}
    t._add(states, "a", "a", now, "put back")
    assert states["a"]["way"] == "copy"
    got = decide.climb("a", tailtables._add_ways(), decide.Facts(
        {"at": here, "digest": True, "rereads": True, "same_shape": True,
         "lost": eng.lost}))
    assert got.path == "copy" and "cleaned up" in got.reason
    eng.lost = None
    # it left with no digest of its target to hold it to
    states = {"a": dict(left, digest=None)}
    t._add(states, "a", "a", now, "put back")
    assert states["a"]["way"] == "copy"
