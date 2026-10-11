"""The seam a table added to a running MySQL tail joins at (backlog
P0-PARITY, `tail.added`), without a server: the GTIDs the source has
executed when the copy begins are the mark, and a change is left out of the
tail only when its GTID is in that set.

* `snapshot_mark` is the executed set, spaces and case taken out, and none
  where the server gives no GTIDs, so that nothing is then left out;
* `mark_covers` says a change's GTID is in the set - ranges, lists, another
  server's uuid, a tag - and says nothing (None) of a change that has no
  GTID, so that it is applied;
* a table copying with its mark taken is held to it by the tail
  (`tailtables`): a change the copy read is out, one it did not is applied.
"""
from migkit import tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine
from migkit.engines.mysql import MySQLEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("tail.added", "mysql")]

EP = Endpoint(host="x", port=1, user="x", password="x")
UUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
OTHER = "5b3c9a10-71ca-11e1-9e33-c80aa9429562"


def _engine(answer):
    eng = MySQLEngine(Hop(name="m", engine="mysql", source=EP, target=EP,
                          databases=["d"]))

    def q(side, sql, *a, **k):
        if isinstance(answer, Exception):
            raise answer
        return answer
    eng._q = q
    return eng


def test_the_mark_is_the_executed_set_where_the_server_gives_gtids():
    got = _engine([("ON", f"{UUID}:1-5:7,\n{OTHER.upper()}:3")]) \
        .snapshot_mark("src", "d")
    assert got == f"{UUID}:1-5:7,{OTHER}:3"
    assert _engine([("ON_PERMISSIVE", f"{UUID}:1-2")]) \
        .snapshot_mark("src", "d") == f"{UUID}:1-2"


def test_there_is_no_mark_where_gtids_are_off_or_unknown():
    assert _engine([("OFF", "")]).snapshot_mark("src", "d") is None
    assert _engine([("OFF_PERMISSIVE", f"{UUID}:1")]) \
        .snapshot_mark("src", "d") is None
    # nothing executed yet: no mark to hold a change to
    assert _engine([("ON", "")]).snapshot_mark("src", "d") is None
    assert _engine([]).snapshot_mark("src", "d") is None
    # a server with no such variable (MariaDB's GTIDs are another thing)
    assert _engine(RuntimeError("unknown system variable")) \
        .snapshot_mark("src", "d") is None


def test_a_change_is_covered_when_its_gtid_is_in_the_set():
    mark = f"{UUID}:1-5:7,{OTHER}:3"
    cov = MySQLEngine.mark_covers
    for n, want in ((1, True), (5, True), (6, False), (7, True),
                    (8, False)):
        assert cov("src", "d", mark, {"txn": f"{UUID}:{n}"}) is want, n
    assert cov("src", "d", mark, {"txn": f"{OTHER}:3"}) is True
    assert cov("src", "d", mark, {"txn": f"{OTHER}:4"}) is False
    # a server the set does not name has nothing in it
    assert cov("src", "d", mark, {"txn": "ffffffff-0000-0000-0000-"
                                         "000000000000:1"}) is False


def test_a_tagged_gtid_is_held_to_the_intervals_after_its_tag():
    mark = f"{UUID}:1-5:blue:10-20"
    cov = MySQLEngine.mark_covers
    assert cov("src", "d", mark, {"txn": f"{UUID}:blue:15"}) is True
    assert cov("src", "d", mark, {"txn": f"{UUID}:blue:5"}) is False
    # the untagged interval is not the tagged one
    assert cov("src", "d", mark, {"txn": f"{UUID}:15"}) is False
    assert cov("src", "d", mark, {"txn": f"{UUID}:blue:21"}) is False


def test_a_change_with_no_gtid_or_no_mark_is_not_known_covered():
    cov = MySQLEngine.mark_covers
    mark = f"{UUID}:1-5"
    assert cov("src", "d", mark, {}) is None
    assert cov("src", "d", mark, {"txn": 12}) is None
    assert cov("src", "d", mark, {"txn": f"{UUID}:x"}) is None
    assert cov("src", "d", None, {"txn": f"{UUID}:1"}) is None


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, eng):
        self.hop, self.src_engine = hop, eng

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def test_a_copying_table_leaves_out_only_what_its_mark_covers(tmp_path):
    hop = Hop(name="tt", engine="mysql", source=EP, target=EP,
              databases=["d"])
    hop.report_dir = lambda db=None: tmp_path
    eng = _engine([("ON", f"{UUID}:1-5")])
    t = tailtables.TableSet(_Pair(hop, eng), "d", tmp_path / "tail.json",
                            lambda m: None)
    t.file.said = None
    t._settle = lambda: None
    t.entries = {t._entry("d.t"): {"done": True,
                                   "mark_before": f"{UUID}:1-5"}}
    st = {"state": "copying"}
    inside = {"txn": f"{UUID}:3"}
    after = {"txn": f"{UUID}:6"}
    assert t._verdict("d.t", st, inside) == "out"
    assert t._verdict("d.t", st, after) == "apply"
    # a change that says nothing of its GTID is applied, never dropped
    assert t._verdict("d.t", st, {}) == "apply"
