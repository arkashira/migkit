"""SQL Server's change feed is Change Tracking, whose version is not a time
(backlog F7.beat). Each version's commit time is asked of
`sys.dm_tran_commit_table` - one query per batch, the answer kept - and
marked on the transaction (`canon.ended(..., ts=)`), so the tail's beat
says `through` and `lag`. Where the account cannot read that table no time
is made up: no `ts`, no lag, and the beat says why. Run on stubs: the
server is not asked here, so the statement itself is unproved on a real one.
"""
import json
import time

from migkit import tailctl, ui
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine


def _engine(ages=None, refuse=None):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    eng = MSSQLEngine(Hop(name="h", engine="mssql", source=ep, target=ep,
                          databases=["d"]))
    asked = []

    def kept(side, db, sql, args=None):
        asked.append((sql, list(args or [])))
        if refuse:
            raise RuntimeError(refuse)
        return [[v, ages[v]] for v in args if v in ages]
    eng._kept_rows = kept
    eng._all_tracked = lambda side, db: None
    eng._tracked = lambda side, db: {"dbo.t"}
    eng.in_scope = lambda db, t: True
    eng.neutral_key = lambda side, db, t: ["id"]
    eng.neutral_columns = lambda side, db, t: [("id", "int"), ("v", "int")]
    eng._qualified = lambda side, db, t: "dbo.t"
    return eng, asked


def _feed(eng, versions):
    rows = [(v, "U", i, i, i, 1) for i, v in enumerate(versions)]
    eng._ct_read = lambda side, db, token, read: (
        [("dbo.t", ["id"], ["id", "v"], rows)], 99)


def test_each_transaction_is_marked_with_the_time_its_server_committed_it():
    eng, asked = _engine({11: 30, 12: 5})
    _feed(eng, [11, 11, 12])
    before = time.time()
    out, now = eng.neutral_changes("src", "d", 10)
    ends = [c for c in out if c.get("txn_end")]
    assert [c["txn_at"] for c in ends] == [11, 12] and now == 99
    # the age the server gives is taken from the moment of its answer
    assert before - 30 <= ends[0]["txn_ts"] <= time.time() - 30 + 1
    assert before - 5 <= ends[1]["txn_ts"] <= time.time() - 5 + 1
    assert "txn_ts" not in out[0]
    assert len(asked) == 1 and asked[0][1] == [12, 11]
    assert "sys.dm_tran_commit_table" in asked[0][0]


def test_one_query_per_batch_and_a_known_version_is_not_asked_again():
    eng, asked = _engine({11: 3, 12: 2, 13: 1})
    _feed(eng, [11, 12])
    eng.neutral_changes("src", "d", 10)
    _feed(eng, [11, 12])
    eng.neutral_changes("src", "d", 10)
    assert len(asked) == 1
    _feed(eng, [12, 13])
    out, _ = eng.neutral_changes("src", "d", 10)
    assert len(asked) == 2 and asked[1][1] == [13]
    assert all(c.get("txn_ts") for c in out if c.get("txn_end"))


def test_the_cache_is_bounded():
    eng, _ = _engine({v: 1 for v in range(100)})
    eng.COMMIT_KEPT = 10
    eng._commit_times("src", "d", set(range(50)))
    kept = eng._commit_cache[("src", "d")]
    assert len(kept) == 10 and min(kept) == 40


def test_a_version_the_commit_table_no_longer_holds_has_no_time():
    eng, _ = _engine({12: 5})
    _feed(eng, [11, 12])
    out, _ = eng.neutral_changes("src", "d", 10)
    ends = [c for c in out if c.get("txn_end")]
    assert "txn_ts" not in ends[0] and ends[1]["txn_ts"]
    assert eng.commit_time_gap("src", "d") is None


def test_an_account_refused_the_commit_table_gets_no_time_and_a_reason():
    eng, asked = _engine(refuse="The SELECT permission was denied on the"
                                " object 'dm_tran_commit_table'")
    _feed(eng, [11, 12])
    out, _ = eng.neutral_changes("src", "d", 10)
    ends = [c for c in out if c.get("txn_end")]
    # the position is still marked; a time is not made up
    assert [c["txn_at"] for c in ends] == [11, 12]
    assert not any("txn_ts" in c for c in out)
    why = eng.commit_time_gap("src", "d")
    assert "VIEW SERVER STATE" in why and "permission was denied" in why
    # not asked again every batch
    _feed(eng, [13])
    eng.neutral_changes("src", "d", 12)
    assert len(asked) == 1
    # but tried again after a while
    eng._commit_gaps[("src", "d")] = (why, time.monotonic() - 1)
    eng.neutral_changes("src", "d", 12)
    assert len(asked) == 2


def test_the_gap_closes_when_the_lookup_works_again():
    eng, _ = _engine({11: 2})
    eng._commit_gaps = {("src", "d"): ("old", time.monotonic() - 1)}
    assert eng._commit_times("src", "d", {11})[11]
    assert eng.commit_time_gap("src", "d") is None


def test_the_beat_says_the_lag_is_unknown_never_0(tmp_path):
    where = tmp_path / "db"
    where.mkdir()
    with tailctl.Running(where):
        tailctl.beat(where, time.time(), 5,
                     lag_unknown="the account cannot read the commit times")
        got = json.loads((where / tailctl.BEAT).read_text())
        assert got["lag"] is None and got["through"] is None
        st = tailctl.state(where)
        assert st["lag"] is None and "commit times" in st["lag_unknown"]
        lines = "\n".join(ui._tail_metrics(tmp_path, 'hop="h"'))
        assert "migkit_tail_commit_lag_seconds" not in lines
        assert "migkit_tail_applied_through_age_seconds" not in lines
        tailctl.beat(where, time.time(), 5, through=time.time() - 3, lag=2.0)
        st = tailctl.state(where)
        assert st["lag"] == 2.0 and st["lag_unknown"] is None
