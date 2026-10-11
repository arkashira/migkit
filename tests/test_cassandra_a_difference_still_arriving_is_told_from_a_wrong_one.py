"""A difference on a Cassandra target is told apart from one still arriving
(P0-PARITY.verify): while migkit's tail runs, a row the check finds apart
may be a change the tail has read and not yet applied. The check waits
until the tail's saved position has reached where the source's clock stood
when the difference was found - the fence - looks again, and calls the
difference one only if it is still there; what converged was still
arriving, and the finding says so.

No server: the cluster is `tests/cassandra_fake.py`. The tail is stood in
for by two things the test controls - that one is running, and the
moment it has caught up, when what it would have written is written.
"""
from migkit import tailctl
from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from migkit.engines.hetero import HeteroEngine
from tests import cassandra_fake as fake

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.confirm", "cassandra")]

COLS = {"id": "int", "name": "text"}


def _setup(tmp_path, monkeypatch, catches_up):
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "orders", COLS, ["id"], rows=[
        {"id": 1, "name": "a"}, {"id": 2, "name": "new"}])
    dst.table("shop", "orders", COLS, ["id"], rows=[
        {"id": 1, "name": "a"}, {"id": 2, "name": "old"}])
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: True)
    fenced = []

    def fence(self, db, lsn, timeout=300):
        """The tail has read as far as `lsn`: what it was carrying is
        written."""
        fenced.append(lsn)
        if catches_up:
            dst.keyspaces["shop"]["orders"].rows[(2,)]["name"] = "new"
        return True
    monkeypatch.setattr(HeteroEngine, "fence_wait", fence)
    return CassandraEngine(hop), fenced


def test_a_difference_the_tail_was_still_carrying_is_not_one(
        tmp_path, monkeypatch):
    eng, fenced = _setup(tmp_path, monkeypatch, catches_up=True)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "ok", got.detail
    assert "still arriving through migkit's change tail" in got.detail
    assert fenced and "at" in fenced[0]


def test_a_difference_that_survives_the_fence_is_a_real_one(tmp_path,
                                                            monkeypatch):
    eng, fenced = _setup(tmp_path, monkeypatch, catches_up=False)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "diff" and fenced
    assert got.rows_apart["orders"]["changed"] == 1


def test_with_no_tail_running_nothing_is_waited_for(tmp_path, monkeypatch):
    eng, fenced = _setup(tmp_path, monkeypatch, catches_up=True)
    monkeypatch.setattr(tailctl, "alive", lambda *a, **k: False)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "diff" and not fenced
