"""While migkit's own follow runs, a check on an OpenSearch hop waits for
it to reach where the source's shards stand, and looks again: a difference
that was a change still on its way converges and reads ok, and one made on
the target alone stays a difference (P0-PARITY.verify).

The follow is a stand-in that moves the saved position and applies the
change after a moment, the way the real one does; what is proved is the
check's wait and its second look, on a model of the cluster that needs no
server (`tests/opensearch_fake`).
"""
import threading
import time

from migkit import tailctl
from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.confirm", "opensearch")]

PROPS = {"title": {"type": "text"}, "n": {"type": "long"}}


def _engine(tmp_path):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], options={"fence_timeout": 20})
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _level_clusters():
    src, dst = Cluster(), Cluster()
    docs = {str(i): {"title": f"t{i}", "n": i} for i in range(1, 6)}
    for c in (src, dst):
        c.index("shop.orders", PROPS, docs, shards=2)
    return src, dst


def _the_follow(eng, src, dst, tmp_path, applies):
    """A follow that has the source's position as of now saved, and - a
    moment after - applies `applies` to the target and saves the position
    it has then."""
    token = tmp_path / tailctl.TOKEN
    tailctl.begin(eng, "shop", token, eng.log_position("src", "shop"))

    def carry():
        time.sleep(0.4)
        applies()
        env = tailctl.position(token)
        tailctl.moved(token, env, eng.log_position("src", "shop"))
    thread = threading.Thread(target=carry, daemon=True)
    return thread


def test_a_change_the_follow_has_not_applied_is_still_arriving(
        tmp_path, monkeypatch):
    src, dst = _level_clusters()
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: True)
    with installed(src, dst):
        eng = _engine(tmp_path)
        carry = _the_follow(eng, src, dst, tmp_path, lambda: dst.write(
            "shop.orders", "6", {"title": "t6", "n": 6}))
        # written on the source after the position was saved, not yet
        # on the target
        src.write("shop.orders", "6", {"title": "t6", "n": 6})
        carry.start()
        got = [r for r in eng.check_data("shop") if r.check == "data"]
        carry.join(10)
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
        assert "still arriving" in got[0].detail, got[0].detail


def test_a_difference_made_on_the_target_alone_is_still_one(
        tmp_path, monkeypatch):
    src, dst = _level_clusters()
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: True)
    with installed(src, dst):
        eng = _engine(tmp_path)
        carry = _the_follow(eng, src, dst, tmp_path, lambda: None)
        dst.write("shop.orders", "3", {"title": "someone else's", "n": 3})
        carry.start()
        got = [r for r in eng.check_data("shop") if r.check == "data"]
        carry.join(10)
        assert [r.status for r in got] == ["diff"], [r.detail for r in got]
        assert "still arriving" not in got[0].detail


def test_with_no_follow_running_a_difference_is_not_waited_on(tmp_path):
    src, dst = _level_clusters()
    src.write("shop.orders", "6", {"title": "t6", "n": 6})
    with installed(src, dst):
        eng = _engine(tmp_path)
        began = time.monotonic()
        got = [r for r in eng.check_data("shop") if r.check == "data"]
        assert [r.status for r in got] == ["diff"]
        assert time.monotonic() - began < 5
