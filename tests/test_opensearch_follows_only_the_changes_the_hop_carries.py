"""An OpenSearch follow carries only the kinds of change the hop's
`mapping.ops` names for each index (P0-PARITY.scope): a document the target
lacks is an insert, one it holds an update, and a document gone from the
source is a delete. A kind the hop keeps off the target is read, counted as
read, left out, and the position goes on - on a model of the cluster that
needs no server (`tests/opensearch_fake`).
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.ops", "opensearch")]

PROPS = {"title": {"type": "text"}, "n": {"type": "long"}}


def _engine(tmp_path, ops=None):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osf", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"],
              mapping={"ops": ops} if ops else {})
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _pair_of_clusters():
    src, dst = Cluster(), Cluster()
    docs = {str(i): {"title": f"t{i}", "n": i} for i in range(6)}
    src.index("shop.orders", PROPS, docs, shards=2)
    dst.index("shop.orders", PROPS, docs, shards=2)
    return src, dst


def _look(eng, was=None):
    return eng._follow_index("shop", "orders", was, lambda m: None)


def _changed(src):
    src.write("shop.orders", "new", {"title": "fresh", "n": 100})
    src.write("shop.orders", "1", {"title": "changed", "n": 1})
    src.delete("shop.orders", "2")


@pytest.mark.parametrize("ops, inserted, updated, deleted", [
    (None, True, True, True),
    ("insert", True, False, False),
    ("insert,update", True, True, False),
    ("update,delete", False, True, True),
    ("delete", False, False, True),
])
def test_only_the_changes_the_hop_carries_reach_the_target(
        tmp_path, ops, inserted, updated, deleted):
    src, dst = _pair_of_clusters()
    with installed(src, dst):
        eng = _engine(tmp_path, {"*": ops} if ops else None)
        # the follow begins level with the source
        position, _, _ = _look(eng)
        _changed(src)
        position, carried, settled = _look(eng, position)
        assert settled
        got = dst.sources("shop.orders")
        assert ("new" in got) is inserted, got
        assert (got.get("1", {}).get("title") == "changed") is updated, got
        assert ("2" not in got) is deleted, got
        # a look again finds nothing past the position: what was left out
        # is not read a second time
        again, more, _ = _look(eng, position)
        assert more == 0 and again == position


def test_a_table_named_in_the_ops_is_held_to_its_own_rule(tmp_path):
    src, dst = _pair_of_clusters()
    src.index("shop.audit", PROPS, {"a": {"title": "x", "n": 1}})
    dst.index("shop.audit", PROPS, {"a": {"title": "x", "n": 1}})
    with installed(src, dst):
        eng = _engine(tmp_path, {"*": "insert,update,delete",
                                 "shop.audit": "insert"})
        first, _, _ = _look(eng)
        audit, _, _ = eng._follow_index("shop", "audit", None,
                                        lambda m: None)
        src.delete("shop.audit", "a")
        src.write("shop.audit", "b", {"title": "y", "n": 2})
        src.delete("shop.orders", "3")
        _look(eng, first)
        eng._follow_index("shop", "audit", audit, lambda m: None)
        assert set(dst.sources("shop.audit")) == {"a", "b"}
        assert "3" not in dst.sources("shop.orders")
