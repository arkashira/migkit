"""The rest of an OpenSearch hop's deep check, on a model of the cluster:
where the target is ahead of the source (movement.target-ahead), whether
its shards are on the move or hold an `_id` twice (structure.sharding), the
objects beside the indexes (structure.objects), who may do what to them
(access.table-grants, structure.row-security) and whether its dates are
held shifted in time (value.timezone).

Most of these ask the cluster things no migkit request has asked it yet - a
cluster's health by index, the documents' versions, its templates, its
pipelines, its policies, its dates as `epoch_millis` - so their cells wait
on a proof against a real server (`UNPROVED`); what is shown here is the
logic over the answers the API documents. The two about roles read them as
`migkit users` does, a request already proved on a server, and are proved
here.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.access.table-grants", "opensearch"),
          ("verify.structure.row-security", "opensearch")]

PROPS = {"title": {"type": "text"}, "n": {"type": "long"},
         "at": {"type": "date"}}


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _docs():
    return {str(i): {"title": f"t{i}", "n": i,
                     "at": f"2024-03-0{i}T10:00:00Z"} for i in range(1, 6)}


def _twins(**dst):
    src, other = Cluster(), Cluster()
    src.index("shop.orders", PROPS, _docs())
    other.index("shop.orders", PROPS, _docs(), **dst)
    return src, other


def _deep(tmp_path, src, dst, **kw):
    with installed(src, dst):
        return {r.scope.split(" ", 1)[1]: r
                for r in _engine(tmp_path, **kw).check_deep("shop")}


# ---- the target ahead of the source ---------------------------------------

def _copied_with_the_sources_versions(src, dst):
    for i, d in src.indexes["shop.orders"]["docs"].items():
        dst.write("shop.orders", i, d["_source"],
                  version=(d["_primary_term"] << 43) | d["_seq_no"])


def test_a_document_written_again_on_the_target_is_ahead(tmp_path):
    src, dst = _twins()
    dst.indexes["shop.orders"]["docs"].clear()
    _copied_with_the_sources_versions(src, dst)
    level = _deep(tmp_path, src, dst)["boundary"]
    assert level.status == "ok", level
    assert level.category == "movement.target-ahead"
    # something else writes document 3 on the target: its version goes on
    # from the source's, past it
    dst.write("shop.orders", "3", {"title": "edited there", "n": 3,
                                   "at": "2024-03-03T10:00:00Z"})
    ahead = _deep(tmp_path, src, dst)["boundary"]
    assert ahead.status == "diff", ahead
    assert "orders: 1 documents newer on the target (3)" in ahead.detail
    # and the source writing it again puts the source level, then past
    src.write("shop.orders", "3", {"title": "edited here", "n": 3,
                                   "at": "2024-03-03T10:00:00Z"})
    src.write("shop.orders", "3", {"title": "edited here too", "n": 3,
                                   "at": "2024-03-03T10:00:00Z"})
    assert _deep(tmp_path, src, dst)["boundary"].status == "ok"


def test_a_copy_through_rows_carries_no_version_and_is_not_judged(tmp_path):
    src, dst = _twins()
    dst.write("shop.orders", "3", {"title": "edited there", "n": 3,
                                   "at": "2024-03-03T10:00:00Z"})
    got = _deep(tmp_path, src, dst)["boundary"]
    assert got.status == "ok", got


# ---- the shards ---------------------------------------------------------------

def test_shards_on_the_move_make_a_count_a_moving_target(tmp_path):
    src, dst = _twins()
    dst.indexes["shop.orders"]["relocating"] = 2
    dst.indexes["shop.orders"]["initializing"] = 1
    got = _deep(tmp_path, src, dst)["sharding"]
    assert got.status == "diff" and got.category == "structure.sharding"
    assert "orders: 3 shards moving or starting" in got.detail
    dst.indexes["shop.orders"]["relocating"] = 0
    dst.indexes["shop.orders"]["initializing"] = 0
    assert _deep(tmp_path, src, dst)["sharding"].status == "ok"


def test_an_id_held_in_two_shards_is_found(tmp_path):
    src, dst = _twins(shards=2)

    def pages(method, path, body, query):
        hit = {"_index": "shop.orders", "_id": "1", "_score": None}
        if "scroll_id" in (body or {}):
            return {"_scroll_id": "s", "hits": {"hits": []}}
        return {"_scroll_id": "s", "hits": {"hits": [
            hit, dict(hit), dict(hit, _id="2")]}}
    dst.answers["/shop.orders/_search"] = ("POST", pages)
    dst.answers["/_search/scroll"] = ("POST", pages)
    got = _deep(tmp_path, src, dst)["sharding"]
    assert got.status == "diff"
    assert "orders: 1 documents share an _id" in got.detail


# ---- what lies beside the indexes ----------------------------------------------

def _templates(cluster, body):
    cluster.answers["/_index_template"] = {"index_templates": [
        {"name": "orders-template", "index_template": body}]}


def test_an_alias_a_template_a_pipeline_a_policy_missing_are_found(
        tmp_path):
    src, dst = Cluster(), Cluster()
    settings = {"default_pipeline": "stamp"}
    src.index("shop.orders", PROPS, _docs(), settings=settings, aliases={
        "orders-live": {}, "orders-recent": {"filter": {"range": {
            "at": {"gte": "2024-03-04"}}}}})
    dst.index("shop.orders", PROPS, _docs(), settings=settings,
              aliases={"orders-live": {}})
    template = {"index_patterns": ["shop.*"], "priority": 5,
                "template": {"settings": {"number_of_shards": 1}}}
    _templates(src, template)
    src.answers["/_ingest/pipeline/stamp"] = {"stamp": {"processors": []}}
    src.answers["/_plugins/_ism/explain/shop.orders"] = {
        "shop.orders": {
            "index.plugins.index_state_management.policy_id": "age"}}
    src.answers["/_plugins/_ism/policies/age"] = {"policy": {
        "default_state": "hot", "states": [{"name": "hot"}]}}
    got = _deep(tmp_path, src, dst)["objects"]
    assert got.status == "diff" and got.category == "structure.objects"
    for words in ("view orders-recent on orders: missing on the target",
                  "template orders-template: missing on the target",
                  "pipeline stamp: missing on the target",
                  "policy age: missing on the target"):
        assert words in got.detail, got.detail
    assert "orders-live" not in got.detail


def test_objects_that_agree_are_found_to_agree_and_one_that_differs_is_named(
        tmp_path):
    src, dst = Cluster(), Cluster()
    for c in (src, dst):
        c.index("shop.orders", PROPS, _docs(), aliases={"orders-live": {}})
    body = {"index_patterns": ["shop.*"], "template": {"settings": {
        "number_of_shards": 1}}}
    _templates(src, body)
    _templates(dst, body)
    assert _deep(tmp_path, src, dst)["objects"].status == "ok"
    _templates(dst, dict(body, priority=9))
    got = _deep(tmp_path, src, dst)["objects"]
    assert got.status == "diff"
    assert "template orders-template: differs" in got.detail


def test_the_hops_objects_leave_a_kind_out(tmp_path):
    src, dst = Cluster(), Cluster()
    src.index("shop.orders", PROPS, _docs(), aliases={
        "orders-recent": {"filter": {"term": {"n": 1}}}})
    dst.index("shop.orders", PROPS, _docs())
    assert _deep(tmp_path, src, dst)["objects"].status == "diff"
    left = _deep(tmp_path, src, dst, objects={"views": False})["objects"]
    assert left.status == "ok", left


def test_an_object_of_an_index_the_hop_leaves_out_is_not_compared(tmp_path):
    src, dst = Cluster(), Cluster()
    src.index("shop.audit", PROPS, _docs(), aliases={"audit-live": {}})
    dst.index("shop.audit", PROPS, _docs())
    got = _deep(tmp_path, src, dst, exclude=["shop.audit"])["objects"]
    assert got.status == "ok", got


# ---- who may do what ---------------------------------------------------------

ROLES = "/_plugins/_security/api/roles"


def _role(index_patterns, actions, **more):
    return {"index_permissions": [dict(index_patterns=index_patterns,
                                       allowed_actions=actions, **more)]}


def _security(cluster, roles):
    cluster.answers[ROLES] = ("GET", roles)
    cluster.answers["/_plugins/_security/api/internalusers"] = ("GET", {})
    cluster.answers["/_plugins/_security/api/rolesmapping"] = ("GET", {})


def test_neither_cluster_running_the_plugin_is_no_difference(tmp_path):
    src, dst = _twins()
    for c in (src, dst):
        c.answers["/_plugins/_security/api"] = None
    got = _deep(tmp_path, src, dst)
    assert got["grants"].status == "ok"
    assert got["grants"].category == "access.table-grants"
    assert got["rls"].status == "ok"
    assert got["rls"].category == "structure.row-security"


def test_a_role_that_grants_less_on_the_target_is_a_grants_finding(tmp_path):
    src, dst = _twins()
    _security(src, {"analyst": _role(["shop.*"], ["read", "search"])})
    _security(dst, {"analyst": _role(["shop.*"], ["read"])})
    got = _deep(tmp_path, src, dst)["grants"]
    assert got.status == "diff"
    assert "analyst on orders: read, search -> read" in got.detail


def test_a_role_missing_on_the_target_is_found(tmp_path):
    src, dst = _twins()
    _security(src, {"analyst": _role(["shop.orders"], ["read"])})
    _security(dst, {})
    got = _deep(tmp_path, src, dst)["grants"]
    assert got.status == "diff"
    assert "analyst may read on orders on the source; the role is missing" \
        in got.detail


def test_a_role_that_lost_its_row_filter_is_a_row_security_finding(
        tmp_path):
    src, dst = _twins()
    _security(src, {"clerk": _role(["shop.*"], ["read"],
                                   dls='{"term": {"n": 1}}',
                                   masked_fields=["title"])})
    _security(dst, {"clerk": _role(["shop.*"], ["read"])})
    got = _deep(tmp_path, src, dst)
    assert got["rls"].status == "diff"
    assert "clerk on orders" in got["rls"].detail
    assert got["grants"].status == "ok"


def test_the_hops_objects_leave_roles_out(tmp_path):
    src, dst = _twins()
    _security(src, {"clerk": _role(["shop.*"], ["read"])})
    _security(dst, {"clerk": _role(["shop.*"], [])})
    assert _deep(tmp_path, src, dst)["grants"].status == "diff"
    left = _deep(tmp_path, src, dst, objects={"roles": False})
    assert left["grants"].status == "ok"


def test_roles_that_cannot_be_read_are_an_error_not_an_ok(tmp_path):
    src, dst = _twins()
    src.answers[ROLES] = ("GET", RuntimeError("GET /x: 503 unavailable"))
    got = _deep(tmp_path, src, dst)
    assert got["grants"].status == "error" and got["rls"].status == "error"


# ---- the dates ---------------------------------------------------------------

def test_dates_all_held_hours_apart_are_a_timezone_finding(tmp_path):
    src, dst = Cluster(), Cluster()
    src.index("shop.orders", PROPS, _docs())
    shifted = {i: dict(d, at=d["at"].replace("T10", "T13"))
               for i, d in _docs().items()}
    dst.index("shop.orders", PROPS, shifted)
    got = _deep(tmp_path, src, dst)["timeshift"]
    assert got.status == "diff" and got.category == "value.timezone"
    assert "orders.at: every row shifted 10800s" in got.detail
    level = _deep(tmp_path, *_twins())["timeshift"]
    assert level.status == "ok", level


def test_dates_that_differ_one_by_one_are_left_to_the_row_comparison(
        tmp_path):
    src, dst = Cluster(), Cluster()
    src.index("shop.orders", PROPS, _docs())
    odd = _docs()
    odd["1"]["at"] = "2024-03-01T11:00:00Z"
    odd["4"]["at"] = "2024-03-04T19:30:00Z"
    dst.index("shop.orders", PROPS, odd)
    assert _deep(tmp_path, src, dst)["timeshift"].status == "ok"


@pytest.mark.parametrize("name", ["boundary", "sharding", "objects",
                                  "grants", "rls", "timeshift"])
def test_each_is_a_finding_of_its_own_category(tmp_path, name):
    got = _deep(tmp_path, *_twins())
    assert name in got and got[name].category
