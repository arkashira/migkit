"""What `assess` reads of an OpenSearch cluster before anything moves:
each side's health, the room the target has for the move (disk and
shards), the writes it refuses, who else writes into it, the processors
it gives, and the work the move leaves to hands (P0-PARITY.all).

The questions go to parts of the API no migkit request has asked yet (the
allocation table, the node statistics, the task list, the data streams,
the replication statistics), so the cells wait on a proof against a real
server (`UNPROVED`); what is shown here is the logic over the answers the
API documents, on a model of the cluster that needs no server
(`tests/opensearch_fake`).
"""
from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

PROPS = {"title": {"type": "text"}, "n": {"type": "long"}}
GIB = 1 << 30


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    eng = OpenSearchEngine(hop)
    eng.WRITER_PROBE = 0
    return eng


def _cluster(**kw):
    c = Cluster()
    c.index("shop.orders", PROPS, {"1": {"title": "a", "n": 1}}, **kw)
    return c


def _disks(cluster, *nodes):
    """Nodes as (free GiB, total GiB)."""
    cluster.answers["/_cat/allocation"] = [
        {"node": f"n{i}", "disk.avail": str(f * GIB),
         "disk.total": str(t * GIB)} for i, (f, t) in enumerate(nodes)]


def _sized(cluster, table, gib):
    cluster.indexes[f"shop.{table}"]["bytes"] = int(gib * GIB)


def _room(tmp_path, src, dst, **kw):
    with installed(src, dst):
        return _engine(tmp_path, **kw)._room_gaps("shop")


def test_a_move_the_target_has_room_for_reads_as_one(tmp_path):
    src, dst = _cluster(replicas=1), _cluster()
    _sized(src, "orders", 10)
    _disks(dst, (100, 200), (100, 200))
    got = _room(tmp_path, src, dst)
    assert got.status == "ok", got
    assert "about 20.0 GiB" in got.detail      # primaries and a replica each


def test_what_the_target_already_holds_of_the_index_is_not_needed_again(
        tmp_path):
    src, dst = _cluster(replicas=1), _cluster()
    _sized(src, "orders", 40)
    _sized(dst, "orders", 40)
    _disks(dst, (10, 200))
    assert _room(tmp_path, src, dst).status == "ok"


def test_a_target_without_the_disk_is_a_diff_before_it_is_a_stopped_load(
        tmp_path):
    src, dst = _cluster(replicas=1), _cluster()
    _sized(src, "orders", 80)
    # two nodes of 200 GiB with 100 free: the low watermark (85% used)
    # keeps 30 GiB of each free, so each takes 70
    _disks(dst, (100, 200), (100, 200))
    got = _room(tmp_path, src, dst)
    assert got.status == "diff", got
    assert "needs about 160.0 GiB" in got.detail
    assert "140.0 GiB" in got.detail
    _disks(dst, (100, 200), (100, 200), (100, 200))
    assert _room(tmp_path, src, dst).status == "ok"


def test_a_move_that_fills_most_of_the_room_is_said(tmp_path):
    src, dst = _cluster(replicas=0), _cluster()
    _sized(src, "orders", 120)
    _disks(dst, (100, 200), (100, 200))
    got = _room(tmp_path, src, dst)
    assert got.status == "warn", got
    assert "most of the 140.0 GiB" in got.detail


def test_a_target_that_gives_no_figures_is_unknown_not_roomy(tmp_path):
    src, dst = _cluster(), _cluster()
    dst.answers["/_cat/allocation"] = []
    got = _room(tmp_path, src, dst)
    assert got.status == "warn" and "unknown" in got.detail


def test_a_watermark_given_as_a_size_is_the_free_space_it_keeps():
    from migkit.engines.opensearch import kept_free
    assert kept_free("85%", 200 * GIB) == 30 * GIB
    assert kept_free("0.9", 100 * GIB) == 10 * GIB
    assert kept_free("50gb", 200 * GIB) == 50 * GIB
    assert kept_free("soon", 200 * GIB) is None


def test_the_shards_a_move_adds_are_held_to_what_the_cluster_allows(
        tmp_path):
    src = Cluster()
    for i in range(30):
        src.index(f"shop.t{i}", PROPS, {}, shards=3, replicas=1)
    dst = Cluster()
    dst.index("shop.old", PROPS, {}, shards=1, replicas=0)
    dst.cluster_settings = {"cluster.max_shards_per_node": "100"}
    _disks(dst, (100, 200))
    got = _room(tmp_path, src, dst)
    assert got.status == "diff", got
    assert "adds 180 shards and the target has room for 99 more" \
        in got.detail


def test_a_blocked_or_closed_index_of_the_target_is_found(tmp_path):
    dst = Cluster()
    dst.index("shop.orders", PROPS, {}, settings={"blocks.write": "true"})
    dst.index("shop.lines", PROPS, {})
    dst.index("shop.audit", PROPS, {})
    dst.indexes["shop.lines"]["status"] = "close"
    with installed(Cluster(), dst):
        got = _engine(tmp_path)._write_blocks("shop")
        assert got.status == "diff", got
        assert "orders is write" in got.detail
        assert "lines is closed" in got.detail
        left = _engine(tmp_path, exclude=["shop.orders", "shop.lines"]
                       )._write_blocks("shop")
        assert left.status == "ok", left


def test_a_read_only_cluster_is_found(tmp_path):
    dst = _cluster()
    dst.cluster_settings = {"cluster.blocks.read_only": "true"}
    with installed(Cluster(), dst):
        got = _engine(tmp_path)._write_blocks("shop")
    assert got.status == "diff" and "whole cluster" in got.detail


def test_the_preflight_names_both_and_assess_runs_them(tmp_path):
    from migkit.engines.opensearch import OpenSearchEngine
    assert OpenSearchEngine.PREFLIGHT == ("_room_gaps", "_write_blocks")
    src, dst = _cluster(), _cluster()
    _disks(dst, (100, 200))
    with installed(src, dst):
        items = _engine(tmp_path)._preflight_items()
    scopes = [i["item"] for i in items]
    assert "shop target room" in scopes and "shop target writes" in scopes


# ---- the cluster's own figures ---------------------------------------------

def test_the_processors_and_the_busy_threads_are_the_data_nodes(tmp_path):
    c = _cluster()
    c.answers["/_nodes/os"] = {"nodes": {
        "a": {"roles": ["data", "ingest"], "os": {"allocated_processors": 8}},
        "b": {"roles": ["data_hot"], "os": {"allocated_processors": 4}},
        "c": {"roles": ["cluster_manager"],
              "os": {"allocated_processors": 16}}}}
    c.answers["/_nodes/stats/thread_pool"] = {"nodes": {
        "a": {"thread_pool": {"write": {"active": 3},
                              "search": {"active": 1}}},
        "b": {"thread_pool": {"write": {"active": 2}}}}}
    with installed(c):
        assert _engine(tmp_path).capacity("dst", "shop") == {
            "cpus": 12, "running": 6}


def test_a_figure_the_cluster_does_not_answer_is_left_out(tmp_path):
    c = _cluster()
    c.answers["/_nodes"] = RuntimeError("GET /_nodes: 403 forbidden")
    with installed(c):
        assert _engine(tmp_path).capacity("dst", "shop") == {}


def test_the_free_bytes_are_what_the_nodes_will_take(tmp_path):
    c = _cluster()
    _disks(c, (100, 200), (50, 100))
    with installed(c):
        assert _engine(tmp_path).free_bytes("dst", "shop") == (
            70 + 35) * GIB
    c.answers["/_cat/allocation"] = []
    with installed(c):
        assert _engine(tmp_path).free_bytes("dst", "shop") is None


# ---- who else writes ----------------------------------------------------------

def test_a_reindex_a_follower_and_a_writer_are_each_named(tmp_path):
    dst = _cluster()
    dst.answers["/_tasks"] = {"nodes": {"n": {"tasks": {"n:1": {
        "action": "indices:data/write/reindex",
        "description": "reindex from [old] to [shop.orders][_doc]"}, "n:2": {
        "action": "indices:data/write/update/byquery",
        "description": "update-by-query [shop.other]"}}}}}
    dst.answers["/_plugins/_replication/follower_stats"] = {
        "index_stats": {"shop.orders": {}}}
    totals = iter([100, 160])

    def stats(method, path, body, query):
        return {"indices": {"shop.orders": {"primaries": {"indexing": {
            "index_total": next(totals)}}}}}
    dst.answers["/shop.*/_stats/indexing"] = stats
    with installed(Cluster(), dst):
        got = _engine(tmp_path).stream_writers("shop")
    names = [w for w, pausable in got]
    assert all(pausable is False for _, pausable in got)
    assert "a running reindex into shop.orders" in names
    assert "replication of another cluster's index into shop.orders" in names
    assert any("something writing into the target's indexes (60 documents"
               in n for n in names), names


def test_a_target_nobody_writes_in_has_no_writers(tmp_path):
    dst = _cluster()
    dst.answers["/shop.*/_stats/indexing"] = {"indices": {"shop.orders": {
        "primaries": {"indexing": {"index_total": 7}}}}}
    with installed(Cluster(), dst):
        assert _engine(tmp_path).stream_writers("shop") == []


def test_assess_says_none_found_where_nothing_writes(tmp_path):
    dst = _cluster()
    dst.answers["/shop.*/_stats/indexing"] = {"indices": {}}
    _disks(dst, (100, 200))
    with installed(_cluster(), dst):
        items = _engine(tmp_path)._writer_items("shop")
    assert [i["level"] for i in items] == ["pass"], items


# ---- the work left to hands ---------------------------------------------------

def test_the_work_the_move_leaves_is_counted_and_named(tmp_path):
    src = Cluster()
    src.index("shop.orders", PROPS, {}, settings={
        "default_pipeline": "stamp"}, aliases={
            "orders-live": {}, "orders-recent": {"filter": {"term": {
                "n": 1}}}})
    src.index("shop.old", PROPS, {})
    src.indexes["shop.old"]["status"] = "close"
    src.answers["/_index_template"] = {"index_templates": [
        {"name": "orders-template", "index_template": {
            "index_patterns": ["shop.*"]}}]}
    src.answers["/_ingest/pipeline/stamp"] = {"stamp": {"processors": []}}
    src.answers["/_plugins/_ism/explain/shop.orders"] = {"shop.orders": {
        "index.plugins.index_state_management.policy_id": "age"}}
    src.answers["/_plugins/_ism/policies/age"] = {"policy": {"states": []}}
    src.answers["/_data_stream"] = {"data_streams": [
        {"name": "shop.logs"}, {"name": "other.logs"}]}
    with installed(src, _cluster()):
        inv = _engine(tmp_path)._handwork()
    rows = {r["item"]: r for r in inv.rows()}
    aliases = rows["objects whose contents no data mover copies"]["detail"]
    for words in ("aliases: 2", "orders-live", "templates: 1",
                  "ingest pipelines: 1", "data streams: 1"):
        assert words in aliases, aliases
    assert "other.logs" not in aliases
    decide = rows["settings that need a decision rather than a copy"]
    assert "closed indexes: 1" in decide["detail"]
    assert "lifecycle policies" in decide["detail"]
    assert "policy age" in decide["detail"]


def test_a_probe_that_could_not_run_is_unknown_never_none(tmp_path):
    src = _cluster()
    src.answers["/_data_stream"] = RuntimeError("GET /_data_stream: 503 down")
    with installed(src, _cluster()):
        inv = _engine(tmp_path)._handwork()
    detail = " ".join(r["detail"] for r in inv.rows())
    assert "UNKNOWN" in detail and "data streams" in detail, detail


def test_assess_reads_health_and_the_work_left(tmp_path):
    src, dst = _cluster(), _cluster()
    dst.answers["/_cluster/health"] = {"status": "red",
                                       "number_of_data_nodes": 2,
                                       "active_shards": 4,
                                       "unassigned_shards": 1}
    with installed(src, dst):
        items = _engine(tmp_path)._assess_extra()
    health = {i["scope"]: i for i in items if i["item"] == "cluster health"}
    assert health["source cluster"]["level"] == "pass"
    assert health["target cluster"]["level"] == "fail"
    assert "missing primary shards" in health["target cluster"]["detail"]
