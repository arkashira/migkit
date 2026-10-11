"""What a Cosmos DB check says beyond the items' digest (backlog
P0-PARITY.verify, .all), against a model of the service's SDK
(`tests/cosmos_fake.py`, no real account):

* **null, empty, absent**: a JSON null, an empty value and a field left out
  are three things; a copy writes a field the source left out as a null,
  and the deep check names every field whose three counts moved;
* **kinds of value**: a field holding a string on one side and a number on
  the other is named;
* **properties**: the indexing policy, the default time to live (it expires
  other items), the conflict policy, the partition key (where an item is
  placed) and the computed properties, container by container;
* **stored procedures, functions and triggers** are compared by name and
  body, the kinds the hop carries (`objects`) only;
* **the assessment**: items sized as they lie, the change feed the hop
  follows by asked before the copy, what the move leaves for a person
  counted, and the throughput the target containers provision.

What the model cannot say is said in the test: the SDK's script, throughput
and account calls are modelled on its documentation, not run against a
service - the cells that read them are owed a server's proof
(`capabilities.UNPROVED`).
"""
import pytest

from tests import cosmos_fake as cf

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.value.null-empty", "cosmosdb"),
    ("verify.value.type-drift", "cosmosdb"),
    ("verify.structure.indexes", "cosmosdb"),
    ("verify.structure.collection-options", "cosmosdb"),
    ("verify.structure.sharding", "cosmosdb"),
    ("verify.value.generated", "cosmosdb"),
    ("move.sized", "cosmosdb"),
    ("assess.preflight", "cosmosdb"),
]


@pytest.fixture
def account(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    client = cf.installed(monkeypatch)
    client.create_database("csrc")
    client.create_database("cdst")
    return client


def _pair(account, name="t", **kw):
    src = account.get_database_client("csrc")
    dst = account.get_database_client("cdst")
    return (src.create_container(name, cf.PartitionKey("/region"), **kw),
            dst.create_container(name, cf.PartitionKey("/region")))


def _deep(eng, scope):
    return next(r for r in eng.check_deep("csrc")
                if r.scope == f"csrc {scope}")


def test_a_field_left_out_that_arrived_a_null_is_named(account, tmp_path):
    s = account.get_database_client("csrc").create_container(
        "t", cf.PartitionKey("/region"))
    cf.put(s, "o1", region="eu", note=None, e="")
    cf.put(s, "o2", region="eu", note="n", e="")
    cf.put(s, "o3", region="eu", e="x")
    eng = cf.engine(tmp_path)
    cf.copy(eng)
    # the copy made the container as the source is described
    got = _deep(eng, "null-missing")
    assert got.status == "diff" and got.category == "value.null-empty"
    assert "t.note: null/empty/absent 1/0/1 on the source, 2/0/0 on the" \
        " target" in got.detail, got.detail
    assert ".e:" not in got.detail, got.detail


def test_an_empty_value_that_arrived_absent_is_named(account, tmp_path):
    s, d = _pair(account)
    cf.put(s, "o1", region="eu", e="")
    cf.put(d, "o1", region="eu")
    got = _deep(cf.engine(tmp_path), "null-missing")
    assert got.status == "diff" and "t.e: null/empty/absent 0/1/0" \
        in got.detail, got.detail


def test_a_field_that_holds_another_kind_of_value_is_named(account,
                                                           tmp_path):
    s, d = _pair(account)
    cf.put(s, "o1", region="eu", price=10, tags=["a"])
    cf.put(d, "o1", region="eu", price="10", tags=["a"])
    got = _deep(cf.engine(tmp_path), "field types")
    assert got.status == "diff" and got.category == "value.type-drift"
    assert got.detail.startswith("t.price: integer -> string"), got.detail
    assert "tags" not in got.detail


def test_a_container_too_large_to_read_whole_is_not_called_clean(
        account, tmp_path):
    s, d = _pair(account)
    for i in range(5):
        cf.put(s, f"o{i}", region="eu", v="x")
        cf.put(d, f"o{i}", region="eu", v="x")
    eng = cf.engine(tmp_path)
    eng.DEEP_MOST = 3
    for scope in ("null-missing", "field types"):
        got = _deep(eng, scope)
        assert got.status == "warn" and "more than 3 items, not read whole" \
            in got.detail, got.detail


def test_a_different_indexing_policy_is_named(account, tmp_path):
    s, d = _pair(account, indexing_policy={
        "indexingMode": "consistent", "automatic": True,
        "includedPaths": [{"path": "/*"}],
        "excludedPaths": [{"path": '/"_etag"/?'}, {"path": "/big/*"}],
        "compositeIndexes": [[{"path": "/a", "order": "ascending"},
                              {"path": "/b", "order": "descending"}]]})
    got = _deep(cf.engine(tmp_path), "indexes")
    assert got.status == "diff", got.detail
    assert 't: excluded ["/big/*"] on the source, [] on the target' \
        in got.detail and "composite" in got.detail, got.detail


def test_a_different_default_ttl_is_named_as_what_expires_items(account,
                                                               tmp_path):
    _pair(account, default_ttl=3600)
    got = _deep(cf.engine(tmp_path), "container options")
    assert got.status == "diff" and got.category == \
        "structure.collection-options"
    assert "t.defaultTtl: 3600 on the source, null on the target" \
        in got.detail and "expires other items" in got.fix_hint, got


def test_a_different_partition_key_is_named(account, tmp_path):
    src = account.get_database_client("csrc")
    dst = account.get_database_client("cdst")
    src.create_container("t", cf.PartitionKey("/region"))
    dst.create_container("t", cf.PartitionKey("/id"))
    got = _deep(cf.engine(tmp_path), "sharding")
    assert got.status == "diff" and "t:" in got.detail \
        and '"/region"' in got.detail and '"/id"' in got.detail, got.detail


def test_a_different_computed_property_is_named(account, tmp_path):
    s, d = _pair(account)
    s.props["computedProperties"] = [
        {"name": "cp_lower", "query": "SELECT VALUE LOWER(c.name) FROM c"}]
    got = _deep(cf.engine(tmp_path), "generated")
    assert got.status == "diff" and got.category == "value.generated"
    assert "cp_lower" in got.detail, got.detail


def test_equal_containers_are_equal_in_every_finding(account, tmp_path):
    s, d = _pair(account)
    for c in (s, d):
        cf.put(c, "o1", region="eu", e="", v=1)
    eng = cf.engine(tmp_path)
    for scope in ("null-missing", "field types", "indexes",
                  "container options", "sharding", "generated", "objects",
                  "triggers"):
        assert _deep(eng, scope).status == "ok", scope


# ---- stored code --------------------------------------------------------------

def test_scripts_are_compared_by_name_and_body_for_the_kinds_the_hop_takes(
        account, tmp_path):
    s, d = _pair(account)
    s.scripts.sprocs.update(bulk="function(){return 1}", gone="x")
    d.scripts.sprocs.update(bulk="function(){return 2}")
    s.scripts.udfs.update(tax="function(v){return v*2}")
    d.scripts.udfs.update(tax="function(v){return v*2}")
    s.scripts.triggers.update(audit=("Pre", "Create", "function(){}"))
    eng = cf.engine(tmp_path)
    objs = _deep(eng, "objects")
    assert objs.status == "diff" and "t: procedure bulk differs on the" \
        " target" in objs.detail and "t: procedure gone is not on the" \
        " target" in objs.detail and "tax" not in objs.detail, objs.detail
    trig = _deep(eng, "triggers")
    assert trig.status == "diff" and "t: trigger audit is not on the target" \
        in trig.detail, trig.detail
    # a hop that carries no triggers, and procedures by name, holds the
    # scripts to that
    narrow = cf.engine(tmp_path, objects={"triggers": False,
                                          "procedures": ["t.bulk"]})
    assert _deep(narrow, "triggers").status == "ok"
    got = _deep(narrow, "objects")
    assert "bulk" in got.detail and "gone" not in got.detail, got.detail


# ---- the move and the assessment --------------------------------------------------

def test_items_are_sized_as_they_lie(account, tmp_path):
    s, _ = _pair(account)
    cf.put(s, "o1", region="eu", v="x" * 100)
    cf.put(s, "o2", region="eu", v="y" * 1000)
    eng = cf.engine(tmp_path)
    got = eng.row_sizes("src", "csrc")["t"]
    assert 1000 < got["largest"] < 1100 and got["exact"] is True
    assert got["rows"] == 2 and got["mean"] < got["largest"]
    eng.SIZE_SAMPLE = 1
    assert eng.row_sizes("src", "csrc", ["t"])["t"]["exact"] is False


def test_the_change_feed_the_hop_follows_by_is_asked_before_the_copy(
        account, tmp_path):
    _pair(account)
    ok = cf.engine(tmp_path)._follow_readiness("csrc")
    assert ok.status == "ok" and "all-versions-and-deletes" in ok.detail
    cf.account(avad=False)
    bad = cf.engine(tmp_path)._follow_readiness("csrc")
    assert bad.status == "warn" and "does not serve the all-versions" \
        in bad.detail and "change_feed: latest" in bad.fix_hint, bad
    latest = cf.engine(tmp_path)
    latest.hop.source.options["change_feed"] = "latest"
    got = latest._follow_readiness("csrc")
    assert got.status == "warn" and "carries no delete" in got.detail
    rows = [r for r in cf.engine(tmp_path)._preflight_items()
            if r["item"] == "csrc change feed"]
    assert rows and rows[0]["level"] == "warn", rows


def test_what_the_move_leaves_for_a_person_is_counted_and_named(account,
                                                               tmp_path):
    s, _ = _pair(account, default_ttl=60,
                 unique_key_policy={"uniqueKeys": [{"paths": ["/sku"]}]},
                 indexing_policy={"indexingMode": "consistent",
                                  "automatic": True,
                                  "includedPaths": [{"path": "/*"}],
                                  "excludedPaths": [{"path": '/"_etag"/?'},
                                                    {"path": "/x/*"}]})
    s.scripts.sprocs.update(bulk="f")
    s.scripts.triggers.update(audit=("Pre", "All", "g"))
    account.get_database_client("csrc").create_container(
        "plain", cf.PartitionKey("/region"))
    rows = cf.engine(tmp_path)._assess_extra()
    said = " | ".join(f"{r['item']}: {r['detail']}" for r in rows)
    prereq = next(r for r in rows if "must exist on the target" in r["item"])
    assert prereq["level"] == "fail" and "unique keys" in prereq["detail"] \
        and "(t)" in prereq["detail"], prereq
    left = next(r for r in rows if "objects whose contents no data mover"
                in r["item"])
    assert "default time to live, which expires items: 1 (t)" \
        in left["detail"] and "indexing policies other than the default:" \
        " 1 (t)" in left["detail"], left
    code = next(r for r in rows if "routines, triggers and views" in r["item"])
    assert "procedures : 1 (t.bulk)" in code["detail"] \
        and "triggers : 1 (t.audit)" in code["detail"], code
    assert "plain" not in said


def test_the_throughput_the_target_provisions_is_measured(account, tmp_path):
    _, d = _pair(account)
    eng = cf.engine(tmp_path)
    assert eng.capacity("dst", "csrc") == {}
    rows = [r for r in eng._assess_extra()
            if r["item"] == "the target's request units"]
    assert rows[0]["level"] == "warn"
    d.throughput = 4000
    assert eng.capacity("dst", "csrc") == {"request_units_per_second": 4000,
                                           "provisioned": 1}
    rows = [r for r in eng._assess_extra()
            if r["item"] == "the target's request units"]
    assert rows[0]["level"] == "pass" and "4,000 request units" \
        in rows[0]["detail"], rows


def test_what_else_writes_into_a_target_is_named(account, tmp_path):
    _, d = _pair(account)
    eng = cf.engine(tmp_path)
    assert eng.stream_writers("csrc") == []
    d.props["defaultTtl"] = 30
    cf.account().multi_write = True
    got = [w for w, pausable in eng.stream_writers("csrc")]
    assert any("expiring t's items" in w for w in got), got
    assert any("other regions writing" in w for w in got), got
    assert eng._writer_items("csrc")[0]["level"] == "fail"


# ---- the indexes ---------------------------------------------------------------------

def test_composite_indexes_and_unique_keys_are_read_and_the_first_made(
        account, tmp_path):
    s, d = _pair(account, indexing_policy={
        "indexingMode": "consistent", "automatic": True,
        "includedPaths": [{"path": "/*"}], "excludedPaths": [],
        "compositeIndexes": [[{"path": "/a", "order": "ascending"},
                              {"path": "/b", "order": "ascending"}]]},
        unique_key_policy={"uniqueKeys": [{"paths": ["/sku"]}]})
    eng = cf.engine(tmp_path)
    assert eng.neutral_indexes("src", "csrc", "t") == [
        ("composite_1", False, ["a", "b"], True),
        ("unique_1", True, ["sku"], True)]
    assert eng.neutral_indexes("dst", "csrc", "t") == []
    eng.execute_ddl("dst", "csrc", eng.neutral_create_index_sql(
        "dst", "csrc", "t", "composite_1", False, ["a", "b"]))
    assert eng.neutral_indexes("dst", "csrc", "t") == [
        ("composite_1", False, ["a", "b"], True)]
    assert _deep(eng, "indexes").status == "ok"
    with pytest.raises(RuntimeError, match="only when it is made"):
        eng.execute_ddl("dst", "csrc", eng.neutral_create_index_sql(
            "dst", "csrc", "t", "unique_1", True, ["sku"]))


def test_a_large_item_is_read_whole_and_the_batches_are_cut_by_bytes(
        account, tmp_path, monkeypatch):
    """An item is 2 MB at most; a thousand of them in one batch would be
    two gigabytes, so a batch ends at the bytes it holds."""
    from migkit import pieces
    s, _ = _pair(account)
    for i in range(6):
        cf.put(s, f"o{i}", region="eu", v="x" * 400)
    monkeypatch.setattr(pieces, "BATCH_BYTES", 1000)
    eng = cf.engine(tmp_path)
    cols = eng.neutral_columns("src", "csrc", "t")
    sizes = [len(b) for b in eng.neutral_batches("src", "csrc", "t", cols,
                                                 size=1000)]
    assert sum(sizes) == 6 and max(sizes) < 6 and len(sizes) > 1, sizes
