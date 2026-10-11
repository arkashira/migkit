"""On Redis the scope is key patterns and databases, and one answer
(`RedisEngine.in_scope`) serves every path: the counts, the data and the
schema checks, the deep checks, and the copy's listing and its keys
(P0-PARITY.scope). Run against a real server, started here."""
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "redis"), ("scope.deep", "redis"),
          ("scope.copy", "redis")]

pytestmark = needs_server


class _Saved(dict):
    def save(self):
        pass


def _seed(pair):
    src, dst = (p.client() for p in pair)
    for i in range(5):
        src.set(f"order:{i}", i)
        dst.set(f"order:{i}", i)
    # a cache the target's application fills for itself, and one thing the
    # source has under that pattern that the target never will
    dst.set("cache:mine", "target-owned")
    src.set("cache:theirs", "x", ex=3600)
    src.xadd("cache:events", {"a": 1}, id="1000-0")
    return src, dst


def _said(results):
    return " | ".join(f"{r.scope} {r.status} {r.detail}" for r in results)


def test_counts_data_and_schema_read_only_what_the_hop_takes(
        clean, tmp_path):
    _seed(clean)
    whole = engine(clean, tmp_path, options={"deep": True})
    assert whole.check_counts("0")[0].status == "diff"
    assert any(r.status == "diff" for r in whole.check_data("0"))
    assert any("stream" in r.detail and r.status == "diff"
               for r in whole.check_schema("0"))
    left = engine(clean, tmp_path, exclude=["cache:*"],
                  options={"deep": True})
    counts = left.check_counts("0")
    assert counts[0].status == "ok" and "5" in counts[0].detail, counts
    data = left.check_data("0")
    assert [r.status for r in data] == ["ok"], _said(data)
    assert "cache:" not in _said(data)
    assert all(r.status == "ok" for r in left.check_schema("0")), \
        _said(left.check_schema("0"))


def test_the_deep_checks_read_only_what_the_hop_takes(clean, tmp_path):
    src, dst = _seed(clean)
    src.set("order:0", "ttl", ex=5000)
    dst.set("order:0", "ttl", ex=5000)
    dst.set("cache:theirs", "x")        # lost its ttl on the target
    whole = engine(clean, tmp_path)
    assert any("lost their ttl" in r.detail for r in whole.check_deep("0"))
    left = engine(clean, tmp_path, exclude=["cache:*"])
    got = left.check_deep("0")
    assert "cache:" not in _said(got), _said(got)
    assert all(r.status in ("ok", "warn") for r in got), _said(got)


def test_the_copy_lists_the_keyspaces_the_hop_takes(clean, tmp_path):
    assert engine(clean, tmp_path).list_move_tables("0") == [("", "0")]
    left = engine(clean, tmp_path, exclude=["5"])
    assert left.list_move_tables("5") == []
    assert left.list_move_tables("0") == [("", "0")]
    only = engine(clean, tmp_path, include=["order:*"])
    assert only.list_move_tables("0") == [("", "0")]


def test_the_copy_carries_only_the_keys_the_hop_takes(clean, tmp_path):
    src, dst = _seed(clean)
    dst.flushall()
    left = engine(clean, tmp_path, exclude=["cache:*"])
    left.move_table("0", "", "0", 1000, _Saved(), lambda m: None)
    assert sorted(dst.keys("*")) == [f"order:{i}" for i in range(5)]
