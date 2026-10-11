"""What `assess` tells about a Redis move before it starts: the pre-flight
checks that predict what the move will hit, the room the servers have, and
the work the move leaves behind (P0-PARITY.all). Run against a real
server, started here."""
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.preflight", "redis"), ("assess.capacity", "redis"),
          ("assess.handwork", "redis")]

pytestmark = needs_server


def test_a_database_the_target_lacks_is_found_before_the_copy(
        clean, tmp_path):
    eng = engine(clean, tmp_path, databases=["0", "20"])
    assert eng._database_on_target("0").status == "ok"
    got = eng._database_on_target("20")
    assert got.status == "diff" and "16 databases" in got.detail, got


def test_the_preflight_rows_fail_what_the_move_would_hit(clean, tmp_path):
    eng = engine(clean, tmp_path, databases=["0", "20"])
    rows = {r["item"]: r["level"] for r in eng._preflight_items()
            if "database" in r["item"]}
    assert rows == {"db0 target database": "pass",
                    "db20 target database": "fail"}, rows


def test_keys_the_copy_would_remove_are_said(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    assert engine(clean, tmp_path)._keys_the_copy_removes("0").status == "ok"
    dst.set("old", "from an earlier attempt")
    dst.set("cache:mine", "x")
    assert engine(clean, tmp_path)._keys_the_copy_removes("0").status \
        == "warn"
    # what the hop leaves out is the target's own, and not removed
    left = engine(clean, tmp_path, exclude=["old", "cache:*"])
    assert left._keys_the_copy_removes("0").status == "ok"


def test_the_room_the_servers_have(clean, tmp_path):
    eng = engine(clean, tmp_path)
    got = eng.capacity("dst", "0")
    assert got["free_connections"] > 0 and got["replica"] is False, got
    assert eng.free_bytes("dst", "0") is None        # no limit set
    clean[1].client().config_set("maxmemory", 100 * 2 ** 20)
    free = eng.free_bytes("dst", "0")
    assert 0 < free < 100 * 2 ** 20, free

def test_what_the_move_leaves_behind_is_counted_and_named(clean, tmp_path):
    import redis
    src = clean[0].client()
    src.set("k", "v")
    redis.Redis(port=clean[0].port, db=3).set("elsewhere", "v")
    src.acl_setuser("app", enabled=True, passwords=["+pw"],
                    keys=["cache:*"], commands=["+get"])
    src.function_load("#!lua name=mylib\n"
                      "redis.register_function('f', function() return 1"
                      " end)")
    rows = engine(clean, tmp_path)._assess_extra()
    said = " | ".join(r["detail"] for r in rows)
    assert "db3 (1 keys)" in said, said
    assert "app" in said, said
    assert "mylib" in said, said
    assert any("need hands" in r["item"] for r in rows), rows
    # a hop that names the database leaves nothing behind in it
    both = engine(clean, tmp_path, databases=["0", "3"])
    assert "db3" not in " | ".join(r["detail"] for r in both._assess_extra())
