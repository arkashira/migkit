"""Two deep checks on Redis (P0-PARITY.verify): a target whose streams have
handed out ids the source's have not is ahead of it, and an account that
reaches other key patterns on the target than on the source is a grant that
differs. Run against a real server, started here."""
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.movement.target-ahead", "redis"),
          ("verify.access.table-grants", "redis")]

pytestmark = needs_server


def _deep(eng, word):
    return [r for r in eng.check_deep("0") if r.scope.endswith(word)][0]


def test_a_stream_the_target_has_written_to_is_ahead(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    src.xadd("events", {"a": 1}, id="1000-0")
    dst.xadd("events", {"a": 1}, id="1000-0")
    eng = engine(clean, tmp_path)
    assert _deep(eng, "boundary").status == "ok"
    dst.xadd("events", {"a": 2}, id="2000-0")
    got = _deep(eng, "boundary")
    assert got.status == "diff" and "events" in got.detail, got
    assert got.category == "movement.target-ahead"


def test_a_target_behind_is_lag_not_a_finding(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    src.xadd("events", {"a": 1}, id="1000-0")
    src.xadd("events", {"a": 2}, id="1000-1")
    dst.xadd("events", {"a": 1}, id="1000-0")
    got = _deep(engine(clean, tmp_path), "boundary")
    assert got.status == "ok" and "1 behind" in got.detail, got


def test_an_id_deleted_on_the_target_still_counts(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    src.xadd("events", {"a": 1}, id="1000-0")
    dst.xadd("events", {"a": 1}, id="1000-0")
    dst.xadd("events", {"a": 2}, id="3000-0")
    dst.xdel("events", "3000-0")
    assert _deep(engine(clean, tmp_path), "boundary").status == "diff"


def _account(server, keys):
    server.client().acl_setuser("app", enabled=True, passwords=["+pw"],
                                keys=keys, commands=["+get"])


def test_an_account_that_reaches_other_keys_is_a_grant_that_differs(
        clean, tmp_path):
    _account(clean[0], ["cache:*"])
    _account(clean[1], ["*"])
    got = _deep(engine(clean, tmp_path), "grants")
    assert got.status == "diff" and "app" in got.detail, got
    assert got.category == "access.table-grants"


def test_accounts_that_reach_the_same_keys_are_the_same(clean, tmp_path):
    _account(clean[0], ["cache:*", "order:*"])
    _account(clean[1], ["order:*", "cache:*"])
    got = _deep(engine(clean, tmp_path), "grants")
    assert got.status == "ok" and "1 accounts" in got.detail, got
