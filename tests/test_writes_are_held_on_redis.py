"""A Redis source's writes are held at cutover on a server of the test
lanes: a second session's write is refused while the accounts are held
(`redis_accounts`) and waits while the server is paused (`redis_pause`),
migkit's own account is refused too (nothing migkit does to a source at
cutover is a write), both are given back by `thaw_source`, a hold whose
process was killed outright is found and lifted by `leftovers`, and
`doctor` names the rung."""
import pytest

from migkit import freeze
from tests import holdkit, servers, waits
from tests.conftest import needs_docker

PROVES = [("cutover.freeze", "redis"), ("cutover.read-only-target", "redis")]

pytestmark = needs_docker

pair = servers.fixture("redis")


def _setup(pair, tmp_path):
    from migkit.engines.redis import RedisEngine
    root = pair.src.client()
    root.flushall()
    root.execute_command("ACL", "SETUSER", "app", "reset", "on",
                         ">CHANGE_ME-app", "~*", "&*", "+@all")
    hop = holdkit.hop_of("redis", pair.src, pair.dst, ["0"], tmp_path)
    return hop, RedisEngine(hop), root


def _app(pair, **kw):
    import redis
    return redis.Redis(host=pair.src.host, port=pair.src.port,
                       username="app", password="CHANGE_ME-app",
                       decode_responses=True, **kw)


def _set(client, key):
    import redis
    for _ in range(3):
        try:
            return client.set(key, "1")
        except redis.exceptions.ConnectionError:
            continue
    return client.set(key, "1")


def test_a_second_session_is_refused_while_the_accounts_are_held(pair,
                                                                 tmp_path):
    import redis
    hop, eng, root = _setup(pair, tmp_path)
    try:
        app = _app(pair)
        app.set("before", "1")
        assert freeze.choose(hop, eng, ["0"], "database").rung.name == \
            "redis_accounts"
        freeze.freeze_source(hop, eng, ["0"], lambda m: None,
                             "redis_accounts")
        with pytest.raises(redis.exceptions.NoPermissionError):
            _set(app, "during")
        with pytest.raises(redis.exceptions.NoPermissionError):
            _set(root, "during")
        assert root.get("before") == "1", "reads go on"
        freeze.thaw_source(hop, eng, ["0"], lambda m: None)
        _set(_app(pair), "after")
        assert root.get("during") is None and root.get("after") == "1"
        assert freeze.server_state(hop) is None
    finally:
        root.execute_command("ACL", "DELUSER", "app")


def test_a_second_session_waits_while_the_server_is_paused(pair, tmp_path):
    import redis
    hop, eng, root = _setup(pair, tmp_path)
    try:
        assert freeze.choose(hop, eng, ["0"]).rung.name == "redis_pause"
        freeze.freeze_source(hop, eng, ["0"], lambda m: None, "redis_pause")
        waiting = _app(pair, socket_timeout=1, retry_on_timeout=False)
        with pytest.raises(redis.exceptions.TimeoutError):
            waiting.set("gave-up", "1")
        waiting.close()
        freeze.thaw_source(hop, eng, ["0"], lambda m: None)
        _app(pair).set("after", "1")
        assert root.get("gave-up") is None
    finally:
        root.execute_command("CLIENT", "UNPAUSE")
        root.execute_command("ACL", "DELUSER", "app")


def test_a_hold_whose_process_was_killed_is_found_and_lifted(pair, tmp_path):
    import redis
    hop, eng, root = _setup(pair, tmp_path)
    try:
        def refuses():
            with pytest.raises(redis.exceptions.NoPermissionError):
                _set(_app(pair), "during")

        def accepts():
            waits.until(lambda: _set(_app(pair), "after"), 30,
                        "the application's write is taken again")
        holdkit.lifted_after_a_crash(hop, eng, "redis_accounts", ["0"],
                                     accepts, refuses)
    finally:
        root.execute_command("ACL", "DELUSER", "app")
