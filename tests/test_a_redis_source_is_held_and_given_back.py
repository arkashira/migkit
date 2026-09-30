"""A Redis source held at cutover, read back, and given back (`freeze.RedisHold`).

Per account: every account's write commands taken away (`-@write`), its
rules recorded whole and put back whole - the application's connection
from before ended, its next write refused, and the server asked whether
each account could still write (`ACL DRYRUN`). The whole server: every
client's writes held (`CLIENT PAUSE WRITE`) - a write sent while it is on
waits, and one whose client gave up is never applied - and let go.
"""
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

NAME, PORT, PW = "migkit-test-hold-redis", 16350, "CHANGE_ME-src"


def _up(port):
    end = time.time() + 60
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.5)
    return False


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:6379", "redis:7.4", "redis-server",
                    "--requirepass", PW], check=True, capture_output=True)
    try:
        assert _up(PORT), "redis never answered"
        time.sleep(1)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _r(user=None, password=PW, **kw):
    import redis
    return redis.Redis(port=PORT, username=user, password=password,
                       decode_responses=True, **kw)


def _write(client, key):
    """A write through a client whose connection the hold may have
    ended: the client opens a new one, as an application does."""
    import redis
    for _ in range(3):
        try:
            return client.set(key, "1")
        except redis.exceptions.ConnectionError:
            continue
    return client.set(key, "1")


@pytest.fixture
def app(server, tmp_path):
    root = _r()
    root.flushall()
    root.execute_command("ACL", "SETUSER", "app", "reset", "on",
                         ">CHANGE_ME-app", "~*", "&*", "+@all")
    hop = Hop(name="rh", engine="redis",
              source=Endpoint(host="127.0.0.1", port=PORT, user="",
                              password=PW),
              target=Endpoint(host="127.0.0.1", port=PORT, user="",
                              password=PW),
              databases=["0"], options={"cutover": True})
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    yield hop, _r("app", "CHANGE_ME-app")
    try:
        root.execute_command("CLIENT", "UNPAUSE")
    except Exception:  # noqa: BLE001 - before 6.2, or not paused
        pass
    root.execute_command("ACL", "DELUSER", "app")


def test_every_account_is_refused_read_back_and_given_its_rules_back(app):
    import redis

    from migkit.engines.redis import RedisEngine
    hop, client = app
    client.set("before", "1")
    eng = RedisEngine(hop)
    got = freeze.choose(hop, eng, ["0"], "database")
    assert got.rung.name == "redis_accounts", got
    before = _r().execute_command("ACL", "LIST")
    said = []
    freeze.freeze_source(hop, eng, ["0"], said.append, "redis_accounts")
    # the application's connection from before is gone, and its next
    # write is refused - so is migkit's own account's: nothing migkit
    # does to a source at cutover is a write
    with pytest.raises(redis.exceptions.NoPermissionError):
        _write(client, "during")
    with pytest.raises(redis.exceptions.NoPermissionError):
        _write(_r(), "during")
    held = freeze.proof(hop, eng, "0", "redis_accounts")
    assert all(ok for ok, _ in held), held
    # a way that did not hold is said: the account made able to write
    # again behind the hold's back reads back as still writing
    _r().execute_command("ACL", "SETUSER", "app", "+set")
    held = freeze.proof(hop, eng, "0", "redis_accounts")
    assert (False, "still able to write: app") in held, held
    freeze.thaw_source(hop, eng, ["0"], said.append)
    assert _r().execute_command("ACL", "LIST") == before
    _r("app", "CHANGE_ME-app").set("after", "1")
    assert _r().get("during") is None and _r().get("after") == "1"
    assert freeze.server_state(hop) is None


def test_the_whole_server_holds_writes_and_lets_them_go(app):
    import redis

    from migkit.engines.redis import RedisEngine
    hop, client = app
    client.set("before", "1")
    eng = RedisEngine(hop)
    got = freeze.choose(hop, eng, ["0"])
    assert got.rung.name == "redis_pause", got
    said = []
    freeze.freeze_source(hop, eng, ["0"], said.append, "redis_pause")
    # held, not refused: a write waits, and one whose client gave up
    # waiting is never applied
    waiting = _r("app", "CHANGE_ME-app", socket_timeout=1,
                 retry_on_timeout=False)
    with pytest.raises(redis.exceptions.TimeoutError):
        waiting.set("gave-up", "1")
    waiting.close()
    assert _r().get("before") == "1", "reads go on"
    held = freeze.proof(hop, eng, "0", "redis_pause")
    assert all(ok for ok, _ in held), held
    freeze.thaw_source(hop, eng, ["0"], said.append)
    _r("app", "CHANGE_ME-app").set("after", "1")
    assert _r().get("after") == "1"
    assert _r().get("gave-up") is None, "a write its client gave up on" \
                                        " landed after the hold"
    assert freeze.server_state(hop) is None
