"""A Redis target made a replica of the source (REPLICAOF), as the stream
of a Redis hop and its fence.

Measured on Redis 7.4 before any of it was written:
* a replica's first sync empties the target - every database of it - and
  loads the source's snapshot: a key the target had of its own, and one in
  a database the source never used, were both gone once the link was up
* a 6.2 target of a 7.4 source cannot read the snapshot (`Can't handle RDB
  format version 12`): the link stays down for good, and the target had
  been emptied already
* REPLICAOF answers OK when the replica cannot sign in; the reason goes to
  the target's log only, and the source records it in ACL LOG

So the plan refuses a hop whose keys a replica would carry or erase
wrongly, signs the replica in as an account that can only replicate, and
reads the link back. The check's confirm pass waits on the replica's
offset: a change that has not arrived yet is not called a difference, and
a key only the target has still is.
"""
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST, OLD = ("migkit-test-rrepl-src", "migkit-test-rrepl-dst",
                 "migkit-test-rrepl-old")
SRC_PORT, DST_PORT, OLD_PORT = 15847, 15848, 15849
SRC_PW, DST_PW = "CHANGE_ME-src", "CHANGE_ME-dst"


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
def servers():
    # the host's network, so the target reaches the source at the address
    # migkit does, and the plan runs as written
    runs = ((SRC, "redis:7", SRC_PORT, SRC_PW),
            (DST, "redis:7", DST_PORT, DST_PW),
            (OLD, "redis:6.2", OLD_PORT, DST_PW))
    for name, image, port, pw in runs:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        "host", image, "redis-server", "--port", str(port),
                        "--requirepass", pw], check=True, capture_output=True)
    try:
        if not all(_up(port) for _, _, port, _ in runs):
            pytest.skip("this docker does not publish the host's network")
        yield
    finally:
        for name, *_ in runs:
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _r(port, pw, db=0):
    import redis
    return redis.Redis(port=port, password=pw, db=db, decode_responses=True)


def _hop(port=DST_PORT, **extra):
    return Hop(name="rr", engine="redis",
               source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                               password=SRC_PW),
               target=Endpoint(host="127.0.0.1", port=port, user="",
                               password=DST_PW), **extra)


@pytest.fixture
def fresh(servers, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for port, pw in ((SRC_PORT, SRC_PW), (DST_PORT, DST_PW),
                     (OLD_PORT, DST_PW)):
        c = _r(port, pw)
        c.execute_command("REPLICAOF", "NO", "ONE")
        c.config_set("replica-read-only", "yes")
        c.flushall()
    src = _r(SRC_PORT, SRC_PW)
    for i in range(500):
        src.set(f"k{i}", i)
    src.hset("h", mapping={"a": 1, "b": 2})
    _r(SRC_PORT, SRC_PW, 2).set("in2", "x")
    yield


def _run(monkeypatch, hop, copy_data=True, drop=False):
    from migkit import cli
    from migkit.engines.redis import RedisEngine
    shown = []
    monkeypatch.setattr(cli.console, "print",
                        lambda *a, **k: shown.append(" ".join(map(str, a))))
    eng = RedisEngine(hop)
    cli._replicate(hop, eng, None, copy_data, drop, True)
    return eng, "\n".join(shown)


def _refused(monkeypatch, hop):
    with pytest.raises(SystemExit) as e:
        _run(monkeypatch, hop)
    return str(e.value)


def test_the_target_follows_under_an_account_of_its_own(fresh, monkeypatch):
    dst = _r(DST_PORT, DST_PW)
    dst.set("stale", "from an earlier attempt")
    eng, shown = _run(monkeypatch, _hop())
    assert f"dst: REPLICAOF 127.0.0.1 {SRC_PORT}" in shown, shown
    assert ("src: ACL SETUSER migkit_repl reset on >****"
            " +psync +replconf +ping") in shown, shown
    assert "link up" in shown and "NOT replicating" not in shown, shown
    # the plan says what the first sync replaces
    assert "replaces what the target holds - 1 key in db0" in shown, shown
    # the target signs in as the replica's account, never as the source
    assert dst.config_get("masteruser")["masteruser"] == "migkit_repl"
    assert dst.config_get("masterauth")["masterauth"] not in ("", SRC_PW)
    assert dst.config_get("masterauth")["masterauth"] not in shown
    assert dst.get("stale") is None
    assert dst.dbsize() == 501 and _r(DST_PORT, DST_PW, 2).get("in2") == "x"

    src = _r(SRC_PORT, SRC_PW)
    for i in range(500, 3000):
        src.set(f"k{i}", "v" * 100)
    at = eng.src_lsn(None)
    assert eng.fence_wait(None, at, timeout=30) is True
    assert int(dst.info("replication")["master_repl_offset"]) >= \
        int(at.rsplit(":", 1)[1])
    assert dst.dbsize() == 3001

    # the replica's account may replicate and nothing else
    import redis
    secret = dst.config_get("masterauth")["masterauth"]
    as_replica = redis.Redis(port=SRC_PORT, username="migkit_repl",
                             password=secret)
    with pytest.raises(redis.exceptions.NoPermissionError):
        as_replica.get("k1")

    # a repair beside a replica would write into a read-only server
    assert eng.stream_writers("0") == [
        (f"the replica of 127.0.0.1:{SRC_PORT}", False)]

    # torn down: the target keeps every key and takes writes, the account
    # is gone from the source
    _, shown = _run(monkeypatch, _hop(), drop=True)
    assert dst.info("replication")["role"] == "master"
    assert dst.dbsize() == 3001 and dst.set("after", "1")
    assert dst.config_get("masterauth")["masterauth"] == ""
    assert "migkit_repl" not in src.execute_command("ACL", "USERS")


def _cut(dst):
    """The replica's link cut and kept down: it signs in again with a
    password the source does not know. Returns the one it had."""
    secret = dst.config_get("masterauth")["masterauth"]
    dst.config_set("masterauth", "wrong")
    dst.execute_command("CLIENT", "KILL", "TYPE", "master")
    return secret


def test_a_refused_sign_in_is_said(fresh, monkeypatch):
    eng, _ = _run(monkeypatch, _hop())
    dst = _r(DST_PORT, DST_PW)
    _cut(dst)
    said = eng.replication_status("0", "INFO replication")
    assert said.endswith("link down, NOT replicating: the source refused"
                         " the replica's sign-in"), said
    dst.execute_command("REPLICAOF", "NO", "ONE")


def test_a_hop_the_replica_would_carry_wrongly_is_refused(fresh,
                                                          monkeypatch):
    dst = _r(DST_PORT, DST_PW)
    # excluded keys: the first sync would empty them
    said = _refused(monkeypatch, _hop(exclude=["0.session:*"]))
    assert said.startswith("the hop excludes keys, and a Redis replica"
                           " carries every key of the source"), said
    # a database the hop does not name, on the source: carried
    said = _refused(monkeypatch, _hop(databases=["0"]))
    assert said.startswith("the source has keys in db2, which the hop does"
                           " not name"), said
    # and on the target: emptied
    _r(SRC_PORT, SRC_PW, 2).flushdb()
    _r(DST_PORT, DST_PW, 5).set("own", "kept")
    said = _refused(monkeypatch, _hop())
    assert said.startswith("the target has keys in db5, which the hop does"
                           " not name"), said
    assert "Nothing was set up" in said and "tail" not in said, said
    assert dst.info("replication")["role"] == "master"
    assert _r(DST_PORT, DST_PW, 5).get("own") == "kept"


def test_an_older_target_is_refused_before_it_is_emptied(fresh,
                                                         monkeypatch):
    old = _r(OLD_PORT, DST_PW)
    old.set("own", "kept")
    said = _refused(monkeypatch, _hop(port=OLD_PORT))
    assert said.startswith("the target runs 6.2"), said
    assert "an older server does not read" in said, said
    assert old.info("replication")["role"] == "master"
    assert old.get("own") == "kept"


def test_a_change_still_arriving_is_not_a_difference(fresh, monkeypatch):
    eng, _ = _run(monkeypatch, _hop())
    dst, src = _r(DST_PORT, DST_PW), _r(SRC_PORT, SRC_PW)
    # the link cut, and kept down, while the source changes
    secret = _cut(dst)
    src.set("k1", "changed")
    src.delete("k2")
    src.set("new", "1")
    assert dst.get("k1") == "1" and dst.get("new") is None
    # and back while the check waits on its fence
    back = threading.Timer(2, lambda: _r(DST_PORT, DST_PW).config_set(
        "masterauth", secret))
    back.start()
    try:
        got = eng.check_data("0")
    finally:
        back.join()
    assert [r.status for r in got] == ["ok"], got
    assert "the difference was still arriving (round 1: fence" in \
        got[0].detail, got[0].detail
    assert dst.get("k1") == "changed" and dst.get("new") == "1"

    # a key only the target has is still one once the replica has caught up
    dst.config_set("replica-read-only", "no")
    dst.set("stray", "the target's own")
    got = eng.check_data("0")
    extra = [r for r in got if r.status == "diff"]
    assert len(extra) == 1 and "1 of" in extra[0].detail \
        and "stray" in extra[0].detail, got
    dst.execute_command("REPLICAOF", "NO", "ONE")

