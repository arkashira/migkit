"""The clone plugin, asked of a real MySQL 8.4 and put on and taken off
there (the owner, 2026-09-30: a change on the source is found possible on
the server, never guessed).

Two servers: one as it ships, one whose plugin directory holds no clone
library (`--plugin-dir=/tmp`), so the server itself refuses the plugin.
Held: the probe says possible for a user who may put a plugin on and a
DBA's for one who may not; migkit puts it on for the length of the work
and takes it off after it and after a failure, the plugin read back from
the server each time; a server's refusal is recorded in its own words and
is the answer from then on, with nothing left behind; a change a killed
run left is taken off first.
"""
import socket
import subprocess
import time

import pytest

ON, OFF = "migkit-test-offer-my", "migkit-test-offer-my-nolib"
ON_PORT, OFF_PORT = 16300, 16301


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _ready(name, port, most=180):
    end = time.time() + most
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                             "select 1"], capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


def _rm():
    subprocess.run(["docker", "rm", "-f", "-v", ON, OFF],
                   capture_output=True)


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    _rm()
    for name, port, extra in ((ON, ON_PORT, []),
                              (OFF, OFF_PORT, ["--plugin-dir=/tmp"])):
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:3306", "mysql:8.4"] + extra,
                       check=True, capture_output=True)
    try:
        _ready(ON, ON_PORT)
        _ready(OFF, OFF_PORT)
        my(ON, "create user 'reader'@'%' identified by 'test';"
               " grant select on *.* to 'reader'@'%';")
        yield
    finally:
        _rm()


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _engine(port, user="root"):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name=f"offer-{port}-{user}", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=port, user=user,
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user=user,
                              password="test"),
              options={"physical": {"source": f"127.0.0.1:{port}"}})
    return hop, MySQLEngine(hop)


def _facts(hop, eng):
    from migkit import physical
    return physical.MyFacts(hop, eng, "full")


def _plugin(name):
    return my(name, "select plugin_status from information_schema.plugins"
                    " where plugin_name = 'clone'")


def test_possible_for_a_user_who_may_and_a_dbas_for_one_who_may_not(
        servers):
    from migkit import decide, physical
    hop, eng = _engine(ON_PORT)
    got = physical.CLONE_PLUGIN.stand(_facts(hop, eng))
    assert got.state == decide.POSSIBLE, got
    hop, eng = _engine(ON_PORT, "reader")
    got = physical.CLONE_PLUGIN.stand(_facts(hop, eng))
    assert got.state == decide.BY_DBA, got
    assert "INSERT and DELETE on mysql.plugin" in got.why, got


def test_put_on_for_the_work_and_taken_off_after_and_on_failure(servers):
    from migkit import physical, sourcechange
    hop, eng = _engine(ON_PORT)
    facts = _facts(hop, eng)
    assert _plugin(ON) == ""
    with sourcechange.making(hop, physical.CLONE_PLUGIN, facts,
                             lambda s: eng._q("src", s), "", print,
                             version=facts["src"]["version"]):
        assert _plugin(ON) == "ACTIVE"
    assert _plugin(ON) == ""
    with pytest.raises(SystemExit, match="the work failed"):
        with sourcechange.making(hop, physical.CLONE_PLUGIN, facts,
                                 lambda s: eng._q("src", s), "", print):
            assert _plugin(ON) == "ACTIVE"
            raise SystemExit("the work failed")
    assert _plugin(ON) == ""
    states = [e["state"] for e in sourcechange._record(hop)["made"]]
    assert states == ["undone", "undone"], states


def test_the_servers_refusal_is_recorded_and_is_the_answer(servers):
    from migkit import decide, physical, sourcechange
    hop, eng = _engine(OFF_PORT)
    facts = _facts(hop, eng)
    # nothing the server lets a client read says the library is missing:
    # possible, until the server is asked to load it
    assert physical.CLONE_PLUGIN.stand(facts).state == decide.POSSIBLE
    with pytest.raises(sourcechange.Refused) as e:
        with sourcechange.making(hop, physical.CLONE_PLUGIN, facts,
                                 lambda s: eng._q("src", s), "", print,
                                 version=facts["src"]["version"]):
            pytest.fail("the work ran over a refused change")
    assert "mysql_clone.so" in str(e.value), e.value
    assert _plugin(OFF) == ""
    again = physical.CLONE_PLUGIN.stand(_facts(hop, eng))
    assert again.state == decide.IMPOSSIBLE, again
    assert "the server refused it" in again.why and "mysql_clone.so" in \
        again.why, again


def test_a_change_a_killed_run_left_is_taken_off_first(servers):
    from migkit import physical, sourcechange
    hop, eng = _engine(ON_PORT)
    my(ON, "INSTALL PLUGIN clone SONAME 'mysql_clone.so';")
    sourcechange._save(hop, {"made": [{
        "id": "clone_plugin-1", "change": "clone_plugin",
        "said": "the clone plugin", "state": "made",
        "make": [sourcechange.CLONE_MAKE], "undo": [sourcechange.CLONE_UNDO],
        "source": f"127.0.0.1:{ON_PORT}", "db": "", "at": "earlier"}]})
    assert _plugin(ON) == "ACTIVE"
    assert sourcechange.undo_left(hop, lambda s: eng._q("src", s), print)
    assert _plugin(ON) == ""
    # and the probe reads it as absent again: offered, not assumed there
    from migkit import decide
    assert physical.CLONE_PLUGIN.stand(_facts(hop, eng)).state == \
        decide.POSSIBLE
