"""Users and their grants on Redis: the same comparison every engine gets.

`migkit users HOP test` refused a Redis hop outright. Redis has users -
`ACL SETUSER`, with key patterns, channels and command permissions - and
`ACL LIST` shows every password as its SHA-256, so the comparison can say
a password differs without anyone reading one.
"""
import pathlib
import socket
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-racl-src", "migkit-test-racl-dst"
SRC_PORT, DST_PORT = 15665, 15666


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def pair():
    import redis
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:6379", "redis:7"], check=True,
                       capture_output=True)
    try:
        assert _wait(SRC_PORT) and _wait(DST_PORT)
        yield (redis.Redis(port=SRC_PORT, decode_responses=True),
               redis.Redis(port=DST_PORT, decode_responses=True))
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def seeded(pair, tmp_path):
    s, t = pair
    for c in (s, t):
        for user in c.acl_users():
            if user != "default":
                c.acl_deluser(user)
    s.execute_command("ACL", "SETUSER", "app", "on", ">CHANGE_ME_app",
                      "~orders:*", "+get", "+set")
    s.execute_command("ACL", "SETUSER", "report", "on", ">CHANGE_ME_rep",
                      "~*", "+@read")
    s.execute_command("ACL", "SETUSER", "ops", "on", ">CHANGE_ME_ops",
                      "~*", "+@all")
    # the target: app identical, report with another password, ops missing
    t.execute_command("ACL", "SETUSER", "app", "on", ">CHANGE_ME_app",
                      "~orders:*", "+get", "+set")
    t.execute_command("ACL", "SETUSER", "report", "on", ">CHANGE_ME_other",
                      "~*", "+@read")
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""), databases=["0"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_missing_users_and_different_passwords_are_named(seeded):
    from migkit import users
    out, _ = users.compare(seeded, say=lambda m: None)
    assert out["missing_on_target"] == ["ops"], out
    assert out["password_differs"] == ["report"], out
    assert out["result"] == "gap"


def test_a_difference_in_permissions_is_its_own_finding(pair, seeded):
    from migkit import users
    s, t = pair
    t.execute_command("ACL", "SETUSER", "ops", "on", ">CHANGE_ME_ops",
                      "~*", "+@read")
    t.execute_command("ACL", "SETUSER", "report", "on", "resetpass",
                      ">CHANGE_ME_rep")
    out, _ = users.compare(seeded, say=lambda m: None)
    assert out["missing_on_target"] == [], out
    assert out["rules_differ"] == ["ops"], out
    assert out["password_differs"] == [], out


def test_no_password_reaches_the_report(seeded):
    from migkit import users
    said = []
    out, _ = users.compare(seeded, say=said.append)
    text = " ".join(said) + str(out)
    assert "CHANGE_ME" not in text, text


def test_identical_users_pass(pair, seeded):
    from migkit import users
    s, t = pair
    t.execute_command("ACL", "SETUSER", "report", "on", "resetpass",
                      ">CHANGE_ME_rep")
    t.execute_command("ACL", "SETUSER", "ops", "on", ">CHANGE_ME_ops",
                      "~*", "+@all")
    out, _ = users.compare(seeded, say=lambda m: None)
    assert out["result"] == "pass", out


def test_create_carries_the_missing_user_with_its_password(pair, seeded):
    """The user lands exactly as the source has it, and its password works
    on the target, though nobody read it."""
    import redis

    from migkit import users
    users.create(seeded, apply=True, say=lambda m: None)
    s, t = pair
    assert "ops" in t.acl_users()
    logged_in = redis.Redis(port=DST_PORT, username="ops",
                            password="CHANGE_ME_ops", decode_responses=True)
    assert logged_in.ping()
    # the one that already existed with another password is reported, not
    # overwritten
    out, _ = users.compare(seeded, say=lambda m: None)
    assert out["password_differs"] == ["report"], out


def test_rollback_removes_only_what_create_made(pair, seeded):
    from migkit import users
    users.create(seeded, apply=True, say=lambda m: None)
    users.rollback(seeded, apply=True, say=lambda m: None)
    s, t = pair
    assert "ops" not in t.acl_users()
    assert "app" in t.acl_users() and "report" in t.acl_users()


def test_a_dry_run_changes_nothing_and_shows_no_hash(pair, seeded):
    from migkit import users
    said = []
    users.create(seeded, apply=False, say=said.append)
    s, t = pair
    assert "ops" not in t.acl_users()
    assert not [m for m in said if "#" in m and "ops" in m], said
