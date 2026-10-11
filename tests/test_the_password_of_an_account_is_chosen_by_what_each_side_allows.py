"""How an account's password reaches the target is one choice, made through
the decision engine for MySQL, PostgreSQL and Oracle alike
(P0-DECISION-ALL.coverage, the `users` task), without a server.

* the source's own hash is the first way - the account keeps the very
  password it had - where it can be read and the target takes it;
* the passwords file's is the next, and it is said to be given;
* an account with neither is not made, and the reason names why each way
  was passed over;
* the plan each engine builds is the one it built before the ladder: the
  same statements, the same accounts skipped.
"""
import pytest

from migkit import decide, userways

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = []


def _ask(**kw):
    base = dict(hash_ok=True, hash_why="no hash", secret_ok=True,
                secret_why="no secret")
    base.update(kw)
    return userways.password("app", **base)


def test_the_sources_hash_is_the_first_way_even_where_a_password_is_given():
    got = _ask()
    assert got.path == userways.HASH
    assert "very password it had" in got.reason


def test_the_passwords_file_is_the_next_way_and_the_reason_says_why():
    got = _ask(hash_ok=False, hash_why="the target has no such plugin")
    assert got.path == userways.FILE
    assert "the target has no such plugin" in got.reason


def test_an_account_with_neither_way_is_not_made_and_both_are_said():
    got = _ask(hash_ok=False, hash_why="no hash is readable",
               secret_ok=False, secret_why="the file has no entry")
    assert got.path is None
    assert got.reason == "no hash is readable; the file has no entry"
    assert [r.name for r, _ in got.passed] == [userways.HASH, userways.FILE]


def test_the_choice_is_told_to_the_decision_record():
    told = []
    decide.LISTENERS.append(told.append)
    try:
        _ask(hash_ok=False)
    finally:
        decide.LISTENERS.remove(told.append)
    assert [c.work for c in told] == ["app: its password"]
    assert told[0].path == userways.FILE


def test_the_users_task_is_routed_through_the_engine():
    assert decide.TASKS["users"][1] == ("routed", "userways")
    assert "users" not in [t for t, _ in decide.task_gaps()]


# ---- MySQL -----------------------------------------------------------------

def _my_account(auth=b"$A$005$salt", plugin="caching_sha2_password"):
    return {"auth": auth, "plugin": plugin, "role": False, "maria": False,
            "attrs": {"password_expired": "N", "account_locked": "N",
                      "password_lifetime": None, "max_user_connections": "0"},
            "ssl": {"ssl_cipher": "", "x509_issuer": "", "x509_subject": ""},
            "default_roles": [], "grants": []}


def _my(plugins, passwords, plugin="mysql_native_password"):
    from migkit import users
    s = {("app", "%"): _my_account(plugin=plugin)}
    out = {"missing_on_target": ["app@%"], "grants_missing": [],
           "attributes_differ": []}
    return users._my_plan(out, s, {}, plugins=plugins, passwords=passwords)


def test_mysql_carries_the_hash_where_the_target_signs_in_with_its_plugin():
    steps, skipped = _my({"mysql_native_password"}, {"app": "from-file"})
    assert skipped == []
    assert "IDENTIFIED WITH mysql_native_password AS 0x" in steps[0]["sql"]
    assert "secret" not in steps[0]


def test_mysql_carries_the_hash_where_the_targets_plugins_are_not_known():
    steps, skipped = _my(None, {})
    assert skipped == [] and "AS 0x" in steps[0]["sql"]


def test_mysql_gives_the_file_password_where_the_target_lacks_the_plugin():
    steps, skipped = _my({"caching_sha2_password"}, {"app@%": "from-file"})
    assert skipped == []
    (st,) = steps
    assert st["sql"].startswith("CREATE USER %s@%s IDENTIFIED BY %s")
    assert st["secret"] == "from-file" and st["given"] is True
    assert "from-file" not in st["shown"]
    assert "the target has no mysql_native_password" in st["shown"]


def test_mysql_skips_an_account_neither_way_can_make_and_says_why():
    steps, skipped = _my({"caching_sha2_password"}, {})
    assert steps == []
    ((name, why),) = skipped
    assert name == "app@%"
    assert "does not sign in with mysql_native_password" in why
    assert "the passwords file has none for it" in why


def test_mysql_never_makes_an_account_with_no_password_that_may_sign_in():
    from migkit import users
    s = {("open", "%"): _my_account(auth=b"")}
    out = {"missing_on_target": ["open@%"], "grants_missing": [],
           "attributes_differ": []}
    steps, skipped = users._my_plan(out, s, {})
    assert steps == []
    assert "may sign in" in skipped[0][1]


# ---- PostgreSQL -------------------------------------------------------------

def _pg(hashes, passwords, login=True):
    from migkit import users
    role = {"login": login, "superuser": False, "createdb": False,
            "createrole": False, "inherit": True, "replication": False,
            "bypassrls": False, "connlimit": -1, "valid_until": None,
            "member_of": []}
    out = {"missing_on_target": ["app"], "grants_missing": [],
           "attributes_differ": []}
    return users._pg_plan(out, {"app": role}, {}, hashes, passwords)


def test_postgresql_prefers_the_hash_then_the_file_then_says_why():
    steps, skipped = _pg({"app": "SCRAM-SHA-256$x"}, {"app": "file"})
    assert skipped == [] and steps[0]["secret"] == "SCRAM-SHA-256$x"
    assert steps[0]["given"] is False
    steps, skipped = _pg({}, {"app": "file"})
    assert skipped == [] and steps[0]["secret"] == "file"
    assert steps[0]["given"] is True
    steps, skipped = _pg({}, {})
    assert steps == []
    ((name, why),) = skipped
    assert name == "app"
    assert "no hash is readable on the source" in why
    assert "the passwords file has no entry for it" in why


def test_postgresql_makes_a_role_that_cannot_sign_in_without_a_password():
    steps, skipped = _pg({}, {}, login=False)
    assert skipped == []
    assert "no password: nologin role" in steps[0]["shown"]
    assert "secret" not in steps[0]


# ---- Oracle -----------------------------------------------------------------

def _oracle(monkeypatch, passwords, hash_=""):
    from migkit import users
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="t", engine="oracle", source=ep, target=ep,
              databases=["ORCL"])
    v = {"name": "APP", "hash": hash_, "tablespace": "USERS",
         "profile": "DEFAULT", "locked": False, "roles": [],
         "privileges": [], "grants": [], "quotas": []}
    out = {"missing_on_target": ["user:APP"], "grants_missing": []}
    monkeypatch.setattr(users, "_compare",
                        lambda hop, say: (out, {"user:APP": v}, {}))
    monkeypatch.setattr(users, "_ora_rows", lambda *a, **k: [])
    said = []
    users._ora_create(hop, False, passwords, said.append)
    return " | ".join(said)


def test_oracle_carries_the_hash_when_it_can_be_read(monkeypatch):
    said = _oracle(monkeypatch, {"APP": "file"}, hash_="S:ABCDEF0123;T:0f")
    assert "IDENTIFIED BY <hash or password>" in said
    assert "skipped" not in said and "file" not in said


def test_oracle_gives_the_file_password_when_the_hash_is_not_readable(
        monkeypatch):
    said = _oracle(monkeypatch, {"APP": "file"})
    assert "IDENTIFIED BY <hash or password>" in said
    assert "skipped" not in said and "file" not in said.replace(
        "passwords file", "")


@pytest.mark.parametrize("passwords", [{}, {"APP": 'has"quote'}])
def test_oracle_skips_an_account_neither_way_can_make_and_says_why(
        monkeypatch, passwords):
    said = _oracle(monkeypatch, passwords)
    assert "skipped user:APP" in said
    assert "its password hash is not readable here" in said
    assert "the passwords file has none for it that Oracle takes" in said
    assert "has\"quote" not in said
