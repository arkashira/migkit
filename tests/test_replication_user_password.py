"""The replication user migkit makes has a password nobody else knows.

The MySQL plan creates `migkit_repl@'%'` on the source - a user any host
may sign in as. Shown as a plan for a person to run, its password is the
placeholder `CHANGE_ME` for them to replace. Run by `migkit move --mode cdc
--go`, it used to run the placeholder too: a replication login on the
source whose password is printed in a public repository.
"""
import types

import pytest

from migkit.config import Endpoint, Hop


@pytest.fixture
def run(monkeypatch):
    from migkit import cli
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=3306, user="admin",
                              password="CHANGE_ME_src"),
              target=Endpoint(host="10.0.0.2", port=3306, user="admin",
                              password="CHANGE_ME_dst"),
              databases=["shop"])
    eng = MySQLEngine(hop)
    monkeypatch.setattr(eng, "_brands",
                        lambda: [types.SimpleNamespace(name="mysql")])
    monkeypatch.setattr(eng, "_gtid_state", lambda brand: (True, "gtid ON"))
    # no server here: the binlog position comes back as nothing
    monkeypatch.setattr(eng, "_q", lambda side, sql, *a, **k: [])
    monkeypatch.setattr(eng, "replication_status", lambda db, sql: "ok")
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)

    def go(apply):
        ran, shown = [], []
        monkeypatch.setattr(eng, "apply_replication_stmt",
                            lambda side, db, stmt: ran.append((side, stmt)))
        monkeypatch.setattr(cli.console, "print",
                            lambda *a, **k: shown.append(" ".join(map(str, a))))
        cli._replicate(hop, eng, "shop", False, False, apply)
        return ran, "\n".join(shown)
    return go


def _password(statements):
    import re
    found = set()
    for _, stmt in statements:
        found |= set(re.findall(r"identified by '([^']*)'", stmt))
        found |= set(re.findall(r"SOURCE_PASSWORD = '([^']*)'", stmt))
    return found


def test_a_plan_that_is_only_shown_keeps_the_placeholder(run):
    ran, shown = run(False)
    assert ran == []
    assert "identified by 'CHANGE_ME'" in shown, shown


def test_a_plan_migkit_runs_uses_one_fresh_password_everywhere(run):
    ran, shown = run(True)
    used = _password(ran)
    assert len(used) == 1, used
    secret = used.pop()
    assert "CHANGE_ME" not in secret and len(secret) >= 24, secret
    # the user is made with it, reset to it if it was there, and the
    # replica signs in with it
    assert any(s.startswith("create user") for _, s in ran)
    assert any(s.startswith("alter user") for _, s in ran)
    assert any("SOURCE_PASSWORD" in s for side, s in ran if side == "dst")
    assert secret not in shown, shown


def test_each_run_gets_its_own(run):
    first, _ = run(True)
    second, _ = run(True)
    assert _password(first) != _password(second)
