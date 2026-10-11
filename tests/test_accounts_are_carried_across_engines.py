"""Accounts carried across engines (backlog R13-HETERO): a pair reads each
side's accounts in the one shape (`Engine.accounts`, `users.across`) - an
engine's own reader, or PostgreSQL's, MySQL's, SQL Server's and Oracle's
read in it by `users` - compares them, makes what the target lacks in the
target engine's own statements, and takes it back.

Held here without a server: MySQL's and PostgreSQL's catalogues answered
from recorded result shapes. What is held: a role and a user made, the
user with the passwords file's password (no engine's stored password is
another's) and skipped without one; memberships; a table privilege on a
table the target renames granted on the target's name; a grant MySQL has
words for and PostgreSQL has not said, not carried; a table the hop
leaves out never compared; the plan run on --apply and every step taken
back by the rollback, last first; and an engine whose accounts are read
in no shape but its own said as not compared, never as the same.
"""
import json

import pytest

from migkit import users
from migkit.config import Endpoint, Hop

PROVES = [("verify.users", "hetero")]


class _Cur:
    def __init__(self, conn):
        self.conn, self.rows = conn, []

    def execute(self, sql, args=None):
        self.conn.said.append(sql)
        got = self.conn.answer(sql)
        if isinstance(got, Exception):
            raise got
        self.rows = list(got or [])

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


class _Conn:
    def __init__(self, answer, said):
        self.answer, self.said = answer, said

    def cursor(self):
        return _Cur(self)

    def close(self):
        pass


def _mysql_source(sql):
    """A MySQL source: an application user and a reporting role."""
    if "from mysql.user" in sql:
        return [("app", "%", "N"), ("reporting", "%", "Y"),
                ("noone", "%", "N"), ("mysql.sys", "localhost", "Y")]
    if "mysql.role_edges" in sql:
        return [("reporting", "app")]
    if "user_privileges" in sql:
        return [("'reporting'@'%'", "SELECT"), ("'app'@'%'", "PROCESS")]
    if "schema_privileges" in sql:
        return [("'app'@'%'", "shop", "INSERT"),
                ("'app'@'%'", "shop", "CREATE VIEW")]
    if "table_privileges" in sql:
        return [("'app'@'%'", "shop", "orders", "UPDATE"),
                ("'app'@'%'", "shop", "audit_log", "DELETE"),
                ("'noone'@'%'", "crm", "leads", "SELECT")]
    return []


def _hop(tmp_path, **options):
    hop = Hop(name="my-to-pg", engine="hetero",
              source=Endpoint(host="10.0.0.7", port=3306, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.8", port=5432, user="migkit",
                              password="CHANGE_ME"),
              databases=["shop"],
              exclude=["audit_log"],
              mapping={"tables": {"shop.orders": "orders_v2"}},
              options={"source_engine": "mysql", "target_engine": "postgres",
                       **options})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


@pytest.fixture
def sides(monkeypatch):
    """The source's catalogue, and the target's: roles, the grants in its
    database, and every statement it was sent."""
    got = {"said": [], "pg_said": [], "pg_roles": {
        "migkit": {"login": True, "member_of": []}},
        "pg_grants": []}

    def mysql_conn(ep):
        return _Conn(_mysql_source, got["said"])

    def pg_conn(ep, dbname="postgres"):
        def answer(sql):
            if "role_table_grants" in sql:
                return list(got["pg_grants"])
            got["pg_said"].append((dbname, sql))
            return []
        return _Conn(answer, [])
    monkeypatch.setattr(users, "_mysql_conn", mysql_conn)
    monkeypatch.setattr(users, "_pg_conn", pg_conn)
    monkeypatch.setattr(users, "_pg_roles",
                        lambda ep: dict(got["pg_roles"]))
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "neutral_tables",
                        lambda self, side, db: ["public.orders_v2",
                                                "public.items"])
    return got


def test_each_side_is_read_in_the_one_shape(tmp_path, sides):
    hop = _hop(tmp_path)
    s, t, said, unread = users.across(hop)
    assert unread == []
    assert s["user:app"]["login"] is True
    assert s["user:app"]["roles"] == ["reporting"]
    assert s["role:reporting"]["login"] is False
    # a global privilege is the privilege on each of the hop's databases;
    # a table renamed on the target is named by the source's name; one the
    # hop leaves out is not there, nor another database's
    assert s["role:reporting"]["grants"] == ["SELECT ON shop.*"]
    assert s["user:app"]["grants"] == ["INSERT ON shop.*",
                                       "UPDATE ON shop.orders"]
    assert s["user:noone"]["grants"] == []
    # what MySQL grants that PostgreSQL has no word for is said, not carried
    assert any("CREATE VIEW ON shop.*" in x for x in said), said
    assert any("passwords are not carried" in x for x in said), said
    assert all(v["hash"] is None for v in s.values())
    assert set(t) == {"user:migkit"}


def test_the_compare_finds_what_the_target_lacks(tmp_path, sides):
    out, _ = users.compare(_hop(tmp_path), say=lambda *_: None)
    assert out["missing_on_target"] == ["role:reporting", "user:app",
                                        "user:noone"]
    assert out["password_differs"] is None
    assert out["result"] == "gap"


def test_create_makes_them_in_the_targets_own_statements(tmp_path, sides):
    hop = _hop(tmp_path)
    lines = []
    with pytest.raises(SystemExit):
        # the target still differs after (it is a recording, not a
        # server): `create` ends with the gap rather than a clean exit
        users.create(hop, apply=True, passwords={"app": "s3cret'x"},
                     say=lines.append)
    ran = [sql for _, sql in sides["pg_said"]]
    assert 'CREATE ROLE "reporting"' in ran
    assert any(r.startswith('CREATE ROLE "app" LOGIN PASSWORD') for r in ran)
    # the password is bound as a literal, its quote doubled - never shown
    assert "'s3cret''x'" in next(r for r in ran if '"app" LOGIN' in r)
    assert not any("s3cret" in line for line in lines)
    assert 'GRANT "reporting" TO "app"' in ran
    assert 'GRANT INSERT ON ALL TABLES IN SCHEMA public TO "app"' in ran
    # the source's orders is the target's orders_v2, in its own database
    assert ('shop', 'GRANT UPDATE ON "public"."orders_v2" TO "app"') in \
        sides["pg_said"]
    # no password in the passwords file: skipped, and said
    assert not any('"noone"' in r for r in ran)
    assert any("skipped user:noone" in line for line in lines)
    record = json.loads((tmp_path / "user-sync-created.json").read_text())
    assert record[-1]["engine"] == "hetero"
    assert "s3cret" not in json.dumps(record)


def test_rollback_takes_every_step_back_last_first(tmp_path, sides):
    hop = _hop(tmp_path)
    with pytest.raises(SystemExit):
        users.create(hop, apply=True, passwords={"app": "pw"},
                     say=lambda *_: None)
    sides["pg_said"].clear()
    users.rollback(hop, apply=True, say=lambda *_: None)
    undone = [sql for _, sql in sides["pg_said"]]
    assert undone[0].startswith("REVOKE UPDATE ON")
    assert undone[-1] == 'DROP ROLE "reporting"'
    assert 'DROP ROLE "app"' in undone
    assert (tmp_path / "user-sync-created.json.rolled-back").exists()


def test_an_engine_read_in_no_shape_but_its_own_is_not_compared(
        tmp_path, sides):
    hop = _hop(tmp_path)
    hop.options["target_engine"] = "redis"
    out, _ = users.compare(hop, say=lambda *_: None)
    assert out["result"] == "unknown"
    assert any("redis" in x for x in out["not_compared"])


def test_the_capability_is_read_off_the_code():
    from migkit import capabilities
    assert capabilities.implemented("hetero", "users")
