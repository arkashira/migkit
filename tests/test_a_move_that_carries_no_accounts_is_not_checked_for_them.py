"""A move that does not carry accounts is not checked for them (backlog
P0-SCOPE.6a) - where the operator says so.

`accounts: false` (or `objects: {roles: false}`) says the target's
accounts are made apart from this move: every check of users, roles and
grants (the deep check's, `users`, the assessment's) skips with a detail
that says the hop does not carry them. A skip is "not compared", never
"the same": `users verify` ends as incomplete, and no repair grants what
the move does not carry.

A DTS job told not to move accounts (Tencent's `IsMigrateAccount` false,
Alibaba's `privilegeMigration` false) is not taken as saying so: the
application needs its accounts on the target all the same, and their
absence there - grants by the hundred, measured on a job that moved
none - is the finding the check is for. The importers say it beside the
hop and leave the check to show it.

The docker tests run one engine at a time (`-k postgres`, `-k mysql`).
"""
import json
from pathlib import Path

import pytest

from migkit import hopkeys, imports, users
from migkit.config import Endpoint, Hop, build_hop
from migkit.verdict import VERDICTS
from tests import mysql_pair
from tests.conftest import psql
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

SAMPLES = Path(__file__).resolve().parent / "samples"


def _no_accounts(engine="mysql", **kw):
    return Hop(name="na", engine=engine, source=Endpoint(),
               target=Endpoint(), options={"accounts": False}, **kw)


def test_a_tencent_job_that_moves_no_accounts_is_said_and_still_checked():
    got = imports.read(SAMPLES / "tencent-dts-migration.json")
    assert "accounts" not in (got.raw.get("options") or {})
    said = " | ".join(got.notes)
    assert "does not move accounts" in said and "users create" in said
    hop = build_hop(got.name, dict(got.raw, target="mysql://app@10.0.0.22"))
    assert users.carried(hop)


def test_an_alibaba_job_that_moves_no_accounts_is_said_and_still_checked():
    job = json.loads((SAMPLES / "alibaba-dts-job.json").read_text())
    job["Reserve"] = json.dumps({"privilegeMigration": "false",
                                 "targetTableMode": "2"})
    got = imports.from_alibaba_dts(job)
    assert "accounts" not in (got.raw.get("options") or {})
    assert "privilegeMigration false" in " | ".join(got.notes)


def test_the_operator_says_it_either_way():
    assert not users.carried(_no_accounts())
    assert not users.carried(Hop(name="na", engine="mysql",
                                 source=Endpoint(), target=Endpoint(),
                                 objects={"roles": False}))
    assert users.carried(Hop(name="na", engine="mysql", source=Endpoint(),
                             target=Endpoint()))


def test_a_hop_naming_its_roles_is_held_to_those_alone(monkeypatch):
    """`objects: {roles: [app_*]}` carries the application's accounts and
    no other; it was read as every account, and the target's own - a
    monitoring user, a DBA's - read as a gap."""
    hop = Hop(name="r", engine="postgres", source=Endpoint(),
              target=Endpoint(), objects={"roles": ["app_*"]})
    assert users.carried(hop)
    assert users.role_names(hop) == ["app_*"]
    src = {"app_rw": {"member_of": ["app_ro"]}, "app_ro": {},
           "dba_kim": {}, "reporting": {}}
    dst = {"app_rw": {"member_of": []}, "monitor": {}}
    monkeypatch.setattr(users, "_pg_roles",
                        lambda ep: src if ep is hop.source else dst)
    monkeypatch.setattr(users, "_pg_hashes", lambda ep: None)
    out, _ = users.compare(hop, say=lambda *_: None)
    assert out["missing_on_target"] == ["app_ro"]
    assert out["extra_on_target"] == []
    assert out["grants_missing"] == ["app_rw"]
    assert out["source_users"] == 2
    # MySQL's accounts are a user at a host, named by the user
    assert users.role_taken(Hop(name="m", engine="mysql", source=Endpoint(),
                                target=Endpoint(),
                                objects={"roles": ["app"]}), "app@%")


def test_users_verify_is_not_compared_and_never_a_pass(monkeypatch):
    hop = _no_accounts()
    monkeypatch.setattr(users, "get_hop", lambda name: hop)
    monkeypatch.setattr(users, "compare", lambda *a, **kw: pytest.fail(
        "compared the accounts of a hop that does not carry them"))
    said = []
    with pytest.raises(SystemExit) as e:
        users.run("na", "verify", say=said.append)
    assert e.value.code == VERDICTS["incomplete"].exit
    assert "VERIFY: pass" not in said
    assert users.NOT_CARRIED in " ".join(said), said
    said.clear()
    users.run("na", "test", say=said.append)
    assert users.NOT_CARRIED in " ".join(said), said


def test_the_mysql_grant_check_skips_and_its_repair_grants_nothing():
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_no_accounts())
    got = eng._check_grants("shop")
    assert [(r.scope, r.status, r.detail) for r in got] == \
        [("shop grants", "skip", users.NOT_CARRIED)]
    assert eng._grant_repair("shop") is None


def test_the_option_is_declared():
    assert hopkeys.HOP_OPTIONS["accounts"].kind == "bool"
    assert users.carried(Hop(name="h", engine="mysql", source=Endpoint(),
                             target=Endpoint()))


def test_postgres_grants_are_skipped_and_not_repaired(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    role = ("do $$ begin if not exists (select from pg_roles where rolname ="
            " 'naapp') then create role naapp; end if; end $$;")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, role + " create table public.t (id serial primary key)")
    psql(pg_pair["src"], "grant select on public.t to naapp;"
                         " grant usage on sequence public.t_id_seq to naapp")

    def eng(options):
        hop = Hop(name="na", engine="postgres",
                  source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                                  user="postgres", password="test"),
                  target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                                  user="postgres", password="test"),
                  databases=["postgres"], options=options)
        hop.report_dir = lambda db=None, _p=tmp_path: _p
        return PostgresEngine(hop)
    got = {r.scope: r for r in eng({"accounts": False}).check_deep(
        "postgres")}
    for scope in ("postgres grants", "postgres seq-grants"):
        assert got[scope].status == "skip", got[scope]
        assert got[scope].detail == users.NOT_CARRIED
    assert eng({"accounts": False})._grant_repair("postgres") is None
    # a hop that carries them still says the grants differ
    got = {r.scope: r for r in eng({}).check_deep("postgres")}
    assert got["postgres grants"].status == "diff", got["postgres grants"]
    assert got["postgres seq-grants"].status == "diff"
    assert eng({})._grant_repair("postgres") is not None
    # and the assessment does not hold the roles to the source's either
    roles = [i for i in eng({"accounts": False}).assess()
             if i["item"].startswith(("login roles", "role passwords"))]
    assert roles == [{"level": "info", "scope": "instance",
                      "item": "login roles present on target",
                      "detail": users.NOT_CARRIED}], roles


def test_mysql_the_assessment_does_not_hold_the_accounts_either(my_pair):
    for side in ("src", "dst"):
        my(side, "create database if not exists na")
    try:
        items = mysql_pair.engine("na", {"accounts": False}).assess()
    finally:
        for side in ("src", "dst"):
            my(side, "drop database if exists na")
    got = [i for i in items if i["item"] in (
        "user accounts present on target", "account passwords match source",
        "cannot read mysql.user")]
    assert got == [{"level": "info", "scope": "instance",
                    "item": "user accounts present on target",
                    "detail": users.NOT_CARRIED}], got
