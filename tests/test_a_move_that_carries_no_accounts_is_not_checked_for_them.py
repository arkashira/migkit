"""A move that does not carry accounts is not checked for them (backlog
P0-SCOPE.6a).

A DTS job told not to move accounts (Tencent's `IsMigrateAccount` false,
Alibaba's `privilegeMigration` false) was read into a hop with a note
saying so, and nothing else: the deep check's grants and `migkit users
verify` went on reporting the target's users and grants as differences
the job was never asked to carry. The hop says it now - `accounts: false`,
written by both importers - and every check of users, roles and grants
(the deep check's, `users`, the assessment's) skips with a detail that
says the hop does not carry them. A skip is "not compared", never "the
same": `users verify` ends as incomplete, and no repair grants what the
move does not carry.

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


def test_a_tencent_job_that_moves_no_accounts_says_so_in_the_hop():
    got = imports.read(SAMPLES / "tencent-dts-migration.json")
    assert got.raw["options"] == {"accounts": False}
    assert "accounts: false" in " | ".join(got.notes)
    raw = dict(got.raw, target="mysql://app@10.0.0.22")
    assert not [p for p in hopkeys.check({"hops": {got.name: raw}})
                if p.level == "error"]
    hop = build_hop(got.name, raw)
    assert not users.carried(hop)


def test_a_tencent_job_that_moves_them_leaves_the_hop_as_it_was():
    job = json.loads((SAMPLES / "tencent-dts-migration.json").read_text())
    body = job.get("Response", job)
    body["MigrateOption"]["IsMigrateAccount"] = True
    got = imports.from_tencent_dts(job)
    assert "accounts" not in (got.raw.get("options") or {})
    assert users.carried(build_hop(got.name, dict(
        got.raw, target="mysql://app@10.0.0.22")))


def test_an_alibaba_job_that_moves_no_accounts_says_so_in_the_hop():
    job = json.loads((SAMPLES / "alibaba-dts-job.json").read_text())
    # where its reserved settings say nothing, nothing is assumed
    assert "options" not in imports.from_alibaba_dts(dict(job)).raw
    job["Reserve"] = json.dumps({"privilegeMigration": "false",
                                 "targetTableMode": "2"})
    got = imports.from_alibaba_dts(job)
    assert got.raw["options"] == {"accounts": False}
    assert "privilegeMigration false" in " | ".join(got.notes)
    job["Reserve"] = {"privilegeMigration": True}
    assert "options" not in imports.from_alibaba_dts(job).raw


def test_across_engines_the_option_joins_the_pairs_own():
    job = json.loads((SAMPLES / "alibaba-dts-job.json").read_text())
    job["DestinationEndpoint"] = {"EngineName": "POSTGRESQL",
                                  "Ip": "10.0.0.43", "UserName": "app"}
    job["Reserve"] = '{"privilegeMigration": false}'
    got = imports.from_alibaba_dts(job)
    assert got.raw["options"] == {"source_engine": "mysql",
                                  "target_engine": "postgres",
                                  "accounts": False}


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
