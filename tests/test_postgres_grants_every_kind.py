"""Every kind of privilege is compared, and the repair puts back each kind
it finds missing (audit 2026-10-01, SC11).

Only table and sequence grants were compared: a schema's USAGE, the
database's CONNECT, a routine's EXECUTE, a column's grant, the default
privileges the application's new tables get after the cutover, a grant
to PUBLIC and a grant's WITH GRANT OPTION were never looked at. A source
that took EXECUTE from PUBLIC was "the same" as a target that still gave
it to everyone.
"""
import pytest

from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("pg")

OBJECTS = """
create schema app;
create table app.t (id int primary key, secret text, v text);
create function app.f() returns int language sql as 'select 1';
"""

GRANTS = """
grant usage on schema app to reader;
grant select (id, v) on app.t to reader;
grant insert on app.t to reader with grant option;
revoke execute on function app.f() from public;
grant execute on function app.f() to reader;
alter default privileges in schema app grant select on tables to reader;
"""


@pytest.fixture
def eng(pair, tmp_path):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    db = pair.name
    for server in (pair.src, pair.dst):
        server.sql(f'drop database if exists "{db}" with (force)')
        server.sql(f'create database "{db}"')
        if not server.sql("select 1 from pg_roles where rolname ="
                          " 'reader'"):
            server.sql("create role reader login password"
                       " 'CHANGE_ME-reader'")
        server.sql(OBJECTS, db)
    pair.src.sql(GRANTS + f'grant connect on database "{db}" to reader;',
                 db)
    hop = Hop(name="grants", engine="postgres", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    got = PostgresEngine(hop)
    yield got
    got.release_kept()


def _said(eng, gaps):
    return {eng._grant_said(g) for g in gaps}


def test_every_kind_missing_on_the_target_is_found(pair, eng):
    miss, extra, smiss, seen, _ = eng._grant_gaps(pair.name)
    said = " | ".join(sorted(_said(eng, miss)))
    for kind in ("on schema", "on column", "on function",
                 "on default tables", "on database"):
        assert kind in said, said
    assert "reader INSERT (with grant option) on table" in said, said
    # the target still lets everyone run what the source took from them
    assert any(e.startswith("PUBLIC\x1fFUNCTION") for e in extra), extra
    assert not smiss


def test_the_repair_puts_back_what_it_found_and_undoes_it(pair, eng):
    db = pair.name
    action = eng._grant_repair(db)
    assert action is not None and action.undo
    assert any("WITH GRANT OPTION" in s for s in action.statements)
    assert any(s.startswith("ALTER DEFAULT PRIVILEGES FOR ROLE")
               for s in action.statements), action.statements
    eng.apply(db, action)
    miss, _, smiss, _, _ = eng._grant_gaps(db)
    assert miss == [] and smiss == [], miss
    # a column grant, made as one, reads as one on the target
    got = pair.dst.sql("select has_column_privilege('reader', 'app.t',"
                       " 'secret', 'select'), has_column_privilege('reader',"
                       " 'app.t', 'v', 'select')", db)
    assert got == [(False, True)], got
    eng.apply(db, type(action)(action.scope, action.kind, action.undo, [],
                               ""))
    miss, _, _, _, _ = eng._grant_gaps(db)
    assert miss, "the undo put nothing back"


def test_grants_that_match_are_the_same(pair, eng):
    db = pair.name
    pair.dst.sql(GRANTS + f'grant connect on database "{db}" to reader;',
                 db)
    miss, extra, smiss, seen, _ = eng._grant_gaps(db)
    assert (miss, extra, smiss) == ([], [], []), (miss, extra)
    assert seen > 0
