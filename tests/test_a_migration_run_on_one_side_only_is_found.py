"""The schema check reads the application's migration history on both
sides and finds a migration recorded on one only (26.versions)."""
from migkit import appversions
from tests.conftest import needs_docker, psql
from tests.mysql_pair import engine, my, my_pair  # noqa: F401 - a fixture


def test_the_comparison_names_what_differs():
    one = {"Flyway": {("1", "init"), ("2", "add")}, "Rails": {("2020",)}}
    two = {"Flyway": {("1", "init")}, "Alembic": {("abc",)}}
    got = {tool: (status, detail)
           for status, tool, detail in appversions.compare(one, two)}
    assert got["Flyway"][0] == "diff" and "1 recorded on the source only" in \
        got["Flyway"][1]
    assert got["Rails"][0] == "diff" and "source only" in got["Rails"][1]
    assert got["Alembic"][0] == "diff" and "target only" in got["Alembic"][1]
    same = appversions.compare(one, one)
    assert all(s == "ok" for s, _, _ in same), same
    assert appversions.compare({}, {}) == []


@needs_docker
def test_mysql_finds_a_migration_on_one_side_only(my_pair):
    for side in ("src", "dst"):
        my(side, "drop database if exists vers; create database vers;"
                 " create table vers.flyway_schema_history (version"
                 " varchar(20), description varchar(50), success boolean);"
                 " create table vers.alembic_version (version_num"
                 " varchar(32))")
        my(side, "insert into vers.flyway_schema_history values"
                 " ('1', 'init', true), ('2', 'add', true);"
                 " insert into vers.alembic_version values ('abc')")
    eng = engine("vers")
    assert [r.status for r in eng.check_app_versions("vers")] == ["ok", "ok"]
    my("src", "insert into vers.flyway_schema_history values"
              " ('3', 'later', true), ('4', 'failed', false)")
    got = eng.check_app_versions("vers")
    flyway = next(r for r in got if "Flyway" in r.scope)
    assert flyway.status == "diff", got
    # the failed one does not count as applied
    assert "1 recorded on the source only" in flyway.detail, flyway.detail
    my("dst", "drop table vers.alembic_version")
    got = {r.scope: r for r in eng.check_app_versions("vers")}
    assert "keeps its history on the source only" in \
        got["vers Alembic history"].detail


@needs_docker
def test_postgresql_finds_a_migration_on_one_side_only(pg_pair):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    for side in ("src", "dst"):
        psql(pg_pair[side],
             'drop table if exists django_migrations, "__EFMigrationsHistory";'
             " create table django_migrations (id serial, app text, name text);"
             " insert into django_migrations (app, name) values"
             " ('auth', '0001'), ('shop', '0001');"
             ' create table "__EFMigrationsHistory" ("MigrationId" text);'
             ' insert into "__EFMigrationsHistory" values (\'20260101_init\')')
    psql(pg_pair["dst"], "delete from django_migrations where app = 'shop'")
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    eng = PostgresEngine(Hop(name="vers", engine="postgres",
                             source=ep("src"), target=ep("dst"),
                             databases=["postgres"]))
    got = {r.scope: r for r in eng.check_app_versions("postgres")}
    assert got["postgres Django history"].status == "diff", got
    assert "shop/0001" in got["postgres Django history"].detail
    assert got["postgres Entity Framework history"].status == "ok", got


# ---- every engine, through the neutral contract; every tool's shape; the
# cutover's gate on it ------------------------------------------------------

from types import SimpleNamespace  # noqa: E402

import pytest  # noqa: E402

from migkit import cutover  # noqa: E402
from migkit.engines.base import Engine  # noqa: E402


class Fake(Engine):
    """An engine whose tables are lists of dicts, read the neutral way."""

    def __init__(self, src, dst, mapping=None):
        self.hop = SimpleNamespace(mapping=mapping or {})
        self.sides = {"src": src, "dst": dst}

    def neutral_tables(self, side, db):
        return [f"app.{t}" for t in self.sides[side]]

    def neutral_columns(self, side, db, table):
        names = []
        for r in self.sides[side][table.split(".", 1)[1]]:
            names += [k for k in r if k not in names]
        return [(n, "text") for n in names]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows = self.sides[side][table.split(".", 1)[1]]
        return [[r.get(c) for c, _ in columns] for r in rows], None


def fly(*rows):
    return [{"installed_rank": i, "version": v, "description": d,
             "checksum": c, "success": ok}
            for i, (v, d, c, ok) in enumerate(rows, 1)]


def tools_of(src, dst):
    return {a.tool: a for a in appversions.apart(
        *appversions.read(Fake(src, dst), "app"))}


def test_every_tool_is_told_by_its_table_and_columns():
    side = {
        "flyway_schema_history": fly(("1", "init", 1, 1)),
        "databasechangelog": [{"ID": "1", "AUTHOR": "a", "FILENAME": "x.xml",
                               "ORDEREXECUTED": 1, "MD5SUM": "m",
                               "EXECTYPE": "EXECUTED"}],
        "alembic_version": [{"version_num": "abc"}],
        "django_migrations": [{"id": 1, "app": "auth", "name": "0001_init"}],
        "schema_migrations": [{"version": 3, "dirty": False}],
        "__EFMigrationsHistory": [{"MigrationId": "20260101_Init",
                                   "ProductVersion": "8.0"}],
        "CHANGE_HISTORY": [{"VERSION": "1.1", "SCRIPT": "V1.1__a.sql",
                            "CHECKSUM": "c", "STATUS": "Success",
                            "INSTALLED_ON": "2026"}],
        "knex_migrations": [{"id": 1, "name": "20260101_a.js", "batch": 1}],
        "_prisma_migrations": [{"migration_name": "20260101_init",
                                "started_at": "t", "finished_at": "t",
                                "rolled_back_at": None, "checksum": "c"}],
        "SequelizeMeta": [{"name": "20260101-a.js"}],
        "migrations": [{"id": 1, "timestamp": 1700, "name": "Init1700"}],
        "changelog": [{"fileName": "20260101-a.js", "appliedAt": "t"}],
        "mongockChangeLog": [{"changeId": "c1", "author": "a",
                              "state": "EXECUTED", "timestamp": 1}],
    }
    got = tools_of(side, side)
    assert set(got) == set(appversions.BY_NAME) - {"Rails", "dbmate"}, got
    assert all(a.status == "ok" for a in got.values()), got
    # without `dirty` the table is Rails' where Rails' own table stands
    # beside it, and dbmate's where it does not
    rails = {"schema_migrations": [{"version": "2026"}],
             "ar_internal_metadata": [{"key": "env"}]}
    assert set(tools_of(rails, rails)) == {"Rails"}
    rails.pop("ar_internal_metadata")
    assert set(tools_of(rails, rails)) == {"dbmate"}


def test_one_side_only_out_of_order_and_failed_are_found():
    src = {"flyway_schema_history": fly(("1", "a", 1, 1), ("2", "b", 2, 1),
                                        ("3", "c", 3, 1))}
    dst = {"flyway_schema_history": fly(("1", "a", 1, 1), ("3", "c", 3, 1),
                                        ("4", "d", 4, 0))}
    a = tools_of(src, dst)["Flyway"]
    assert a.status == "diff"
    assert "1 recorded on the source only (e.g. 2/b)" in a.detail, a.detail
    assert "older than the latest the target records (2/b)" in a.detail
    assert "1 recorded as failed on the target (4/d)" in a.detail
    assert [lvl for lvl, _ in a.cutover] == ["gate", "gate"], a.cutover


def test_a_checksum_two_ways_and_failed_rows_in_other_shapes():
    def prisma(*rows):
        return {"_prisma_migrations": [
            {"migration_name": n, "started_at": n, "finished_at": f,
             "rolled_back_at": None, "checksum": c} for n, f, c in rows]}
    a = tools_of(prisma(("1_a", "1", "x"), ("2_b", None, "y")),
                 prisma(("1_a", "1", "z")))["Prisma"]
    assert "another checksum on each side (1_a)" in a.detail, a.detail
    assert "failed on the source (2_b)" in a.detail
    assert [lvl for lvl, w in a.cutover if "failed" in w] == ["warn"]

    def g(v, d=False):
        return {"schema_migrations": [{"version": v, "dirty": d}]}
    a = tools_of(g(5), g(6))["golang-migrate"]
    assert "target only" in a.detail and "source only" not in a.detail
    a = tools_of(g(6), g(6, True))["golang-migrate"]
    # a dirty version is not applied: the target stands short of it
    assert a.status == "diff" and "failed on the target (6)" in a.detail
    assert any(lvl == "gate" and "force" in w for lvl, w in a.cutover)

    def m(*states):
        return {"mongockChangeLog": [
            {"changeId": f"c{i}", "author": "a", "state": s, "timestamp": i}
            for i, s in enumerate(states)]}
    a = tools_of(m("EXECUTED", "ROLLED_BACK"), m("EXECUTED", "FAILED"))
    assert a["Mongock"].detail == "Mongock: 1 recorded as failed on the" \
        " target (c1/a): the tool stops there", a["Mongock"].detail


def test_the_cutover_gate_refuses_a_migration_the_target_lacks():
    src = {"django_migrations": [{"id": 1, "app": "shop", "name": "0001"},
                                 {"id": 2, "app": "shop", "name": "0002"}]}
    dst = {"django_migrations": [{"id": 1, "app": "shop", "name": "0001"}]}
    rows = cutover._version_rows(Fake(src, dst), "app", "fail")
    assert [r[0] for r in rows] == ["fail"], rows
    assert "shop/0002" in rows[0][1] and "check again" in rows[0][1]
    # the target ahead only warns
    assert [r[0] for r in cutover._version_rows(Fake(dst, src), "app",
                                                "fail")] == ["warn"]
    # a cutover run by hand (its gate a warning) never fails on it
    assert {r[0] for r in cutover._version_rows(Fake(src, dst), "app",
                                                "warn")} == {"warn"}
    assert cutover._version_rows(Fake(dst, dst), "app", "fail") == []
    assert cutover._version_rows(Fake(src, {}), "app", "fail")[0][0] == \
        "fail"
    assert cutover._version_rows(Fake({}, src), "app", "fail")[0][0] == \
        "warn"


def test_migkits_own_changes_are_said_with_how_the_tool_records_them():
    side = {"__EFMigrationsHistory": [{"MigrationId": "1_Init",
                                       "ProductVersion": "8.0"}]}
    eng = Fake(side, side, {"columns": {"public.orders": {"drop": ["x"]}}})
    got = {r.scope: r for r in eng.check_app_versions("app")}
    said = got["app Entity Framework unrecorded history"]
    assert said.status == "warn" and "public.orders" in said.detail
    assert 'insert into "app"."__EFMigrationsHistory" ("MigrationId",' \
        ' "ProductVersion")' in said.fix_hint, said.fix_hint
    assert got["app Entity Framework history"].status == "ok"
    rows = cutover._version_rows(eng, "app", "fail")
    assert [r[0] for r in rows] == ["warn"] and "record it" in rows[0][1]
    assert [r.status for r in Fake(side, side).check_app_versions(
        "app")] == ["ok"]


def test_an_engine_that_cannot_list_its_tables_says_nothing():
    class Bare(Engine):
        def __init__(self):
            self.hop = SimpleNamespace(mapping={})
    assert Bare().check_app_versions("app") == []


def test_a_finding_the_schema_check_said_is_not_said_again():
    assert appversions.said_in([{"check": "schema",
                                 "scope": "app Flyway history"}])
    assert not appversions.said_in([{"check": "schema", "scope": "app"}])


@pytest.mark.parametrize("tool", appversions.SPECS, ids=lambda t: t.name)
def test_every_tool_says_how_a_change_is_recorded(tool):
    assert tool.record and tool.said
    assert "liquibase" not in (tool.said + tool.record).lower()
