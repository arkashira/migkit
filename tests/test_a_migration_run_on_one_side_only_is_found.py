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
