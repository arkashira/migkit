"""The interlock on the target, on two PostgreSQL servers (backlog R23, 3).

What each side says it is - its system identifier, when its postmaster
started, the address it answers on, its version, the database's size -
is shown in the plan and held to at `--go`: the source given as the target
(by another name for the same server too) is refused, as is a target
holding rows the hop says nothing about, and a target the hop was pointed
at after the plan was shown. A move into an empty target goes on as it
always has.

The databases are the test's own, not `postgres`: the maintenance
database is never mapped to another name (`PostgresEngine._d`), so a hop
mapping it writes the one of that name - measured on the first run of
these tests, where the interlock then called the source's own database
another one; it now reads the name from the engine.
"""
import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

TABLES = ("create table public.t (id int primary key, v text);"
          " create table public.u (id int primary key, v text)")


def _ok(port, sql, db="postgres"):
    got = psql(port, sql, db)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _databases(pg_pair, drop_only=False):
    for port, name in ((pg_pair["src"], "app"), (pg_pair["dst"], "app"),
                       (pg_pair["src"], "other")):
        _ok(port, f"drop database if exists {name} with (force)")
        if not drop_only:
            _ok(port, f"create database {name}")
            _ok(port, TABLES, name)


@pytest.fixture
def pair(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    _databases(pg_pair)
    _ok(pg_pair["src"], "insert into public.t select g, 'v' || g from"
                        " generate_series(1, 20) g;"
                        " insert into public.u values (1, 'one')", "app")
    yield pg_pair
    _databases(pg_pair, drop_only=True)


def _hop(tmp_path, src, dst, host="127.0.0.1", target_db="", options=""):
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  il:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {src}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: {host}, port: {dst}, user: postgres,"
        " password: test}\n    databases: [app]\n"
        + (f"    db_map: {{app: {target_db}}}\n" if target_db else "")
        + (f"    options: {{{options}}}\n" if options else ""))


def _move(monkeypatch, *argv, via="builtin"):
    from migkit import cli
    monkeypatch.setenv("MIGKIT_MOVER", via)
    got = CliRunner().invoke(cli.main, ["move", "il", *argv])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _count(port, db="app", table="public.t"):
    return _ok(port, f"select count(*) from {table}", db)


def test_the_plan_shows_both_servers_and_a_move_goes_on(pair, tmp_path,
                                                       monkeypatch):
    _hop(tmp_path, pair["src"], pair["dst"])
    got, said = _move(monkeypatch)
    assert got.exit_code == 0, said
    for port in (pair["src"], pair["dst"]):
        sysid = _ok(port, "select system_identifier from"
                          " pg_control_system()")
        assert f"cluster {sysid}" in said, said
    assert said.count("postgres 16.") >= 2, said
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code == 0, said
    assert _count(pair["dst"]) == "20"


def test_the_bulk_path_moves_into_an_empty_target_as_before(pair, tmp_path,
                                                           monkeypatch):
    from migkit import movers
    if not (movers.which("pg_dump") and movers.which("pg_restore")):
        pytest.skip("the PostgreSQL dump programs are not installed")
    _hop(tmp_path, pair["src"], pair["dst"])
    got, said = _move(monkeypatch, "--go", via="pgdump")
    assert got.exit_code == 0, said
    assert _count(pair["dst"]) == "20"


def test_the_source_as_the_target_is_refused(pair, tmp_path, monkeypatch):
    # the same server by another name: its own words say so
    _hop(tmp_path, pair["src"], pair["src"], host="localhost")
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code != 0, said
    assert "the target is the source itself" in said, said
    assert "the same server by what the server says of itself" in said, said
    assert _count(pair["src"]) == "20"
    # the same server into another database is a target like any other
    _hop(tmp_path, pair["src"], pair["src"], target_db="other")
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code == 0, said
    assert _count(pair["src"], "other") == "20"
    assert _count(pair["src"]) == "20"


def test_a_target_holding_rows_is_refused_until_the_hop_says(pair, tmp_path,
                                                            monkeypatch):
    _ok(pair["dst"], "insert into public.t values (99, 'theirs')", "app")
    _hop(tmp_path, pair["src"], pair["dst"])
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code != 0, said
    assert "already holds rows on the target: public.t." in said, said
    assert _ok(pair["dst"], "select string_agg(id::text, ',') from"
                            " public.t", "app") == "99"
    assert _count(pair["dst"], table="public.u") == "0"
    # kept: left as it is, the rest copied
    _hop(tmp_path, pair["src"], pair["dst"], options="target_rows: keep")
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code == 0, said
    assert _ok(pair["dst"], "select string_agg(id::text, ',') from"
                            " public.t", "app") == "99"
    assert _count(pair["dst"], table="public.u") == "1"
    _hop(tmp_path, pair["src"], pair["dst"], options="target_rows: replace")
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code == 0, said
    assert _count(pair["dst"]) == "20"
    assert _ok(pair["dst"], "select count(*) from public.t where id = 99",
               "app") == "0"


def test_the_bulk_path_leaves_a_kept_table_out(pair, tmp_path, monkeypatch):
    from migkit import movers
    if not (movers.which("pg_dump") and movers.which("pg_restore")):
        pytest.skip("the PostgreSQL dump programs are not installed")
    _ok(pair["dst"], "insert into public.t values (99, 'theirs')", "app")
    _hop(tmp_path, pair["src"], pair["dst"], options="target_rows: keep")
    got, said = _move(monkeypatch, "--go", via="pgdump")
    assert got.exit_code == 0, said
    assert _ok(pair["dst"], "select string_agg(id::text, ',') from"
                            " public.t", "app") == "99"
    assert _count(pair["dst"], table="public.u") == "1"


def test_a_target_changed_between_the_plan_and_go_is_refused(pair, tmp_path,
                                                            monkeypatch):
    _hop(tmp_path, pair["src"], pair["dst"])
    got, said = _move(monkeypatch)
    assert got.exit_code == 0, said
    # pointed at the other server between the plan and --go
    _hop(tmp_path, pair["src"], pair["src"], target_db="other")
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code != 0, said
    assert "not the server the plan showed" in said, said
    assert _count(pair["src"], "other") == "0"
    # shown again, it is the one --go writes to
    assert _move(monkeypatch)[0].exit_code == 0
    got, said = _move(monkeypatch, "--go")
    assert got.exit_code == 0, said
    assert _count(pair["src"], "other") == "20"
