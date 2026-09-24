"""The application cannot write to the target until cutover, when the hop
asks for that (`protect_target`).

Measured before choosing how, on PostgreSQL 16: the role-level read-only
setting reached new sessions only - one already connected kept writing -
and the application can switch it off; revoking a write privilege stopped
a connected session on its next statement; and a role that owns its table
can grant the privilege back to itself. So the freeze revokes what each
role holds, and where the role can still write afterwards (owner, inherited,
granted to everyone) it also makes the role read-only in that database and
ends its open sessions. Everything it took is given back exactly.
"""
import subprocess
import time

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROLES = ("create role app login password 'CHANGE_ME';"
         " create role grp nologin;"
         " create role member login password 'CHANGE_ME' in role grp;"
         " create role owner_app login password 'CHANGE_ME';")


def _setup(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.t, public.owned;"
                   " drop role if exists app, member, grp, owner_app")
        psql(port, ROLES)
    psql(pg_pair["dst"],
         "create table public.t (v int);"
         " grant select, insert, update, delete on public.t to app;"
         " grant select, insert on public.t to grp;"
         " create table public.owned (v int);"
         " alter table public.owned owner to owner_app;")


def _engine(pg_pair, tmp_path, **options):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="frz", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              options={"protect_target": True, **options})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _as(pg_pair, role, sql):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=CHANGE_ME", "migkit-test-pg-dst",
         "psql", "-h", "127.0.0.1", "-U", role, "-d", "postgres", "-At",
         "-c", sql], capture_output=True, text=True)
    return got.returncode, (got.stdout + got.stderr).strip()


def test_every_kind_of_role_is_stopped_and_given_back(pg_pair, tmp_path):
    from migkit import freeze
    _setup(pg_pair)
    eng = _engine(pg_pair, tmp_path)
    assert sorted(freeze.app_roles(eng.hop, eng, "postgres")) == [
        "app", "member", "owner_app"]
    # a session of the owner, connected before the freeze
    held = subprocess.Popen(
        ["docker", "exec", "-e", "PGPASSWORD=CHANGE_ME",
         "migkit-test-pg-dst", "psql", "-h", "127.0.0.1", "-U", "owner_app",
         "-d", "postgres", "-c", "select pg_sleep(20)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    said = []
    freeze.freeze(eng.hop, eng, "postgres", said.append)
    text = " | ".join(said)
    assert held.wait(10) != 0, "the owner's open session was ended"
    for role, table in (("app", "t"), ("member", "t"),
                        ("owner_app", "owned")):
        rc, out = _as(pg_pair, role, f"insert into public.{table}"
                                     " values (1)")
        assert rc != 0, (role, out)
    assert "app - 3 write grants revoked" in text, text
    assert "owner_app" in text and "sessions were ended" in text, text
    # reading still works
    assert _as(pg_pair, "app", "select count(*) from public.t")[0] == 0

    freeze.thaw(eng.hop, eng, "postgres", said.append)
    for role, table in (("app", "t"), ("member", "t"),
                        ("owner_app", "owned")):
        rc, out = _as(pg_pair, role, f"insert into public.{table}"
                                     " values (2)")
        assert rc == 0, (role, out)
    assert freeze.state(eng.hop, "postgres") is None


def test_migkits_own_account_keeps_writing(pg_pair, tmp_path):
    from migkit import freeze
    _setup(pg_pair)
    eng = _engine(pg_pair, tmp_path, app_roles=["app", "postgres"])
    assert freeze.app_roles(eng.hop, eng, "postgres") == ["app"]
    freeze.freeze(eng.hop, eng, "postgres", lambda m: None)
    try:
        psql(pg_pair["dst"], "insert into public.t values (9)")
        assert psql(pg_pair["dst"], "select count(*) from public.t"
                    ).stdout.strip() == "1"
    finally:
        freeze.thaw(eng.hop, eng, "postgres", lambda m: None)


def test_move_freezes_and_tearing_the_stream_down_gives_it_back(
        pg_pair, tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, freeze, movers
    _setup(pg_pair)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  frz:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    options: {protect_target: true, app_roles: [app]}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")
    monkeypatch.setattr(movers, "run_via", lambda *a, **k: [])
    monkeypatch.setattr(cli, "_replicate", lambda *a, **k: None)
    runner = CliRunner()
    runner.invoke(cli.main, ["move", "frz", "--mode", "full", "--go"])
    assert _as(pg_pair, "app", "insert into public.t values (1)")[0] != 0
    got = runner.invoke(cli.main, ["doctor"])
    assert "kept from the application's writes - app" in got.output, \
        got.output
    runner.invoke(cli.main, ["move", "frz", "--mode", "cdc", "--drop",
                             "--go"])
    assert _as(pg_pair, "app", "insert into public.t values (2)")[0] == 0
    hop = cfg.get_hop("frz")
    assert freeze.state(hop, "postgres") is None


def test_an_engine_without_it_refuses_before_copying(tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, movers
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  m:\n    engine: mongodb\n"
        "    source: {host: 10.0.0.1, port: 27017, user: u,"
        " password: CHANGE_ME}\n"
        "    target: {host: 10.0.0.2, port: 27017, user: u,"
        " password: CHANGE_ME}\n"
        "    databases: [app]\n    options: {protect_target: true}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    copied = []
    monkeypatch.setattr(movers, "run_via", lambda *a, **k: copied.append(a))
    got = CliRunner().invoke(cli.main, ["move", "m", "--mode", "full",
                                        "--go"])
    said = str(got.exception or "") + got.output
    assert got.exit_code != 0 and "not available for mongodb" in said, said
    assert copied == []


MY, MY_PORT = "migkit-test-frz-my", 15697


def _my(sql, user="root", password="test"):
    got = subprocess.run(["docker", "exec", MY, "mysql", f"-u{user}",
                          f"-p{password}", "-h127.0.0.1", "--protocol=tcp",
                          "-N", "-e", sql], capture_output=True, text=True)
    return got.returncode, (got.stdout + got.stderr).strip()


def test_mysql_accounts_are_frozen_per_database_and_given_back(tmp_path):
    """Database-level and table-level grants are taken back and the open
    session ended - measured, a database-level revoke alone let a connected
    session keep inserting. Server-wide privileges and roles cannot be taken
    for one database, so they are named and left."""
    import socket

    from migkit import freeze
    from migkit.engines.mysql import MySQLEngine
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as sk:
                sk.settimeout(2)
                if (_my("select 1")[0] == 0
                        and sk.connect_ex(("127.0.0.1", MY_PORT)) == 0):
                    break
            time.sleep(2)
        assert _my(
            "create database app; create table app.t (v int);"
            " create table app.u (v int);"
            " create user dbapp identified by 'CHANGE_ME';"
            " grant select, insert, update, delete on app.* to dbapp;"
            " create user tblapp identified by 'CHANGE_ME';"
            " grant select, insert on app.u to tblapp;"
            " create user globapp identified by 'CHANGE_ME';"
            " grant insert on *.* to globapp;"
            " create role writer; grant insert on app.* to writer;"
            " create user roleapp identified by 'CHANGE_ME';"
            " grant writer to roleapp; set default role writer to roleapp;"
            )[0] == 0
        ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                      password="test")
        hop = Hop(name="frzmy", engine="mysql", source=ep, target=ep,
                  databases=["app"], options={"protect_target": True})
        hop.report_dir = lambda db=None: tmp_path
        eng = MySQLEngine(hop)
        held = subprocess.Popen(
            ["docker", "exec", MY, "mysql", "-udbapp", "-pCHANGE_ME",
             "-h127.0.0.1", "--protocol=tcp", "app", "-e",
             "select sleep(20)"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)
        said = []
        freeze.freeze(hop, eng, "app", said.append)
        text = " | ".join(said)
        assert held.wait(10) != 0, "the open session was ended"
        assert _my("insert into app.t values (1)", "dbapp", "CHANGE_ME"
                   )[0] != 0
        assert _my("insert into app.u values (1)", "tblapp", "CHANGE_ME"
                   )[0] != 0
        assert "globapp - still able to write here: it holds write" in text
        assert "roleapp - still able to write here: it is granted the" \
               " roles writer" in text, text
        assert _my("select count(*) from app.t", "dbapp", "CHANGE_ME"
                   )[0] == 0, "reading still works"

        freeze.thaw(hop, eng, "app", said.append)
        assert _my("insert into app.t values (2)", "dbapp", "CHANGE_ME"
                   )[0] == 0
        assert _my("insert into app.u values (2)", "tblapp", "CHANGE_ME"
                   )[0] == 0
        assert freeze.state(hop, "app") is None
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
