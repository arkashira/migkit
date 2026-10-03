"""A managed source held at the cutover by what its admin may do, on a
server set up the way the service sets up the account it hands over
(`freeze`): no superuser anywhere.

PostgreSQL: an admin that may create roles and end sessions - and not
change the server's settings, nor own the database the application's
role made its tables in. The right to connect is not its to take, so the
owner is kept out by a connection limit of none, recorded first and given
back exactly.

MySQL: an admin without SUPER, CONNECTION_ADMIN or SYSTEM_VARIABLES_ADMIN
that ends another account's session only through a procedure of the
service's (a stand-in of `mysql.rds_kill`, running KILL as its definer);
and the whole server made read-only through a stand-in of the provider's
parameter group - `read_only` set by another account, as the provider's
API does - which stops the admin as well, and is given back.

The servers are real; the provider's API is a stand-in that changes the
server the way the provider would.
"""
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.freeze", ("postgres", "mysql"))]

pytestmark = pytest.mark.docker

PG, PG_PORT = "migkit-test-mgd-pg", 16436
MY, MY_PORT = "migkit-test-mgd-my", 16437


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _up(name, port, image, inner, env):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                    f"127.0.0.1:{port}:{inner}", *sum((["-e", e]
                                                       for e in env), []),
                    image], check=True, capture_output=True)


def _down(name):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _tcp(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _hop(tmp_path, engine, port, db, **options):
    ep = Endpoint(host="127.0.0.1", port=port, user="admin",
                  password="CHANGE_ME")
    hop = Hop(name="mgd", engine=engine, source=ep, target=ep,
              databases=[db], options={"cutover": {"budget": 30},
                                       "app_roles": ["app"], **options})
    root = tmp_path / "reports"

    def report_dir(d=""):
        out = root / d if d else root
        out.mkdir(parents=True, exist_ok=True)
        return out
    hop.report_dir = report_dir
    return hop


# --- PostgreSQL ----------------------------------------------------------------

def _psql(sql, user="postgres", db="postgres", password="test"):
    got = subprocess.run(["docker", "exec", "-e", f"PGPASSWORD={password}",
                          PG, "psql", "-h", "127.0.0.1", "-U", user, "-d", db,
                          "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pg():
    _up(PG, PG_PORT, "postgres:16", "5432", ["POSTGRES_PASSWORD=test"])
    try:
        assert _tcp(PG_PORT)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True).returncode \
                    == 0:
                break
            time.sleep(1)
        time.sleep(2)
        # the account the service hands over: it may make roles and end
        # sessions, see every session's transaction - nothing more
        _psql("create role admin login password 'CHANGE_ME' createrole"
              " createdb; grant pg_signal_backend, pg_monitor to admin")
        # the application's role, made by that admin (which holds the
        # admin option on it, as the maker does)
        _psql("create role app login password 'CHANGE_ME'", user="admin",
              password="CHANGE_ME")
        # its database and tables its own, the admin's neither
        _psql("create database shop owner app")
        _psql("create table public.orders (id serial primary key, tag"
              " text); insert into public.orders (tag) values ('seed')",
              user="app", db="shop", password="CHANGE_ME")
        yield
    finally:
        _down(PG)


def _app_pg():
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="app",
                            password="CHANGE_ME", dbname="shop",
                            connect_timeout=5)
    conn.autocommit = True
    return conn


def test_an_owner_is_kept_out_by_its_connection_limit_and_let_back(
        pg, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = _hop(tmp_path, "postgres", PG_PORT, "shop")
    eng = PostgresEngine(hop)
    got = freeze.choose(hop, eng, ["shop"])
    assert got.rung is not None and got.rung.name == "pg_role_limit", \
        got.reason
    assert "not migkit's account's to take the right to connect" in \
        got.reason, got.reason
    assert freeze.cannot(hop, eng, "shop", "pg_role_limit") == []
    # without the way's own condition the right to connect is asked for,
    # which this admin does not have
    assert any("does not own the database" in s
               for s in freeze.cannot(hop, eng, "shop", "pg_database"))
    before = _psql("select rolconnlimit from pg_roles where rolname = 'app'")
    open_ = _app_pg()
    open_.cursor().execute("begin; insert into public.orders (tag) values"
                           " ('in flight')")
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, got.rung.name)
    held = freeze.proof(hop, eng, "shop", got.rung.name)
    assert all(ok for ok, _ in held), held
    assert _psql("select rolconnlimit from pg_roles where rolname = 'app'") \
        == "0"
    with pytest.raises(Exception):
        open_.cursor().execute("commit")
    with pytest.raises(Exception, match="too many connections"):
        _app_pg()
    # what the session had not committed is not there
    assert _psql("select count(*) from public.orders where tag ="
                 " 'in flight'", db="shop") == "0"
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    assert _psql("select rolconnlimit from pg_roles where rolname = 'app'") \
        == before
    conn = _app_pg()
    conn.cursor().execute("insert into public.orders (tag) values"
                          " ('after')")
    conn.close()
    assert _psql("select string_agg(tag, ',' order by id) from"
                 " public.orders", db="shop") == "seed,after"
    assert freeze.state(hop, "shop", "src") is None


def test_a_role_in_another_database_is_not_limited_there_too(pg, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _psql("create database reports owner app")
    try:
        hop = _hop(tmp_path, "postgres", PG_PORT, "shop")
        got = freeze.choose(hop, PostgresEngine(hop), ["shop"])
        assert got.rung is None and "reports" in got.reason, got.reason
    finally:
        _psql("drop database reports")


# --- MySQL -------------------------------------------------------------------------

def _my(sql, user="root", password="test"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", f"-u{user}",
                          f"-p{password}", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


#: the service's procedure that ends another account's session, as its
#: definer - a stand-in, in a schema of its own
KILLER = "migkit_standin.rds_kill"


@pytest.fixture(scope="module")
def my():
    _up(MY, MY_PORT, "mysql:8.4", "3306", ["MYSQL_ROOT_PASSWORD=test"])
    try:
        assert _tcp(MY_PORT)
        for _ in range(120):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        _my("""
create database shop;
create table shop.orders (id int auto_increment primary key,
                          tag varchar(64));
insert into shop.orders (tag) values ('seed');
create user admin@'%' identified by 'CHANGE_ME';
grant select, insert, update, delete, create, drop, alter, index,
      references, create view, show view, trigger, create routine,
      alter routine, execute, event, lock tables, process, reload,
      replication slave, replication client, create user
      on *.* to admin@'%' with grant option;
grant session_variables_admin, xa_recover_admin on *.* to admin@'%';
create user app@'%' identified by 'CHANGE_ME';
grant select, insert, update, delete on shop.* to app@'%';
create database migkit_standin;
delimiter //
create definer = root@'%' procedure migkit_standin.rds_kill(
    in thread bigint) sql security definer
begin
  kill thread;
end//
delimiter ;
grant execute on procedure migkit_standin.rds_kill to admin@'%';
""")
        yield
    finally:
        _down(MY)


def _app_my():
    import pymysql
    return pymysql.connect(host="127.0.0.1", port=MY_PORT, user="app",
                           password="CHANGE_ME", database="shop",
                           autocommit=True, connect_timeout=5)


def test_sessions_are_ended_through_the_services_procedure(my, tmp_path,
                                                           monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(freeze, "KILL_PROCEDURES", (KILLER,))
    hop = _hop(tmp_path, "mysql", MY_PORT, "shop")
    eng = MySQLEngine(hop)
    assert freeze._my_kill_way(eng, "src") == KILLER
    got = freeze.choose(hop, eng, ["shop"])
    # a schema the hop does not move is on the server: per database
    assert got.rung.name == "my_database", got.reason
    assert freeze.cannot(hop, eng, "shop", got.rung.name) == []
    app = _app_my()
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, got.rung.name)
    held = freeze.proof(hop, eng, "shop", got.rung.name)
    assert all(ok for ok, _ in held), held
    with pytest.raises(Exception):
        app.cursor().execute("insert into orders (tag) values ('lost')")
    fresh = _app_my()
    with pytest.raises(Exception):
        fresh.cursor().execute("insert into orders (tag) values"
                               " ('refused')")
    fresh.close()
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    back = _app_my()
    back.cursor().execute("insert into orders (tag) values ('after')")
    back.close()
    assert _my("select group_concat(tag order by id) from shop.orders") == \
        "seed,after"


class Group:
    """The provider's parameter group, as its API applies it: `read_only`
    set on the server by an account of the provider's own."""

    def __init__(self):
        self.calls = []

    def set_parameters(self, values, log):
        before = {"read_only": _my("select @@global.read_only")}
        _my(f"set global read_only = {int(values['read_only'])}")
        self.calls.append(dict(values))
        return before


def test_read_only_through_the_parameter_group_stops_its_admin_as_well(
        my, tmp_path, monkeypatch):
    from migkit import managed, sourcechange
    from migkit.clouds import Where
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(freeze, "KILL_PROCEDURES", (KILLER,))
    group = Group()
    rds = managed.find("aws", "rds", "mysql")
    monkeypatch.setattr(managed, "adapter", lambda hop, eng, which: group)
    monkeypatch.setattr(managed, "side", lambda hop, eng, which:
                        managed.Side(which, rds, Where("aws", "rds")))
    monkeypatch.setattr(freeze, "_my_param", lambda hop, eng, side="src": {
        "state": "possible", "before": "0"})
    hop = _hop(tmp_path, "mysql", MY_PORT, "shop",
               source_changes={"param_read_only": "yes"})
    eng = MySQLEngine(hop)
    said = []
    freeze.hold_for(hop, eng).hold(["shop"], "my_param_read_only",
                                   said.append)
    assert group.calls == [{"read_only": "1"}]
    held = freeze.proof(hop, eng, "shop", "my_param_read_only")
    assert all(ok for ok, _ in held), held
    import pymysql
    admin = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="admin",
                            password="CHANGE_ME", database="shop",
                            autocommit=True)
    with pytest.raises(pymysql.err.MySQLError, match="read.only"):
        admin.cursor().execute("insert into orders (tag) values ('admin')")
    admin.close()
    app = _app_my()
    with pytest.raises(pymysql.err.MySQLError, match="read.only"):
        app.cursor().execute("insert into orders (tag) values ('app')")
    app.close()
    assert [e["change"] for e in sourcechange.left_on_source(hop)] == [
        "param_read_only"]
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    assert group.calls[-1] == {"read_only": "0"}
    assert _my("select @@global.read_only") == "0"
    assert sourcechange.left_on_source(hop) == []
    app = _app_my()
    app.cursor().execute("insert into orders (tag) values ('writable')")
    app.close()
