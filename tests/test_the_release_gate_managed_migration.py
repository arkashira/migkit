"""The release gate: a managed migration's whole path, the way a real one
runs it - managed PostgreSQL, MySQL and a MongoDB-compatible document
store on one cloud into the same on another, several databases each, an
application writing all the way through - rehearsed first, then run:

    rehearse -> forecast -> move -> check -> change stream -> cutover with
    a stream back -> point of no return -> evidence, and zero difference
    at the end

Not a cloud account: two servers in docker per engine, set up as the
services set up the account they hand over (`managed`'s catalogue says
what each gives), and named as those services (`managed: {service: ...}`)
so the plan, the DBA's words and the freeze are theirs:

* no superuser on either side - PostgreSQL's admin may make roles and
  databases, read the log (a role attribute, as `rds_replication` gives),
  end sessions, see every session's transaction and subscribe; MySQL's has
  no SUPER, CONNECTION_ADMIN or SYSTEM_VARIABLES_ADMIN and ends another
  account's session through a procedure of its service's (a stand-in of
  `mysql.rds_kill`); the document store's has no write block and no lock
  (as DocumentDB), so it is held database by database;
* the servers' settings are locked: logical decoding, GTIDs and the log
  are on from the start, as the services' parameter groups set them, and
  nothing migkit's account may change;
* the provider's API is a stand-in that takes a restore point of each
  side before the cutover's clock and says so.

Simplified against the services, and said: the old MySQL source takes
its stream back as a replica through `CHANGE REPLICATION SOURCE` (its
admin is given REPLICATION_SLAVE_ADMIN), where RDS would take
`mysql.rds_set_external_source` - which `MySQLEngine._replica_procedure`
calls wherever the server has it; the document store has no stream back,
the engine's own path having none, so its zero difference is the
cutover's fenced final check and every write the application was told of
found once where it now writes.

One pair of servers runs at a time: each engine's are started, used and
removed before the next.
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from tests import cutover_harness as harness

pytestmark = [pytest.mark.docker, pytest.mark.release_gate]

NET = "migkit-test-gate-net"
PORTS = {"postgres": (16430, 16431), "mysql": (16432, 16433),
         "mongodb": (16434, 16435)}
#: the services the sides are named as: a managed engine on one cloud into
#: the same engine on another
SERVICES = {"postgres": (("aws", "rds"), ("tencent", "tencentdb")),
            "mysql": (("aws", "rds"), ("tencent", "tencentdb")),
            "mongodb": (("aws", "documentdb"), ("tencent", "tencentdb"))}
REAL, UAT = ("a", "b"), ("uat_a", "uat_b")
SEEDS = 200


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _names(engine):
    return (f"migkit-test-gate-{engine[:2]}-src",
            f"migkit-test-gate-{engine[:2]}-dst")


def _remove(engine):
    for n in _names(engine):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _tcp(port, timeout=240):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _run(*argv, check=True, stdin=None):
    got = subprocess.run(list(argv), input=stdin, capture_output=True,
                         text=True)
    if check:
        assert got.returncode == 0, (argv[:6], got.stderr[-800:])
    return got.stdout.strip()


# --- each engine's servers, as its service hands them over -----------------------

class Pg:
    engine = "postgres"

    def up(self):
        for (name, port) in zip(_names(self.engine), PORTS[self.engine]):
            _run("docker", "run", "-d", "--name", name, "--network", NET,
                 "-e", "POSTGRES_PASSWORD=test", "-p",
                 f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                 "wal_level=logical", "-c", "max_replication_slots=20",
                 "-c", "max_wal_senders=20")
        for name, port in zip(_names(self.engine), PORTS[self.engine]):
            assert _tcp(port)
            end = time.time() + 120
            while subprocess.run(["docker", "exec", name, "pg_isready",
                                  "-U", "postgres"],
                                 capture_output=True).returncode:
                assert time.time() < end, f"{name} never answered"
                time.sleep(1)
        time.sleep(2)

    def sql(self, side, sql, db="postgres", user="postgres",
            password="test"):
        name = _names(self.engine)[0 if side == "src" else 1]
        return _run("docker", "exec", "-e", f"PGPASSWORD={password}", name,
                    "psql", "-h", "127.0.0.1", "-U", user, "-d", db, "-At",
                    "-v", "ON_ERROR_STOP=1", "-c", sql)

    def admin(self, side, sql, db="postgres"):
        return self.sql(side, sql, db, "admin", "CHANGE_ME")

    def prepare(self):
        for side in ("src", "dst"):
            # the account the service hands over, and nothing more
            self.sql(side, "create role admin login password 'CHANGE_ME'"
                           " createdb createrole replication;"
                           " grant pg_signal_backend, pg_monitor,"
                           " pg_create_subscription to admin")
        self.admin("src", "create role app login password 'CHANGE_ME'")
        secret = self.sql("src", "select rolpassword from pg_authid where"
                                 " rolname = 'app'")
        # the application's account carried by the DBA, its password's
        # hash and all (a managed admin reads no hash)
        self.sql("dst", f"create role app login password '{secret}'")
        for db in REAL + UAT:
            for side in ("src", "dst"):
                self.admin(side, f"create database {db}")
                self.admin(side, "create table public.orders (id bigserial"
                                 " primary key, tag text not null);"
                                 " create table public.items (id bigserial"
                                 " primary key, name text);"
                                 " grant select, insert, update, delete on"
                                 " all tables in schema public to app;"
                                 " grant usage on all sequences in schema"
                                 " public to app", db)
            self.admin("src", "insert into public.orders (tag) select"
                              f" 'seed'||g from generate_series(1, {SEEDS})"
                              " g; insert into public.items (name) select"
                              " 'item'||g from generate_series(1, 50) g", db)

    def connect(self, side, db):
        import psycopg2
        conn = psycopg2.connect(host="127.0.0.1",
                                port=PORTS[self.engine][0 if side == "src"
                                                        else 1],
                                user="app", password="CHANGE_ME", dbname=db,
                                connect_timeout=3)
        return conn

    def tags(self, side, db):
        return self.admin(side, "select coalesce(string_agg(tag, ' '), '')"
                                " from public.orders", db).split()

    def endpoint(self, side):
        return {"host": "127.0.0.1",
                "port": PORTS[self.engine][0 if side == "src" else 1],
                "user": "admin", "password": "CHANGE_ME"}

    def routes(self):
        a, b = _names(self.engine)
        pa, pb = PORTS[self.engine]
        return {f"127.0.0.1:{pa}": f"{a}:5432", f"127.0.0.1:{pb}": f"{b}:5432"}

    back = True


class My:
    engine = "mysql"

    def up(self):
        for i, (name, port) in enumerate(zip(_names(self.engine),
                                             PORTS[self.engine]), 1):
            _run("docker", "run", "-d", "--name", name, "--network", NET,
                 "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                 f"127.0.0.1:{port}:3306", "mysql:8.4", f"--server-id={i}",
                 "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
        for name, port in zip(_names(self.engine), PORTS[self.engine]):
            assert _tcp(port)
            end = time.time() + 240
            while subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                  "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                  "-e", "select 1"],
                                 capture_output=True).returncode:
                assert time.time() < end, f"{name} never answered"
                time.sleep(2)

    def sql(self, side, sql, user="root", password="test"):
        name = _names(self.engine)[0 if side == "src" else 1]
        return _run("docker", "exec", "-i", name, "mysql", f"-u{user}",
                    f"-p{password}", "-N", "-B", stdin=sql)

    ADMIN = """
create user admin@'%' identified by 'CHANGE_ME';
grant select, insert, update, delete, create, drop, alter, index,
      references, create view, show view, trigger, create routine,
      alter routine, execute, event, lock tables, process, reload,
      replication slave, replication client, create user
      on *.* to admin@'%' with grant option;
grant session_variables_admin, xa_recover_admin, replication_slave_admin
      on *.* to admin@'%';
create database migkit_standin;
delimiter //
create definer = root@'%' procedure migkit_standin.rds_kill(
    in thread bigint) sql security definer
begin
  kill thread;
end//
delimiter ;
grant execute on procedure migkit_standin.rds_kill to admin@'%';
create user app@'%' identified by 'CHANGE_ME';
"""

    def prepare(self):
        for side in ("src", "dst"):
            self.sql(side, self.ADMIN)
            for db in REAL + UAT:
                self.sql(side, f"""
create database {db};
create table {db}.orders (id bigint auto_increment primary key,
                          tag varchar(64) not null);
create table {db}.items (id bigint auto_increment primary key,
                         name varchar(64));
grant select, insert, update, delete on {db}.* to app@'%';
""")
        for db in REAL + UAT:
            self.sql("src", f"""
insert into {db}.orders (tag) with recursive g(n) as (select 1 union all
  select n + 1 from g where n < {SEEDS}) select concat('seed', n) from g;
insert into {db}.items (name) with recursive g(n) as (select 1 union all
  select n + 1 from g where n < 50) select concat('item', n) from g;
""")

    def connect(self, side, db):
        import pymysql
        return pymysql.connect(host="127.0.0.1",
                               port=PORTS[self.engine][0 if side == "src"
                                                       else 1],
                               user="app", password="CHANGE_ME", database=db,
                               connect_timeout=3, read_timeout=10,
                               write_timeout=10)

    def tags(self, side, db):
        return self.sql(side, f"select tag from {db}.orders").split()

    def endpoint(self, side):
        return {"host": "127.0.0.1",
                "port": PORTS[self.engine][0 if side == "src" else 1],
                "user": "admin", "password": "CHANGE_ME"}

    def routes(self):
        a, b = _names(self.engine)
        pa, pb = PORTS[self.engine]
        return {f"127.0.0.1:{pa}": f"{a}:3306", f"127.0.0.1:{pb}": f"{b}:3306"}

    back = True


class Mongo:
    engine = "mongodb"

    def up(self):
        start = ("openssl rand -base64 756 > /tmp/key && chmod 400 /tmp/key"
                 " && chown 999:999 /tmp/key && exec docker-entrypoint.sh"
                 " mongod --replSet rs0 --keyFile /tmp/key --bind_ip_all")
        for name, port in zip(_names(self.engine), PORTS[self.engine]):
            _run("docker", "run", "-d", "--name", name, "--network", NET,
                 "-p", f"127.0.0.1:{port}:27017", "-e",
                 "MONGO_INITDB_ROOT_USERNAME=root", "-e",
                 "MONGO_INITDB_ROOT_PASSWORD=test", "--entrypoint", "bash",
                 "mongo:7", "-c", start)
        for name, port in zip(_names(self.engine), PORTS[self.engine]):
            assert _tcp(port)
            for _ in range(90):
                got = subprocess.run(
                    ["docker", "exec", name, "mongosh", "-u", "root", "-p",
                     "test", "--quiet", "--eval",
                     "try { rs.status().ok } catch (e) { rs.initiate({_id:"
                     " 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
                     ".ok }"], capture_output=True, text=True)
                if got.stdout.strip().endswith("1"):
                    break
                time.sleep(2)
            root = self.client(port, "root", "test")
            for _ in range(60):
                try:
                    if root.admin.command("hello").get("isWritablePrimary"):
                        break
                except Exception:  # noqa: BLE001 - not up yet
                    pass
                time.sleep(1)

    @staticmethod
    def client(port, user, password, db="admin"):
        import pymongo
        return pymongo.MongoClient(
            f"mongodb://{user}:{password}@127.0.0.1:{port}/{db}"
            "?directConnection=true", serverSelectionTimeoutMS=10000)

    def root(self, side):
        return self.client(PORTS[self.engine][0 if side == "src" else 1],
                           "root", "test")

    def prepare(self):
        for side in ("src", "dst"):
            c = self.root(side)
            # what a document store hands its admin: every database's
            # reads and writes, its users, its sessions - no write block,
            # no lock
            c.admin.command("createRole", "sessions", privileges=[
                {"resource": {"cluster": True},
                 "actions": ["killAnySession", "killop", "inprog",
                             "listDatabases", "serverStatus", "top"]}],
                roles=[])
            c.admin.command("createUser", "admin", pwd="CHANGE_ME", roles=[
                "readWriteAnyDatabase", "userAdminAnyDatabase",
                "dbAdminAnyDatabase", "clusterMonitor", "sessions"])
            for db in REAL + UAT:
                c[db].command("createUser", "app", pwd="CHANGE_ME",
                              roles=[{"role": "readWrite", "db": db}])
        src = self.root("src")
        for db in REAL + UAT:
            src[db].orders.insert_many([{"tag": f"seed{i}"}
                                        for i in range(1, SEEDS + 1)])
            src[db].items.insert_many([{"name": f"item{i}"}
                                       for i in range(1, 51)])

    def connect(self, side, db):
        return self.client(PORTS[self.engine][0 if side == "src" else 1],
                           "app", "CHANGE_ME", db)

    def tags(self, side, db):
        return [d["tag"] for d in self.root(side)[db].orders.find({}, {
            "tag": 1})]

    def endpoint(self, side):
        return {"host": "127.0.0.1",
                "port": PORTS[self.engine][0 if side == "src" else 1],
                "user": "admin", "password": "CHANGE_ME",
                "uri_options": "directConnection=true"}

    def routes(self):
        return {}

    back = False


class MongoWriter(harness.Writer):
    """The application on the document store: one document at a time,
    each acknowledged alone, as `harness.Writer` writes a row."""

    def __init__(self, env, route, db):
        super().__init__(lambda where: env.connect(where, db), route)
        self.db = db

    def run(self):
        while not self.halt.is_set():
            where = self.route.read_text().strip()
            tag = uuid.uuid4().hex
            began = time.time()
            try:
                c = self._conn(where)
                c[self.db].orders.insert_one({"tag": tag})
                self.log.append((began, time.time(), where, "ok", tag))
            except Exception as e:  # noqa: BLE001 - the application's error
                self.log.append((began, time.time(), where, "error",
                                 f"{tag} {str(e).strip()[:120]}"))
                old = self.conn.pop(where, None)
                try:
                    old.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.05)
            time.sleep(0.01)


ENGINES = {"postgres": Pg, "mysql": My, "mongodb": Mongo}


@pytest.fixture(params=list(ENGINES))
def pair(request, tmp_path, monkeypatch):
    """One engine's two servers, set up as their services hand them over,
    started for this engine alone and removed after."""
    import migkit.config as cfg
    env = ENGINES[request.param]()
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    _remove(env.engine)
    try:
        env.up()
        env.prepare()
        harness.install(env.routes(), monkeypatch)
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
        env.tmp, env.reports = tmp_path, tmp_path / "reports"
        env.conf = tmp_path / "hops.yaml"
        monkeypatch.setattr(cfg, "CONF", str(env.conf))
        yield env
    finally:
        _remove(request.param)
        subprocess.run(["docker", "network", "rm", NET],
                       capture_output=True)


def _service(engine, side):
    from migkit import managed
    provider, where = SERVICES[engine][0 if side == "src" else 1]
    got = managed.find(provider, where, engine)
    assert got is not None, (provider, where, engine)
    return got


def _hops(env):
    """The real hop and the rehearsal of it, on the same two servers."""
    routes = {db: env.tmp / f"route-{db}" for db in REAL + UAT}
    for r in routes.values():
        r.write_text("src")

    def side(which, ident):
        return {**env.endpoint(which),
                "managed": {"service": _service(env.engine, which).id,
                            "id": ident, "region": "10-test"}}

    def hop(dbs, **more):
        flip = "; ".join(f"printf dst > {routes[d]}" for d in dbs)
        options = {"protect_target": True, "fence_timeout": 120,
                   "state": {"mirror": str(env.tmp / "mirror")},
                   "cutover": {"budget": 60, "flip": flip,
                               "restore_points": ["source", "target"]},
                   **({"reverse": "at_cutover"} if env.back else {}),
                   **more}
        return {"engine": env.engine, "source": side("src", "src-db"),
                "target": side("dst", "dst-db"), "databases": list(dbs),
                "options": options}
    uat = hop(UAT, rehearses={"hop": "prod",
                              "databases": dict(zip(UAT, REAL))})
    uat["goal"] = "rehearse"
    env.conf.write_text(yaml.safe_dump({"hops": {
        "prod": hop(REAL), "uat": uat}}))
    return routes


def _cli(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _background(env, hop, mode):
    """The move run in a process of its own - a stream it starts may run
    on until the cutover tears it down."""
    root = Path(__file__).resolve().parents[1]
    run = {**os.environ, "MIGKIT_CONF": str(env.conf),
           "MIGKIT_REPORTS": str(env.reports),
           "MIGKIT_TEST_ROUTES": ",".join(f"{a}={b}" for a, b in
                                          env.routes().items()),
           "PYTHONPATH": str(root)}
    log = open(env.tmp / f"{hop}-{mode}.log", "w")
    return subprocess.Popen([sys.executable, str(Path(harness.__file__)),
                             "move", hop, "--mode", mode, "--go"], env=run,
                            stdout=log, stderr=subprocess.STDOUT)


def _caught_up(env, dbs, writers=(), timeout=240):
    """Every row the source has, and every write acknowledged so far, on
    the target."""
    def done():
        for d in dbs:
            want = set(env.tags("src", d))
            for w in writers:
                if w.db == d:
                    want |= set(w.acked())
            if not want <= set(env.tags("dst", d)):
                return False
        return True
    return harness.wait_for(done, timeout, 1)


def _writers(env, routes, dbs):
    out = []
    for d in dbs:
        if env.engine == "mongodb":
            w = MongoWriter(env, routes[d], d)
        else:
            w = harness.Writer(lambda where, d=d: env.connect(where, d),
                               routes[d], "public.orders"
                               if env.engine == "postgres" else "orders")
            w.db = d
        w.start()
        out.append(w)
    for w in out:
        assert harness.wait_for(lambda w=w: len(w.acked()) >= 20, 60), \
            w.errors()[-3:]
    return out


class Provider:
    """The providers' API, a stand-in: a restore point of an instance
    taken and said, as `cloudapi.Adapter.restore_point` does."""
    taken = []

    def __init__(self, api, conf):
        self.api, self.conf = api, conf

    def credentials(self):
        return object(), "the stand-in's own"

    def find(self, host, port=None):
        return None

    def instance(self, names=()):
        from migkit import cloudapi
        return cloudapi.Instance(
            provider=self.api, product=self.conf.get("product", ""),
            id=self.conf["id"], parameters={
                n: cloudapi.Param("0", True, False) for n in names},
            group="shared-group", group_shared=True)

    def restore_point(self, name, log):
        from migkit import cloudapi
        Provider.taken.append((self.api, self.conf["id"], name))
        log(f"restore point {name} of {self.conf['id']} ready")
        return cloudapi.RestorePoint(id=name, kind="snapshot",
                                     status="available", ready=True)


def _no_diff(env, hop):
    got, said = None, ""
    for _ in range(30):
        got, said = _cli("check", hop, "--only", "counts,data")
        if got.exit_code == 0:
            return said
        time.sleep(2)
    raise AssertionError(said)


def test_a_managed_pair_goes_the_whole_way_and_ends_equal(pair,
                                                          monkeypatch):
    from migkit import cloudapi, freeze, managed
    env = pair
    monkeypatch.setattr(freeze, "KILL_PROCEDURES",
                        ("migkit_standin.rds_kill",))
    monkeypatch.setattr(cloudapi, "adapter", Provider)
    monkeypatch.setattr(cloudapi, "offered", lambda a, p, w: True)
    Provider.taken = []
    routes = _hops(env)

    # the plan names the pair, and the services' own ways
    got, said = _cli("assess", "prod")
    assert f"the pair {managed.pair_said(_service(env.engine, 'src'), _service(env.engine, 'dst'))}" in said, said[:2000]

    # --- the rehearsal, to its end ---------------------------------------
    got, said = _cli("assess", "uat")
    assert "rehearses prod, the same path" in said, said[:3000]
    uat = _background(env, "uat", "full+cdc")
    try:
        assert _caught_up(env, UAT), "the rehearsal's copy never arrived"
        _no_diff(env, "uat")
        got, said = _cli("move", "uat", "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0 and "cut over:" in said, said[-2000:]
    finally:
        uat.terminate()
        uat.wait(30)

    # --- the forecast the rehearsal gives ----------------------------------
    got, said = _cli("assess", "prod")
    assert "the rehearsal uat" in said and "ended equal" in said, \
        said[:3000]
    assert "forecast" in said, said[:3000]
    # the source's freeze, as its service's admin allows it
    assert "the source's freeze, as its service allows" in said

    # --- the real run: an application writing all the way through ---------
    writers = _writers(env, routes, REAL)
    prod = _background(env, "prod", "full+cdc")
    try:
        assert _caught_up(env, REAL, writers), \
            "the copy and the stream never caught up"
        # the check while the application writes and the stream carries
        # it, fenced at a position the stream has passed
        _no_diff(env, "prod")
        got, said = _cli("move", "prod", "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0 and "cut over:" in said, said[-3000:]
        assert "the pair" in said and "restore point" in said, said[:3000]
        # a restore point of each side through its provider, before the
        # clock
        taken = [(api, ident) for api, ident, _ in Provider.taken]
        assert ("aws", "src-db") in taken and ("tencent", "dst-db") in \
            taken, Provider.taken
        rec = json.loads((env.reports / "prod" / "cutover-runs.jsonl")
                         .read_text().splitlines()[-1])
        assert rec["state"] == "flipped", rec
        assert set(rec["restore_points"]) == {"source", "target"}, rec
        assert rec["freeze_way"] == {"postgres": "pg_database",
                                     "mysql": "my_database",
                                     "mongodb": "mongo_database"}[
            env.engine], rec
        flipped = time.time()
        assert all(harness.wait_for(lambda w=w: len(w.oks_after(flipped))
                                    >= 20, 60) for w in writers)
    finally:
        for w in writers:
            w.stop()
    for w in writers:
        assert not [e for e in w.log if e[2] == "src" and e[3] == "ok"
                    and e[0] > flipped], "the old source took a write"
        have = env.tags("dst", w.db)
        assert len(have) == len(set(have)), "a row written twice"
        assert set(w.acked()) <= set(have), "an acknowledged row is lost"
        assert set(have) <= w.attempted() | {f"seed{i}" for i in range(
            1, SEEDS + 1)}, "rows nobody wrote"

    # --- the stream back, and nothing different at the end -------------------
    if env.back:
        for w in writers:
            assert harness.wait_for(lambda w=w: set(w.acked()) <= set(
                env.tags("src", w.db)), 120), \
                "the stream back never reached the old source"
        _no_diff(env, "prod")
    else:
        # no stream back: the fenced final check of the cutover proved the
        # two equal at the freeze, and the target holds every write since
        assert rec["phases"]["verify"].get("ended"), rec
        for d in REAL:
            assert set(env.tags("src", d)) <= set(env.tags("dst", d)), d

    # --- the point of no return ------------------------------------------
    got, said = _cli("move", "prod", "--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0, said[-2000:]
    runs = [json.loads(x) for x in (env.reports / "prod" /
                                    "cutover-runs.jsonl").read_text()
            .splitlines()]
    assert runs[-1]["state"] == "no-return", runs[-1]
    prod.terminate()
    prod.wait(30)
    # the old source is writable again, and past the point of no return
    # nothing streams back to it
    if env.engine != "mongodb":
        conn = env.connect("src", REAL[0])
        cur = conn.cursor()
        cur.execute("insert into " + ("public.orders" if env.engine ==
                                      "postgres" else "orders")
                    + " (tag) values ('old source, after')")
        conn.commit()
        conn.close()

    # --- the evidence: the last check, equal ---------------------------------
    got, said = _cli("report", "prod")
    assert got.exit_code == 0, said
    where = re.search(r"evidence: (\S+)/evidence\.html", said)
    assert where, said
    doc = json.loads((Path(where.group(1)) / "evidence.json").read_text())
    assert doc["verdict"]["status"] == "same", doc["verdict"]
