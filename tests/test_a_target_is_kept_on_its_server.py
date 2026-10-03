"""The target kept from the application on its own server, and given back
exactly (`protect_target`, backlog BAR-3): ClickHouse, CockroachDB, TiDB,
MongoDB and Redis held by their accounts' rights with migkit's own
account writing on; OpenSearch, where nothing holds a writer and leaves
migkit's, with the writes that are not migkit's named; Oracle by its
grants and its schema's owner locked; and a CockroachDB source frozen at
cutover by the same hold. One container at a time, each removed after.
"""
import contextlib
import socket
import subprocess
import time

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("cutover.read-only-target", ("clickhouse", "cockroachdb", "tidb",
                                  "opensearch", "redis", "mongodb",
                                  "oracle")),
    ("cutover.freeze", "cockroachdb"),
]

pytestmark = needs_docker


@contextlib.contextmanager
def _container(name, port, inner, image, *args, env=(), ready=None,
               wait=180):
    """One server of `image` on 127.0.0.1:`port`, up until `ready()`
    answers, removed after."""
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    cmd = ["docker", "run", "-d", "--name", name, "-p",
           f"127.0.0.1:{port}:{inner}"]
    for e in env:
        cmd += ["-e", e]
    subprocess.run(cmd + [image, *args], check=True, capture_output=True)
    try:
        end = time.time() + wait
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    try:
                        if ready is None or ready():
                            break
                    except Exception:
                        pass
            time.sleep(1)
        else:
            pytest.fail(f"{image} never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _hop(tmp_path, engine, ep, db, **options):
    hop = Hop(name="kept", engine=engine, source=ep, target=ep,
              databases=[db], options={"protect_target": True, **options})
    reports = tmp_path / "reports"

    def report_dir(d=""):
        out = reports / d if d else reports
        out.mkdir(parents=True, exist_ok=True)
        return out
    hop.report_dir = report_dir
    return hop


# --- ClickHouse ----------------------------------------------------------------

CH, CH_PORT = "migkit-test-kept-ch", 16510


def _ch_client(user="default", password="test"):
    import clickhouse_connect
    return clickhouse_connect.get_client(host="127.0.0.1", port=CH_PORT,
                                         username=user, password=password)


def _ch_grants(client, who):
    return sorted(str(r[0]) for r in client.query(
        f"show grants for {who}").result_rows)


def test_a_clickhouse_target_is_held_and_given_back(tmp_path):
    with _container(CH, CH_PORT, 8123, "clickhouse/clickhouse-server:24.8",
                    env=("CLICKHOUSE_PASSWORD=test",
                         "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1"),
                    ready=lambda: _ch_client().command("select 1") == 1):
        root = _ch_client()
        for s in ("create database app",
                  "create table app.t (v Int32) engine = MergeTree order by v",
                  "create user app identified by 'CHANGE_ME'",
                  "grant select, insert on app.* to app",
                  "create role writer",
                  "grant insert on app.* to writer",
                  "create user rep identified by 'CHANGE_ME'",
                  "grant select on app.* to rep", "grant writer to rep",
                  "create user ops identified by 'CHANGE_ME'",
                  "grant all on *.* to ops with grant option"):
            root.command(s)
        before = {u: _ch_grants(root, u) for u in ("app", "rep", "ops",
                                                    "writer")}
        from migkit.engines.clickhouse import ClickHouseEngine
        ep = Endpoint(host="127.0.0.1", port=CH_PORT, user="default",
                      password="test")
        hop = _hop(tmp_path, "clickhouse", ep, "app")
        eng = ClickHouseEngine(hop)
        said = []
        freeze.protect(hop, eng, "app", said.append)
        for user in ("app", "rep", "ops"):
            with pytest.raises(Exception):
                _ch_client(user, "CHANGE_ME").command(
                    "insert into app.t values (1)")
        # read still works, and migkit's own account writes on
        assert _ch_client("app", "CHANGE_ME").command(
            "select count() from app.t") == 0
        root.command("insert into app.t values (7)")
        record = freeze.state(hop, "app", "dst")
        h = freeze.hold_for(hop, eng, "dst")
        assert all(ok for ok, _ in h.kept("app", record)), said
        # a grant made since is read back as not held
        root.command("grant insert on app.t to app")
        assert not all(ok for ok, _ in h.kept("app", record))
        root.command("revoke insert on app.t from app")
        freeze.thaw(hop, eng, "app", said.append)
        assert {u: _ch_grants(root, u) for u in before} == before
        _ch_client("app", "CHANGE_ME").command(
            "insert into app.t values (2)")
        assert freeze.state(hop, "app", "dst") is None


# --- CockroachDB, a target and a source -------------------------------------------

CRDB, CRDB_PORT = "migkit-test-kept-crdb", 16511


def _crdb(statements, user="root", db="app"):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=CRDB_PORT, user=user,
                            dbname=db, sslmode="disable")
    conn.autocommit = True
    try:
        cur = conn.cursor()
        rows = []
        for s in statements:
            cur.execute(s)
            rows = cur.fetchall() if cur.description else []
        return rows
    finally:
        conn.close()


def _crdb_engine(tmp_path, **options):
    from migkit.engines.cockroachdb import CockroachEngine
    ep = Endpoint(host="127.0.0.1", port=CRDB_PORT, user="root",
                  password="", options={"sslmode": "disable"})
    hop = _hop(tmp_path, "cockroachdb", ep, "app", **options)
    return hop, CockroachEngine(hop)


def test_a_cockroachdb_target_is_held_and_a_source_frozen(tmp_path):
    with _container(CRDB, CRDB_PORT, 26257,
                    "cockroachdb/cockroach:latest-v25.2",
                    "start-single-node", "--insecure",
                    ready=lambda: _crdb(["select 1"], db="defaultdb")):
        _crdb(["create database app"], db="defaultdb")
        _crdb(["create table t (v int primary key)", "create user app",
               "grant connect on database app to app",
               "grant select, insert, update on table t to app"])
        hop, eng = _crdb_engine(tmp_path)
        before = _crdb(["select grantee, privilege_type from"
                        " information_schema.table_privileges where"
                        " table_name = 't' order by 1, 2"])
        freeze.protect(hop, eng, "app", lambda m: None)
        with pytest.raises(Exception):
            _crdb(["insert into t values (1)"], user="app")
        _crdb(["insert into t values (2)"])
        h = freeze.hold_for(hop, eng, "dst")
        assert all(ok for ok, _ in h.kept("app", freeze.state(hop, "app",
                                                               "dst")))
        freeze.thaw(hop, eng, "app", lambda m: None)
        assert _crdb(["select grantee, privilege_type from"
                      " information_schema.table_privileges where"
                      " table_name = 't' order by 1, 2"]) == before
        _crdb(["insert into t values (3)"], user="app")
        # the same hold freezes a source: a session of the application's
        # holding a transaction open is ended, and read back as none
        import psycopg2
        held = psycopg2.connect(host="127.0.0.1", port=CRDB_PORT,
                                user="app", dbname="app", sslmode="disable")
        held.cursor().execute("insert into t values (4)")
        src = freeze.hold_for(hop, eng, "src")
        got = freeze.choose(hop, eng, ["app"], "database",
                            need=(freeze.OURS,))
        assert got.rung is not None and got.rung.name == "pgw_grants", got
        # before the hold, the read back says not held
        assert [ok for ok, _ in src.held("app", "pgw_grants")
                if not ok], "a grant left and a transaction open"
        src.hold(["app"], "pgw_grants", lambda m: None)
        proof = src.held("app", "pgw_grants")
        assert all(ok for ok, _ in proof), proof
        with pytest.raises(Exception):
            held.commit()
        freeze.thaw(hop, eng, "app", lambda m: None, side="src")
        assert _crdb(["select count(*) from t where v = 4"]) == [(0,)]
        _crdb(["insert into t values (5)"], user="app")


# --- TiDB ------------------------------------------------------------------------

TIDB, TIDB_PORT = "migkit-test-kept-tidb", 16512


def _tidb(sql, user="root", password=""):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=TIDB_PORT, user=user,
                           password=password, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def test_a_tidb_target_is_held_by_mysqls_way(tmp_path):
    with _container(TIDB, TIDB_PORT, 4000, "pingcap/tidb:v8.5.1",
                    ready=lambda: _tidb("select 1")):
        for s in ("create database app", "create table app.t (v int)",
                  "create user app identified by 'CHANGE_ME'",
                  "grant select, insert on app.* to app"):
            _tidb(s)
        from migkit.engines.tidb import TiDBEngine
        ep = Endpoint(host="127.0.0.1", port=TIDB_PORT, user="root",
                      password="")
        hop = _hop(tmp_path, "tidb", ep, "app", app_roles=["app"])
        eng = TiDBEngine(hop)
        freeze.protect(hop, eng, "app", lambda m: None)
        with pytest.raises(Exception):
            _tidb("insert into app.t values (1)", "app", "CHANGE_ME")
        assert _tidb("select count(*) from app.t", "app", "CHANGE_ME")
        _tidb("insert into app.t values (2)")
        freeze.thaw(hop, eng, "app", lambda m: None)
        _tidb("insert into app.t values (3)", "app", "CHANGE_ME")
        assert freeze.state(hop, "app", "dst") is None


# --- OpenSearch: named, not held -------------------------------------------------

OS, OS_PORT = "migkit-test-kept-os", 16513


def _os(method, path, body=None):
    import json
    import urllib.request
    req = urllib.request.Request(
        f"http://127.0.0.1:{OS_PORT}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as got:
        return json.loads(got.read() or b"{}")


def test_an_opensearch_targets_writes_that_are_not_migkits_are_named(
        tmp_path):
    with _container(OS, OS_PORT, 9200, "opensearchproject/opensearch:2.19.1",
                    env=("discovery.type=single-node",
                         "DISABLE_SECURITY_PLUGIN=true",
                         "DISABLE_INSTALL_DEMO_CONFIG=true",
                         "OPENSEARCH_JAVA_OPTS=-Xms384m -Xmx384m"),
                    ready=lambda: _os("GET", "/_cluster/health")["status"]
                    in ("green", "yellow")):
        _os("PUT", "/app.t", {"settings": {"number_of_replicas": 0}})
        from migkit.engines.opensearch import OpenSearchEngine
        hop = _hop(tmp_path, "opensearch",
                   Endpoint(host="127.0.0.1", port=OS_PORT), "app")
        eng = OpenSearchEngine(hop)
        freeze.protect(hop, eng, "app", lambda m: None)
        record = freeze.state(hop, "app", "dst")
        assert record["way"] == "os_named"
        # migkit's own writes, through its own writer, are counted
        eng._bulk("dst", ({"_op_type": "index", "_index": "app.t",
                           "_id": str(i), "_source": {"v": i}}
                          for i in range(5)))
        eng._bulk("dst", ({"_op_type": "delete", "_index": "app.t",
                           "_id": "missing"},))
        h = freeze.hold_for(hop, eng, "dst")
        got = h.kept("app", record)
        assert all(ok for ok, _ in got), got
        # the application's one write is named
        _os("PUT", "/app.t/_doc/x?refresh=true", {"v": 99})
        got = h.kept("app", record)
        assert not got[0][0] and "1 writes that are not migkit's" in \
            got[0][1], got
        freeze.thaw(hop, eng, "app", lambda m: None)
        assert not (hop.report_dir() / freeze.OURS_FILE).exists()


# --- Redis -----------------------------------------------------------------------

REDIS, REDIS_PORT = "migkit-test-kept-redis", 16514


def test_a_redis_targets_accounts_are_refused_but_migkits(tmp_path):
    import redis
    with _container(REDIS, REDIS_PORT, 6379, "redis:7.2",
                    ready=lambda: redis.Redis(port=REDIS_PORT).ping()):
        root = redis.Redis(port=REDIS_PORT)
        root.execute_command("ACL", "SETUSER", "migkit", "on", ">CHANGE_ME",
                             "~*", "&*", "+@all")
        root.execute_command("ACL", "SETUSER", "app", "on", ">CHANGE_ME",
                             "~*", "+@read", "+@write")
        before = sorted(root.execute_command("ACL", "LIST"))
        from migkit.engines.redis import RedisEngine
        ep = Endpoint(host="127.0.0.1", port=REDIS_PORT, user="migkit",
                      password="CHANGE_ME")
        hop = _hop(tmp_path, "redis", ep, "0")
        eng = RedisEngine(hop)
        freeze.protect(hop, eng, "0", lambda m: None)
        app = redis.Redis(port=REDIS_PORT, username="app",
                          password="CHANGE_ME")
        with pytest.raises(redis.exceptions.NoPermissionError):
            app.set("k", "1")
        redis.Redis(port=REDIS_PORT, username="migkit",
                    password="CHANGE_ME").set("k", "2")
        h = freeze.hold_for(hop, eng, "dst")
        assert all(ok for ok, _ in h.kept("0", freeze.state(hop, "0",
                                                            "dst")))
        freeze.thaw(hop, eng, "0", lambda m: None)
        assert sorted(root.execute_command("ACL", "LIST")) == before
        app.set("k", "3")


# --- MongoDB ---------------------------------------------------------------------

MONGO, MONGO_PORT = "migkit-test-kept-mongo", 16515


def _mongo(user, password, db="admin"):
    import pymongo
    return pymongo.MongoClient(
        f"mongodb://{user}:{password}@127.0.0.1:{MONGO_PORT}/{db}"
        "?directConnection=true", serverSelectionTimeoutMS=5000)


def test_a_mongodb_targets_accounts_read_but_do_not_write(tmp_path):
    import pymongo
    with _container(MONGO, MONGO_PORT, 27017, "mongo:7",
                    env=("MONGO_INITDB_ROOT_USERNAME=root",
                         "MONGO_INITDB_ROOT_PASSWORD=test"),
                    ready=lambda: _mongo("root", "test").admin.command(
                        "ping")):
        root = _mongo("root", "test")
        root.app.command("createUser", "app", pwd="CHANGE_ME",
                         roles=[{"role": "readWrite", "db": "app"}])
        from migkit.engines.mongodb import MongoEngine
        ep = Endpoint(host="127.0.0.1", port=MONGO_PORT, user="root",
                      password="test")
        hop = _hop(tmp_path, "mongodb", ep, "app")
        eng = MongoEngine(hop)
        freeze.protect(hop, eng, "app", lambda m: None)
        app = _mongo("app", "CHANGE_ME", "app")
        with pytest.raises(pymongo.errors.OperationFailure):
            app.app.t.insert_one({"v": 1})
        assert app.app.t.count_documents({}) == 0
        root.app.t.insert_one({"v": 2})
        freeze.thaw(hop, eng, "app", lambda m: None)
        app.app.t.insert_one({"v": 3})
        roles = root.app.command("usersInfo", "app")["users"][0]["roles"]
        assert roles == [{"role": "readWrite", "db": "app"}]


# --- Oracle ----------------------------------------------------------------------

ORA, ORA_PORT = "migkit-test-kept-ora", 16516


def _ora(user, password, sql=None):
    import oracledb
    conn = oracledb.connect(user=user, password=password,
                            dsn=f"127.0.0.1:{ORA_PORT}/FREEPDB1")
    if sql is None:
        return conn
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


@pytest.mark.vm
def test_an_oracle_target_is_held_by_its_grants_and_its_owner_locked(
        tmp_path):
    with _container(ORA, ORA_PORT, 1521,
                    "gvenzl/oracle-free:slim-faststart",
                    env=("ORACLE_PASSWORD=test", "APP_USER=APP",
                         "APP_USER_PASSWORD=CHANGE_ME"),
                    ready=lambda: _ora("system", "test").close() is None,
                    wait=600):
        _ora("APP", "CHANGE_ME", "create table t (v number)")
        _ora("system", "test",
             "create user bob identified by \"CHANGE_ME\"")
        _ora("system", "test", "grant create session to bob")
        _ora("APP", "CHANGE_ME", "grant select, insert on t to bob")
        from migkit.engines.oracle import OracleEngine
        ep = Endpoint(host="127.0.0.1", port=ORA_PORT, user="system",
                      password="test", options={"service": "FREEPDB1"})
        hop = _hop(tmp_path, "oracle", ep, "app")
        eng = OracleEngine(hop)
        freeze.protect(hop, eng, "app", lambda m: None)
        with pytest.raises(Exception):
            _ora("bob", "CHANGE_ME", "insert into app.t values (1)")
        with pytest.raises(Exception):
            _ora("APP", "CHANGE_ME")
        _ora("system", "test", "insert into app.t values (2)")
        freeze.thaw(hop, eng, "app", lambda m: None)
        _ora("bob", "CHANGE_ME", "insert into app.t values (3)")
        _ora("APP", "CHANGE_ME", "insert into t values (4)")
