"""Brand detection through the engines, against the forks themselves.

The unit test pins how the replies are read. This one pins that the engines
actually ask, and that the answer reaches `assess` - because the failure being
fixed is not a wrong string, it is a report that says the two sides match.
"""
import socket
import subprocess
import time

import pytest


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]

REDIS, VALKEY = "migkit-test-var-redis", "migkit-test-var-valkey"
CRDB = "migkit-test-var-crdb"
MARIA = "migkit-test-var-maria"
REDIS_PORT, VALKEY_PORT = 16389, 16388
CRDB_PORT = 26258
MARIA_PORT = 13389


def _wait(port, timeout=150):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _hop(engine, sport, dport, user="postgres", password="test", db=None):
    from migkit.config import Endpoint, Hop
    return Hop(name="v", engine=engine,
               source=Endpoint(host="127.0.0.1", port=sport, user=user,
                               password=password),
               target=Endpoint(host="127.0.0.1", port=dport, user=user,
                               password=password),
               db_map={db: db} if db else {})


# --------------------------------------------------------------- redis fork

@pytest.fixture(scope="module")
def redis_pair():
    for n, p, img in ((REDIS, REDIS_PORT, "redis:7"),
                      (VALKEY, VALKEY_PORT, "valkey/valkey:8")):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:6379", img], check=True, capture_output=True)
    for p in (REDIS_PORT, VALKEY_PORT):
        assert _wait(p)
    yield
    for n in (REDIS, VALKEY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


@pytest.fixture(scope="module")
def redis_engine(redis_pair):
    from migkit.engines.redis import RedisEngine
    eng = RedisEngine(_hop("redis", REDIS_PORT, VALKEY_PORT, user="",
                           password=""))
    # an engine that cannot reach either side would let every assertion below
    # pass for the wrong reason
    assert eng._client("src").ping() and eng._client("dst").ping()
    return eng


def test_redis_and_valkey_are_told_apart_through_the_engine(redis_engine):
    src, dst = redis_engine._brands()
    assert (src.name, dst.name) == ("redis", "valkey")
    assert dst.version.startswith("8."), dst.version


def test_valkeys_reported_redis_version_is_not_what_migkit_calls_its_version(
        redis_engine):
    """Measured on the container: Valkey 8 answers `redis_version:7.2.4`.
    The engine used to return exactly that, so a Redis 7.2.4 source and this
    target compared as an exact match."""
    import redis as _r
    raw = _r.Redis(host="127.0.0.1", port=VALKEY_PORT,
                   decode_responses=True).info("server")
    assert raw["redis_version"].startswith("7."), raw["redis_version"]
    sv, dv = redis_engine._server_versions()
    assert dv != raw["redis_version"]
    assert dv == raw["valkey_version"]


def test_assess_refuses_to_pass_the_version_row_across_two_brands(
        redis_engine):
    """The regression this whole file exists for: two different pieces of
    software behind one protocol must not produce a version row that reads
    as agreement."""
    items = redis_engine.assess()
    ver = [i for i in items if i["item"] == "server version match"]
    assert ver, [i["item"] for i in items]
    assert ver[0]["level"] == "warn", ver[0]
    assert "different software" in ver[0]["detail"], ver[0]

    pairing = [i for i in items if i["item"] == "brand pairing"]
    assert pairing and pairing[0]["level"] == "warn", items
    brands = {i["item"]: i for i in items if i["item"].endswith(" brand")}
    assert "redis" in brands["source brand"]["detail"]
    assert "valkey" in brands["target brand"]["detail"]


# ----------------------------------------------------------- postgres fork

@pytest.fixture(scope="module")
def crdb():
    """CockroachDB alone; the PostgreSQL side is the shared session pair."""
    subprocess.run(["docker", "rm", "-f", CRDB], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CRDB, "-p",
                    f"{CRDB_PORT}:26257", "cockroachdb/cockroach:v23.2.5",
                    "start-single-node", "--insecure"],
                   check=True, capture_output=True)
    assert _wait(CRDB_PORT)
    yield CRDB_PORT
    subprocess.run(["docker", "rm", "-f", CRDB], capture_output=True)


@pytest.fixture(scope="module")
def pg_engine(pg_pair, crdb, tmp_path_factory):
    from migkit.engines.postgres import PostgresEngine
    hop = _hop("postgres", pg_pair["src"], crdb, db="postgres")
    hop.target.user = "root"
    hop.target.password = ""
    hop.report_dir = lambda db=None: tmp_path_factory.mktemp("var")
    eng = PostgresEngine(hop)
    for _ in range(40):
        try:
            if eng._psql("dst", "postgres", "select 1").strip() == "1":
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        pytest.fail("cockroach never answered a query")
    return eng


def test_a_cockroach_target_is_not_reported_as_postgres(pg_engine):
    src, dst = pg_engine._brands()
    assert src.name == "postgres", src
    assert dst.name == "cockroachdb", dst
    assert "v23" in dst.version, dst.version


def test_cockroach_reports_a_postgres_version_that_is_not_its_own(pg_engine):
    """`show server_version` answers 13.0.0 whatever release it is, which is
    the number every version comparison in migkit was built on."""
    assert pg_engine._psql("dst", "postgres",
                           "show server_version").strip().startswith("13.")


def test_the_change_marker_pieces_are_measured_dead_on_cockroach(pg_engine):
    """The limit recorded in `variants.py` is checked against the server here
    rather than trusted: `pg_stat_all_tables` is present and empty, and every
    `relfilenode` is 0. A marker built from those is the same string forever,
    so every table would skip its scan and report as proved equal."""
    pg_engine._psql("dst", "postgres",
                    "create table if not exists marker_probe (id int primary"
                    " key, v int); insert into marker_probe values (1, 1)"
                    " on conflict (id) do update set v = marker_probe.v + 1")
    stats = pg_engine._psql("dst", "postgres",
                            "select count(*) from pg_catalog.pg_stat_all_tables"
                            ).strip()
    node = pg_engine._psql("dst", "postgres",
                           "select distinct relfilenode::text from pg_class"
                           ).strip()
    assert stats == "0", stats
    assert set(node.split()) == {"0"}, node

    from migkit import variants as v
    why = pg_engine._brands()[1].cannot(v.CHANGE_MARKER)
    assert "relfilenode" in why and "skip" in why


def test_assess_names_the_brand_before_it_compares_versions(pg_engine):
    items = pg_engine.assess()
    order = [i["item"] for i in items]
    assert order.index("target brand") < order.index("server version match")
    ver = [i for i in items if i["item"] == "server version match"][0]
    assert ver["level"] == "warn", ver


# -------------------------------------------------------------- mysql fork

@pytest.fixture(scope="module")
def maria_engine():
    subprocess.run(["docker", "rm", "-f", MARIA], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MARIA, "-e",
                    "MARIADB_ROOT_PASSWORD=test", "-p", f"{MARIA_PORT}:3306",
                    "mariadb:11"], check=True, capture_output=True)
    assert _wait(MARIA_PORT)
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", MARIA_PORT, MARIA_PORT, user="root",
                           password="test"))
    for _ in range(45):
        try:
            if eng._q("src", "select 1")[0][0] == 1:
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        subprocess.run(["docker", "rm", "-f", MARIA], capture_output=True)
        pytest.fail("mariadb never answered a query")
    yield eng
    subprocess.run(["docker", "rm", "-f", MARIA], capture_output=True)


def test_mariadb_is_named_rather_than_counted_as_mysql(maria_engine):
    src, _ = maria_engine._brands()
    assert src.name == "mariadb", src
    assert "MariaDB" in src.version, src.version


def test_the_recorded_gtid_finding_holds_against_the_server(maria_engine):
    """`show variables like 'gtid_mode'` returns zero rows here - not OFF,
    nothing - and migkit's replication plan reads `gtid[0][1] == 'ON'` off
    that result. An empty result is falsy, so it reads as "GTID is off" and
    the plan is generated from file/pos coordinates without a word."""
    assert maria_engine._q("src", "show variables like 'gtid_mode'") == ()
    assert maria_engine._q("src", "show variables like 'gtid_current_pos'")

    from migkit import variants as v
    why = maria_engine._brands()[0].cannot(v.CDC_POSITION)
    assert "gtid_current_pos" in why, why
