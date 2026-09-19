"""Two different engines answering the same three questions.

`test_canon_cross_engine.py` proves the rendering agrees when the SQL is
written by hand. This one proves the engines themselves ask for it: the table
list, the declared types and the digest all come back through the engine
objects, which is what a cross-engine comparison will actually call.

The digest is computed inside each server. Only a count and a number cross the
network, whatever the size of the table - which is the difference between this
and a validator that fetches both sides' rows to a middle box to compare them.
"""
import socket
import subprocess
import time

import pytest

from migkit import canon

MY, PG = "migkit-test-neutral-my", "migkit-test-neutral-pg"
MY_PORT, PG_PORT = 13385, 15485


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

# the same logical table on both sides, with the types each engine spells it
DDL_MY = """create table shape (
    id bigint primary key,
    name varchar(50),
    amount decimal(12,4),
    ratio double,
    flag tinyint(1),
    made datetime(6))"""
DDL_PG = """create table shape (
    id bigint primary key,
    name varchar(50),
    amount numeric(12,4),
    ratio double precision,
    flag boolean,
    made timestamp(6))"""
ROWS_MY = ("(1,'café',-0.05,1e20,1,'2026-01-01 00:00:00'),"
           "(2,'',0.0001,0.000001,0,'2026-09-18 13:45:06.123456'),"
           "(3,null,null,null,null,null)")
ROWS_PG = ("(1,'café',-0.05,1e20,true,'2026-01-01 00:00:00'),"
           "(2,'',0.0001,0.000001,false,'2026-09-18 13:45:06.123456'),"
           "(3,null,null,null,null,null)")


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _hop(engine, port, user, password, db):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password=password)
    return Hop(name="n", engine=engine, source=ep, target=ep,
               db_map={db: db})


@pytest.fixture(scope="module")
def my_engine():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    assert _wait(MY_PORT)
    # seeded through the client rather than through the engine: the engine's
    # connection does not autocommit, so an insert issued that way left the
    # table empty and every comparison below would have been over nothing
    def cli(sql, db=None):
        cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
               "--default-character-set=utf8mb4", "-N", "-B"]
        if db:
            cmd += ["-D", db]
        r = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)
        return r
    for _ in range(60):
        if cli("create database if not exists nx").returncode == 0:
            break
        time.sleep(2)
    else:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
        pytest.fail("mysql never accepted a connection")
    assert cli(DDL_MY, "nx").returncode == 0
    assert cli(f"insert into shape values {ROWS_MY}", "nx").returncode == 0
    assert cli("select count(*) from shape", "nx").stdout.strip() == "3"

    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", MY_PORT, "root", "test", "nx"))
    eng._cli = cli
    yield eng
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture(scope="module")
def pg_engine(tmp_path_factory):
    # deliberately not the shared `pg_pair`: its autouse cleaner drops every
    # public table before each test, and this module's table has to survive
    # the whole file
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PG_PORT)
    from migkit.engines.postgres import PostgresEngine
    hop = _hop("postgres", PG_PORT, "postgres", "test", "postgres")
    hop.report_dir = lambda db=None: tmp_path_factory.mktemp("neutral")
    eng = PostgresEngine(hop)
    for _ in range(40):
        try:
            if eng._psql("src", "postgres", "select 1").strip() == "1":
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
        pytest.fail("postgres never accepted a connection")
    eng._psql("src", "postgres", DDL_PG)
    eng._psql("src", "postgres", f"insert into shape values {ROWS_PG}")
    assert eng._psql("src", "postgres",
                     "select count(*) from shape").strip() == "3"
    yield eng
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def _cols(eng, side, db, table):
    """Declared types turned into canon classes, refusing anything unmapped."""
    out = []
    for name, declared in eng.neutral_columns(side, db, table):
        cls, why = canon.comparable(eng.CANON_ENGINE, declared)
        assert cls, (eng.CANON_ENGINE, name, declared, why)
        out.append((name, cls))
    return out


def test_each_engine_lists_the_table_it_was_given(my_engine, pg_engine):
    assert "shape" in my_engine.neutral_tables("src", "nx")
    assert "public.shape" in pg_engine.neutral_tables("src", "postgres")


def test_the_declared_types_differ_and_both_map(my_engine, pg_engine):
    """If the two sides ever start reporting the same type names, the digest
    agreement below stops being evidence of anything."""
    my = dict(my_engine.neutral_columns("src", "nx", "shape"))
    pg = dict(pg_engine.neutral_columns("src", "postgres", "public.shape"))
    assert my["flag"].startswith("tinyint") and pg["flag"] == "boolean"
    assert my["made"].startswith("datetime")
    assert pg["made"].startswith("timestamp")
    assert my["ratio"] == "double" and pg["ratio"] == "double precision"


def test_the_declared_type_carries_its_own_numbers(my_engine, pg_engine):
    """`information_schema.data_type` answers `character varying` and keeps
    the 50 somewhere else, so a target built from it came out wider than the
    source. `format_type` answers with the number attached, which is what
    `canon.params` reads."""
    from migkit import canon
    pg = dict(pg_engine.neutral_columns("src", "postgres", "public.shape"))
    my = dict(my_engine.neutral_columns("src", "nx", "shape"))
    assert canon.params(pg["name"]) == (50,), pg["name"]
    assert canon.params(pg["amount"]) == (12, 4), pg["amount"]
    assert canon.params(pg["made"]) == (6,), pg["made"]
    assert canon.params(my["name"]) == (50,), my["name"]
    assert canon.params(my["amount"]) == (12, 4), my["amount"]


def test_the_count_and_digest_agree_across_the_two_engines(my_engine,
                                                           pg_engine):
    a = my_engine.neutral_digest("src", "nx", "shape",
                                 _cols(my_engine, "src", "nx", "shape"))
    b = pg_engine.neutral_digest(
        "src", "postgres", "public.shape",
        _cols(pg_engine, "src", "postgres", "public.shape"))
    assert a == b, (a, b)
    assert a[0] == 3
    assert a[1] not in ("0", "")


def test_a_changed_row_on_one_side_moves_only_that_sides_digest(my_engine,
                                                                pg_engine):
    """Otherwise the agreement above could come from a digest that ignores
    the data."""
    pg_cols = _cols(pg_engine, "src", "postgres", "public.shape")
    before = pg_engine.neutral_digest("src", "postgres", "public.shape",
                                      pg_cols)
    pg_engine._psql("src", "postgres",
                    "update shape set name = 'cafe' where id = 1")
    try:
        after = pg_engine.neutral_digest("src", "postgres", "public.shape",
                                         pg_cols)
        assert after != before
        assert after[0] == before[0] == 3      # a value moved, not a row
        my = my_engine.neutral_digest("src", "nx", "shape",
                                      _cols(my_engine, "src", "nx", "shape"))
        assert my == before and my != after
    finally:
        pg_engine._psql("src", "postgres",
                        "update shape set name = 'café' where id = 1")


def test_a_null_row_is_not_silently_equal_to_an_empty_one(my_engine):
    """Row 3 is all NULL and row 2 holds an empty string. The length prefix
    is what keeps those apart - a NULL is marked with a length that is not a
    number, so no literal can impersonate it."""
    cols = _cols(my_engine, "src", "nx", "shape")
    both = my_engine.neutral_digest("src", "nx", "shape", cols)[1]
    my_engine._cli("update shape set name = '' where id = 3", "nx")
    try:
        changed = my_engine.neutral_digest("src", "nx", "shape", cols)[1]
        assert changed != both
    finally:
        my_engine._cli("update shape set name = null where id = 3", "nx")
    assert my_engine.neutral_digest("src", "nx", "shape", cols)[1] == both


def test_an_engine_without_the_contract_says_so_rather_than_answering():
    """An empty table list would read as "compared, nothing wrong"."""
    from migkit.engines.redis import RedisEngine
    eng = RedisEngine(_hop("redis", 1, "", "", "0"))
    assert eng.CANON_ENGINE == ""
    for call in (lambda: eng.neutral_tables("src", "0"),
                 lambda: eng.neutral_columns("src", "0", "t"),
                 lambda: eng.neutral_digest("src", "0", "t", [])):
        with pytest.raises(NotImplementedError) as e:
            call()
        assert "not clean" in str(e.value)
