"""Db2's own types carried, compared and refused exactly (backlog W6): a
DECFLOAT at every digit it holds, its NaN and infinities and an exponent
past any other engine's decimal; an XML column as its document; VARGRAPHIC
text; a distinct type as the built-in type it is made from - between two
Db2 schemas and into PostgreSQL. Into a DECFLOAT(16) a number of seventeen
digits, and into a VARGRAPHIC(4) text of five UTF-16 units, are refused
before the move.

Before: a DECFLOAT was read through a double; XML had no class, so a
table holding it could not be built; a distinct type was a name no other
engine knew.

IBM's Db2 Community image runs on x86-64 only: under Rosetta in the VM
(`tools/with_docker_lock.py --vm migkit`), where its first start builds
the instance and the database, which takes minutes.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

DB2, PORT, PW = "migkit-test-w6-db2", 16544, "CHANGE_ME-db2"
PG, PG_PORT = "migkit-test-w6-db2pg", 16545
IMAGE = "icr.io/db2_community/db2:latest"

SEED = [
    "create distinct type app.money_t as decimal(12,2) with comparisons",
    "create table app.d (id integer not null primary key, x decfloat(34),"
    " s decfloat(16), doc xml, g vargraphic(10), m app.money_t)",
    "insert into app.d values (1, decfloat('1.50'),"
    " decfloat('1234567890123456'), xmlparse(document '<a><b>1</b></a>'),"
    " 'ไทย', app.money_t(12.50))",
    "insert into app.d values (2, decfloat('NaN'), decfloat('-Infinity'),"
    " null, null, null)",
    "insert into app.d values (3, decfloat('1E+6000'), null, null, null,"
    " null)",
    "insert into app.d (id) values (4)",
    "create table app.narrow (id integer not null primary key,"
    " s decfloat(16), g vargraphic(4))",
]


def _connect():
    import ibm_db_dbi
    return ibm_db_dbi.connect(f"DATABASE=TESTDB;HOSTNAME=127.0.0.1;"
                              f"PORT={PORT};PROTOCOL=TCPIP;UID=db2inst1;"
                              f"PWD={PW};CONNECTTIMEOUT=10;", "", "")


def _sql(*statements):
    conn = _connect()
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = [tuple(r) for r in cur.fetchall()] if cur.description \
                else None
        conn.commit()
        return out
    finally:
        conn.close()


def pg(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (DB2, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", DB2, "--platform",
                        "linux/amd64", "--privileged", "-p",
                        f"127.0.0.1:{PORT}:50000", "-e", "LICENSE=accept",
                        "-h", "db2server", "-e", "DB2INSTANCE=db2inst1",
                        "-e", f"DB2INST1_PASSWORD={PW}", "-e",
                        "DBNAME=testdb", "-e", "TO_CREATE_SAMPLEDB=false",
                        "-e", "REPODB=false", "-e", "ARCHIVE_LOGS=false",
                        "-e", "AUTOCONFIG=false", IMAGE],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                       check=True, capture_output=True)
        end = time.time() + 2400
        while time.time() < end:
            logs = subprocess.run(["docker", "logs", DB2],
                                  capture_output=True, text=True)
            if "Setup has completed" in logs.stdout + logs.stderr:
                try:
                    _sql("values 1")
                    break
                except Exception:  # noqa: BLE001 - not answering yet
                    pass
            time.sleep(10)
        else:
            pytest.fail("Db2 never finished setting up")
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(3)
        for s in SEED:
            _sql(s)
        pg("create database cx", db="postgres")
        pg("create table narrow (id int primary key, s numeric, g text);"
           " insert into narrow values (1, 12345678901234567, 'ab'),"
           " (2, 1, 'ab\U0001D11Ec')")
        yield
    finally:
        for n in (DB2, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _db2():
    return Endpoint(host="127.0.0.1", port=PORT, user="db2inst1",
                    password=PW, options={"database": "TESTDB"})


def _pg():
    return Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")


def _pair(tmp_path, src, dst, db_map):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name=f"{src}2{dst}", engine="hetero",
              source=_db2() if src == "db2" else _pg(),
              target=_db2() if dst == "db2" else _pg(),
              databases=list(db_map), db_map=db_map,
              options={"source_engine": src, "target_engine": dst})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdict(eng, db, table):
    got = [r for r in eng.check_data(db)
           if r.check == "data" and r.scope.endswith(table)]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_decfloat_xml_graphic_and_a_distinct_type_between_two_schemas(
        servers, tmp_path):
    eng = _pair(tmp_path, "db2", "db2", {"app": "app_new"})
    eng.move_table("app", "", "d", 100, Checkpoint(), [].append)
    q = ("select id, char(x), char(s), xmlserialize(doc as varchar(200)),"
         " g, char(m) from {}.d order by id")
    assert _sql(q.format("app_new")) == _sql(q.format("app"))
    # the distinct type, made of a DECIMAL(12,2), is built as one
    assert _sql("select typename from syscat.columns where tabschema ="
                " 'APP_NEW' and tabname = 'D' and colname = 'M'") == \
        [("DECIMAL",)]
    got = _verdict(eng, "app", ".d")
    assert got.status == "ok", got.detail
    _sql("update app_new.d set x = decfloat('1.51') where id = 1")
    try:
        assert _verdict(eng, "app", ".d").status == "diff"
    finally:
        _sql("update app_new.d set x = decfloat('1.50') where id = 1")


def test_decfloat_goes_to_postgresql_at_every_digit(servers, tmp_path):
    eng = _pair(tmp_path, "db2", "postgres", {"app": "cx"})
    eng.move_table("app", "", "d", 100, Checkpoint(), [].append)
    assert pg("select coalesce(x::text, '-') from d where id in (1, 2, 4)"
              " order by id") == "1.50\nNaN\n-"
    assert pg("select length(x::text), left(x::text, 2) from d where"
              " id = 3") == "6001|10"
    assert pg("select s::text from d where id in (1, 2) order by id") == \
        "1234567890123456\n-Infinity"
    assert pg("select g, m from d where id = 1") == "ไทย|12.50"
    assert pg("select doc::text from d where id = 1") == "<a><b>1</b></a>"
    got = _verdict(eng, "app", ".d")
    assert got.status == "ok", got.detail
    pg("update d set s = 1234567890123457 where id = 1")
    assert _verdict(eng, "app", ".d").status == "diff"


def test_digits_and_units_a_db2_column_cannot_hold_are_refused(servers,
                                                                tmp_path):
    eng = _pair(tmp_path, "postgres", "db2", {"cx": "app"})
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "narrow", 100, Checkpoint(),
                       [].append)
    said = str(refused.value)
    assert "narrow.s: 1 row holds more than 16 significant digits" in said, \
        said
    assert "narrow.g: 1 row holds more than 4 UTF-16 code units" in said, \
        said
    assert _sql("select count(*) from app.narrow") == [(0,)]
