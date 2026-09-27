"""The MySQL table copier and the shared copier writing into MySQL through
the server's bulk load, where the server takes one.

Measured before: rows went in as multi-row inserts, and the load was left
out because the client setting it needs lets a server read any file the
client can. The connection now answers a request for a file only with the
rows of the statement that asked, under a name made for it. Measured, a
million rows copied by one range: 15.6s as inserts, 10.9s loaded with the
next batch read while one is written; with four ranges in processes, 7.3s
and 6.8s on a 2-CPU server. A YEAR of 0 went in as 2000 the first time the
load ran (the text "0" is the year 2000, the number 0 is 0000) and the
range read back different, which is why the second copy of a range is
always made with inserts.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, PORT = "migkit-test-mysql-load", 15872

TYPES = """create table t (id bigint primary key, ti tinyint,
 ui int unsigned, bu bigint unsigned, f float, d double,
 dec1 decimal(30,10), dt date, dtm datetime(6), ts timestamp(3) null,
 tm time(6), yr year, c char(5), v varchar(200), tx mediumtext, bl blob,
 bn binary(4), vb varbinary(20), bt bit(10), en enum('a','b','c d'),
 st set('x','y','z'), js json, g point,
 l1 varchar(20) character set latin1)"""

ROWS = r"""set session sql_mode='';
insert into t values
(1, -128, 4294967295, 18446744073709551615, 1.5e-5,
 2.2250738585072014e-308, 12345678901234567890.0123456789, '0000-00-00',
 '0000-00-00 00:00:00', null, '-838:59:59.000000', 2155, 'ab',
 'tab\there\nnewline\\back \\N null-looking, comma', repeat('x', 70000),
 unhex('00ff5c095c4e0a'), unhex('00000000'), '', b'1010101010', 'c d',
 'x,z', '{"a": [1, "t\\tb"], "b": null}', ST_GeomFromText('POINT(1 2)'),
 'café'),
(2, null, null, null, null, null, null, null, null, null, null, null, null,
 null, null, null, null, null, null, null, null, null, null, null),
(3, 0, 0, 0, -0.0, 1e308, -0.0000000001, '2024-02-29',
 '2024-02-29 23:59:59.999999', '2024-01-01 00:00:00.123', '-00:30:00.5',
 0, '', '', '', '', unhex('5c4e0000'), unhex('5c4e'), b'0', 'a', '', '[]',
 ST_GeomFromText('POINT(0 0)'), ''),
(4, 1, 1, 1, 3.4e38, -1.7976931348623157e308, 0, '9999-12-31',
 '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07.999', '838:59:59',
 1901, 'é😀', '😀 emoji \\ \t', 'multi\r\nline', null, null, null, null,
 'b', 'x,y,z', '"str"', null, 'plain')"""


def my(sql, db=""):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"] + ([db] if db else []),
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"{PORT}:3306", "-e", "MYSQL_ROOT_PASSWORD=test",
                    "mysql:8.4", "--local-infile=1"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine(tmp_path, src="s", dst="s2", workers=1):
    from migkit import ranges
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="ld", engine="mysql", source=ep, target=ep,
              databases=[src], db_map={src: dst}, workers=workers)
    hop.report_dir = lambda db=None: tmp_path
    ranges.active = ranges.Slots(workers)
    return MySQLEngine(hop)


def _fingerprint(db):
    cols = my("select group_concat(column_name order by ordinal_position)"
              " from information_schema.columns where table_schema ="
              f" '{db}' and table_name = 't'").split(",")
    exprs = ",".join(f"coalesce(hex(`{c}`), '<null>')" if c != "g" else
                     "coalesce(hex(st_asbinary(g)), '<null>')" for c in cols)
    return my(f"select count(*), md5(group_concat(concat_ws('|', {exprs})"
              " order by id separator '#')) from t", db)


def _counting(monkeypatch):
    from migkit.engines import mysql
    real, used = mysql._load, []

    def counted(*a, **k):
        got = real(*a, **k)
        # what the server took, counted once it has
        used.append(got)
        return got
    monkeypatch.setattr(mysql, "_load", counted)
    return used


@pytest.fixture
def typed(server):
    my("drop database if exists s; drop database if exists s2;"
       " create database s; create database s2;")
    my(TYPES, "s")
    my(TYPES, "s2")
    my(ROWS, "s")


def _move(eng, tmp_path, said=None):
    from migkit.cli import _Checkpoint
    eng.move_table("s", "t", "", 500_000, _Checkpoint(tmp_path / "m.json"),
                   (said if said is not None else []).append)


def test_every_type_lands_as_an_insert_would_land_it(typed, tmp_path,
                                                     monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "LOAD_FROM", 1)
    used = _counting(monkeypatch)
    _move(_engine(tmp_path), tmp_path)
    # every row through the load: one first, to learn their size, then
    # the rest
    assert sum(used) == 4 and used[0] == 1, used
    assert _fingerprint("s2") == _fingerprint("s")
    # and a YEAR of 0 is 0000, not the 2000 the text "0" loads as
    assert my("select yr from t where id = 3", "s2") == "0000"


def test_the_server_is_sent_only_the_rows_it_was_offered(server, typed,
                                                        tmp_path):
    """A load naming any other file - a server asking for what it likes is
    the attack the client setting opens - is sent nothing and raised."""
    import pymysql
    eng = _engine(tmp_path)
    conn = eng._conn("dst")
    try:
        assert hasattr(conn, "feed")
        with conn.cursor() as cur:
            cur.execute("create table s2.stolen (line text)")
            for armed in (None, (b"rows-offered", b"1\n")):
                conn.feed = armed
                with pytest.raises(pymysql.err.OperationalError,
                                   match="did not offer; it was sent"
                                         " nothing"):
                    cur.execute("load data local infile '/etc/hosts' into"
                                " table s2.stolen")
                # the connection is still in step with the server
                cur.execute("select count(*) from s2.stolen")
                assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_a_value_the_column_cannot_hold_stops_as_an_insert_stops(
        typed, tmp_path, monkeypatch):
    """A load takes a value too long for its column with a warning and
    goes on; the range is written again as inserts, which refuse it."""
    import pymysql

    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "LOAD_FROM", 1)
    my("alter table t modify v varchar(3)", "s2")
    used, said = _counting(monkeypatch), []
    with pytest.raises(pymysql.err.DataError, match="Data too long"):
        _move(_engine(tmp_path), tmp_path, said)
    # the load was made - of the first row, which is the one too long -
    # and not taken
    assert used == [] and any("the bulk load was not taken as it was (1 of"
                              " 1 rows taken, 1 warnings)" in m
                              for m in said), said
    assert my("select count(*) from t", "s2") == "0"


def test_a_range_that_reads_back_different_is_copied_again_as_inserts(
        typed, tmp_path, monkeypatch):
    from migkit.engines import mysql
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "LOAD_FROM", 1)
    real = mysql._load_text

    def wrong(conn, rows, width, types=()):
        kinds, data = real(conn, rows, width, types)
        # the load mis-renders a value, as it did YEAR 0
        return kinds, data.replace(b"\tab\t", b"\tzz\t")
    monkeypatch.setattr(mysql, "_load_text", wrong)
    said = []
    _move(_engine(tmp_path), tmp_path, said)
    assert any("copying it again as inserts" in m for m in said), said
    assert _fingerprint("s2") == _fingerprint("s")


def test_a_server_that_takes_no_load_is_written_by_inserts(
        typed, tmp_path, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "LOAD_FROM", 1)
    my("set global local_infile = 0")
    try:
        used = _counting(monkeypatch)
        _move(_engine(tmp_path), tmp_path)
        assert used == []
        assert _fingerprint("s2") == _fingerprint("s")
    finally:
        my("set global local_infile = 1")


def test_ranges_in_processes_load_and_read_back(server, tmp_path,
                                               monkeypatch):
    from migkit import ranges
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    my("drop database if exists s; drop database if exists s2;"
       " create database s; create database s2;")
    table = ("create table t (id bigint primary key, payload varchar(64),"
             " n decimal(12,2), raw varbinary(16), yr year)")
    my(table, "s")
    my(table, "s2")
    my("set session cte_max_recursion_depth = 1000000; insert into t with"
       " recursive g(n) as (select 1 union all select n + 1 from g where"
       " n < 80000) select n, concat('row-', n), n / 7, unhex(md5(n)),"
       " if(n % 3 = 0, 0, 1990 + n % 50) from g; analyze table t", "s")
    said = []
    eng = _engine(tmp_path, workers=4)
    _move(eng, tmp_path, said)
    from migkit.cli import _Checkpoint
    st = _Checkpoint(tmp_path / "m.json")["s.t"]
    assert len(st["ranges"]) >= 4 and st["done"], st
    q = ("select count(*), md5(group_concat(concat_ws('|', id, payload, n,"
         " hex(raw), yr) order by id)) from t")
    assert my(q, "s2") == my(q, "s")
    assert my("select @@global.local_infile") == "1"


def test_the_shared_copier_loads_into_mysql(server, pg_pair, tmp_path,
                                           monkeypatch):
    """PostgreSQL to MySQL through the copier every pair shares: bytes,
    empty strings, NULLs and a YEAR written by the load, read back equal."""
    from migkit.engines.hetero import HeteroEngine
    my("drop database if exists back; create database back; create table"
       " back.items (id bigint primary key, name varchar(40), raw"
       " varbinary(16), note varchar(10), yr year, flags bit(8))")
    got = psql(pg_pair["src"], "create table public.items (id bigint"
                               " primary key, name text, raw bytea, note"
                               " text, yr int, flags smallint); insert into"
                               " public.items select g, 'item-' || g,"
                               " decode(md5(g::text), 'hex'), case when"
                               " g % 4 = 0 then '' when g % 4 = 1 then null"
                               " else 'n' end, case when g % 2 = 0 then 0"
                               " else 2001 end, g % 256 from"
                               " generate_series(1, 5000) g")
    assert got.returncode == 0, got.stderr
    hop = Hop(name="pl", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="root",
                              password="test"),
              databases=["postgres"], db_map={"postgres": "back"},
              options={"source_engine": "postgres",
                       "target_engine": "mysql"})
    hop.report_dir = lambda db=None: tmp_path
    used = _counting(monkeypatch)
    eng = HeteroEngine(hop)
    from migkit.cli import _Checkpoint
    eng.move_table("postgres", "public", "items", 500_000,
                   _Checkpoint(tmp_path / "m.json"), [].append)
    # a YEAR of 0 and a BIT compared as the numbers they hold: the digest
    # had read them as `0000` and as bytes, and called equal rows different
    got = [(r.status, r.detail) for r in eng.check_data("postgres")
           if r.check == "data"]
    assert [g[0] for g in got] == ["ok"], got
    # a BIT given a number is not a load's to write: the batch went as
    # inserts, whole
    assert used == [], used
    assert my("select count(*), sum(note = ''), sum(note is null),"
              " sum(yr = 0), sum(flags + 0) from back.items") == \
        "5000\t1250\t1250\t2500\t" + str(sum(g % 256
                                               for g in range(1, 5001)))
    my("alter table back.items drop column flags; truncate back.items")
    psql(pg_pair["src"], "alter table public.items drop column flags")
    eng = HeteroEngine(hop)
    eng.move_table("postgres", "public", "items", 500_000,
                   _Checkpoint(tmp_path / "m2.json"), [].append)
    assert sum(used) == 5000, used
    assert [r.status for r in eng.check_data("postgres")
            if r.check == "data"] == ["ok"]


def test_a_large_table_goes_in_with_its_indexes_set_aside(
        server, tmp_path, monkeypatch):
    """The MySQL copier's index window: the secondary indexes off while the
    rows go in, a unique one kept, and all of a table's built back in one
    statement."""
    from migkit import ranges
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(MySQLEngine, "INDEX_WINDOW_FROM", 50_000)
    my("drop database if exists s; drop database if exists s2;"
       " create database s; create database s2;")
    table = ("create table t (id bigint primary key, a varchar(40), b int,"
             " c decimal(12,2), u varchar(20), index t_a (a),"
             " index t_bc (b, c), unique index t_u (u))")
    my(table, "s")
    my(table, "s2")
    my("set session cte_max_recursion_depth = 1000000; insert into t with"
       " recursive g(n) as (select 1 union all select n + 1 from g where"
       " n < 60000) select n, md5(n), n % 97, n / 3, concat('u-', n) from"
       " g; analyze table t", "s")
    shape = ("select index_name, group_concat(column_name order by"
             " seq_in_index), max(non_unique) from"
             " information_schema.statistics where table_schema = '{}' and"
             " table_name = 't' group by index_name order by index_name")
    before = my(shape.format("s2"))
    alters, real = [], MySQLEngine._q

    def watching(self, side, sql, args=None, fresh=False):
        if side == "dst" and sql.upper().startswith("ALTER TABLE"):
            alters.append(sql)
        return real(self, side, sql, args, fresh)
    monkeypatch.setattr(MySQLEngine, "_q", watching)
    said = []
    _move(_engine(tmp_path, workers=4), tmp_path, said)
    assert "2 indexes dropped for the load and rebuilt after" in said, said
    # dropped one by one, built back in one statement
    adds = [a for a in alters if "ADD INDEX" in a]
    assert len(adds) == 1 and adds[0].count("ADD INDEX") == 2, alters
    assert not any("t_u" in a for a in alters), alters
    assert my(shape.format("s2")) == before
    q = ("select count(*), md5(group_concat(concat_ws('|', id, a, b, c, u)"
         " order by id)) from t")
    assert my(q, "s2") == my(q, "s")


def test_mysql_says_what_it_can_give(server, tmp_path):
    got = _engine(tmp_path).capacity("src", "s")
    # the sandbox's server: 151 connections less those in use, and the CPUs
    # its default resource group may run on
    assert 100 <= got["free_connections"] <= 151, got
    assert got["cpus"] >= 1 and got["replica"] is False, got


def test_rows_of_large_values_are_read_a_few_at_a_time(server, tmp_path,
                                                       monkeypatch):
    """A fixed ten thousand rows a read, whatever they held: of
    twenty-megabyte values, far more than the machine has."""
    from migkit.engines import mysql
    my("drop database if exists s; drop database if exists s2;"
       " create database s; create database s2;")
    table = "create table t (id int primary key, body longblob)"
    my(table, "s")
    my(table, "s2")
    my("set session cte_max_recursion_depth = 100; insert into t with"
       " recursive g(n) as (select 1 union all select n + 1 from g where"
       " n < 24) select n, repeat(char(65 + n % 26), 4 * 1024 * 1024) from"
       " g", "s")
    real, sizes = mysql._batch_for, []

    def watched(rows, most, budget):
        got = real(rows, most, 16 * 2 ** 20)
        sizes.append(got)
        return got
    monkeypatch.setattr(mysql, "_batch_for", watched)
    _move(_engine(tmp_path), tmp_path)
    # four-megabyte rows, sixteen megabytes a read: three at a time after
    # the first, which is one to learn their size
    assert sizes and set(sizes) == {3}, sizes
    q = "select count(*), sum(length(body)), md5(group_concat(md5(body)))" \
        " from t"
    assert my(q, "s2") == my(q, "s")
