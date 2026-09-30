"""MySQL into MySQL with no row passing through migkit (backlog R20 items
1 and 4): each range rendered by the source's own client, in SQL, as the
line the target's pinned bulk load reads, under a consistent snapshot that
digests the range first; the lines passed into the load as they arrive;
the range committed only where the target's digest of what it took, the
counts and the load's silence agree. `unique_checks = 0` only where the
table is emptied for the load and the source enforces every unique index
the target has; `sql_log_bin = 0` only where the hop states its target has
no replicas. And the whole table's ways - these ranges, the rows' own way,
the parallel dump and load - timed against each other a byte, table by
table.

Measured before: every value of every range became a Python object here,
was hashed here (`Tally`) and rendered again as the load's text
(`_load_text`).
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-pass-my", 16184
ROWS = 40_000


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
                    f"127.0.0.1:{PORT}:3306", "-e", "MYSQL_ROOT_PASSWORD=test",
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


TABLE = ("create table big (id bigint primary key, payload varchar(64),"
         " n decimal(12,2), raw varbinary(16), note text, at datetime(6),"
         " ts timestamp(3) null, doc json, kind enum('a','b','c'),"
         " flags bit(8), f double, y year, maybe varchar(8))")

FILL = (" insert into big with recursive g(n) as (select 1 union all select"
        " n + 1 from g where n < {rows}) select n, concat('row-', n,"
        " if(n % 7 = 0, concat(char(9), 'tab', char(10), 'nl', char(92),"
        " 'bs', char(13)), '')), n / 7, unhex(md5(n)), if(n % 5 = 0, '',"
        " repeat('t', n % 50)), timestamp('2026-01-01 00:00:00.123456') +"
        " interval n second, timestamp('2026-02-01') + interval n minute,"
        " json_object('n', n, 's', concat('a', char(10), 'b')),"
        " elt(1 + n % 3, 'a', 'b', 'c'), n % 256, n / 3.0,"
        " 1901 + n % 200, if(n % 3 = 0, null, 'x') from g;")


@pytest.fixture
def seeded(server):
    my("drop database if exists shop; drop database if exists shop_copy;"
       " create database shop; create database shop_copy;")
    my(f"{TABLE}; set session cte_max_recursion_depth = 1000000;"
       + FILL.format(rows=ROWS) + " analyze table big", "shop")
    my(TABLE, "shop_copy")


def _hop(tmp_path, **options):
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="mypass", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_copy"}, workers=4,
              options=options)
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.fixture
def engine(seeded, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    return MySQLEngine(_hop(tmp_path))


Q = ("set session group_concat_max_len = 1000000000; select count(*),"
     " md5(group_concat(concat_ws('|', id, hex(payload), n, hex(raw),"
     " hex(note), at, ts, doc, kind, flags + 0, f, y, ifnull(maybe, 'NULL'))"
     " order by id separator ',')) from big;")


def _same():
    a = my(Q, "shop")
    assert int(a.split()[0]) > 0, a
    return a == my(Q, "shop_copy")


def _move(engine, tmp_path, said=None, table="big"):
    from migkit.cli import _Checkpoint
    engine.move_table("shop", "", table, 500_000,
                      _Checkpoint(tmp_path / "move.json"),
                      (said if said is not None else []).append)
    return _Checkpoint(tmp_path / "move.json")[f"shop.{table}"]


def _statements(monkeypatch):
    import pymysql
    seen = []
    real = pymysql.cursors.Cursor.execute

    def execute(self, query, args=None):
        seen.append(str(query))
        return real(self, query, args)
    monkeypatch.setattr(pymysql.cursors.Cursor, "execute", execute)
    return seen


def test_every_range_passes_with_no_row_through_migkit(
        engine, tmp_path, monkeypatch):
    from migkit.engines import mysql
    from migkit.tally import Tally

    def no(*a, **k):
        raise AssertionError("a row passed through migkit")
    monkeypatch.setattr(Tally, "row", no)
    monkeypatch.setattr(mysql, "_load_text", no)
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"] and len(st["ranges"]) >= 4, st
    assert sorted(st["ranges_done"]) == sorted(a for a, _ in st["ranges"])
    assert _same()
    assert not any("as rows" in m for m in said), said


def test_unique_checks_are_left_to_the_source_only_for_a_table_emptied(
        engine, tmp_path, monkeypatch):
    seen = _statements(monkeypatch)
    st = _move(engine, tmp_path)
    assert len(st["ranges"]) > 1
    assert "set unique_checks = 1" in seen
    assert "set unique_checks = 0" not in seen
    assert _same()
    # one range, the table emptied in the transaction that loads it
    my("drop table big; create table big (id bigint primary key, v int,"
       " u int, unique key (u)); insert into big values (1, 1, 1), (2, 2,"
       " 2), (3, 3, 3)", "shop")
    my("drop table big; create table big (id bigint primary key, v int,"
       " u int, unique key (u)); insert into big values (9, 9, 9)",
       "shop_copy")
    (tmp_path / "move.json").unlink()
    seen.clear()
    _move(engine, tmp_path, table="big")
    assert "set unique_checks = 0" in seen, seen
    got = my("select group_concat(id order by id) from big", "shop_copy")
    assert got == "1,2,3", got
    # a unique index the source does not enforce: its checks stay on
    my("alter table big add unique key only_here (v)", "shop_copy")
    my("delete from big", "shop_copy")
    (tmp_path / "move.json").unlink()
    seen.clear()
    _move(engine, tmp_path, table="big")
    assert "set unique_checks = 1" in seen and \
        "set unique_checks = 0" not in seen, seen


def _logged(db=""):
    got = my("show binary log status")
    return got.split()[0], int(got.split()[1])


def test_the_binlog_is_left_off_only_where_the_hop_says_no_replicas(
        seeded, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    my("flush binary logs")
    before = _logged()
    st = _move(MySQLEngine(_hop(tmp_path)), tmp_path)
    assert st["done"] and _same()
    after = _logged()
    written = after[1] if after[0] != before[0] else after[1] - before[1]
    # every row went to the log: far more than the rows' own bytes
    assert written > ROWS * 50, (before, after)
    my("delete from big", "shop_copy")
    (tmp_path / "move.json").unlink()
    my("flush binary logs")
    before = _logged()
    st = _move(MySQLEngine(_hop(tmp_path, target_has_replicas=False)),
               tmp_path)
    assert st["done"] and _same()
    after = _logged()
    written = after[1] if after[0] != before[0] else after[1] - before[1]
    assert written < ROWS, (before, after)


def test_a_column_the_load_cannot_read_back_goes_as_rows(engine, tmp_path):
    my("drop table big; create table big (id bigint primary key, g"
       " geometry not null srid 0); insert into big values (1,"
       " st_geomfromtext('point(1 2)')), (2, st_geomfromtext('point(3"
       " 4)'))", "shop")
    my("drop table big; create table big (id bigint primary key, g"
       " geometry not null srid 0)", "shop_copy")
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"]
    assert any("g (geometry) has no text the load reads back" in m
               for m in said), said
    q = "select group_concat(st_astext(g) order by id) from big"
    assert my(q, "shop") == my(q, "shop_copy")


def test_a_target_that_changes_rows_is_copied_again_as_rows_and_stopped(
        engine, tmp_path):
    from migkit.cli import _Checkpoint
    my("create trigger shout before insert on big for each row"
       " set new.payload = if(new.id = 25000, upper(new.payload),"
       " new.payload)", "shop_copy")
    said = []
    with pytest.raises(SystemExit) as e:
        engine.move_table("shop", "", "big", 500_000,
                          _Checkpoint(tmp_path / "move.json"), said.append)
    assert "copied twice, and the target reads back" in str(e.value)
    assert any("passed server to server, both servers digest it"
               " differently - copying it again as rows" in m
               for m in said), said


def test_the_ways_of_a_whole_table_are_timed_against_each_other(
        engine, tmp_path, monkeypatch):
    """Tables large enough are each copied a way not yet timed, in the
    list's order - the pass-through, the rows' own way, the parallel dump
    and load - and each is proved; then the cheapest a byte."""
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "STREAM_FROM", 1)
    for t in ("big2", "big3", "big4"):
        my(f"create table {t} like big; insert into {t} select * from big",
           "shop")
        my(f"create table {t} like big", "shop_copy")
    said = []
    for t in ("big", "big2", "big3", "big4"):
        st = _move(engine, tmp_path, said, table=t)
        assert st["done"], (t, st)
        q = Q.replace(" from big;", f" from {t};")
        assert my(q, "shop") == my(q, "shop_copy"), t
    # the engine's own chooser's costs, which it reports
    costs = engine._way_cost[engine._work_key(engine.TABLE_WORK)]
    assert any(w["work"] == engine.TABLE_WORK
               for w in engine.measured_ways())
    assert {"pass-through", "rows"} <= set(costs), costs
    # the dump either proved itself and was timed, or said why not
    assert "dump" in costs or any("did not finish" in m for m in said), said
    last = [m for m in said if m.startswith("shop.big4: ")][0]
    assert "measured the cheapest" in last or "did not" in last, last


def test_rows_of_large_values_pass_as_single_lines(engine, tmp_path):
    """A four-megabyte value is one line of eight megabytes of hex, read by
    the source's client and passed on as it arrives."""
    my("drop table big; create table big (id int primary key, body"
       " longblob, t longtext)", "shop")
    my("drop table big; create table big (id int primary key, body"
       " longblob, t longtext)", "shop_copy")
    my("set session cte_max_recursion_depth = 100; insert into big with"
       " recursive g(n) as (select 1 union all select n + 1 from g where"
       " n < 8) select n, repeat(char(65 + n, 0, 10, 92), 1024 * 1024),"
       " repeat(concat('t', char(9), char(10)), 1000000) from g", "shop")
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"] and not any("as rows" in m for m in said), said
    q = ("select count(*), sum(length(body)), md5(group_concat(md5(body),"
         " md5(t) order by id)) from big")
    assert my(q, "shop") == my(q, "shop_copy")
