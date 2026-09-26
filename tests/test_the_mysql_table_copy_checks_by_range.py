"""The MySQL table copier: ranges of equal rows side by side, each read
back from the target, a finished table asked again - and the MySQL to
PostgreSQL copy, which goes through the copier every pair shares.

Measured before:
* ranges written side by side each delete their own span first, and under
  repeatable read two neighbours deadlocked (ERROR 1213); the copy stopped
* the MySQL to PostgreSQL copier wrote a CSV itself: bytes landed as their
  hex digits read as text and every empty string as NULL - 200,000 of
  200,000 rows differed after a move that said it was complete
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, PORT = "migkit-test-mysql-ranges", 15864
ROWS = 60_000


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
                    "mysql:8.4"], check=True, capture_output=True)
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
         " n decimal(12,2), raw varbinary(16), note varchar(10))")


@pytest.fixture
def seeded(server):
    my("drop database if exists shop; drop database if exists shop_copy;"
       " create database shop; create database shop_copy;")
    my(f"{TABLE}; set session cte_max_recursion_depth = 1000000;"
       " insert into big with recursive g(n) as (select 1 union all select"
       f" n + 1 from g where n < {ROWS}) select n, concat('row-', n),"
       " n / 7, unhex(md5(n)), if(n % 5 = 0, '', 'v') from g; analyze"
       " table big", "shop")
    my(TABLE, "shop_copy")


@pytest.fixture
def engine(seeded, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="myr", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_copy"}, workers=4)
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    return MySQLEngine(hop)


def _same():
    q = ("select count(*), md5(group_concat(concat_ws('|', id, payload, n,"
         " hex(raw), note) order by id separator ',')) from big;")
    pre = "set session group_concat_max_len = 1000000000;"
    return my(pre + q, "shop") == my(pre + q, "shop_copy")


def test_ranges_side_by_side_do_not_deadlock(engine, tmp_path):
    from migkit.cli import _Checkpoint
    ck, said = _Checkpoint(tmp_path / "move.json"), []
    engine.move_table("shop", "", "big", 500_000, ck, said.append)
    st = ck["shop.big"]
    assert len(st["ranges"]) >= 4 and st["done"], st
    assert _same()


def test_a_target_that_changes_rows_stops_the_copy_at_the_range(
        engine, tmp_path):
    from migkit.cli import _Checkpoint
    my("create trigger shout before insert on big for each row"
       " set new.payload = if(new.id = 45000, upper(new.payload),"
       " new.payload)", "shop_copy")
    with pytest.raises(SystemExit) as e:
        engine.move_table("shop", "", "big", 500_000,
                          _Checkpoint(tmp_path / "move.json"), [].append)
    msg = str(e.value)
    assert msg.startswith("shop.big id "), msg
    rng = msg.split(":")[0].split("id ")[1]
    lo, hi = (int(x.replace(",", "")) for x in rng.split(" to "))
    assert lo <= 45000 <= hi, msg


def test_a_finished_table_copies_again_only_the_range_that_changed(
        engine, tmp_path):
    from migkit.cli import _Checkpoint
    engine.move_table("shop", "", "big", 500_000,
                      _Checkpoint(tmp_path / "move.json"), [].append)
    my("update big set note = 'changed' where id between 30001 and 30010",
       "shop")
    said = []
    engine.move_table("shop", "", "big", 500_000,
                      _Checkpoint(tmp_path / "move.json"), said.append)
    assert any(m.startswith("shop.big: done earlier, and 1 of ") for m in said), \
        said
    assert _same()


def test_mysql_to_postgresql_carries_bytes_and_empty_strings(
        seeded, pg_pair, tmp_path):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    psql(pg_pair["dst"], "create table public.big (id bigint primary key,"
                         " payload varchar(64), n numeric(12,2), raw bytea,"
                         " note varchar(10))")
    hop = Hop(name="mp", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["shop"], db_map={"shop": "postgres"},
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    eng.move_table("shop", "", "big", 500_000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    got = psql(pg_pair["dst"], "select count(*) filter (where note = ''),"
                               " count(*) filter (where note is null),"
                               " encode(raw, 'hex') from public.big"
                               " where id = 5 group by raw")
    assert got.stdout.strip() == f"1|0|{my('select hex(raw) from big where id = 5', 'shop').lower()}", got.stdout
    assert [r.status for r in eng.check_data("shop")
            if r.check == "data"] == ["ok"]


def _pair(pg_pair, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="mp", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["shop"], db_map={"shop": "postgres"},
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


SHOUT = """
    create or replace function shout() returns trigger language plpgsql as $$
    begin if new.{key} = {at} then new.{col} := upper(new.{col}); end if;
    return new; end $$;
    create trigger shout before insert on public.{table} for each row
    execute function shout();
    alter table public.{table} enable always trigger shout"""


@pytest.mark.parametrize("table, key, at, create", [
    # an integer key: settled by one range digest from each side, then
    # read back row by row where they differ
    ("big", "id", "45000",
     "create table public.big (id bigint primary key, payload varchar(64),"
     " n numeric(12,2), raw bytea, note varchar(10))"),
    # a text key: read back row by row
    ("tk", "k", "'k-00045'",
     "create table public.tk (k varchar(20) primary key, payload text)"),
])
def test_a_pair_batch_the_target_changes_is_written_again_then_named(
        seeded, pg_pair, tmp_path, table, key, at, create):
    from migkit.cli import _Checkpoint
    if table == "tk":
        my("create table tk (k varchar(20) primary key, payload text);"
           " set session cte_max_recursion_depth = 100000; insert into tk"
           " with recursive g(n) as (select 1 union all select n + 1 from g"
           " where n < 100) select concat('k-', lpad(n, 5, '0')),"
           " concat('p', n) from g", "shop")
    assert psql(pg_pair["dst"], create).returncode == 0
    got = psql(pg_pair["dst"], SHOUT.format(table=table, key=key, at=at,
                                            col="payload"))
    assert got.returncode == 0, got.stderr
    said = []
    with pytest.raises(SystemExit) as e:
        _pair(pg_pair, tmp_path).move_table(
            "shop", "", table, 500_000, _Checkpoint(tmp_path / "move.json"),
            said.append)
    msg = str(e.value)
    assert msg.startswith(f"shop.{table}: written twice, 0 rows of a batch"
                          " are not on the target and 1 read back"
                          " different"), msg
    assert f"by key {at.strip(chr(39))}, in payload" in msg, msg
    assert any("writing it again" in m for m in said), said


def test_a_pair_table_with_no_key_is_held_to_what_passed(seeded, pg_pair,
                                                         tmp_path):
    from migkit.cli import _Checkpoint
    my("create table nk (a int, payload varchar(20)); insert into nk values"
       " (1, 'x'), (2, 'y'), (3, 'z')", "shop")
    psql(pg_pair["dst"], "create table public.nk (a int, payload"
                         " varchar(20))")
    got = psql(pg_pair["dst"], SHOUT.format(table="nk", key="a", at="2",
                                            col="payload"))
    assert got.returncode == 0, got.stderr
    said = []
    with pytest.raises(SystemExit) as e:
        _pair(pg_pair, tmp_path).move_table(
            "shop", "", "nk", 500_000, _Checkpoint(tmp_path / "move.json"),
            said.append)
    assert str(e.value).startswith("shop.nk: copied twice in one pass each"
                                   " (it has no key)"), e.value
    assert any("emptying it and copying it again" in m for m in said), said


def test_a_finished_pair_table_is_asked_again(seeded, pg_pair, tmp_path):
    from migkit.cli import _Checkpoint
    psql(pg_pair["dst"], "create table public.big (id bigint primary key,"
                         " payload varchar(64), n numeric(12,2), raw bytea,"
                         " note varchar(10))")
    eng = _pair(pg_pair, tmp_path)
    eng.move_table("shop", "", "big", 500_000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    said = []
    eng.move_table("shop", "", "big", 500_000,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    assert said == ["shop.big: done earlier, and both sides still hold the"
                    " same rows - skipped"], said
    my("update big set payload = 'changed' where id = 7", "shop")
    said = []
    eng.move_table("shop", "", "big", 500_000,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    assert said[0] == ("shop.big: done earlier, and the two sides no longer"
                       " hold the same rows - copying it again"), said
    assert psql(pg_pair["dst"], "select payload from public.big where"
                                " id = 7").stdout.strip() == "changed"
