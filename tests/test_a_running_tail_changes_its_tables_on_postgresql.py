"""A running tail's table set and DDL, on PostgreSQL to PostgreSQL (backlog
L1, L4, L5; `tailtables`), with the tail running the whole time and
every other table going on:

* a table taken out of the hop's `exclude` while it runs is copied beside
  the stream and followed from there, the writes made while it was out
  and while it was copied included; put back in `exclude`, it leaves,
  what its target held kept for it to come back by;
* `ddl: park` - a column added on the source parks only that table, its
  changes set aside on this machine, and it catches up once the target
  has the column;
* `ddl: apply` - a column that takes NULL or has a constant default is
  added on the target at its place, a table emptied on the source is
  emptied there between the rows before and after it, and a default
  worked out when it is taken (`now()`) parks its table instead.

Each ends by `tail.stop` (`tailctl`): the tail applies what it read,
saves its position and returns.
"""
import json
import threading
import time

import pytest

from migkit import tailctl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

DB = "postgres"


def _pair(pg_pair, where, name, exclude=(), options=None):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=[DB], exclude=list(exclude),
              options=dict(options or {}), workers=2)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


def _both(pg_pair, sql):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, sql)
        assert got.returncode == 0, got.stderr


def _src(pg_pair, sql):
    got = psql(pg_pair["src"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _rows(port, table):
    got = psql(port, f"select count(*) || ' ' || md5(coalesce(string_agg("
                     f"t::text, ',' order by t::text), '')) from {table} t")
    return got.stdout.strip() if got.returncode == 0 else got.stderr


class _Tail:
    """The tail in a thread, its lines kept, ended by `tail.stop`."""

    def __init__(self, pair, where):
        self.pair, self.where = pair, where
        self.path = where / "tail-token.json"
        self.lines, self.done = [], threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.wait(lambda: (where / tailctl.PID).exists(), "running")

    def _run(self):
        try:
            self.pair.tail_apply(DB, True, self.path, self.lines.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            self.lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            self.done.set()

    def wait(self, cond, what, seconds=120):
        end = time.time() + seconds
        while time.time() < end:
            if cond():
                return
            if self.done.is_set():
                pytest.fail(f"the tail ended before {what}: {self.lines}")
            time.sleep(0.3)
        pytest.fail(f"never {what}: {self.lines[-20:]}")

    def state(self, table):
        # each table is kept by its whole name (`TableSet.key`)
        try:
            got = json.loads((self.where / "tail-tables.json").read_text())
        except (OSError, ValueError):
            return {}
        return got["tables"].get(f"public.{table}") or {}

    def stop(self):
        tailctl.stop(self.where)
        self.done.wait(60)
        assert self.done.is_set(), self.lines[-10:]
        assert not any(line.startswith("exit:") for line in self.lines), \
            self.lines

    def said(self, words):
        return any(words in line for line in self.lines)


def _drop_slot(pair):
    psql(pair.hop.source.port, "select pg_drop_replication_slot(slot_name)"
         " from pg_replication_slots where slot_name ="
         f" '{pair.src_engine.slot_name()}'")


def test_a_table_taken_out_of_exclude_joins_the_running_tail(
        pg_pair, tmp_path, monkeypatch):
    import migkit.config
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  livejoin:\n    exclude: [public.t2]\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    _both(pg_pair, "drop table if exists public.t1, public.t2;"
                   " create table public.t1 (id int primary key, v text)")
    _src(pg_pair, "create table public.t2 (id int primary key, v text);"
                  " insert into public.t2 select g, 'old' from"
                  " generate_series(1, 5000) g")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "livejoin", exclude=["public.t2"])
    tail = _Tail(pair, where)
    try:
        _src(pg_pair, "insert into public.t1 values (1, 'a');"
                      " update public.t2 set v = 'while out' where id <= 10")
        tail.wait(lambda: _rows(pg_pair["src"], "public.t1")
                  == _rows(pg_pair["dst"], "public.t1"), "t1 carried")
        # t2 not carried while out of scope
        assert "does not exist" in _rows(pg_pair["dst"], "public.t2")
        conf.write_text("hops:\n  livejoin:\n    exclude: []\n")
        stop, n = threading.Event(), [0]

        def writer():
            while not stop.is_set():
                n[0] += 1
                psql(pg_pair["src"], "update public.t2 set v = 'during "
                     f"{n[0]}' where id = {n[0] % 5000 + 1}; insert into"
                     f" public.t1 values ({n[0] + 1}, 'b')")
        w = threading.Thread(target=writer, daemon=True)
        w.start()
        tail.wait(lambda: tail.state("t2").get("state") == "copying",
                  "t2 added")
        tail.wait(lambda: tail.state("t2").get("state") == "live",
                  "t2 caught up")
        stop.set()
        w.join(30)
        _src(pg_pair, "insert into public.t2 values (9001, 'after')")
        for t in ("public.t1", "public.t2"):
            tail.wait(lambda t=t: _rows(pg_pair["src"], t)
                      == _rows(pg_pair["dst"], t), f"{t} level")
        assert tail.said("added to the tail (added to the hop's scope)")
        # the other table went on the whole time
        assert int(_dst(pg_pair, "select count(*) from public.t1")) > 1
        # out again: it leaves, and what its target held is kept
        conf.write_text("hops:\n  livejoin:\n    exclude: [public.t2]\n")
        tail.wait(lambda: tail.state("t2").get("state") == "removed",
                  "t2 removed")
        tail.wait(lambda: tail.state("t2").get("digest"), "t2 digested")
        _src(pg_pair, "insert into public.t2 values (9002, 'gone')")
        _src(pg_pair, "insert into public.t1 values (99999, 'still')")
        tail.wait(lambda: _dst(pg_pair, "select count(*) from public.t1"
                               " where id = 99999") == "1", "t1 goes on")
        assert _dst(pg_pair, "select count(*) from public.t2 where"
                             " id = 9002") == "0"
        # its changes after it joined were applied, each accounted for
        accounts = json.loads((where / "tail-accounts.json").read_text())
        assert accounts["public.t2"]["applied"] > 0, accounts
    finally:
        tail.stop()
        _drop_slot(pair)


def test_a_column_added_parks_only_its_table(pg_pair, tmp_path):
    _both(pg_pair, "drop table if exists public.pa, public.pb;"
                   " create table public.pa (id int primary key, v text);"
                   " create table public.pb (id int primary key, v text)")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "livepark", options={"ddl": "park"})
    tail = _Tail(pair, where)
    try:
        _src(pg_pair, "alter table public.pa add column w int;"
                      " insert into public.pa values (1, 'a', 5);"
                      " insert into public.pb values (1, 'b')")
        tail.wait(lambda: _dst(pg_pair, "select count(*) from public.pb")
                  == "1", "pb carried past pa's DDL")
        tail.wait(lambda: tail.state("pa").get("state") == "parked",
                  "pa parked")
        assert _dst(pg_pair, "select count(*) from public.pa") == "0"
        assert (where / "tail-spool").is_dir()
        assert tail.said("pa: parked")
        _dst(pg_pair, "alter table public.pa add column w int")
        tail.wait(lambda: tail.state("pa").get("state") == "live",
                  "pa level again")
        tail.wait(lambda: _rows(pg_pair["src"], "public.pa")
                  == _rows(pg_pair["dst"], "public.pa"), "pa level")
    finally:
        tail.stop()
        _drop_slot(pair)


def test_what_can_be_carried_at_its_place_is(pg_pair, tmp_path):
    _both(pg_pair, "drop table if exists public.ca, public.cb, public.cc;"
                   " create table public.ca (id int primary key, v text);"
                   " create table public.cb (id int primary key, v text);"
                   " create table public.cc (id int primary key, v text)")
    _both(pg_pair, "insert into public.cb values (1, 'x'), (2, 'y')")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "liveapply", options={"ddl": "apply"})
    tail = _Tail(pair, where)
    try:
        _src(pg_pair, "alter table public.ca add column w int default 7;"
                      " insert into public.ca (id, v) values (1, 'a');"
                      " insert into public.cb values (3, 'z');"
                      " truncate public.cb;"
                      " insert into public.cb values (4, 'after');"
                      " alter table public.cc add column at timestamptz"
                      " default now();"
                      " insert into public.cc (id, v) values (1, 'c')")
        tail.wait(lambda: _rows(pg_pair["src"], "public.ca")
                  == _rows(pg_pair["dst"], "public.ca"), "ca carried")
        assert _dst(pg_pair, "select w from public.ca where id = 1") == "7"
        tail.wait(lambda: _dst(pg_pair, "select string_agg(id::text, ','"
                               " order by id) from public.cb") == "4",
                  "cb emptied at its place")
        tail.wait(lambda: tail.state("cc").get("state") == "parked",
                  "cc parked on a default worked out when taken")
        assert "worked out when it is taken" in tail.state("cc")["why"]
    finally:
        tail.stop()
        _drop_slot(pair)
