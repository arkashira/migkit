"""Rows written between the slot and the copy's first read, between the
tables and before the tail's first batch all reach the target of a
`move --mode full+cdc`, and the slot is made before anything is read.

migkit's own order, driven through a PostgreSQL to PostgreSQL hop taken
by migkit's tail (`engine: hetero`) - a plain `engine: postgres` hop
hands full+cdc to the server's own subscription, whose order is the
server's. The model of the same order, with every interleaving, is
`test_the_position_is_taken_before_the_copy_reads.py`.

What the order leans on, measured here: a commit already in the log and
not yet visible - held by a synchronous standby that never answers - is
waited for by the slot's creation, which returns only once the commit can
be read. A copy that reads after the slot is made cannot miss it.

On MySQL the binlog's end is not such a position: a transaction is in the
binlog before the storage engine commits it. Held there on the sandbox,
the end read in between was past a row nobody could read; the position a
copy takes now waits for the commits under way. The PostgreSQL tests and
the MySQL one run apart (`-k "not mysql"`, `-k mysql`): one pair of
containers at once.
"""
import threading
import time

import pytest

from tests import mysql_pair
from tests.conftest import needs_docker, psql
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

pytestmark = needs_docker

DB = "slotfirst"


def _sql(port, sql, db=DB):
    got = psql(port, sql, db)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture
def fresh(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    for port in (pg_pair["src"], pg_pair["dst"]):
        _sql(port, f"drop database if exists {DB} with (force)", "postgres")
        _sql(port, f"create database {DB}", "postgres")
        for table in ("a", "b"):
            _sql(port, f"create table {table} (id bigint primary key,"
                       " v text)")
    for table in ("a", "b"):
        _sql(pg_pair["src"], f"insert into {table} select g, 'v' || g"
                             " from generate_series(1, 200) g")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  slotfirst:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        f"    databases: [{DB}]\n"
        "    options: {source_engine: postgres, target_engine: postgres}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    try:
        yield pg_pair
    finally:
        # a slot left behind holds the shared source's WAL for good
        _sql(pg_pair["src"], "select pg_drop_replication_slot(slot_name)"
             f" from pg_replication_slots where database = '{DB}'",
             "postgres")
        for port in (pg_pair["src"], pg_pair["dst"]):
            _sql(port, f"drop database if exists {DB} with (force)",
                 "postgres")


def _standby(port, names):
    """The sandbox source waits on a synchronous standby named `names`, or
    on none. `alter system` runs on its own: not in a transaction."""
    _sql(port, "alter system reset synchronous_standby_names" if names is None
         else f"alter system set synchronous_standby_names = {names}",
         "postgres")
    _sql(port, "select pg_reload_conf()", "postgres")


def _same(pg_pair, table):
    q = (f"select count(*) || ':' || coalesce(md5(string_agg(id || '|'"
         f" || v, ',' order by id)), '') from {table}")
    return _sql(pg_pair["src"], q), _sql(pg_pair["dst"], q)


def test_rows_written_around_the_slot_arrive(fresh, monkeypatch):
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.postgres import PostgresEngine
    src = fresh["src"]
    said = []
    n = iter(range(1000, 100000, 10))

    def write(where):
        at = next(n)
        for table in ("a", "b"):
            _sql(src, f"insert into {table} values ({at}, '{where}');"
                      f" update {table} set v = '{where}' where id = {at % 97};"
                      f" delete from {table} where id = {at % 89 + 100}")

    real_point = PostgresEngine.change_point

    def point_then_write(self, side, db):
        got = real_point(self, side, db)
        said.append("position")
        # between the slot and the copy's first read
        write("after the slot")
        return got

    real_move = HeteroEngine.move_table

    def write_around_the_copy(self, db, sch, tbl, chunk, ck, log):
        said.append(f"copy {tbl}")
        write(f"before {tbl}")
        real_move(self, db, sch, tbl, chunk, ck, log)
        write(f"after {tbl}")

    real_tail = HeteroEngine.tail_apply

    def tail_until_quiet(self, db, go, token_path, log):
        # before the tail's first batch
        write("before the tail")
        engine = self.src_engine
        real_changes = type(engine).neutral_changes

        def changes(side, db, token=None, limit=1000):
            got, token = real_changes(engine, side, db, token, limit)
            if not got:
                raise KeyboardInterrupt
            return got, token
        monkeypatch.setattr(engine, "neutral_changes", changes)
        return real_tail(self, db, go, token_path, log)

    monkeypatch.setattr(PostgresEngine, "change_point", point_then_write)
    monkeypatch.setattr(HeteroEngine, "move_table", write_around_the_copy)
    monkeypatch.setattr(HeteroEngine, "tail_apply", tail_until_quiet)
    got = CliRunner().invoke(cli.main, ["move", "slotfirst", "--mode",
                                        "full+cdc", "--db", DB, "--go"])
    out = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, out
    # the slot is made before the first table is read, once
    assert said[0] == "position" and said.count("position") == 1, said
    assert [s for s in said if s.startswith("copy")], said
    for table in ("a", "b"):
        a, b = _same(fresh, table)
        assert a == b, (table, a, b)
    # the rows written in the window are there by content, not only count
    assert _sql(fresh["dst"], "select count(*) from a where v ="
                              " 'after the slot'") != "0"


def test_the_slot_waits_for_a_commit_nobody_can_read_yet(fresh):
    from migkit.config import get_hop
    from migkit.engines import get_engine
    src = fresh["src"]
    eng = get_engine(get_hop("slotfirst")).src_engine
    _standby(src, "'nobody'")
    held = threading.Thread(target=psql, args=(
        src, "insert into a values (5000, 'held')", DB))
    made = {}

    def make():
        made["point"] = eng.change_point("src", DB)
        made["at"] = time.time()
    maker = threading.Thread(target=make)
    try:
        held.start()
        for _ in range(50):
            if _sql(src, "select count(*) from pg_stat_activity where"
                         " wait_event = 'SyncRep'", "postgres") == "1":
                break
            time.sleep(0.2)
        else:
            pytest.fail("the insert never waited on the standby")
        # in the log, and nobody can read it
        assert _sql(src, "select count(*) from a where id = 5000") == "0"
        maker.start()
        maker.join(5)
        # measured: the slot is not made while the commit is out of sight
        assert maker.is_alive(), made
    finally:
        released = time.time()
        _standby(src, None)
        held.join(30)
        maker.join(60)
    assert made.get("at", 0) >= released, made
    assert _sql(src, "select count(*) from a where id = 5000") == "1"


def test_mysql_a_commit_in_flight_is_not_before_the_copys_position(
        my_pair, tmp_path, monkeypatch):
    """MySQL writes a transaction to the binlog before the storage engine
    commits it; the commit is held there for 1.5 s on the sandbox source
    (`binlog_group_commit_sync_delay`) so the moment can be hit. The
    binlog's end read then is past an insert no read can see, and a copy
    reading then misses the row a tail from there skips. The position a
    copy's tail starts from (`MySQLEngine.copy_point`) either has the insert
    after it or returns once a read sees it."""
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    my("src", "drop database if exists cp; create database cp;"
              " create table cp.t (id int primary key)")
    eng = mysql_pair.engine("cp")
    my("src", "set global binlog_group_commit_sync_delay = 1500000")
    insert = threading.Thread(target=my, args=(
        "src", "insert into cp.t values (1)"))
    try:
        insert.start()
        time.sleep(0.4)
        stage = my("src", "select state from information_schema.processlist"
                          " where info like 'insert into cp.t%'")
        unseen = my("src", "select count(*) from cp.t")
        end = eng.change_point("src", "cp")
        point = eng.copy_point("src", "cp")
        seen = my("src", "select count(*) from cp.t")
        insert.join(30)
    finally:
        my("src", "set global binlog_group_commit_sync_delay = 0")
    # the moment was hit: the insert is in the log before the log's end,
    # and nobody could read it
    assert unseen == "0", unseen
    after = my("src", f"show binlog events in '{end['log_file']}'"
                      f" from {end['log_pos']}")
    assert "Write_rows" not in after, (end, after)
    # what the wait keys on: the thread is in its commit throughout
    assert stage == "waiting for handler commit", stage
    events = my("src", f"show binlog events in '{point['log_file']}'"
                       f" from {point['log_pos']}")
    assert seen == "1" or "Write_rows" in events, (point, seen, events)
