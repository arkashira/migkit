"""Indexes that exist, answer no query, and still send a bill.

`CREATE INDEX CONCURRENTLY` is the one DDL that does not roll back. When it
fails - a duplicate key, a cancelled session, a `statement_timeout` meant for
ordinary queries - the index stays behind marked invalid. The planner ignores
it from then on, so it turns up in every "unused index, safe to drop" report,
while the name it holds makes the rebuild that would fix it fail with
`already exists`.

Both states were measured on PostgreSQL 16 before any of this was written,
because they cost differently:

    dupes_u   indisvalid=f indisready=f   0 bytes, never grows
    ready_v   indisvalid=f indisready=t   4.5 MB -> 12.3 MB over 200,000
                                          inserts, and EXPLAIN still chose a
                                          seq scan on the indexed column
    ev_id_idx relkind='I'  indisvalid=f   a partitioned parent, invalid by
                                          design until every partition's
                                          index is attached

Before this check, a target left with two invalid indexes - one of them
sharing a name with a *valid* index on the source - produced a full
`migkit check --deep` report that mentioned the word "invalid" zero times.
The schema comparison said "1 to remove", which reads as a spare index
rather than a broken one, and said nothing at all when the name existed on
both sides.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="idx", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _break_unique(port, table, index):
    """The commonest way to end up with one: a unique index over rows that
    are not unique. The statement fails; the index does not go away."""
    got = psql(port, f"drop table if exists public.{table};"
                     f" create table public.{table} (id bigint, v text);"
                     f" insert into public.{table} select g%50, 'v' from"
                     " generate_series(1,200) g;")
    assert got.returncode == 0, got.stderr
    # its own -c: psql wraps a multi-statement -c in a transaction, and
    # CREATE INDEX CONCURRENTLY cannot run inside one
    failed = psql(port, f"create unique index concurrently {index}"
                        f" on public.{table} (id);")
    assert failed.returncode != 0, "the unique index was supposed to fail"
    assert "duplicat" in failed.stderr.lower(), failed.stderr
    left = psql(port, "select indisvalid::int::text||indisready::int from pg_index i"
                      " join pg_class c on c.oid = i.indexrelid"
                      f" where c.relname = '{index}'")
    assert left.stdout.strip() == "00", left.stdout


def _invalid(engine):
    return engine._invalid_indexes("postgres")


def test_an_invalid_index_on_the_target_is_named(pg_pair, tmp_path):
    _break_unique(pg_pair["dst"], "orders", "orders_uniq")
    got = _invalid(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "target public.orders.orders_uniq" in got.detail, got.detail
    assert "only the name is taken" in got.detail, got.detail
    assert "DROP INDEX CONCURRENTLY" in got.fix_hint, got.fix_hint


def test_the_source_side_is_checked_too(pg_pair, tmp_path):
    """On the source it is a reason not to migrate yet: the schema is
    carrying an index that does not work, and the report that says so is
    the one an operator reads before the move, not after."""
    _break_unique(pg_pair["src"], "ledger", "ledger_uniq")
    got = _invalid(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "source public.ledger.ledger_uniq" in got.detail, got.detail


def test_a_name_valid_on_one_side_and_broken_on_the_other(pg_pair, tmp_path):
    """The case the schema comparison cannot see at all. Same table, same
    index name, both sides - and on the target it is invalid. A diff of
    index names finds nothing to say."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.people;"
                   " create table public.people (id bigint, v text);")
    psql(pg_pair["src"], "insert into public.people select g, 'v' from"
                         " generate_series(1,100) g;")
    psql(pg_pair["src"], "create unique index people_uniq on public.people"
                         " (id);")
    psql(pg_pair["dst"], "insert into public.people select g%50, 'v' from"
                         " generate_series(1,100) g;")
    failed = psql(pg_pair["dst"], "create unique index concurrently"
                                  " people_uniq on public.people (id);")
    assert failed.returncode != 0, failed.stdout

    # the names match on both sides, so a name comparison sees nothing
    names = [psql(p, "select c.relname from pg_index i join pg_class c on"
                     " c.oid = i.indexrelid join pg_class t on t.oid ="
                     " i.indrelid where t.relname = 'people' order by 1"
                     ).stdout.split()
             for p in (pg_pair["src"], pg_pair["dst"])]
    assert names[0] == names[1] == ["people_uniq"], names

    got = _invalid(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "target public.people.people_uniq" in got.detail, got.detail
    assert "source" not in got.detail, got.detail


def test_an_index_that_still_costs_every_write_is_told_apart(pg_pair,
                                                              tmp_path):
    """The expensive state, built the way it happens: the build finishes,
    the validation phase waits for older transactions, and the session dies
    in that wait. What is left is maintained on every insert and used by
    nothing.

    An older snapshot held open is what makes this deterministic rather
    than a race - CREATE INDEX CONCURRENTLY parks there until the snapshot
    goes away.
    """
    port = pg_pair["dst"]
    got = psql(port, "drop table if exists public.invoices;"
                     " create table public.invoices (id bigint, v text);"
                     " insert into public.invoices select g, 'v'||g from"
                     " generate_series(1,20000) g;")
    assert got.returncode == 0, got.stderr

    holder = subprocess.Popen(
        ["docker", "exec", "-e", "PGPASSWORD=test", "migkit-test-pg-dst",
         "psql", "-U", "postgres", "-At", "-c",
         "begin isolation level repeatable read; select 1;"
         " select pg_sleep(60);"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    builder = subprocess.Popen(
        ["docker", "exec", "-e", "PGPASSWORD=test", "migkit-test-pg-dst",
         "psql", "-U", "postgres", "-At", "-c",
         "create index concurrently invoices_v on public.invoices (v);"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            ready = psql(port, "select indisready::int from pg_index i join"
                               " pg_class c on c.oid = i.indexrelid where"
                               " c.relname = 'invoices_v'").stdout.strip()
            if ready == "1":
                break
            time.sleep(0.5)
        else:
            pytest.fail("the index build never reached its waiting phase")
        psql(port, "select pg_cancel_backend(pid) from pg_stat_activity where"
                   " query like 'create index concurrently invoices_v%'")
        builder.wait(timeout=60)
    finally:
        psql(port, "select pg_terminate_backend(pid) from pg_stat_activity"
                   " where query like '%pg_sleep%' and pid <> pg_backend_pid()")
        holder.wait(timeout=30)

    flags = psql(port, "select indisvalid::int::text||indisready::int from pg_index"
                       " i join pg_class c on c.oid = i.indexrelid where"
                       " c.relname = 'invoices_v'").stdout.strip()
    assert flags == "01", f"wanted invalid-but-maintained, got {flags}"

    got = _invalid(_engine(pg_pair, tmp_path))
    assert got.status == "diff", got.detail
    assert "target public.invoices.invoices_v" in got.detail, got.detail
    assert "maintained on every write" in got.detail, got.detail


def test_a_partitioned_parent_is_a_warning_not_a_failure(pg_pair, tmp_path):
    """`CREATE INDEX ... ON ONLY parent` is invalid until every partition's
    index is attached, and that is the documented way to build one a piece
    at a time. Calling it broken would make the check cry wolf on a healthy
    database; saying nothing would hide a partition with no index."""
    port = pg_pair["dst"]
    got = psql(port, "drop table if exists public.events cascade;"
                     " create table public.events (id bigint, at date)"
                     " partition by range (at);"
                     " create table public.events_2025 partition of"
                     " public.events for values from ('2025-01-01') to"
                     " ('2026-01-01');"
                     " create index on only public.events (at);")
    assert got.returncode == 0, got.stderr

    res = _invalid(_engine(pg_pair, tmp_path))
    assert res.status == "warn", res.detail
    assert "public.events" in res.detail, res.detail
    assert "attached" in res.detail, res.detail

    # and a plain invalid index outranks it - a warning must not bury a fault
    _break_unique(port, "orders", "orders_uniq")
    both = _invalid(_engine(pg_pair, tmp_path))
    assert both.status == "diff", both.detail
    assert "orders_uniq" in both.detail, both.detail


def test_a_healthy_pair_counts_what_it_checked(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists public.clean;"
                         " create table public.clean (id bigint primary key,"
                         " v text);"
                         " create index clean_v on public.clean (v);")
        assert got.returncode == 0, got.stderr
    res = _invalid(_engine(pg_pair, tmp_path))
    assert res.status == "ok", res.detail
    assert "4 indexes" in res.detail, res.detail   # pk + clean_v, both sides


def test_the_full_deep_report_carries_it(pg_pair, tmp_path):
    """Wired in, not merely available: this is the report an operator runs."""
    _break_unique(pg_pair["dst"], "orders", "orders_uniq")
    got = [r for r in _engine(pg_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("indexes")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail
    assert "orders_uniq" in got[0].detail


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="i", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    dead = eng._invalid_index_result(
        "x", [("target", "public.t", "t_uniq", False)], [], 9, "hint")
    assert dead.status == "diff" and "only the name is taken" in dead.detail

    costly = eng._invalid_index_result(
        "x", [("target", "public.t", "t_uniq", True)], [], 9, "hint")
    assert costly.status == "diff"
    assert "maintained on every write" in costly.detail

    # a fault and a partitioned parent together: the fault is the verdict
    mixed = eng._invalid_index_result(
        "x", [("source", "public.t", "t_uniq", True)],
        [("target", "public.p", "p_at", )], 9, "hint")
    assert mixed.status == "diff" and "p_at" not in mixed.detail, mixed.detail

    waiting = eng._invalid_index_result(
        "x", [], [("target", "public.p", "p_at")], 9, "hint")
    assert waiting.status == "warn" and "public.p.p_at" in waiting.detail

    fine = eng._invalid_index_result("x", [], [], 9, "hint")
    assert fine.status == "ok" and "9 indexes" in fine.detail

    # nothing to look at is not a clean bill of health
    none = eng._invalid_index_result("x", [], [], 0, "hint")
    assert none.status == "skip", none.detail


def test_mysql_says_there_is_no_such_state_rather_than_ok(tmp_path):
    """Measured on MySQL 8: a failed ADD UNIQUE INDEX over duplicate rows
    rolled back completely - information_schema.statistics for the schema
    came back empty and innodb_indexes held only GEN_CLUST_INDEX - while a
    valid index created straight afterwards did appear in the same query.
    So the empty answer was real. Reporting `ok` would still be wrong: a
    corrupt InnoDB index has no catalog column at all."""
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="i", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._invalid_indexes("x")
    assert got.status == "skip", got.detail
    assert "atomic DDL" in got.detail, got.detail
    assert "1712" in got.detail, got.detail
