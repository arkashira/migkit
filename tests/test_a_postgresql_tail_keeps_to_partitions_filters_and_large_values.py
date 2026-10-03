"""PostgreSQL to PostgreSQL, the change tail and the servers' own
replication held to the hop (backlog P0-SCOPE.6e), on real servers:

* a partition's changes are carried under its root, which the hop takes,
  filters and names: `include: [public.porders]` dropped every change to
  `porders_th`, and a row filter keyed on the parent never applied;
* the publication lists the roots and publishes under them, a partition
  of a table the hop excludes not among them;
* an UPDATE that did not touch a large value stored out of line keeps the
  target's value - the decoder's placeholder was written over it;
* a hop that filters rows sets up no subscription, which would carry
  every row.
"""
import json

import pytest

from migkit import cli
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import _Accounts
from tests.conftest import needs_docker, psql
from tests.test_a_running_tail_changes_its_tables_on_postgresql import (
    DB, _Tail, _drop_slot)

pytestmark = needs_docker


def _hop(pg_pair, where, name, **kw):
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=[DB], workers=2, **kw)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return hop


def _pair(hop):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(hop)._as_pair()


def _on(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


PARTITIONED = (
    "drop table if exists public.porders, public.items cascade;"
    " create table public.porders (id int, region text, v text,"
    " primary key (id, region)) partition by list (region);"
    " create table public.porders_th partition of public.porders"
    " for values in ('TH');"
    " create table public.porders_sg partition of public.porders"
    " for values in ('SG');"
    " create table public.items (id int primary key)")


def test_a_partition_s_changes_go_by_its_root(pg_pair, tmp_path):
    _on(pg_pair["src"], PARTITIONED)
    # the target holds the table unpartitioned
    _on(pg_pair["dst"], "drop table if exists public.porders,"
                        " public.items cascade; create table"
                        " public.porders (id int, region text, v text,"
                        " primary key (id, region));"
                        " create table public.items (id int primary key)")
    where = tmp_path / "r"
    hop = _hop(pg_pair, where, "byroot", include=["public.porders"],
               mapping={"where": {"public.porders": "region = 'TH'"}})
    pair = _pair(hop)
    tail = _Tail(pair, where)
    try:
        _on(pg_pair["src"], "insert into public.porders values"
                            " (1, 'TH', 'a'), (2, 'SG', 'b');"
                            " update public.porders set v = 'c'"
                            " where id = 1; insert into public.items"
                            " values (1)")
        tail.wait(lambda: _on(pg_pair["dst"], "select string_agg(id ||"
                              " region || v, ',' order by id) from"
                              " public.porders") == "1THc",
                  "the partition's rows under its root")
        # the row outside the filter, and the table the hop leaves out
        assert _on(pg_pair["dst"], "select count(*) from"
                                   " public.items") == "0"
        accounts = json.loads((where / "tail-accounts.json").read_text())
        assert "public.porders" in accounts, accounts
        assert "public.porders_th" not in accounts, accounts
    finally:
        tail.stop()
        _drop_slot(pair)


def test_the_publication_lists_the_roots(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _on(pg_pair["src"], PARTITIONED)
    eng = PostgresEngine(_hop(pg_pair, tmp_path, "pub",
                              exclude=["public.porders"]))
    roots = eng.partition_roots("src", DB)
    assert roots["public.porders_th"] == "public.porders"
    assert roots["public.items"] == "public.items"
    # a partition of a table the hop excludes is not published
    assert eng.published_tables(DB) == ["public.items"]
    eng = PostgresEngine(_hop(pg_pair, tmp_path, "pub2",
                              include=["public.porders"]))
    assert eng.published_tables(DB) == ["public.porders"]
    assert eng._via_root_of(DB)
    assert "publish_via_partition_root = true" in eng.replicate_sql(
        DB, copy_data=False)["src"][0]


def test_an_unchanged_large_value_keeps_the_target_s(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        _on(port, "drop table if exists public.docs;"
                  " create table public.docs (id int primary key, n int,"
                  " body text);"
                  # out of line and not compressed: the log carries a
                  # pointer to it, never the value, where it is unchanged
                  " alter table public.docs alter column body set storage"
                  " external;"
                  " insert into public.docs select 1, 0,"
                  " string_agg(md5(g::text), '') from"
                  " generate_series(1, 2000) g")
    where = tmp_path / "r"
    pair = _pair(_hop(pg_pair, where, "toast"))
    tail = _Tail(pair, where)
    try:
        _on(pg_pair["src"], "update public.docs set n = 1 where id = 1")
        tail.wait(lambda: _on(pg_pair["dst"], "select n from public.docs"
                              " where id = 1") == "1", "the update carried")
        body = "select md5(body) from public.docs where id = 1"
        assert _on(pg_pair["dst"], body) == _on(pg_pair["src"], body)
        accounts = json.loads((where / "tail-accounts.json").read_text())
        assert accounts["public.docs"][_Accounts.UNCHANGED] >= 1, accounts
    finally:
        tail.stop()
        _drop_slot(pair)


def test_a_hop_that_filters_rows_sets_up_no_subscription(pg_pair,
                                                         tmp_path):
    """The publication was a plain FOR TABLE list: under `--mode cdc` a
    hop that filters `orders` to one region carried every row of it."""
    from migkit.engines.postgres import PostgresEngine
    for port in (pg_pair["src"], pg_pair["dst"]):
        _on(port, "drop table if exists public.orders; create table"
                  " public.orders (id int primary key, region text)")
    hop = _hop(pg_pair, tmp_path, "filtered",
               mapping={"where": {"public.orders": "region = 'TH'"}})
    eng = PostgresEngine(hop)
    with pytest.raises(SystemExit) as e:
        cli._replicate_one(hop, eng, DB, False, False, True)
    assert "public.orders (its rows are filtered)" in str(e.value)
    assert _on(pg_pair["src"], "select count(*) from pg_publication where"
                               " pubname = 'migkit_filtered'") == "0"
    assert _on(pg_pair["dst"], "select count(*) from"
                               " pg_subscription") == "0"
