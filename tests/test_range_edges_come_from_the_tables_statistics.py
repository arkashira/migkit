"""A table's range edges are read from the planner's histogram where it
has enough buckets, not by a pass over the whole key (F7.edges)."""
import pytest

from migkit import ranges
from tests.conftest import needs_docker, psql


def test_the_histogram_is_thinned_to_the_ranges_the_table_needs():
    bounds = list(range(0, 1001, 10))          # 100 buckets
    got = ranges.thin(bounds, rows=10_000, every=1_000)    # ten ranges
    assert got == [100, 200, 300, 400, 500, 600, 700, 800, 900], got
    # a histogram coarser than the ranges asked for is not used
    assert ranges.thin(bounds, rows=1_000_000, every=1_000) is None
    assert ranges.thin([], rows=10_000, every=1_000) is None
    assert ranges.thin(bounds, rows=500, every=1_000) is None
    # ranges built on the edges cover the key range once
    spans = ranges.split(0, 1000, got)
    assert spans[0][0] == -1 and spans[-1][1] == 1000
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:]))


@needs_docker
def test_a_postgresql_copy_cuts_its_ranges_from_the_statistics(pg_pair,
                                                               tmp_path):
    from migkit import cli
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ddl = "create table big (id bigint primary key, v int)"
    psql(pg_pair["src"], "drop table if exists big;" + ddl)
    psql(pg_pair["dst"], "drop table if exists big;" + ddl)
    psql(pg_pair["src"], "insert into big select g, g % 7 from"
                         " generate_series(1, 300000) g; analyze big")
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    hop = Hop(name="edges", engine="postgres", source=ep("src"),
              target=ep("dst"), databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    assert len(eng._histogram("postgres", "public.big", "id")) > 50
    # a table never analyzed has none, and the key is read instead
    psql(pg_pair["src"], "create table fresh (id bigint primary key);"
                         " insert into fresh select g from"
                         " generate_series(1, 10) g")
    assert eng._histogram("postgres", "public.fresh", "id") == []
    said = []
    eng.move_table("postgres", "public", "big", 10_000,
                   cli._Checkpoint(tmp_path / "ck.json"), said.append)
    both = [psql(pg_pair[side], "select count(*), sum(v) from big").stdout
            for side in ("src", "dst")]
    assert both[0] == both[1] == "300000|899998\n", both
    assert any("range edges from the table's statistics" in m
               for m in said), said
