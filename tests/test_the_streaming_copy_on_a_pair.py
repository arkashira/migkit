"""The streaming copy on a live pair: split where the facts say, measured,
the next run choosing from the numbers, and the large objects carried
with the rows (backlog 0f).

Two tables of 500,000 rows each and a secondary index, on the shared
PostgreSQL 16 pair, with the streaming copy's local build. Measured
before: the copy carried the rows and none of the source's large objects,
and nothing it logged said a table was done.
"""
import json
import subprocess

import pytest

from migkit import movers, planner
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = [needs_docker, pytest.mark.skipif(
    movers.pgcopydb_runner() != "local",
    reason="the streaming copy's local build is not installed here")]

SCHEMA = ("create table t1 (id bigserial primary key, v text, n numeric);"
          " create table t2 (id bigserial primary key, v text, n numeric);"
          " create index t1_v on t1 (v)")


def _hop(pg_pair, tmp_path):
    hop = Hop(name="pair0f", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _digest(port, table):
    return psql(port, f"select count(*) || ':' || sum(hashtext(id::text"
                      f" || v || n::text)) from {table}").stdout.strip()


def _objects(port):
    return psql(port, "select string_agg(oid || '=' || md5(lo_get(oid)),"
                      " ',' order by oid) from pg_largeobject_metadata"
                ).stdout.strip()


def test_split_from_the_facts_then_whole_from_the_measurement(pg_pair,
                                                              tmp_path):
    src, dst = pg_pair["src"], pg_pair["dst"]
    psql(src, SCHEMA)
    psql(dst, SCHEMA)
    for t, k in (("t1", 7), ("t2", 3)):
        psql(src, f"insert into {t} (v, n) select md5(g::text) ||"
                  f" md5((g * {k})::text), g * 1.5 from"
                  " generate_series(1, 500000) g")
    psql(src, "select lo_from_bytea(0, convert_to(repeat('x', 300000),"
              " 'UTF8')); select lo_from_bytea(0, '\\xdeadbeef'::bytea);"
              " analyze")
    hop = _hop(pg_pair, tmp_path)
    ways = []
    for run in range(2):
        said = []
        movers.pgcopydb_move(hop, "postgres", 4, True, said.append)
        for t in ("t1", "t2"):
            assert _digest(dst, t) == _digest(src, t), (run, t)
        # the large objects came with the rows, under the same numbers
        assert _objects(dst) == _objects(src), run
        assert _objects(dst).count("=") == 2
        # each table said as it was done, in migkit's words
        done = [s for s in said if isinstance(s, str) and ": copied (" in s]
        assert len(done) == 2, said
        way = [s for s in said if isinstance(s, str)
               and s.startswith("tables ")]
        assert len(way) == 1, said
        ways.append(way[0])
    # first from the facts: two tables, each more than a quarter
    assert ways[0].startswith("tables over"), ways
    assert "each more than an even share" in ways[0], ways
    # then from what one stream did here: a 57 MB table is less than ten
    # seconds of one stream, so no part of it pays for its query
    assert ways[1].startswith("tables go whole: no part would be longer"
                              " than 10s of one stream"), ways
    kept = json.loads((tmp_path / "throughput.json").read_text())
    split, whole = kept["pgcopydb:split"][0], kept["pgcopydb:whole"][0]
    assert set(split["tables"]) == {"public.t1", "public.t2"}, split
    # split: each table in more than the one part its NULL keys take;
    # whole: one part each
    assert all(p > 2 for _, _, p in split["tables"].values()), split
    assert all(p == 1 for _, _, p in whole["tables"].values()), whole
    got = planner.variants(hop, "pgcopydb")
    assert got["split"]["stream"] > 0 and got["whole"]["stream"] > 0
    # the index set aside for the load was built again after it
    assert psql(dst, "select count(*) from pg_indexes where indexname ="
                     " 't1_v'").stdout.strip() == "1"
    # nothing of the copy left running
    left = subprocess.run(["pgrep", "-f", "pgcopydb copy"],
                          capture_output=True, text=True).stdout.split()
    assert left == [], left
    print("\n".join(ways), {w: round(v["rate"] / 2 ** 20, 1)
                            for w, v in got.items()})
