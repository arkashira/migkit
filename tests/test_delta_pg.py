"""End-to-end delta verify against throwaway docker postgres: baseline,
touch rows on source only, delta flags exactly those pks, window replays
until repaired, then advances clean."""
import textwrap

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(tmp_path, ports):
    import migkit.config as cfg
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="dlt", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=ports["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=ports["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    return PostgresEngine(hop)


def test_delta_verify_full_loop(pg_pair, tmp_path):
    eng = _engine(tmp_path, pg_pair)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table d (id bigint primary key, v text);"
             " insert into d select g, 'v'||g from generate_series(1,500) g;")

    r = eng.delta_verify("postgres")
    assert r[0].status == "ok"
    assert "created" in r[0].detail

    psql(pg_pair["src"], "update d set v = 'touched' where id in (3, 4);"
         " insert into d values (901, 'new');")

    r = eng.delta_verify("postgres")
    assert r[0].status == "diff"
    assert "NOT advanced" in r[0].detail
    tbl = [x for x in r if x.scope.endswith("public.d")][0]
    assert "missing=1" in tbl.detail and "changed=2" in tbl.detail
    pkdir = eng._report("postgres")
    assert (pkdir / "data-public.d.missing").read_text().strip() == "901"
    assert sorted((pkdir / "data-public.d.changed")
                  .read_text().split()) == ["3", "4"]

    # idempotent: nothing fixed, the same window replays
    r = eng.delta_verify("postgres")
    assert r[0].status == "diff"

    psql(pg_pair["dst"], "update d set v = 'touched' where id in (3, 4);"
         " insert into d values (901, 'new');")
    r = eng.delta_verify("postgres")
    assert r[0].status == "ok"
    assert "advanced" in r[0].detail

    r = eng.delta_verify("postgres")
    assert r[0].status == "ok"
    assert "0 changes" in r[0].detail

    eng.delta_teardown("postgres")
    slots = psql(pg_pair["src"],
                 "select count(*) from pg_replication_slots").stdout.strip()
    assert slots == "0"


def test_consistent_snapshot_and_fence(pg_pair, tmp_path):
    eng = _engine(tmp_path, pg_pair)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table c1 (id int primary key, v int);"
             " insert into c1 select g, g from generate_series(1,300) g;"
             " create table c2 (id int primary key);"
             " insert into c2 select g from generate_series(1,50) g;")
    rc, out = eng._fast_consistent("postgres")
    assert rc == 0
    assert "consistent snapshot" in out and "src lsn=" in out
    assert out.count(": OK") == 2

    # no active consumer on the lab source -> fence must say "cannot"
    assert eng.fence_wait("postgres", eng.src_lsn("postgres")) is None

    psql(pg_pair["dst"], "update c1 set v = 0 where id = 7")
    rc, out = eng._fast_consistent("postgres")
    assert rc == 1
    assert "c1: DIFF" in out
    cols = eng._column_fingerprint("postgres", "public.c1")
    assert cols == ["v"]
