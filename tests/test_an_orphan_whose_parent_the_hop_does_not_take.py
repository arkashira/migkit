"""A row pointing into a table the hop does not take is said as that, and
counted after the load that wrote it (backlog P0-SCOPE.6a).

The orphan scan left out a child the hop excludes, and nothing else: a
child the hop takes pointing at a parent it leaves out - a table the
target keeps for itself - was a difference on every run, though nothing
the move did or could do would change it. Now it is a warn that names the
parent the hop does not move. A parent the hop takes, its row filter
cutting the rows a child points at included, is still a difference
(`test_orphans_behind_a_validated_key.py`).

A PostgreSQL load writes with the keys off, so such a child arrives
whether its parent does or not, and nothing said so until the check:
MySQL's move counts only the rows of a table it leaves out pointing at a
parent it replaced. Now PostgreSQL's load counts, key by key, the rows it
wrote whose parent the hop does not take, says them and records them in
the hop's change log (and so in its evidence) - never a failure.

The docker tests run one engine at a time (`-k postgres`, `-k mysql`).
"""
import json

import pytest
from click.testing import CliRunner

from migkit.config import Endpoint, Hop
from tests import mysql_pair
from tests.conftest import psql
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

DDL = ("create table public.warehouse (id int primary key);"
       " create table public.parcel (id int primary key,"
       " wid int references public.warehouse (id))")


def _pg(pg_pair, tmp_path, **scope):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="of", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], **scope)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


@pytest.fixture
def warehouses(pg_pair):
    """The target keeps its own `warehouse`, holding 1 where the source
    holds 1 and 2; the parcels the move carries point at both."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, DDL)
    psql(pg_pair["src"], "insert into public.warehouse values (1), (2);"
                         " insert into public.parcel values (1, 1), (2, 2)")
    psql(pg_pair["dst"], "insert into public.warehouse values (1)")
    yield


def test_postgres_a_parent_the_hop_leaves_out_is_a_warn_that_names_it(
        pg_pair, tmp_path, warehouses):
    # as a load writes them: with the keys off
    psql(pg_pair["dst"], "set session_replication_role = replica;"
                         " insert into public.parcel values (1, 1), (2, 2)")
    got = _pg(pg_pair, tmp_path, exclude=["warehouse"])._fk_orphans(
        "postgres")
    assert got.status == "warn", got
    assert "warehouse, which this hop does not move" in got.detail, got
    # the same row with the parent in the hop is a difference
    got = _pg(pg_pair, tmp_path)._fk_orphans("postgres")
    assert got.status == "diff" and "1 orphan rows" in got.detail, got


def test_postgres_the_load_counts_the_rows_whose_parent_it_does_not_take(
        pg_pair, tmp_path, warehouses):
    psql(pg_pair["dst"], "set session_replication_role = replica;"
                         " insert into public.parcel values (1, 1), (2, 2)")
    eng = _pg(pg_pair, tmp_path, exclude=["warehouse"])
    said = eng._orphans_left("postgres")
    assert len(said) == 1 and "1 row points at warehouse" in said[0], said
    assert "the hop does not take that table" in said[0], said
    entry = json.loads((tmp_path / "changelog.jsonl").read_text()
                       .splitlines()[-1])
    assert entry["op"] == "orphans-left" and entry["db"] == "postgres"
    assert entry["found"] == [{"table": "parcel",
                               "constraint": "parcel_wid_fkey",
                               "parent": "warehouse", "rows": 1,
                               "why": "the hop does not take that table"}]


def test_postgres_a_row_filter_on_the_parent_is_counted_too(
        pg_pair, tmp_path, warehouses):
    psql(pg_pair["dst"], "set session_replication_role = replica;"
                         " insert into public.parcel values (1, 1), (2, 2)")
    eng = _pg(pg_pair, tmp_path,
              mapping={"where": {"public.warehouse": "id <= 1"}})
    said = eng._orphans_left("postgres")
    assert len(said) == 1 and "row filter" in said[0], said


def test_postgres_a_move_says_them_and_keeps_them_in_the_evidence(
        pg_pair, tmp_path, monkeypatch, warehouses):
    import migkit.config as cfg
    from migkit import export
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  of:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    exclude: [warehouse]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "of", "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert "1 row points at warehouse rows that are not there" in said, said
    # the target's own table is as it was
    assert psql(pg_pair["dst"], "select count(*) from public.warehouse"
                ).stdout.strip() == "1"
    steps = export.gather(cfg.get_hop("of"))["steps"]
    assert [s for s in steps if s["op"] == "orphans-left"
            and s["found"][0]["parent"] == "warehouse"
            and s["found"][0]["rows"] == 1], steps


def test_a_hop_that_takes_everything_whole_reads_nothing_more():
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(Hop(name="of", engine="postgres", source=Endpoint(),
                             target=Endpoint(), databases=["app"]))

    def never(*a, **kw):
        raise AssertionError("read on a hop that leaves nothing out")
    eng._psql = never
    assert eng._orphans_left("app") == []


# -- MySQL ---------------------------------------------------------------------

@pytest.fixture
def my_warehouses(my_pair):
    ddl = ("drop database if exists orf; create database orf; use orf;"
           " create table warehouse (id int primary key);"
           " create table parcel (id int primary key, wid int,"
           " foreign key (wid) references warehouse (id));")
    my("src", ddl + " insert into warehouse values (1), (2);"
                    " insert into parcel values (1, 1), (2, 2);")
    # loaded as a load writes: with the keys off
    my("dst", ddl + " insert into warehouse values (1);"
                    " set foreign_key_checks = 0;"
                    " insert into parcel values (1, 1), (2, 2);")
    yield
    for side in ("src", "dst"):
        my(side, "drop database if exists orf")


def test_mysql_a_parent_the_hop_leaves_out_is_a_warn_that_names_it(
        my_warehouses):
    eng = mysql_pair.engine("orf")
    eng.hop.exclude = ["warehouse"]
    got = eng._fk_orphans("orf")
    assert got.status == "warn", got
    assert "warehouse, which this hop does not move" in got.detail, got
    eng.hop.exclude = []
    got = eng._fk_orphans("orf")
    assert got.status == "diff" and "1 orphan rows" in got.detail, got
