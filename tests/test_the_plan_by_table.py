"""A move says where each table goes and why, before it copies anything.

The per-table decisions correctness forces - a table the hop excludes, a
row filter the bulk copy cannot apply - were made in three places and said
per table nowhere. They are one plan now, drawn from the source's own
catalogue in one query, and the dry run reads it out.
"""
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _hop(engine="postgres", exclude=(), where=None):
    return Hop(name="p", engine=engine,
               source=Endpoint(host="10.0.0.1", port=5432, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="u",
                               password="CHANGE_ME"),
               databases=["appdb"], exclude=list(exclude),
               mapping={"where": where or {}})


def test_each_table_gets_one_path_and_its_reason():
    from migkit import planner
    got = planner.plan(
        _hop(exclude=["audit"], where={"orders": "region = 'apac'"}),
        "appdb", "pgdump",
        ["public.orders", "public.audit", "public.people"],
        facts={"public.people": {"rows": 1200, "key": False}})
    by = {d.table: d for d in got}
    assert by["public.audit"].path == planner.LEFT
    assert by["public.orders"].path == planner.COPIER
    assert by["public.people"].path == planner.BULK
    assert "about 1,200 rows, no key" in str(by["public.people"])


def test_a_path_that_filters_itself_keeps_its_filtered_tables():
    from migkit import planner
    got = planner.plan(_hop("mysql", where={"orders": "x = 1"}), "appdb",
                       "mydumper", ["orders"], qualifier=None)
    assert [d.path for d in got] == [planner.BULK]


def test_the_unusual_tables_are_always_shown():
    from migkit import planner
    tables = [f"public.t{i:03}" for i in range(100)] + ["public.zz"]
    got = planner.lines(planner.plan(_hop(exclude=["zz"]), "appdb",
                                     "pgdump", tables))
    assert any("public.zz: left alone" in line for line in got), got
    assert got[-1].strip().startswith("... and 81 more"), got[-1]


@needs_docker
def test_postgres_reads_its_facts_in_one_query(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    psql(pg_pair["src"], "create table public.keyed (id int primary key);"
                         " create table public.bare (v int);"
                         " insert into public.keyed select generate_series(1,50);"
                         " analyze public.keyed")
    hop = Hop(name="p", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    got = PostgresEngine(hop).table_facts("src", "postgres")
    # and its size on disk, which the plan adds up
    assert {k: got["public.keyed"][k] for k in ("rows", "key")} == \
        {"rows": 50, "key": True}, got
    assert got["public.keyed"]["bytes"] > 0, got
    # never analysed: unknown, not zero
    assert {k: got["public.bare"][k] for k in ("rows", "key")} == \
        {"rows": None, "key": False}, got


@needs_docker
def test_the_dry_run_reads_the_plan_out(pg_pair, tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, movers
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    psql(pg_pair["src"], "create table public.orders (id int primary key,"
                         " region text); create table public.audit"
                         " (id int primary key)")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  p:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    exclude: [audit]\n"
        "    mapping: {where: {orders: \"region = 'apac'\"}}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(movers, "chosen", lambda engine, table="": "pgdump")
    got = CliRunner().invoke(cli.main, ["move", "p", "--mode", "full"])
    said = " ".join(got.output.split())
    assert got.exit_code == 0, said
    assert "public.audit: left alone - the hop excludes it" in said, said
    assert "public.orders: table by table - its row filter" in said, said
    assert not [t for t in TOOLS if t in said.lower()], said
