"""The move's plan says how much it carries and the room it needs.

From the source's catalogue: the tables' and indexes' size on disk, for
the tables the move carries (not the ones the hop leaves alone), and what
the target's log grows by while it loads, at the rate measured:
PostgreSQL wrote 79 MB of WAL loading 78 MB of table and index; MySQL a
42.8 MB binlog loading 52 MB of table data.
"""
from click.testing import CliRunner

from migkit import planner
from tests.conftest import needs_docker, psql


def test_the_line_adds_up_what_is_carried_and_not_what_is_left():
    decisions = [planner.Decision("public.a", planner.BULK, ""),
                 planner.Decision("public.audit", planner.LEFT, "")]
    facts = {"public.a": {"bytes": 50 * 2 ** 20, "index_bytes": 28 * 2 ** 20},
             "public.audit": {"bytes": 900 * 2 ** 20, "index_bytes": 0}}
    got = planner.size_line(decisions, facts, "postgres")
    assert got.startswith("about 78.0 MB to carry (50.0 MB of tables,"
                          " 28.0 MB of indexes"), got
    assert "its WAL grows by about 78.0 MB" in got, got
    got = planner.size_line(decisions, facts, "mysql")
    assert "its binlog (where it keeps one) grows by about 40.0 MB" in got, \
        got
    assert planner.size_line(decisions, {"public.a": {"rows": 1}},
                             "postgres") is None


@needs_docker
def test_a_postgres_plan_says_it(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    psql(pg_pair["src"], "create table public.a (id int primary key, v text);"
                         " insert into public.a select g, md5(g::text)"
                         " from generate_series(1, 5000) g")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  sz:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    got = CliRunner().invoke(cli.main, ["move", "sz"])
    said = " ".join(got.output.split())
    assert "size: about" in said and "to carry" in said, said
    assert "its WAL grows by about" in said, said


def test_the_cost_of_carrying_it_at_the_price_the_hop_gives():
    """migkit cannot read a price, so it says one only where the hop gives
    it - and prices the rows that cross, not the indexes built after."""
    decisions = [planner.Decision("public.a", planner.BULK, "")]
    facts = {"public.a": {"bytes": 10 * 2 ** 30, "index_bytes": 2 ** 30}}
    got = planner.size_line(decisions, facts, "postgres", 0.09)
    assert "carrying the tables' 10.0 GB across costs about 0.90 at the" \
        " 0.09 per GB the hop gives" in got, got
    assert "costs" not in planner.size_line(decisions, facts, "postgres")
    got = planner.size_line(decisions, facts, "postgres", "cheap")
    assert "is not a number, so no cost is said" in got, got
