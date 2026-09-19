"""The statistics a bulk load leaves behind, and who fixes them.

The data lands, every structural check passes, the counts match, the
checksums match - and the target reads slowly, because the planner has never
looked at the tables. It gets reported as the new engine being slower than
the old one.

Measured on PostgreSQL 16 immediately after loading 200,000 rows:

    reltuples            -1        (14+ for "never counted")
    last_analyze         null
    last_autoanalyze     null
    n_mod_since_analyze  200000

Autoanalyze does catch that one up within a naptime, because the load itself
exceeds its threshold. What it never catches up is a table loaded with
`autovacuum_enabled = false` - which is a real practice during bulk loads,
and a real thing to forget to undo. That table is what the tests below use,
because it is the state that does not heal itself.

Before this, `migkit check --deep` reported eighteen green checks on a
target whose planner had never seen a single one of its tables.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = 20000


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="stats", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _seed(pg_pair):
    """The same table on both sides, and on the target it is one autovacuum
    will never touch."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        extra = (" with (autovacuum_enabled = false)"
                 if port == pg_pair["dst"] else "")
        got = psql(port, "drop table if exists public.loaded;"
                         " create table public.loaded (id bigint primary key,"
                         f" v text){extra};"
                         " insert into public.loaded select g, 'v'||g from"
                         f" generate_series(1,{ROWS}) g;")
        assert got.returncode == 0, got.stderr


def _statistics(results):
    got = [r for r in results if r.scope.endswith("statistics")]
    assert got, [r.scope for r in results]
    return got[0]


def test_a_target_the_planner_has_never_looked_at_is_reported(pg_pair,
                                                               tmp_path):
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path)
    got = _statistics(eng.check_deep("postgres"))
    assert got.status == "diff", got.detail
    assert "public.loaded" in got.detail, got.detail
    assert "no statistics" in got.detail, got.detail
    assert "vacuumdb" in got.fix_hint or "migkit move" in got.fix_hint


def test_settling_the_target_is_what_makes_it_green(pg_pair, tmp_path):
    """The check is only worth having because the tool can fix it: this is
    the same command an operator would run, run by migkit."""
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path)
    assert _statistics(eng.check_deep("postgres")).status == "diff"

    said = eng.settle_target("postgres")
    assert said and "analyzed" in said, said

    after = _statistics(eng.check_deep("postgres"))
    assert after.status == "ok", after.detail
    # and the server agrees, not just migkit
    got = psql(pg_pair["dst"],
               "select last_analyze is not null or last_autoanalyze is not"
               " null from pg_stat_user_tables where relname = 'loaded';")
    assert got.stdout.strip() == "t", got.stdout


def test_a_table_rewritten_since_its_statistics_were_taken_is_a_warning(
        pg_pair, tmp_path):
    """Not an error - the numbers exist, they are just old. The threshold is
    autovacuum's own scale factor rather than one migkit invented."""
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path)
    eng.settle_target("postgres")
    assert _statistics(eng.check_deep("postgres")).status == "ok"

    psql(pg_pair["dst"], "update public.loaded set v = v || 'x'"
                         f" where id <= {ROWS // 4};")
    got = _statistics(eng.check_deep("postgres"))
    assert got.status == "warn", got.detail
    assert "public.loaded" in got.detail
    assert "10%" in got.detail, got.detail


def test_the_verdict_needs_no_server(tmp_path):
    """The shared part, so an engine that gathers the facts differently
    still reaches the same judgement."""
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="s", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    # names chosen so a substring check cannot match ordinary prose
    never = eng._planner_stats_result(
        "x", [("never_seen", 100, False, 100),
              ("already_done", 100, True, 0)], "hint")
    assert never.status == "diff", never.detail
    assert "never_seen" in never.detail
    assert "already_done" not in never.detail, never.detail

    stale = eng._planner_stats_result(
        "x", [("moved_a_lot", 1000, True, 500),
              ("barely_touched", 1000, True, 1)], "hint")
    assert stale.status == "warn", stale.detail
    assert "moved_a_lot" in stale.detail
    assert "barely_touched" not in stale.detail, stale.detail

    fine = eng._planner_stats_result(
        "x", [("quiet", 1000, True, 99)], "hint")
    assert fine.status == "ok", fine.detail
    # exactly at the threshold is not over it
    edge = eng._planner_stats_result("x", [("quiet", 1000, True, 100)],
                                     "hint")
    assert edge.status == "ok", edge.detail


def test_mysql_says_what_it_cannot_prove_rather_than_guessing(tmp_path):
    """The two cheap MySQL signals were measured to lie in opposite
    directions, so the engine reports the gap instead of a verdict."""
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="s", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._planner_stats("x")
    assert got.status == "skip", got.detail
    assert "50,000" in got.detail and "19" in got.detail, got.detail
    assert "guesswork" in got.detail
