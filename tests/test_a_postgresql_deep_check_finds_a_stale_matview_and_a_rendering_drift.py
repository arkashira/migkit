"""PostgreSQL's deep check finds a materialized view the target holds
unpopulated, and a value that one side renders differently from the other
(P0-PARITY.verify)."""
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.materialized-views", "postgres"),
          ("verify.value.rendering", "postgres")]


def _engine(pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    hop = Hop(name="deepmv", engine="postgres", source=ep("src"),
              target=ep("dst"), databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _scope(results, name):
    return [r for r in results if r.scope == f"postgres {name}"]


@needs_docker
def test_a_matview_left_unpopulated_is_found(pg_pair, tmp_path):
    for side in ("src", "dst"):
        psql(pg_pair[side], "drop materialized view if exists mv;"
                            " drop table if exists base;"
                            " create table base (id int primary key,"
                            " v int); insert into base values (1, 1)")
    psql(pg_pair["src"], "create materialized view mv as select * from base")
    psql(pg_pair["dst"], "create materialized view mv as select * from base"
                         " with no data")
    got = _scope(_engine(pg_pair, tmp_path).check_deep("postgres"),
                 "matviews")
    assert [r.status for r in got] == ["diff"], got
    assert "not populated on target" in got[0].detail
    assert got[0].category == "structure.materialized-views"
    psql(pg_pair["dst"], "refresh materialized view mv")
    got = _scope(_engine(pg_pair, tmp_path).check_deep("postgres"),
                 "matviews")
    assert [r.status for r in got] == ["ok"], got


@needs_docker
def test_a_value_one_side_renders_differently_is_found(pg_pair, tmp_path):
    # the same duration kept as days and hours on one side and as hours on
    # the other: the text a client reads is not the same
    for side, value in (("src", "1 day 2 hours"), ("dst", "26 hours")):
        psql(pg_pair[side], "drop table if exists spans;"
                            " create table spans (id int primary key,"
                            f" d interval); insert into spans values (1,"
                            f" '{value}')")
    got = _scope(_engine(pg_pair, tmp_path).check_deep("postgres"), "render")
    assert [r.status for r in got] == ["diff"], got
    assert "spans.d" in got[0].detail and "renders differently" in \
        got[0].detail
    assert got[0].category == "value.rendering"
    psql(pg_pair["dst"], "update spans set d = '1 day 2 hours'")
    got = _scope(_engine(pg_pair, tmp_path).check_deep("postgres"), "render")
    assert [r.status for r in got] == ["ok"], got
