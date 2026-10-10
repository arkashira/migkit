"""A target whose key range runs past the source's is found by the deep
check - writes landing on the target, or applied twice (P0-PARITY.verify,
movement.target-ahead)."""
from tests.conftest import needs_docker, psql
from tests.mysql_pair import engine as my_engine
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.movement.target-ahead", ("postgres", "mysql"))]


@needs_docker
def test_postgresql_finds_the_target_ahead(pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    for side in ("src", "dst"):
        psql(pg_pair[side], "drop table if exists ahead;"
                            " create table ahead (id int primary key)")
    psql(pg_pair["src"], "insert into ahead values (1), (2), (3)")
    psql(pg_pair["dst"], "insert into ahead values (1), (2), (3), (100)")
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    hop = Hop(name="ahead", engine="postgres", source=ep("src"),
              target=ep("dst"), databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    got = [r for r in PostgresEngine(hop).check_deep("postgres")
           if r.scope == "postgres boundary"]
    assert [r.status for r in got] == ["diff"], got
    assert "AHEAD of source" in got[0].detail
    assert got[0].category == "movement.target-ahead"


@needs_docker
def test_mysql_finds_the_target_ahead(my_pair):
    for side in ("src", "dst"):
        my(side, "drop database if exists ahead; create database ahead;"
                 " create table ahead.t (id int primary key)")
    my("src", "insert into ahead.t values (1), (2), (3)")
    my("dst", "insert into ahead.t values (1), (2), (3), (100)")
    got = [r for r in my_engine("ahead").check_deep("ahead")
           if r.scope == "ahead boundary"]
    assert [r.status for r in got] == ["diff"], got
    assert "AHEAD of source" in got[0].detail
    assert got[0].category == "movement.target-ahead"
