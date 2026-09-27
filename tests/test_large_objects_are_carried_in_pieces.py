"""A whole-database table copy carries the large objects its rows point
at, a piece at a time (backlog R10).

Measured before: the table copier copied the `oid` in each row and not the
object it named - after a move, the deep check found every reference
dangling (`test_large_objects.py`), and the bulk dump carried them only
because it was told `-b`. Now the objects follow the tables under the same
oids, each read and written 8 MB at a time - never held whole - and one
already there is compared piece by piece and written again only where a
piece differs.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="postgres",
                        password="test")
    hop = Hop(name="lo", engine="postgres", source=ep(pg_pair["src"]),
              target=ep(pg_pair["dst"]), databases=["lodb"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.fixture
def lodb(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop database if exists lodb")
        assert psql(port, "create database lodb").returncode == 0
    got = psql(pg_pair["src"], """
        create table docs (id int primary key, body oid);
        insert into docs values
          (1, lo_from_bytea(0, 'a small document')),
          -- 10 MB: two pieces
          (2, lo_from_bytea(0, decode(repeat(md5('x'), 655360), 'hex')));
        """, db="lodb")
    assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "create table docs (id int primary key, body oid)",
         db="lodb")
    return pg_pair


def _digest(port):
    return psql(port, "select string_agg(d.id || ':' || md5(lo_get(d.body)),"
                      " ',' order by d.id) from docs d", db="lodb"
                ).stdout.strip()


def test_the_objects_follow_the_rows(lodb, tmp_path):
    from migkit.cli import _Checkpoint
    eng = _engine(lodb, tmp_path)
    eng.move_table("lodb", "public", "docs", 500_000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    said = []
    assert eng.carry_database_objects("lodb", said.append) == 2
    assert said == ["large objects: 2 carried, 0 written again, 0 already"
                    " the same"], said
    assert _digest(lodb["dst"]) == _digest(lodb["src"])
    assert "10485760" == psql(lodb["dst"], "select length(lo_get(body))"
                                          " from docs where id = 2",
                              db="lodb").stdout.strip()
    deep = [r for r in eng.check_deep("lodb")
            if r.scope == "lodb large objects"]
    assert deep and deep[0].status == "ok", [r.detail for r in deep]
    # again: nothing to write, each compared a piece at a time
    said = []
    assert eng.carry_database_objects("lodb", said.append) == 0
    assert said[0].endswith("2 already the same"), said
    # one changed on the target is written again, the other left
    psql(lodb["dst"], "select lo_put(body, 0, 'X'::bytea) from docs where"
                      " id = 1", db="lodb")
    said = []
    assert eng.carry_database_objects("lodb", said.append) == 1
    assert said == ["large objects: 0 carried, 1 written again, 1 already"
                    " the same"], said
    assert _digest(lodb["dst"]) == _digest(lodb["src"])
