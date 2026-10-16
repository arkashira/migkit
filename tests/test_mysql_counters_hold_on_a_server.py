"""MySQL's modification counters hold on a server (R20.11): an untouched
table keeps its marker and is not read again, and an insert, an update, a
delete, a truncate and an alter each move the part of the marker that says
what happened - read from `performance_schema` and `information_schema` on
a MySQL 8.4 server, not scripted.

A server restart is told from cleared counters by the marker's first part,
the Unix second the server began (`FACTS_SQL`), which two reads of one
server keep alike; a restart itself is not made here, as the pair is
shared by the lane.
"""
import pytest

from migkit import unchanged
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.counters", "mysql")]

pair = servers.fixture("mysql")
ENTRY = unchanged.COUNTERS["mysql"]


def _engine(pair, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="counters", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _marker(eng, side, db):
    got, why = eng._change_markers(side, db)
    assert not why, why
    return got["t"]


def test_each_change_moves_the_part_of_the_marker_that_names_it(
        pair, tmp_path):
    db, s = pair.name, pair.src
    s.sql(f"drop table if exists `{db}`.t")
    s.sql(f"create table `{db}`.t (id int primary key, v int)")
    s.sql(f"insert into `{db}`.t values (1, 1), (2, 2), (3, 3)")
    eng = _engine(pair, tmp_path)
    steps = [
        ("insert", f"insert into `{db}`.t values (4, 4)", "inserted"),
        ("update", f"update `{db}`.t set v = v + 1 where id = 1", "updated"),
        ("delete", f"delete from `{db}`.t where id = 4", "deleted"),
        ("truncate", f"truncate table `{db}`.t", "emptied"),
        ("alter", f"alter table `{db}`.t add column w int", "column"),
    ]
    before = _marker(eng, "src", db)
    # nothing written: the same marker, the server's start alike
    assert _marker(eng, "src", db) == before
    assert ENTRY.parts(before)["started"] not in ("", "None", "0")
    assert not ENTRY.dead(before) and ENTRY.evidence(before)
    for name, sql, word in steps:
        s.sql(sql)
        now = _marker(eng, "src", db)
        said = " ".join(ENTRY.explain(before, now))
        assert now != before, name
        assert word in said, (name, said)
        # the server did not restart: its start is the same
        assert ENTRY.parts(now)["started"] == ENTRY.parts(before)["started"]
        assert not unchanged.skippable({"src": before, "dst": before},
                                       now, now), name
        before = now
    assert _marker(eng, "src", db) == before


def test_cleared_counters_are_told_from_a_restart(pair, tmp_path):
    db, s = pair.name, pair.src
    s.sql(f"drop table if exists `{db}`.t")
    s.sql(f"create table `{db}`.t (id int primary key)")
    s.sql(f"insert into `{db}`.t values (1), (2)")
    eng = _engine(pair, tmp_path)
    before = _marker(eng, "src", db)
    s.sql("truncate table performance_schema.table_io_waits_summary_by_table")
    now = _marker(eng, "src", db)
    said = " ".join(ENTRY.explain(before, now))
    assert "the counters were cleared" in said
    assert ENTRY.parts(now)["started"] == ENTRY.parts(before)["started"]
    # a server that began again is the other cause, said apart
    began = before.split("|")
    began[0] = str(int(began[0]) + 3600)
    assert "the server restarted" in " ".join(
        ENTRY.explain(before, "|".join(began)))


def test_an_untouched_table_is_not_read_again_and_a_touched_one_is(
        pair, tmp_path):
    db = pair.name
    for side in (pair.src, pair.dst):
        side.sql(f"drop table if exists `{db}`.t")
        side.sql(f"create table `{db}`.t (id int primary key, v int)")
        side.sql(f"insert into `{db}`.t values (1, 1), (2, 2)")
    eng = _engine(pair, tmp_path)
    first = [r for r in eng._own_check_data(db) if r.scope == f"{db}.t"]
    assert first[0].status == "ok" and "unchanged" not in first[0].detail
    again = [r for r in eng._own_check_data(db) if r.scope == f"{db}.t"]
    assert again[0].status == "ok"
    assert "unchanged since it was last proved equal" in again[0].detail
    # a write on the source moves its counters: read again, and different
    pair.src.sql(f"update `{db}`.t set v = 9 where id = 2")
    after = [r for r in eng._own_check_data(db) if r.scope == f"{db}.t"]
    assert after[0].status == "diff"
    assert "unchanged" not in after[0].detail
    # a proof taken while the counts are asked for is never skipped
    pair.src.sql(f"update `{db}`.t set v = 2 where id = 2")
    eng._own_check_data(db)
    held = [r for r in eng._own_check_data(db, with_counts=True)
            if r.scope == f"{db}.t"]
    assert "unchanged" not in held[0].detail
