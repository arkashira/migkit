"""The mark migkit's own writes carry on Cassandra, written in the engine's
atomic unit (a logged batch) and read back from the cells' write times.

Proved on a server: the rung is chosen by the decision engine from a probe
of the side itself (a probe ran, the Choice names the cost of the rung it
took), a unit migkit writes carries the mark and an application's write
beside it carries none, a reader that applies what it reads applies the
application's and leaves migkit's out (counts asserted), a unit that fails
in the middle leaves neither its mark nor its rows, `doctor` names the rung,
and teardown takes away what the mark left.
"""
import pytest

from migkit import engines, failpoint, marks
from migkit.config import Endpoint, Hop
from tests import servers
from tests.conftest import needs_docker

PROVES = [("cutover.two-way", ("cassandra",))]

pytestmark = needs_docker

space = servers.fixture("cassandra")

import time

DB = "w15h_cass"


def _eng(space):
    hop = Hop(name="w15h", engine="cassandra", databases=[DB],
              source=space.src.endpoint(), target=space.dst.endpoint(),
              options={"two_way": True})
    return engines.get_engine(hop)


def _table(space):
    s = space.dst.client()
    s.execute(f"create keyspace if not exists {DB} with replication ="
              " {'class': 'SimpleStrategy', 'replication_factor': 1}")
    s.execute(f"create table if not exists {DB}.items (id int primary key,"
              " v text)")
    return s


def test_the_rung_is_chosen_from_a_probe_and_the_choice_names_its_cost(space):
    _table(space)
    eng = _eng(space)
    marks.MEASURED.clear()
    got = marks.choice(eng, "dst", DB)
    assert got.path == "batch" and eng._mark_rung == "batch"
    assert got.cost().measured and got.cost().mean >= 0.0
    assert ("cassandra", "dst", "batch") in marks.MEASURED       # a probe ran


def test_a_batch_carries_the_mark_and_a_reader_skips_only_its_own(space):
    s = _table(space)
    eng = _eng(space)
    s.execute(f"truncate {DB}.items")
    stamp = eng.mark_unit("dst", DB, "batch", 5,
                          [("write", "items", {"id": i, "v": "migkit"})
                           for i in range(3)], run_id="r1")
    eng.mark_unit("dst", DB, None, 0, [("write", "items",
                                       {"id": 100, "v": "app"})])
    rows = {r.id: r.t for r in s.execute(
        f"select id, writetime(v) as t from {DB}.items")}
    stamps = eng.mark_stamps("dst", DB)
    assert stamp in stamps and all(rows[i] == stamp for i in range(3))
    own = {i for i, t in rows.items() if eng.mark_own(stamps, t)}
    assert own == {0, 1, 2}                       # the application's: 100
    applied = [i for i, t in rows.items() if not eng.mark_own(stamps, t)]
    assert applied == [100]
    assert s.execute(f"select seq from {DB}.migkit_origin where origin ="
                     f" 'migkit' and \"batch\" = {stamp}").one().seq == 5


def test_a_batch_the_server_refuses_leaves_neither_mark_nor_rows(
        space, monkeypatch):
    s = _table(space)
    eng = _eng(space)
    s.execute(f"truncate {DB}.items")
    before = s.execute(f"select count(*) as n from {DB}.migkit_origin"
                       ).one().n
    # the last statement names a column that does not exist: the server
    # refuses the logged batch whole
    with pytest.raises(Exception):
        eng.mark_unit("dst", DB, "batch", 6,
                      [("write", "items", {"id": 1, "v": "a"}),
                       ("write", "items", {"id": 2, "nope": "b"})])
    assert s.execute(f"select count(*) as n from {DB}.items").one().n == 0
    assert s.execute(f"select count(*) as n from {DB}.migkit_origin"
                     ).one().n == before
    # and a failure before the batch is sent writes nothing either
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mark.unit.send:1:raise")
    failpoint.reset()
    with pytest.raises(RuntimeError):
        eng.mark_unit("dst", DB, "batch", 7,
                      [("write", "items", {"id": 3, "v": "c"})])
    assert s.execute(f"select count(*) as n from {DB}.items").one().n == 0
    assert s.execute(f"select count(*) as n from {DB}.migkit_origin"
                     ).one().n == before


def test_doctor_names_the_rung_and_teardown_drops_the_origin_table(
        space, tmp_path):
    s = _table(space)
    eng = _eng(space)
    marks.choice(eng, "dst", DB)
    token = str(tmp_path / "tail.token")
    marks._keep_rung(token, eng._mark_rung)
    assert "migkit_origin" in marks.said(eng, token, DB)
    marks.teardown(eng, "dst", DB)
    left = {r.table_name for r in s.execute(
        "select table_name from system_schema.tables where keyspace_name ="
        f" '{DB}'")}
    assert "migkit_origin" not in left and "migkit_probe" not in left
    assert "_mark_rung" not in eng.__dict__

def test_the_rung_is_taken_once_per_tail_and_again_at_a_boundary(
        tmp_path, monkeypatch):
    """No server: the tail takes its rung from the side's facts once
    (`marks.tail_start`), keeps it while the facts hold, and takes it again
    through the same probe and decision at a boundary where they changed."""
    hop = Hop(name="w16f", engine="cassandra", databases=[DB],
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1),
              options={"two_way": True})
    eng = engines.get_engine(hop)
    facts = [{"family": "cassandra", "can_create": True}]
    monkeypatch.setattr(eng, "mark_facts", lambda side, db: dict(facts[0]))
    monkeypatch.setattr(eng, "mark_prove", lambda *a: None)
    monkeypatch.setattr(eng, "mark_timed",
                        lambda side, db, rung, rounds: [1e-5] * rounds)
    said, token = [], tmp_path / "tail.token"
    first = marks.tail_start(eng, DB, True, token, said.append)
    assert first == 'batch' and eng._mark_rung == first
    assert any("carry" in m for m in said)
    said.clear()
    assert marks.tail_round(eng, DB, token, said.append) == first
    assert not said                      # the facts held: nothing chosen
    facts[0] = {"family": "cassandra", "can_create": True, "nodes": 3}
    again = marks.tail_round(eng, DB, token, said.append)
    assert any("choosing the mark again" in m for m in said)
    assert again == 'batch' == eng._mark_rung
    assert marks.kept_rung(token) == again
    assert marks.tail_start(eng, DB, False, token, said.append) is None


def test_the_tails_stamps_are_listed_first_and_its_reader_leaves_them_out(
        space, tmp_path, monkeypatch):
    _table(space)
    eng = _eng(space)
    marks.tail_start(eng, DB, True, tmp_path / "tail.token", lambda m: None)
    assert eng._mark_rung == "batch"
    eng._origin(DB, [111, 222, 111])
    assert {111, 222} <= eng.mark_stamps("dst", DB)
    # the reader of the way back reads the side the stamps are listed on
    monkeypatch.setattr(eng, "mark_stamps",
                        lambda side, db: eng.__class__.mark_stamps(
                            eng, "dst", db))
    monkeypatch.setattr(eng, "_micros", lambda t: t)
    rows = [{"cdc$time": 111, "cdc$operation": 1},
            {"cdc$time": 333, "cdc$operation": 1}]
    kept = eng._carried(DB, "items", rows)
    assert [r["cdc$time"] for r in kept] == [333]
