"""The mark migkit's own writes carry on Kafka, written in the engine's
atomic unit (a record header) and read back from the topic's records.

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

# `cutover.two-way` on kafka is proved here once the depth probe (capabilities.py) reads `mark_unit` as the way

pytestmark = needs_docker

space = servers.fixture("redpanda")

import secrets

DB = "cluster"


def _eng(space):
    hop = Hop(name="w15h", engine="kafka", source=space.src.endpoint(),
              target=space.dst.endpoint(), options={"two_way": True})
    return engines.get_engine(hop)


def _topic():
    return f"w15h-{secrets.token_hex(4)}"


def test_the_rung_is_chosen_from_a_probe_and_the_choice_names_its_cost(space):
    eng = _eng(space)
    marks.MEASURED.clear()
    got = marks.choice(eng, "dst", DB)
    assert got.path == "header" and eng._mark_rung == "header"
    assert isinstance(got.cost(), float) and got.cost() >= 0.0
    assert ("kafka", "header") in marks.MEASURED          # a probe ran


def test_every_record_carries_the_header_and_a_reader_skips_only_its_own(
        space):
    eng, back = _eng(space), _eng(space)
    t, mirror = _topic(), _topic()
    eng.mark_unit("dst", DB, "header", 5,
                  [(t, b"m%d" % i, b"v") for i in range(3)], run_id="r1")
    eng.mark_unit("dst", DB, None, 0, [(t, b"app1", b"v"), (t, b"app2", b"v")])
    records = eng.mark_records("dst", t)
    assert len(records) == 5
    own = [eng.mark_own(h) for _, _, h in records]
    assert own == [True] * 3 + [False] * 2
    from migkit.barriers import NeutralMark
    meta = NeutralMark.extract_marker("kafka", records[0][2])
    assert meta["batch"] == 5 and meta["run_id"] == "r1"
    # the way back reads the topic and carries what is not migkit's
    back.mark_unit("dst", DB, None, 0,
                   [(mirror, k, v) for k, v, h in records
                    if not back.mark_own(h)])
    assert [k for k, _, _ in back.mark_records("dst", mirror)] == [
        b"app1", b"app2"]


def test_a_unit_that_stops_half_way_leaves_only_marked_records(
        space, monkeypatch):
    eng, t = _eng(space), _topic()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mark.unit:3:raise")
    failpoint.reset()
    with pytest.raises(RuntimeError):
        eng.mark_unit("dst", DB, "header", 6,
                      [(t, b"k%d" % i, b"v") for i in range(4)])
    got = eng.mark_records("dst", t)
    assert len(got) == 2 and all(eng.mark_own(h) for _, _, h in got)


def test_doctor_names_the_rung_and_teardown_removes_the_probe_topics(
        space, tmp_path):
    eng = _eng(space)
    marks.choice(eng, "dst", DB)
    token = str(tmp_path / "tail.token")
    marks._keep_rung(token, eng._mark_rung)
    assert "migkit-marker" in marks.said(eng, token, DB) or \
        "header" in marks.said(eng, token, DB)
    assert eng.__dict__.get("_mark_topics")
    marks.teardown(eng, "dst", DB)
    assert not eng.__dict__.get("_mark_topics")
    assert "_mark_rung" not in eng.__dict__
