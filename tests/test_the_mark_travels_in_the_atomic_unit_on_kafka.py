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

PROVES = [("cutover.two-way", ("kafka",))]

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
    assert got.cost().measured and got.cost().mean >= 0.0
    assert ("kafka", "dst", "header") in marks.MEASURED          # a probe ran


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

def test_the_rung_is_taken_once_per_tail_and_again_at_a_boundary(
        tmp_path, monkeypatch):
    """No server: the tail takes its rung from the side's facts once
    (`marks.tail_start`), keeps it while the facts hold, and takes it again
    through the same probe and decision at a boundary where they changed."""
    hop = Hop(name="w16f", engine="kafka", databases=[DB],
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1),
              options={"two_way": True})
    eng = engines.get_engine(hop)
    facts = [{"family": "kafka", "headers": True, "can_create": True}]
    monkeypatch.setattr(eng, "mark_facts", lambda side, db: dict(facts[0]))
    monkeypatch.setattr(eng, "mark_prove", lambda *a: None)
    monkeypatch.setattr(eng, "mark_timed",
                        lambda side, db, rung, rounds: [1e-5] * rounds)
    said, token = [], tmp_path / "tail.token"
    first = marks.tail_start(eng, DB, True, token, said.append)
    assert first == 'header' and eng._mark_rung == first
    assert any("carry" in m for m in said)
    said.clear()
    assert marks.tail_round(eng, DB, token, said.append) == first
    assert not said                      # the facts held: nothing chosen
    facts[0] = {"family": "kafka", "headers": True, "can_create": True, "brokers": 3}
    again = marks.tail_round(eng, DB, token, said.append)
    assert any("choosing the mark again" in m for m in said)
    assert again == 'header' == eng._mark_rung
    assert marks.kept_rung(token) == again
    assert marks.tail_start(eng, DB, False, token, said.append) is None


def test_the_tails_records_carry_the_header_and_its_reader_skips_them(space):
    eng, topic = _eng(space), _topic()
    eng._mark_rung = "header"
    m = type("M", (), {"headers": [("app", b"1")], "value": b"v", "key": b"k",
                       "timestamp": 1, "offset": 0})()
    sent = eng._carried(topic, 0, m, False)
    assert eng.mark_own(sent["headers"]) and ("app", b"1") in sent["headers"]
    eng._mark_rung = None
    assert not eng.mark_own(eng._carried(topic, 0, m, False)["headers"])
