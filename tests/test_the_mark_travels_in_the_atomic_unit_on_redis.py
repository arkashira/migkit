"""The mark migkit's own writes carry on Redis, written in the engine's
atomic unit (MULTI / EXEC) and read back from the server's monitor.

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

# `cutover.two-way` on redis is proved here once the depth probe (capabilities.py) reads `mark_unit` as the way

pytestmark = needs_docker

space = servers.fixture("redis")

DB = "0"


def _eng(space):
    hop = Hop(name="w15h", engine="redis", source=space.src.endpoint(),
              target=space.dst.endpoint(), options={"two_way": True})
    return engines.get_engine(hop)


def test_the_rung_is_chosen_from_a_probe_and_the_choice_names_its_cost(space):
    eng = _eng(space)
    marks.MEASURED.clear()
    got = marks.choice(eng, "dst", DB)
    assert got.path == "multi" and eng._mark_rung == "multi"
    assert got.cost().measured and got.cost().mean >= 0.0
    assert ("redis", "dst", "multi") in marks.MEASURED            # a probe ran


def test_a_unit_opens_with_the_mark_and_a_reader_skips_only_its_own(space):
    eng = _eng(space)
    lines = eng.mark_watch("dst", lambda: (
        eng.mark_unit("dst", DB, "multi", 5,
                      [("set", "w15h:a", "1"), ("set", "w15h:b", "2"),
                       ("del", "w15h:gone")], run_id="r1"),
        space.dst.client().set("w15h:app1", "x")))
    groups = eng.mark_groups(lines)
    own = [g for g in groups if g["own"]]
    assert len(own) == 1
    # the mark is the first command of the transaction it marks
    assert own[0]["commands"][0][:2] == ["HSET", "migkit:origin"]
    assert len(own[0]["commands"]) == 4
    # a reader replaying what is not own applies the application's write
    other = space.src.client()
    other.delete("w15h:a", "w15h:b", "w15h:app1")
    applied = 0
    for g in groups:
        if g["own"]:
            continue
        for argv in g["commands"]:
            if argv[0].upper() == "SET" and argv[1].startswith("w15h:"):
                other.set(argv[1], argv[2])
                applied += 1
    assert applied == 1 and other.get("w15h:app1") == "x"
    assert other.get("w15h:a") is None
    assert space.dst.client().hget("migkit:origin", "batch") == "5"


def test_a_unit_that_fails_before_exec_leaves_neither_mark_nor_keys(
        space, monkeypatch):
    eng, c = _eng(space), space.dst.client()
    c.delete("migkit:origin", "w15h:x", "w15h:y", "w15h:z")
    ops = [("set", "w15h:x", "1"), ("set", "w15h:y", "2"),
           ("set", "w15h:z", "3")]
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mark.unit:2:raise")
    failpoint.reset()
    with pytest.raises(RuntimeError):
        eng.mark_unit("dst", DB, "multi", 6, ops)
    assert c.exists("migkit:origin", "w15h:x", "w15h:y", "w15h:z") == 0
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    failpoint.reset()
    eng.mark_unit("dst", DB, "multi", 6, ops)
    assert c.exists("migkit:origin", "w15h:x", "w15h:y", "w15h:z") == 4


def test_doctor_names_the_rung_and_teardown_removes_the_marker(
        space, tmp_path):
    eng, c = _eng(space), space.dst.client()
    marks.choice(eng, "dst", DB)
    token = str(tmp_path / "tail.token")
    marks._keep_rung(token, eng._mark_rung)
    assert "multi" in marks.said(eng, token, DB)
    eng.mark_unit("dst", DB, "multi", 9, [("set", "w15h:t", "1")])
    assert c.exists("migkit:origin") == 1
    marks.teardown(eng, "dst", DB)
    assert c.exists("migkit:origin") == 0
    assert "_mark_rung" not in eng.__dict__
