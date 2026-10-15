"""Drain a tail to a named position, the cutover approval offered only when
the lag and the last verify allow it, one-way actions held by one guard,
and a hops file with `access:` beside `hops:` taken by the guide. All on
fakes: no server."""
import json
from pathlib import Path

import pytest
import yaml

from migkit import approvals, drainmark, guide, interlock, tailctl


class Hop:
    name = "h"
    options = {}

    def __init__(self, root, options=None):
        self.root = Path(root)
        self.options = options or {}

    def report_dir(self, db=None):
        d = self.root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def target_db(self, db):
        return db


class Fence:
    """A stream's fence: reaches what it is told to, or does not."""
    CANON_ENGINE = "postgres"

    def __init__(self, reach=True, now="0/20"):
        self.reach, self.now, self.waited = reach, now, []

    def src_lsn(self, db):
        return self.now

    def fence_wait(self, db, pos, timeout=300):
        self.waited.append(pos)
        return self.reach


@pytest.fixture
def pg(monkeypatch):
    monkeypatch.setattr(drainmark, "side_engine",
                        lambda hop, eng, side: ("postgres", eng))


# --- the position ------------------------------------------------------------

def test_now_is_where_the_source_stands(pg):
    assert drainmark.named(Hop("."), Fence(now="1/AB"), "d", "now") == "1/AB"


def test_a_position_that_is_not_one_is_refused_with_the_form(pg):
    with pytest.raises(SystemExit, match="0/16B3748"):
        drainmark.named(Hop("."), Fence(), "d", "banana")
    with pytest.raises(SystemExit, match="drain to what"):
        drainmark.named(Hop("."), Fence(), "d", " ")


def test_a_source_with_no_position_says_what_proves_the_drain_instead(pg):
    with pytest.raises(SystemExit, match="mark"):
        drainmark.named(Hop("."), Fence(now=None), "d", "now")


def test_wal_locations_are_ordered_as_numbers_not_text(pg):
    assert drainmark.order("postgres", "0/9", "0/10") == -1
    assert drainmark.order("postgres", "1/0", "0/FFFFFFFF") == 1
    assert drainmark.order("postgres", "A/B", "a/b") == 0
    assert drainmark.order("mysql", "1", "2") is None


# --- the drain ---------------------------------------------------------------

def _run(tmp_path, monkeypatch, fence, go=True, alive=True, held=True,
         verify=None, saved="0/30"):
    hop = Hop(tmp_path)
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: alive)
    monkeypatch.setattr(tailctl, "pause", lambda where, t: held)
    monkeypatch.setattr(tailctl, "saved", lambda path: saved)
    guarded = []
    monkeypatch.setattr(interlock, "before_one_way",
                        lambda *a: guarded.append(a[3]))
    said = []
    got = drainmark.to_position(hop, fence, "d", hop.report_dir("d"), "0/20",
                                5, go, said.append, verify=verify)
    return got, said, guarded, fence


def test_the_drain_waits_pauses_and_reports_the_seam(pg, tmp_path,
                                                     monkeypatch):
    got, said, guarded, fence = _run(tmp_path, monkeypatch, Fence(),
                                     verify=lambda d: (True, ""))
    assert fence.waited == ["0/20"] and guarded == ["drain"]
    assert got["reached"] and got["held"] and got["applied"] == "0/30"
    assert got["past"] == 1 and got["verified"] is True
    assert "past the position" in got["said"] and "same" in got["said"]
    rec = [json.loads(x) for x in
           (tmp_path / "changelog.jsonl").read_text().splitlines()]
    assert rec[-1]["op"] == "drained" and rec[-1]["applied"] == "0/30"


def test_a_check_that_differs_at_the_seam_is_said_not_swallowed(
        pg, tmp_path, monkeypatch):
    got, *_ = _run(tmp_path, monkeypatch, Fence(),
                   verify=lambda d: (False, "2 rows differ"))
    assert got["verified"] is False and "NOT the same" in got["said"]
    assert "2 rows differ" in got["said"]


def test_a_position_not_reached_stops_nothing(pg, tmp_path, monkeypatch):
    paused = []
    monkeypatch.setattr(tailctl, "pause", lambda *a: paused.append(1))
    with pytest.raises(SystemExit, match="Nothing was stopped"):
        _run(tmp_path, monkeypatch, Fence(reach=False))
    assert paused == []


def test_no_tail_running_is_refused_before_anything_is_waited_for(
        pg, tmp_path, monkeypatch):
    fence = Fence()
    with pytest.raises(SystemExit, match="no change tail runs"):
        _run(tmp_path, monkeypatch, fence, alive=False)
    assert fence.waited == []


def test_without_go_the_plan_is_said_and_nothing_waits(pg, tmp_path,
                                                       monkeypatch):
    got, said, guarded, fence = _run(tmp_path, monkeypatch, Fence(), go=False)
    assert got["said"] == "planned" and fence.waited == []
    assert any("would drain" in s for s in said)


# --- the one interlock for one-way actions -----------------------------------

@pytest.fixture
def guarded(monkeypatch):
    side = {"engine": "postgres", "address": "a:1", "database": "d",
            "id": {"system": "x"}, "running": {"since": "t"}}
    box = {"dst": dict(side, address="b:1", id={"system": "y"})}
    monkeypatch.setattr(interlock, "identities",
                        lambda hop, eng, db: (side, box["dst"]))
    monkeypatch.setattr(interlock, "production", lambda hop, dst: None)
    monkeypatch.setattr(interlock, "_mirror", lambda hop, path: None)
    return box


def test_a_one_way_action_never_planned_is_refused_with_go(guarded,
                                                           tmp_path):
    hop, said = Hop(tmp_path), []
    with pytest.raises(SystemExit, match="no plan showed it"):
        interlock.before_one_way(hop, None, "d", "drop", True, said.append)


def test_planned_then_go_passes_and_another_action_is_not_covered(
        guarded, tmp_path):
    hop, said = Hop(tmp_path), []
    interlock.before_one_way(hop, None, "d", "switch", False, said.append)
    assert any("cannot be taken back" in s for s in said)
    interlock.before_one_way(hop, None, "d", "switch", True, said.append)
    with pytest.raises(SystemExit, match="drain was not done"):
        interlock.before_one_way(hop, None, "d", "drain", True, said.append)


def test_a_plan_for_another_target_does_not_cover_the_action(guarded,
                                                             tmp_path):
    hop, said = Hop(tmp_path), []
    interlock.before_one_way(hop, None, "d", "drop", False, said.append)
    guarded["dst"] = dict(guarded["dst"], address="c:1", id={"system": "z"})
    with pytest.raises(SystemExit, match="not the server the plan showed"):
        interlock.before_one_way(hop, None, "d", "drop", True, said.append)


def test_an_action_that_is_not_one_way_is_a_mistake(tmp_path):
    with pytest.raises(ValueError, match="not a one-way action"):
        interlock.before_one_way(Hop(tmp_path), None, "d", "copy", True, print)


# --- the cutover approval ----------------------------------------------------

def _tail(monkeypatch, **state):
    base = {"running": True, "paused": False, "lag": 0.2, "behind": 0.3,
            "lag_unknown": None}
    monkeypatch.setattr(tailctl, "state", lambda where: {**base, **state})


def _verdict(hop, status):
    (hop.report_dir() / "verdict.json").write_text(
        json.dumps({"status": status}))


def test_ready_when_the_lag_is_under_the_bound_and_the_verify_is_same(
        tmp_path, monkeypatch):
    hop = Hop(tmp_path)
    _tail(monkeypatch)
    _verdict(hop, "same")
    got = approvals.readiness(hop, "d")
    assert got["ready"] and got["blocks"] == [] and got["lag"] == 0.3


def test_lag_over_the_bound_blocks_and_names_the_next_step(tmp_path,
                                                           monkeypatch):
    hop = Hop(tmp_path, {"approvals": {"lag_below": 2}})
    _tail(monkeypatch, lag=5.0)
    _verdict(hop, "same")
    got = approvals.readiness(hop, "d")
    assert not got["ready"] and got["bound"] == 2
    assert "5.0s behind" in got["blocks"][0] and "catch up" in got["next"]


@pytest.mark.parametrize("status,word", [(None, "never run"),
                                         ("different", "ended different"),
                                         ("incomplete", "ended incomplete")])
def test_a_verify_that_is_not_same_blocks(tmp_path, monkeypatch, status,
                                          word):
    hop = Hop(tmp_path)
    _tail(monkeypatch)
    if status:
        _verdict(hop, status)
    got = approvals.readiness(hop, "d")
    assert not got["ready"] and word in got["blocks"][0]
    assert "migkit check" in got["next"]


def test_an_unknown_lag_and_a_held_tail_are_not_zero(tmp_path, monkeypatch):
    hop = Hop(tmp_path)
    _verdict(hop, "same")
    _tail(monkeypatch, lag=None, behind=None, lag_unknown="no commit time")
    assert "unknown (no commit time)" in approvals.readiness(
        hop, "d")["blocks"][0]
    _tail(monkeypatch, paused=True)
    assert "held" in approvals.readiness(hop, "d")["blocks"][0]


def test_no_tail_means_the_cutover_own_drain_proves_it(tmp_path,
                                                       monkeypatch):
    monkeypatch.setattr(tailctl, "state", lambda where: None)
    assert approvals.readiness(Hop(tmp_path), "d")["ready"]


def test_require_names_what_blocks_before_asking_any_approver(
        tmp_path, monkeypatch):
    hop = Hop(tmp_path, {"approvals": {"steps": ["cutover"], "count": 1}})
    _tail(monkeypatch, lag=9.0)
    with pytest.raises(SystemExit) as e:
        approvals.require(hop, "cutover", "d")
    text = str(e.value)
    assert "not offered for approval yet" in text and "9.0s behind" in text
    assert "Nothing was asked" in text
    assert not list(tmp_path.rglob("*.request.json"))


# --- the guide and a hops file with `access:` --------------------------------

FILE = """\
hops:
  old:
    source: postgres://u@a/db
    target: postgres://u@b/db
# who sees what
access:
  admin: [alice]
"""


def test_access_beside_hops_is_taken_and_the_new_hop_lands_under_hops(
        tmp_path):
    p = tmp_path / "hops.yaml"
    p.write_text(FILE)
    assert set(guide._existing(p, False)) == {"old"}
    guide._write(p, "new", {"source": "postgres://u@c/db",
                            "target": "postgres://u@d/db"}, False)
    data = yaml.safe_load(p.read_text())
    assert set(data["hops"]) == {"old", "new"}
    assert data["access"] == {"admin": ["alice"]}
    assert "# who sees what\naccess:" in p.read_text()


def test_another_top_level_key_is_still_refused_naming_it(tmp_path):
    p = tmp_path / "hops.yaml"
    p.write_text(FILE + "extras: 1\n")
    with pytest.raises(SystemExit, match="extras") as e:
        guide._existing(p, False)
    assert "`access`" in str(e.value)
