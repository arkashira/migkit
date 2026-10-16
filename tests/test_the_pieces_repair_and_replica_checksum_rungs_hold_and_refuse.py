"""The rungs a large value's pieces, a repair's way and the replica
checksum are chosen by, each held where its needs hold and refused with
its own reason where one does not, on fakes (backlog
AUD-Z.rungs-outside-owed): no piece past a limit the side said, no span
repaired where the key is not one integer, no table copied again whole
where the hop does not allow a replace, and no replica checksum on a
target that does not replicate from the source."""
import pytest

from migkit import decide, pieces, repairways, replicasum
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('pieces', 'Sizing', '<str(s)>', 'holds'),
    ('pieces', 'Sizing', '<str(s)>', 'refuses'),
    ('repairways', 'RUNGS', '<RANGES>', 'holds'),
    ('repairways', 'RUNGS', '<RANGES>', 'refuses'),
    ('repairways', 'RUNGS', '<RELOAD>', 'holds'),
    ('repairways', 'RUNGS', '<RELOAD>', 'refuses'),
    ('repairways', 'RUNGS', '<ROWS>', 'holds'),
    ('repairways', 'RUNGS', '<ROWS>', 'refuses'),
    ('replicasum', 'RUNGS', 'own digest', 'holds'),
    ('replicasum', 'RUNGS', 'replica checksum', 'holds'),
    ('replicasum', 'RUNGS', 'replica checksum', 'refuses'),
]


def _held_then_refused(rung, ok, bad):
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(ok))
        _, out = decide.prune([rung], decide.Facts(bad))
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, rung.name) == [HOLDS, REFUSES], seen
    assert why(seen, rung.name) == out[0][1]
    return out[0][1]


def test_a_piece_is_held_within_the_sides_limits_and_refused_past_them():
    limit = pieces.Limit("max_allowed_packet", pieces.PIECE, "sent")
    sizing = pieces.Sizing(None, "dst", "app", limits=[limit])
    with watched() as seen:
        kept, out = decide.prune(sizing.rungs,
                                 decide.Facts({"limits": sizing.limits}))
    assert kept and out
    assert all(limit.takes(int(r.name)) for r in kept)
    assert all(not limit.takes(int(r.name)) for r, _ in out)
    small, large = kept[0].name, out[0][0].name
    assert branch(seen, small) == [HOLDS]
    assert branch(seen, large) == [REFUSES]
    assert "max_allowed_packet" in why(seen, large)
    assert f"a piece of {int(large):,} bytes is past" in why(seen, large)
    assert sizing.pick() <= pieces.PIECE


ROWS_OK = {"named": 4}
SPANS_OK = {"int key": True, "per row": None, "filter": None,
            "range": None, "spans": [(1, 100)], "spans why": ""}
RELOAD_OK = {"per row": None, "filter": None, "replace": True,
             "writers": None, "copier": True, "copier why": "", "rows": 10}


def _repair(name):
    return next(r for r in repairways.RUNGS if r.name == name)


@pytest.mark.parametrize("name,ok,bad,said", [
    (repairways.ROWS, ROWS_OK, {"named": 0}, "too many places to name"),
    (repairways.RANGES, SPANS_OK, dict(SPANS_OK, **{"int key": False}),
     "not one integer column"),
    (repairways.RANGES, SPANS_OK, dict(SPANS_OK, spans=[],
                                       **{"spans why": "one span differs"}),
     "one span differs"),
    (repairways.RELOAD, RELOAD_OK, dict(RELOAD_OK, replace=False),
     "target_rows: replace"),
    (repairways.RELOAD, RELOAD_OK, dict(RELOAD_OK, writers="a job"),
     "a job writes the target"),
    (repairways.RELOAD, RELOAD_OK, dict(RELOAD_OK, rows=None),
     "does not say how many rows"),
])
def test_a_repair_way_is_held_where_its_needs_hold_and_refused_where_not(
        name, ok, bad, said):
    assert said in _held_then_refused(_repair(name), ok, bad)


def test_the_replica_checksum_needs_a_target_replicating_the_source():
    rung = next(r for r in replicasum.RUNGS if r.name == "replica checksum")
    there = {"change " + c.name: decide.Stand(decide.PRESENT, "there")
             for c in rung.changes}
    ok = dict(there, **{"native replica": True, "installed": True,
                        "statement binlog": True})
    got = _held_then_refused(rung, ok, dict(ok, **{"native replica": False}))
    assert "replicate from this source" in got
    got = _held_then_refused(rung, ok, dict(ok, **{
        "migkit writes": "the hop's own tail"}))
    assert "not for a target migkit writes itself" in got


def test_migkits_own_digest_is_held_everywhere():
    own = next(r for r in replicasum.RUNGS if r.name == "own digest")
    with watched() as seen:
        kept, _ = decide.prune([own], decide.Facts({}))
    assert kept == [own] and branch(seen, "own digest") == [HOLDS]
