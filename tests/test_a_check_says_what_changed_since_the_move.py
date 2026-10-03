"""A check says what changed in each side's schema since the move left it.

A schema tool keeps a snapshot at each migration it runs, and finds drift
against it. migkit kept nothing of the schema a move left: a later check
compared the two sides as they were then, and a column added on the
target after the cutover, or a table altered on the source and not
carried, was a difference with no date. Each move keeps both sides'
shapes (`drift.kept_at_move`); a check says what changed on each side
since (`drift.since_the_move`) - said, not judged: whether the two sides
agree is the schema check's answer beside it.
"""
import pytest

import migkit.config as cfg
from migkit import drift
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


class _Shaped(Engine):
    CANON_ENGINE = "postgres"

    def __init__(self, hop, sides):
        super().__init__(hop)
        self.sides = sides

    def column_catalog(self, side, db):
        return {t: list(cols) for t, cols in self.sides[side].items()}


def _hop(**kw):
    return Hop(name="h", engine="postgres", source=Endpoint(),
               target=Endpoint(), **kw)


def test_a_change_on_the_target_after_the_move_is_said_with_its_side():
    sides = {"src": {"public.orders": [("id", "integer")]},
             "dst": {"public.orders": [("id", "integer")]}}
    hop = _hop()
    eng = _Shaped(hop, sides)
    drift.kept_at_move(hop, eng, "shop")
    assert drift.since_the_move(hop, eng, "shop") == []
    sides["dst"]["public.orders"] = [("id", "integer"), ("note", "text")]
    got = drift.since_the_move(hop, eng, "shop")
    assert [side for side, _, _ in got] == ["dst"]
    assert any("note" in line for line in got[0][2])


def test_a_baseline_kept_under_another_scope_is_not_held_to():
    sides = {"src": {"public.orders": [("id", "integer")]},
             "dst": {"public.orders": [("id", "integer")]}}
    eng = _Shaped(_hop(), sides)
    drift.kept_at_move(eng.hop, eng, "shop")
    sides["src"]["public.orders"] = [("id", "bigint")]
    narrowed = _hop(exclude=["shop.public.audit"])
    eng.hop = narrowed
    assert drift.since_the_move(narrowed, eng, "shop") == []


def test_nothing_kept_nothing_said():
    eng = _Shaped(_hop(), {"src": {}, "dst": {}})
    assert drift.since_the_move(eng.hop, eng, "shop") == []
