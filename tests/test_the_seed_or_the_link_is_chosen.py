"""The copy over the link or a seed shipped on media, chosen by the decision
engine (the owner's scope, 2026-10-01): the link's measured rate against
the export, the shipping the hop states and the load; the deadline each
is held to; both times said. And the source's log held long enough for
the stream to start from the seed's position - and where it is not, a new
position and only what changed since, never the seed shipped again.
"""
import pytest

from migkit import ship
from migkit.config import Endpoint, Hop
from migkit.ship import Times

DAY = 86400.0


def _hop(**options):
    return Hop(name="big", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432, user="app"),
               target=Endpoint(host="10.0.0.2", port=5432, user="app"),
               options=options)


SHIP = {"ship": {"carrier": "disk", "path": "/mnt/seed", "shipping": "7d"}}


def _t(network=None, export=None, shipping=None, load=None, deadline=None):
    return Times(size=300 * 10 ** 12, network=network,
                 link_rate=(300 * 10 ** 12 / network) if network else None,
                 export=export, shipping=shipping, load=load,
                 deadline=deadline)


def test_a_link_that_misses_the_deadline_gives_way_to_the_carrier():
    t = _t(network=41 * DAY, export=DAY, shipping=7 * DAY, load=DAY,
           deadline=14 * DAY)
    got = ship.choose(_hop(**SHIP), t)
    assert got.rung.name == "media", got
    line = ship.said(t, got)
    # both times are said, the parts of the seed's, and why the link is
    # not taken
    assert "41.0 days" in line and "9.0 days" in line, line
    assert "not the copy over the link: it takes about 41.0 days, past the" \
        " deadline of 14.0 days" in line, line
    assert "the shipping 7.0 days" in line and "the deadline 14.0 days" \
        in line, line


def test_a_link_that_meets_it_faster_is_taken_over_the_carrier():
    t = _t(network=2 * DAY, export=DAY, shipping=7 * DAY, load=DAY,
           deadline=14 * DAY)
    got = ship.choose(_hop(**SHIP), t)
    assert got.rung.name == "network", got
    assert "measured the cheapest" in got.reason, got.reason


def test_the_seeds_floor_already_past_the_link_is_enough_to_say():
    """Only the shipping is known of the seed, and it alone is longer than
    the link takes: the link, without the rest being measured."""
    t = _t(network=DAY, shipping=7 * DAY)
    got = ship.choose(_hop(**SHIP), t)
    assert got.rung.name == "network", got


def test_nothing_measured_takes_the_carrier_the_hop_names():
    got = ship.choose(_hop(**SHIP), _t(shipping=7 * DAY))
    assert got.rung.name == "media", got
    got = ship.choose(_hop(), _t(shipping=7 * DAY))
    assert got.rung.name == "network", got


def test_neither_meeting_the_deadline_is_said():
    t = _t(network=41 * DAY, export=DAY, shipping=30 * DAY, load=DAY,
           deadline=14 * DAY)
    got = ship.choose(_hop(**SHIP), t)
    assert got.rung is None, got
    assert "neither meets it" in ship.said(t, got)


def test_the_plan_offers_the_seed_where_the_link_would_miss(monkeypatch):
    t = _t(network=41 * DAY, export=DAY, load=DAY, deadline=14 * DAY)
    monkeypatch.setattr(ship, "times", lambda *a: t)
    line = ship.plan_line(_hop(deadline="14d"), None, "app", "builtin")
    assert "a seed shipped on media would meet it" in line, line
    assert "12.0 days" in line and "ship:" in line, line
    # a link that meets it: nothing to offer, the plan says what it said
    monkeypatch.setattr(ship, "times", lambda *a: _t(network=DAY,
                                                      deadline=14 * DAY))
    assert ship.plan_line(_hop(deadline="14d"), None, "app", "builtin") == ""
    # no carrier and no deadline: nothing is weighed at all
    monkeypatch.setattr(ship, "times", lambda *a: pytest.fail("weighed"))
    assert ship.plan_line(_hop(), None, "app", "builtin") == ""


def test_the_network_copy_chosen_leaves_the_carrier_untouched(
        tmp_path, monkeypatch):
    carrier = tmp_path / "carrier"
    hop = _hop(ship={"carrier": "disk", "path": str(carrier),
                     "shipping": "7d"}, deadline="14d")
    monkeypatch.setattr(ship, "times", lambda *a: _t(
        network=DAY, export=DAY, shipping=7 * DAY, load=DAY,
        deadline=14 * DAY))

    class Eng:
        def databases(self):
            return ["app"]
    monkeypatch.setattr(ship, "_Work", lambda *a: type(
        "W", (), {"db": "app", "dir": tmp_path / "none"})())
    said = []
    got = ship.carry(hop, Eng(), None, "full", True, said.append,
                     via="builtin")
    assert got is None, said
    assert not carrier.exists()
    assert any("the copy over the link" in m for m in said), said


class _Src:
    """A source that says how much longer it keeps its log."""

    def __init__(self, *rooms, lost=None):
        self.rooms, self.lost_why = list(rooms), lost

    def stream_room(self, side, db, token):
        return self.rooms.pop(0) if len(self.rooms) > 1 else self.rooms[0]

    def position_lost(self, side, db, token):
        return self.lost_why


def test_a_log_kept_shorter_than_the_seed_takes_is_said():
    ok, words = ship.room(_Src({"seconds": 3600}), "app", "0/1", 7 * DAY)
    assert ok is False, words
    assert "1.0 hours" in words and "7.0 days" in words, words
    ok, words = ship.room(_Src({"seconds": 30 * DAY}), "app", "0/1", 7 * DAY)
    assert ok is True, words


def test_a_log_capped_in_bytes_is_timed_by_how_fast_it_grows():
    slept = []
    src = _Src({"bytes": 10 ** 9, "held_bytes": 0},
               {"bytes": 10 ** 9, "held_bytes": 10 ** 7})
    ok, words = ship.room(src, "app", "0/1", 7 * DAY, sample=10,
                          sleep=slept.append)
    # a million bytes a second: a gigabyte lasts 1,000 seconds
    assert slept == [10] and ok is False, words
    assert "17 minutes" in words, words
    src = _Src({"bytes": 10 ** 15, "held_bytes": 0},
               {"bytes": 10 ** 15, "held_bytes": 10 ** 7})
    ok, _ = ship.room(src, "app", "0/1", 7 * DAY, sample=10,
                      sleep=slept.append)
    assert ok is True


def test_a_log_nothing_caps_or_nothing_says_is_not_called_enough():
    ok, words = ship.room(_Src({"held_bytes": 5}), "app", "0/1", 7 * DAY,
                          sample=0)
    assert ok is None and "nothing caps" in words, words
    ok, words = ship.room(_Src(None), "app", "0/1", 7 * DAY)
    assert ok is None and "does not say" in words, words
    # no shipping time stated: nothing to hold the log to, and said so
    ok, words = ship.room(_Src({"seconds": 3600}), "app", "0/1", None)
    assert ok is None and "ship: shipping:" in words, words


def test_a_position_the_source_no_longer_keeps_is_found():
    assert ship.lost(_Src({"seconds": 10}), "app", "0/1") is None
    assert "no longer keeps" in ship.lost(_Src({"bytes": 0}), "app", "0/1")
    assert ship.lost(_Src({"seconds": 10}, lost="purged"), "app",
                     "0/1") == "purged"
