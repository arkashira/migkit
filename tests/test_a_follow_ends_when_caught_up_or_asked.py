"""How long a follow runs (backlog F7, P3): until asked to end, or until
it has caught up with where the source's log was when it first read to
its end - which is what a run a schedule started does, and at the latest
at its slot's end (`tailctl.until`, `schedule.slot_end`). The change leg
run from here catches up and ends as it always did, or with
`follow_until: stopped` runs until asked and then ends at the source's
position of that moment (`movers._follow_until_asked`).
"""
import json
import threading
import time

import pytest

from migkit import movers, schedule, tailctl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _hop(options=None):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    return Hop(name="u", engine="postgres", source=ep, target=ep,
               databases=["d"], options=dict(options or {}))


def test_a_follow_runs_until_asked_unless_a_schedule_started_it():
    assert tailctl.until(_hop()) == "stopped"
    assert tailctl.until(_hop(), scheduled=True) == "caught_up"
    assert tailctl.until(_hop({"follow_until": "stopped"}),
                         scheduled=True) == "stopped"
    assert tailctl.until(_hop({"follow_until": "caught-up"})) == "caught_up"
    with pytest.raises(SystemExit):
        tailctl.until(_hop({"follow_until": "forever"}))


def test_a_slot_s_run_knows_when_its_slot_ends(monkeypatch):
    monkeypatch.delenv(schedule.SLOT_ENDS, raising=False)
    assert schedule.slot_end() is None
    monkeypatch.setenv(schedule.SLOT_ENDS, "1790000000.5")
    assert schedule.slot_end() == 1790000000.5


def test_the_change_leg_runs_on_only_where_the_hop_says_so():
    assert movers._follow_runs(_hop()) is False
    assert movers._follow_runs(_hop({"follow_until": "caught_up"})) is False
    assert movers._follow_runs(_hop({"follow_until": "stopped"})) is True


class _Proc:
    def __init__(self):
        self.ended = False

    def poll(self):
        return 0 if self.ended else None


def test_the_leg_ends_at_the_position_of_the_moment_it_is_asked(
        tmp_path, monkeypatch):
    hop = _hop({"follow_until": "stopped"})
    hop.report_dir = lambda db=None: tmp_path
    ended = []
    proc = _Proc()

    def end(d, env, log=None):
        ended.append(time.time())
        proc.ended = True
    monkeypatch.setattr(movers, "_follow_end", end)
    t = threading.Thread(target=movers._follow_until_asked,
                         args=(hop, "d", tmp_path / "follow", {}, proc))
    t.start()
    time.sleep(3)
    # running, and not ended while nobody asked
    assert tailctl.alive(tmp_path) and not ended
    asked = time.time()
    tailctl.stop(tmp_path)
    t.join(30)
    assert ended and ended[0] >= asked
    assert not tailctl.alive(tmp_path)


def test_a_leg_a_slot_started_ends_at_the_slot_s_end(tmp_path,
                                                     monkeypatch):
    """A scheduled leg the hop keeps applying until asked held its slot
    until the schedule's `max_duration` cut it off, and the slot counted
    as failed (audit K14)."""
    hop = _hop({"follow_until": "stopped"})
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setenv(schedule.SLOT_ENDS, str(time.time() + 1))
    ended, said = [], []
    proc = _Proc()

    def end(d, env, log=None):
        ended.append(time.time())
        proc.ended = True
    monkeypatch.setattr(movers, "_follow_end", end)
    began = time.time()
    movers._follow_until_asked(hop, "d", tmp_path / "follow", {}, proc,
                               said.append)
    assert ended and ended[0] - began < 10
    assert "slot ends" in said[0], said
    assert said[-1].startswith("the schedule's slot ended"), said
    assert not tailctl.alive(tmp_path)


DB = "postgres"


def _pair(pg_pair, where, name, options):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=[DB], options=options)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


@needs_docker
def test_a_tail_that_follows_until_caught_up_ends_by_itself(pg_pair,
                                                            tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.cu;"
                   " create table public.cu (id int primary key)")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "caughtup",
                 {"follow_until": "caught_up"})
    path = where / "tail-token.json"
    lines = []
    try:
        pair.tail_start(DB, path)
        psql(pg_pair["src"], "insert into public.cu select g from"
                             " generate_series(1, 300) g")
        began = time.time()
        # returns by itself, having applied what was there to read
        pair.tail_apply(DB, True, path, lines.append)
        assert time.time() - began < 120
        assert any("caught up with the source" in s for s in lines), lines
        got = psql(pg_pair["dst"], "select count(*) from public.cu")
        assert got.stdout.strip() == "300", lines
        assert not tailctl.stopped(where)
    finally:
        psql(pg_pair["src"], "select pg_drop_replication_slot("
                             f"'{pair.src_engine.slot_name()}')")


@needs_docker
def test_a_slot_s_tail_ends_at_the_slot_s_end(pg_pair, tmp_path,
                                              monkeypatch):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.se;"
                   " create table public.se (id int primary key)")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "slotend", {"follow_until": "stopped"})
    path = where / "tail-token.json"
    lines = []
    monkeypatch.setenv(schedule.SLOT_ENDS, str(time.time() + 4))
    try:
        began = time.time()
        pair.tail_apply(DB, True, path, lines.append)
        assert 3 < time.time() - began < 60, lines
        assert any("slot has ended" in s for s in lines), lines
        # the position is saved for the next slot
        assert json.loads(path.read_text())["token"]
    finally:
        psql(pg_pair["src"], "select pg_drop_replication_slot("
                             f"'{pair.src_engine.slot_name()}')")
