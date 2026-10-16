"""The rungs a whole database's move, its server-to-server legs, the
streaming copy's split, a leg's compression and the way a hop's changes
follow are chosen by, each held where its needs hold and refused with its
own reason where one does not, on fakes (backlog
AUD-Z.rungs-outside-owed): no program for an engine it does not move, no
pull or subscription the hop or the servers do not allow, no split the
copy installed cannot make, and no way of following the source that
cannot read it."""
from types import SimpleNamespace

import pytest

from migkit import decide, legs, movers
from migkit.config import Endpoint, Hop
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('legs', 'rungs', 'pgpull', 'holds'),
    ('legs', 'rungs', 'pgpull', 'refuses'),
    ('legs', 'rungs', 'pgsubscribe', 'holds'),
    ('legs', 'rungs', 'pgsubscribe', 'refuses'),
    ('movers', '_compression_choice', '<sq.name>', 'holds'),
    ('movers', '_copy_split', 'split', 'holds'),
    ('movers', '_copy_split', 'split', 'refuses'),
    ('movers', '_copy_split', 'whole', 'holds'),
    ('movers', '_ladder', 'builtin', 'holds'),
    ('movers', '_ladder', 'mirror', 'holds'),
    ('movers', '_ladder', 'mirror', 'refuses'),
    ('movers', '_ladder', 'mongodump', 'holds'),
    ('movers', '_ladder', 'mongodump', 'refuses'),
    ('movers', '_ladder', 'mongosync', 'holds'),
    ('movers', '_ladder', 'mongosync', 'refuses'),
    ('movers', '_ladder', 'mydumper', 'holds'),
    ('movers', '_ladder', 'mydumper', 'refuses'),
    ('movers', '_ladder', 'mysqlsh', 'holds'),
    ('movers', '_ladder', 'mysqlsh', 'refuses'),
    ('movers', '_ladder', 'native', 'holds'),
    ('movers', '_ladder', 'native', 'refuses'),
    ('movers', '_ladder', 'pgcopydb', 'holds'),
    ('movers', '_ladder', 'pgcopydb', 'refuses'),
    ('movers', '_ladder', 'pgdump', 'holds'),
    ('movers', '_ladder', 'pgdump', 'refuses'),
    ('movers', '_ladder', 'pgloader', 'holds'),
    ('movers', '_ladder', 'pgloader', 'refuses'),
    ('movers', '_ladder', 'redisshake', 'holds'),
    ('movers', '_ladder', 'redisshake', 'refuses'),
    ('movers', '_ladder', 'riotx', 'holds'),
    ('movers', '_ladder', 'riotx', 'refuses'),
    ('movers', 'stream_ways', 'follow', 'holds'),
    ('movers', 'stream_ways', 'follow', 'refuses'),
    ('movers', 'stream_ways', 'pipeline', 'holds'),
    ('movers', 'stream_ways', 'pipeline', 'refuses'),
    ('movers', 'stream_ways', 'replica', 'holds'),
    ('movers', 'stream_ways', 'replica', 'refuses'),
    ('movers', 'stream_ways', 'subscription', 'holds'),
    ('movers', 'stream_ways', 'subscription', 'refuses'),
    ('movers', 'stream_ways', 'tail', 'holds'),
    ('movers', 'stream_ways', 'tail', 'refuses'),
]


def _held_then_refused(rung, ok, bad):
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(ok))
        _, out = decide.prune([rung], decide.Facts(bad))
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, rung.name) == [HOLDS, REFUSES], seen
    assert why(seen, rung.name) == out[0][1]
    return out[0][1]


PROGRAMS = ("pgcopydb", "pgdump", "mydumper", "mysqlsh", "pgloader",
            "mongosync", "mongodump", "riotx", "redisshake", "mirror",
            "native")


@pytest.fixture
def installed(monkeypatch):
    """Every program the ladder drives installed here, and an engine
    called `moved` that every one of them moves."""
    monkeypatch.setattr(movers, "supported", lambda e, via: e == "moved")
    monkeypatch.setattr(movers, "which", lambda p: f"/usr/bin/{p}")
    monkeypatch.setattr(movers, "pgcopydb_available", lambda: True)
    monkeypatch.setattr(movers, "_mirror_runner", lambda: "/usr/bin/mm")


@pytest.mark.parametrize("name", PROGRAMS)
def test_a_way_is_held_for_an_engine_it_moves_and_refused_otherwise(
        installed, name):
    rung = next(r for r in movers._ladder() if r.name == name)
    got = _held_then_refused(rung, {"engine": "moved"}, {"engine": "other"})
    assert "does not move this engine's hops" in got


@pytest.mark.parametrize("name", [p for p in PROGRAMS if p != "native"])
def test_a_way_whose_program_is_missing_is_refused(installed, monkeypatch,
                                                   name):
    monkeypatch.setattr(movers, "which", lambda p: None)
    monkeypatch.setattr(movers, "pgcopydb_available", lambda: False)
    monkeypatch.setattr(movers, "_mirror_runner", lambda: None)
    rung = next(r for r in movers._ladder() if r.name == name)
    with watched() as seen:
        _, out = decide.prune([rung], decide.Facts({"engine": "moved"}))
    assert [r for r, _ in out] == [rung]
    assert branch(seen, name) == [REFUSES]


def test_the_table_copier_is_held_for_every_engine():
    rung = next(r for r in movers._ladder() if r.name == "builtin")
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts({"engine": "any"}))
    assert kept == [rung] and branch(seen, "builtin") == [HOLDS]


def _hop(**options):
    return Hop(name="pull", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432, user="app",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="app",
                               password="CHANGE_ME"),
               options=options)


def _leg_facts(**over):
    side = {"version": 160004, "wal_level": "logical", "free_slots": 2,
            "free_senders": 2, "replication": True, "available": True,
            "may_pull": True, "may_subscribe": True, "large_objects": 0}
    facts = {"engine": "postgres", "rtt": {"dst": 500.0},
             "hop": _hop(server_to_server={"source": "10.0.0.1:5432"}),
             "legs": {"source": dict(side), "target": dict(side)},
             "change " + legs.PUBLICATION.name:
                 decide.Stand(decide.PRESENT, "there")}
    facts.update(over)
    return facts


@pytest.mark.parametrize("name,bad,said", [
    ("pgpull", {"hop": _hop()}, "server_to_server"),
    ("pgpull", {"legs": {"source": {}, "target": {"error": "timeout"},
                         }}, "the target could not be asked: timeout"),
    ("pgpull", {"legs": {"source": {}, "target": {"available": False}}},
     "foreign-data extension"),
    ("pgsubscribe", {"legs": {"source": {"version": 150000,
                                         "wal_level": "logical"},
                              "target": {"version": 160000}}},
     "only the same version reads"),
    ("pgsubscribe", {"legs": {"source": {"version": 160000,
                                         "wal_level": "replica"},
                              "target": {"version": 160000}}},
     "wal_level is replica"),
])
def test_a_server_to_server_leg_is_held_where_both_servers_allow_it(
        name, bad, said):
    rung = next(r for r in legs.rungs() if r.name == name)
    got = _held_then_refused(rung, _leg_facts(), _leg_facts(**bad))
    assert said in got, got


def test_the_copy_splits_a_table_only_where_parts_can_be_made(monkeypatch):
    from migkit import planner
    monkeypatch.setattr(planner, "variants", lambda hop, way: {})
    have = {"--split-tables-larger-than", "--split-max-parts"}
    sizes = {"big": 4 << 30, "small": 1 << 20}
    with watched() as seen:
        split = movers._copy_split(None, 4, sizes, have)
        whole = movers._copy_split(None, 4, sizes, frozenset())
    assert split.way == "split" and whole.way == "whole"
    assert branch(seen, "split") == [HOLDS, REFUSES]
    assert branch(seen, "whole") == [HOLDS]
    assert why(seen, "split") == "the copy installed cannot split one"
    with watched() as seen:
        assert movers._copy_split(None, 1, sizes, have).way == "whole"
    assert why(seen, "split") == "one stream"


def test_every_compression_a_leg_has_a_core_for_is_held():
    leg = SimpleNamespace(name="src to dst", link=1e6, near_cores=2,
                          far_cores=2)
    levels = [movers.Squeeze(1, 0.5, 5e7, 9e7),
              movers.Squeeze(3, 0.4, 2e7, 9e7)]
    with watched() as seen:
        choice, by, passed = movers._compression_choice(leg, levels)
    assert set(by) == {"plain", "level 1", "level 3"} and not passed
    for name in by:
        assert branch(seen, name) == [HOLDS]
    assert choice.path in by


STREAM = {"engine": "postgres", "repl": True, "tail": True, "ready": True,
          "costs": {}, "own_refused": None, "publication_risk": None,
          "broker": (True, ""), "drop": None}


def _stream(name):
    return next(r for r in movers.stream_ways() if r.name == name)


def test_the_leg_from_here_is_held_only_where_the_target_cannot_dial(
        monkeypatch):
    monkeypatch.setattr(movers, "follow_selected", lambda: "follow")
    rung = _stream("follow")
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(dict(STREAM)))
    monkeypatch.setattr(movers, "follow_selected", lambda: "subscription")
    with watched() as more:
        _, out = decide.prune([rung], decide.Facts(dict(STREAM)))
    seen += more
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, "follow") == [HOLDS, REFUSES]
    assert "the target can dial the source" in why(seen, "follow")


def test_the_servers_replication_is_refused_where_the_target_cannot_dial(
        monkeypatch):
    monkeypatch.setattr(movers, "follow_selected", lambda: "subscription")
    got = _held_then_refused(_stream("subscription"), STREAM,
                             dict(STREAM, publication_risk="the source"
                                  " publishes every table, new ones too"))
    assert "new ones too" in got
    monkeypatch.setattr(movers, "follow_selected", lambda: "follow")
    with watched() as seen:
        _, out = decide.prune([_stream("subscription")],
                              decide.Facts(dict(STREAM)))
    assert "MIGKIT_CDC=follow" in why(seen, "subscription")


@pytest.mark.parametrize("name,ok,bad,said", [
    ("tail", dict(STREAM, engine="mysql"),
     dict(STREAM, engine="mysql", tail=False), "does not read this engine"),
    ("tail", dict(STREAM, engine="mysql"),
     dict(STREAM, engine="mysql", ready=False), "needs a driver"),
    ("tail", dict(STREAM, engine="mysql"), dict(STREAM),
     "go by its servers' replication"),
    ("replica", dict(STREAM, engine="mysql", ready=False),
     dict(STREAM, ready=False), "is not set up for this engine"),
    ("replica", dict(STREAM, engine="mysql", ready=False),
     dict(STREAM, engine="mysql"), "applies by key"),
    ("pipeline", dict(STREAM, engine="mysql", ready=False),
     dict(STREAM, engine="mysql", ready=False,
          broker=(False, "the log's terms were not accepted")),
     "the log's terms were not accepted"),
    ("pipeline", dict(STREAM, engine="mysql", ready=False),
     dict(STREAM, engine="mysql"), "the change tail runs here"),
])
def test_a_way_of_following_is_held_where_it_reads_the_source(
        monkeypatch, name, ok, bad, said):
    monkeypatch.setattr(movers, "stream_supported", lambda e: True)
    assert said in _held_then_refused(_stream(name), ok, bad)
