"""The ways a hop's changes follow its source are rungs of one ladder, and
the values they carry are the exact ones (backlog 0f, P0 `decide`).

migkit's own change tail, the servers' own replication, the change leg run
from here and the streaming pipeline were four branches of an `if`. They
are rungs now: what each needs, why it was passed over, and a measured
cost ranking the ones that hold - with the order the branches took, so a
hop that asks for nothing takes the way it always took.
"""
import pytest

from migkit import canon, movers
from migkit.config import Endpoint, Hop


def _hop(engine="postgres"):
    return Hop(name="ld", engine=engine,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["appdb"])


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_CDC", raising=False)


@pytest.mark.parametrize("engine,repl,tail,ready,env,path", [
    ("postgres", True, True, True, "", "subscription"),
    ("postgres", True, True, True, "native", "subscription"),
    ("postgres", True, True, True, "follow", "follow"),
    ("mysql", True, True, True, "", "tail"),
    ("mysql", True, True, False, "", "replica"),
    ("mysql", False, True, False, "", "pipeline"),
    ("hetero", False, False, False, "", "pipeline"),
    ("redis", False, False, False, "", None),
])
def test_a_hop_that_asks_for_nothing_takes_the_way_it_took(
        monkeypatch, engine, repl, tail, ready, env, path):
    if env:
        monkeypatch.setenv("MIGKIT_CDC", env)
    got = movers.stream_way(_hop(engine), engine, repl, tail, ready)
    assert got.path == path, got


def test_the_way_passed_over_says_why():
    got = movers.stream_way(_hop("mysql"), "mysql", False, True, False)
    assert got.path == "pipeline"
    assert "needs a driver this machine does not have" in got.reason


def test_a_measured_way_ranks_among_those_that_hold():
    """Where two ways hold and both were timed on this hop, the cheaper is
    taken - the rule every choice follows."""
    from migkit.decide import HopCosts
    hop = _hop("mysql")
    costs = HopCosts(hop, "cdc/")
    costs.saw("tail", 1000, 10.0)
    costs.saw("replica", 1000, 2.0)
    got = movers.stream_way(hop, "mysql", True, True, True)
    assert got.path == "replica", got
    assert [r.name for r in got.ranked][:2] == ["replica", "tail"], got
    costs.saw("replica", 1000, 20.0)
    assert movers.stream_way(hop, "mysql", True, True, True).path == "tail"


def test_a_way_never_timed_is_not_started_only_to_be_timed():
    """`decide` tries what was never timed first; a way of following a
    source is not tried that way - the tail timed, the replica never,
    and the tail goes on."""
    from migkit.decide import HopCosts
    hop = _hop("mysql")
    HopCosts(hop, "cdc/").saw("tail", 1000, 10.0)
    got = movers.stream_way(hop, "mysql", True, True, True)
    assert got.path == "tail", got
    assert "replica" not in [r.name for r in got.ranked], got


def test_a_way_timed_does_not_push_out_one_the_network_forbids():
    """The leg run from here recording its cost cannot put the servers'
    own replication in its place: that way needs the target to dial the
    source, which the hop said it cannot."""
    from migkit.decide import HopCosts
    import os
    hop = _hop()
    HopCosts(hop, "cdc/").saw("follow", 10, 100.0)
    os.environ["MIGKIT_CDC"] = "follow"
    try:
        got = movers.stream_way(hop, "postgres", True, True, True)
    finally:
        del os.environ["MIGKIT_CDC"]
    assert got.path == "follow", got


#: every answer each value option has that loses something
LOSSY = {"time.precision.mode": {"connect"},
         "decimal.handling.mode": {"double", "string"},
         "binary.handling.mode": {"base64", "base64-url-safe", "hex"},
         "bigint.unsigned.handling.mode": {"long"},
         "interval.handling.mode": {"numeric"},
         "enable.time.adjuster": {"true"}}


@pytest.mark.parametrize("mysql,pinned", [
    (False, {"time.precision.mode", "decimal.handling.mode",
             "binary.handling.mode", "interval.handling.mode"}),
    (True, {"time.precision.mode", "decimal.handling.mode",
            "binary.handling.mode", "bigint.unsigned.handling.mode",
            "enable.time.adjuster"})])
def test_no_value_reaches_the_target_rounded(mysql, pinned):
    got = movers.value_modes(mysql)
    assert set(got) == pinned
    for option, value in got.items():
        assert value not in LOSSY[option], (option, value)
    # the classes they carry are ones canon compares exactly
    assert canon.type_class("mysql", "bigint unsigned") == "integer"
    assert canon.type_class("postgres", "numeric(12,2)") == "decimal"


def test_the_generated_connector_carries_the_exact_modes(tmp_path,
                                                         monkeypatch):
    import json
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: False)
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: False)
    monkeypatch.setattr(movers, "_stream_facts",
                        lambda hop, dbs, my: ({d: [] for d in dbs}, None,
                                              None))
    for engine in ("postgres", "mysql"):
        out = movers.stream_codegen(_hop(engine), ["appdb"], engine)
        cfg = json.loads((out / "source-connector.json").read_text())
        for option, value in movers.value_modes(engine == "mysql").items():
            assert cfg["config"][option] == value, (engine, option)
        assert cfg["config"]["time.precision.mode"] != "connect"
