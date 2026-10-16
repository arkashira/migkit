"""Neo4j's settings compared on both servers and the target's statistics
refreshed after a load (backlog R16c): the settings the server lists that
bear on a move read through its own listing and every difference said with
both values; the indexes brought online and resampled, the way - resample
every index, the outdated ones, or only wait - chosen by the decision engine
from the indexes and the rows loaded.

The first half runs against the sandbox's `neo4j` server pair; the second
against answers shaped as the server gives them, including a server that
does not list its settings.
"""
import pytest

from migkit import decide
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker
from tests.test_qdrant_and_neo4j_reach_the_depth_matrix import _neo4j

PROVES = [("params", "neo4j"), ("statistics", "neo4j")]

pair = servers.fixture("neo4j")
cypher = servers._Neo4j.cypher


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _engine(pair, tmp_path):
    from migkit.engines.neo4j import Neo4jEngine
    hop = Hop(name="graph", engine="neo4j",
              source=pair.src.endpoint(tls=False),
              target=pair.dst.endpoint(tls=False), databases=["neo4j"])
    hop.report_dir = lambda db=None: tmp_path
    return Neo4jEngine(hop)


def _setting(server, name, value):
    cypher(server, "system", f"CALL dbms.setConfigValue('{name}', '{value}')")


@needs_docker
def test_a_setting_that_differs_between_the_servers_is_a_finding_with_both_values(
        pair, tmp_path):
    eng = _engine(pair, tmp_path)
    assert [r.status for r in eng.check_params("neo4j")] == ["ok"]
    _setting(pair.src, "db.transaction.timeout", "30s")
    try:
        got = eng.check_params("neo4j")
    finally:
        _setting(pair.src, "db.transaction.timeout", "")
    assert [r.status for r in got] == ["diff"]
    assert "db.transaction.timeout src=30s dst=0s" in got[0].detail
    # the server's own paths and ports are not compared
    assert "server.directories" not in got[0].detail
    assert [r.status for r in eng.check_params("neo4j")] == ["ok"]


@needs_docker
def test_after_a_load_the_indexes_are_online_and_resampled_the_way_decided(
        pair, tmp_path):
    eng = _engine(pair, tmp_path)
    for s in (pair.src, pair.dst):
        cypher(s, "neo4j", "CREATE INDEX cmp_p IF NOT EXISTS FOR (n:P)"
                           " ON (n.k)")
        cypher(s, "neo4j", "UNWIND range(1, 300) AS i CREATE (:P {k: i})")
    try:
        said = eng.settle_target("neo4j")
        choice = eng.settle_choice("neo4j")
        states = eng._index_states("neo4j")
    finally:
        for s in (pair.src, pair.dst):
            cypher(s, "neo4j", "MATCH (n:P) DETACH DELETE n")
            cypher(s, "neo4j", "DROP INDEX cmp_p IF EXISTS")
    assert states and all(st == "ONLINE" for _, st in states)
    assert ("cmp_p", "ONLINE") in states
    # every element was just loaded: more than the server samples by itself
    assert choice.path in ("resample", "outdated")
    assert choice.path != "wait"
    assert said.startswith("refreshed the target's statistics:")
    assert f"{len(states)} indexes online" in said
    assert "300 of 300 elements loaded" in said and choice.said in said


# ---- answers shaped as the server gives them ---------------------------------

def _answers(procedures=("db.prepareForReplanning", "db.resampleIndex",
                         "db.resampleOutdatedIndexes"), states=None,
             sampling="5", count=100):
    ran = []
    states = states if states is not None else [("i1", "ONLINE")]

    def run(word):
        def f(params):
            ran.append(word)
            return []
        return f
    return ran, {
        "dbms.components": [{"name": "Neo4j Kernel", "versions": ["5.26.0"],
                             "edition": "enterprise"}],
        "SHOW INDEXES": lambda p: [{"name": n, "state": s}
                                   for n, s in states],
        "count(n)": [{"n": count}], "count(e)": [{"n": 0}],
        "SHOW SETTINGS": [{"value": sampling}],
        "SHOW PROCEDURES": [{"name": n} for n in procedures],
        "db.awaitIndexes": run("await"),
        "db.prepareForReplanning": run("replan"),
        "db.resampleOutdatedIndexes": run("outdated"),
        "db.resampleIndex": run("one"),
    }


def test_the_way_to_refresh_is_the_decision_engines_from_the_counts(tmp_path):
    ran, answers = _answers()
    eng, _ = _neo4j(tmp_path, answers)
    said = eng.settle_target("neo4j", from_source=False)
    assert ran == ["await", "replan"]
    assert eng.settle_choice("neo4j").path == "resample"
    assert "100 of 100 elements loaded" in said
    # a small share loaded is left to the server's own sampling for the
    # indexes it finds outdated; with no such refresh listed, only the wait
    facts = decide.Facts({"indexes": 2, "rows": 1000, "loaded": 10,
                          "sampling": 5.0,
                          "procedures": ["db.resampleOutdatedIndexes"]})
    assert decide.settle_way(facts).path == "outdated"
    facts["procedures"] = []
    got = decide.settle_way(facts)
    assert got.path == "wait" and len(got.passed) == 2
    assert decide.settle_way(decide.Facts({
        "indexes": 0, "rows": 5, "loaded": 5, "sampling": 5.0,
        "procedures": ["db.resampleIndex"]})).path == "wait"
    assert decide.settle_way(decide.Facts({
        "indexes": 3, "rows": 100, "loaded": 2, "sampling": 5.0,
        "procedures": ["db.resampleIndex", "db.resampleOutdatedIndexes"]})
    ).path == "outdated"


def test_an_index_that_is_not_online_is_said_not_assumed(tmp_path):
    ran, answers = _answers(states=[("i1", "ONLINE"), ("i2", "POPULATING")])
    eng, _ = _neo4j(tmp_path, answers)
    said = eng.settle_target("neo4j", from_source=False)
    assert "1 of 2 indexes of neo4j not online (i2)" in said
    assert not said.startswith("refreshed")


def test_a_server_that_does_not_list_its_settings_is_said_not_raised(tmp_path):
    def refused(params):
        raise RuntimeError("Neo.ClientError.Statement.SyntaxError: no SHOW")
    eng, _ = _neo4j(tmp_path, {
        "dbms.components": [{"name": "Neo4j Kernel", "versions": ["5.26.0"],
                             "edition": "enterprise"}],
        "SHOW SETTINGS": refused})
    got = eng.check_params("neo4j")
    assert [r.status for r in got] == ["error"]
    assert "settings could not be read" in got[0].detail


def test_the_settings_are_read_by_the_servers_major_version(tmp_path):
    def listed(params):
        return [{"name": "dbms.memory.heap.max_size", "value": "512m"},
                {"name": "dbms.directories.data", "value": "/data"}]
    eng, asked = _neo4j(tmp_path, {
        "dbms.components": [{"name": "Neo4j Kernel", "versions": ["4.4.9"],
                            "edition": "enterprise"}],
        "dbms.listConfig": listed})
    got = eng.check_params("neo4j")
    assert [r.status for r in got] == ["ok"]
    assert "1 settings, all equal both sides" in got[0].detail
    assert any("dbms.listConfig" in a for a, _ in asked)
