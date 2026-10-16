"""Every engine has the name a person reads; the graph and time-series
engines fold their rows by their own classes; the partitions' findings
name the hop; and the move goes through the topology's door."""
import types

import pytest

from migkit import canon, cli, engines, planner


def test_every_engine_named_has_a_title():
    assert [n for n in engines.NAMES if n not in engines.TITLES] == []
    assert engines.TITLES["qdrant"] == "Qdrant"
    assert engines.TITLES["neo4j"] == "Neo4j"


def test_the_readme_counts_the_engines_from_the_code():
    from pathlib import Path
    text = (Path(__file__).parent.parent / "README.md").read_text()
    n = len([x for x in engines.NAMES if x not in ("hetero", "generic")])
    assert f"badge/engines-{n}-" in text and f"{n} engines, any two" in text


@pytest.mark.parametrize("engine,cols,row", [
    ("neptune", [("id", "string"), ("properties", "properties")],
     ["v1", '{"a": 1}']),
    ("timestream", [("host", "varchar"), ("v", "double")], ["h", 1.5])])
def test_graph_and_series_rows_fold_by_their_engines_classes(engine, cols,
                                                             row):
    got = canon.fold_batches(cols, [[row]], engine=engine)
    assert got[0] == 1
    with pytest.raises(ValueError):
        canon.fold_batches(cols, [[row]])


def test_neptune_and_timestream_digest_by_their_engine(monkeypatch):
    from migkit.engines import neptune, timestream
    seen = []
    monkeypatch.setattr(canon, "fold_batches",
                        lambda c, b, **k: seen.append(k.get("engine")))
    for cls in (neptune.NeptuneEngine, timestream.TimestreamEngine):
        e = cls.__new__(cls)
        e.neutral_batches = lambda *a, **k: iter(())
        e.neutral_digest("src", "db", "t", [("id", "string")])
    assert seen == ["neptune", "timestream"]


def test_the_partitions_finding_names_the_hop(monkeypatch, tmp_path):
    from migkit import verdict
    from migkit.engines.base import Engine
    asked = []
    monkeypatch.setattr(verdict, "partition_differences",
                        lambda t, s, d, hop=None: asked.append(hop) or [])
    e = Engine.__new__(Engine)
    e.hop = types.SimpleNamespace(name="orders-hop",
                                  report_dir=lambda db="": tmp_path)
    e._partitions_verdict("app", {"t": ({}, {})}, [], "default")
    assert asked == ["orders-hop"]


def _hop(name, nodes=None):
    return types.SimpleNamespace(name=name, options={"nodes": nodes}
                                 if nodes else {}, source=None, target=None)


def test_a_plain_hop_moves_by_the_same_path_as_before(monkeypatch):
    ran = []
    monkeypatch.setattr(cli, "get_hop", lambda n: _hop(n))
    monkeypatch.setattr(cli, "_move_run",
                        lambda *a: ran.append(a) or "moved")
    monkeypatch.setattr(cli, "load_hops", lambda: pytest.fail("read all"))
    assert cli._move_hop("h", "app", "", "full", 500000, False, True) \
        == "moved"
    assert ran == [("h", "app", "", "full", 500000, False, True)]


def test_a_topology_hop_moves_through_the_topology_run(monkeypatch):
    calls = {}
    hop = _hop("h0", {"spaces": {"x:5432": {"t": {"range": [1, 9]}}}})

    class Eng:
        def databases(self):
            return ["app"]

        def neutral_tables(self, side, db):
            return ["t"]

        def neutral_key(self, side, db, t):
            return ["id"]

        def neutral_columns(self, side, db, t):
            return [("id", "integer"), ("v", "text")]
    monkeypatch.setattr(cli, "get_hop", lambda n: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: Eng())
    monkeypatch.setattr(cli, "load_hops", lambda: {"h0": hop})

    def door(h, hops, tables, run_hop, read_rows, **k):
        calls.update(hop=h.name, hops=[x.name for x in hops], tables=tables)
        return planner.TopologyRun(None, done=("x>hub",), lines=("ok",),
                                   final=types.SimpleNamespace(ok=True))
    monkeypatch.setattr(planner, "move_topology", door)
    got = cli._move_hop("h0", "", "", "full", 500000, False, True)
    assert got.done == ("x>hub",)
    assert calls["hops"] == ["h0"] and calls["tables"] == {
        "t": {"key": ["id"], "columns": ["id", "v"]}}
    monkeypatch.setattr(planner, "move_topology", lambda *a, **k:
                        planner.TopologyRun(None, stopped=("h0", "failed")))
    with pytest.raises(SystemExit, match="stopped at h0"):
        cli._move_hop("h0", "", "", "full", 500000, False, True)
