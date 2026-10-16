"""The four rungs the reach guard could not watch as built (backlog
AUD-Z.rung-defects), each mended in its owner so both of its branches are
real, and reached here on fakes under `watched()`:

- the bulk way of `_bulk_or_copier` needs its programs installed here: it
  read a dict always built, so it could never be refused; now a program
  not installed refuses it, named, and the copier is taken from the start;
- Spanner's whole-database restore was handed its fact as a reader where
  a value is taken, so every database copy with `go` raised TypeError
  before either way was pruned; now the restore is held where a backup
  restores the database and refused, with why, where it does not;
- the many-node topology's kind is a rung whose need reads the kind: held
  for a kind migkit knows, refused with the kinds it knows otherwise;
- the spool a seed part is read again into is not chosen among carriers,
  so it is labelled without a rung, and nothing is pruned for it."""
import pytest

from migkit import cli, movers, scenarios, ship, tableway
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why
from tests.test_every_decide_rung_is_reached import outside
from tests.test_many_node_topologies_are_decided import (
    TABLES, _merge, _strides)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("cli", "_bulk_or_copier", "<v>", "holds"),
    ("cli", "_bulk_or_copier", "<v>", "refuses"),
    ("cli", "_bulk_or_copier", "builtin", "holds"),
    ("engines.spanner", "SpannerEngine", "restore", "holds"),
    ("engines.spanner", "SpannerEngine", "restore", "refuses"),
    ("engines.spanner", "SpannerEngine", "tables", "holds"),
    ("scenarios", "topology_case", "topology-unknown", "holds"),
    ("scenarios", "topology_case", "topology-unknown", "refuses"),
]


class _CliHop:
    def __init__(self, where):
        self.where = where

    def report_dir(self, db):
        return self.where


def _bulk(monkeypatch, tmp_path, which, version="8.0.36"):
    way = next(r.name for r in movers._ladder() if r.name != "builtin")
    ran = []
    monkeypatch.setattr(cli, "_bulk_db", lambda *a: ran.append("bulk") or 1)
    monkeypatch.setattr(cli, "_move_full", lambda *a: ran.append("copier"))
    monkeypatch.setattr(movers, "programs", lambda v: ("dumper", "loader"))
    monkeypatch.setattr(movers, "which", which)
    monkeypatch.setattr(movers, "program_version", lambda p: version)
    with watched() as seen:
        cli._bulk_or_copier(_CliHop(tmp_path), None, "app", way, None, 1000,
                            "mysql", go=False)
    return way, ran, seen


def test_a_bulk_way_is_held_with_its_programs_installed(monkeypatch,
                                                        tmp_path):
    way, ran, seen = _bulk(monkeypatch, tmp_path, lambda p: f"/usr/bin/{p}")
    assert ran == ["bulk"]
    assert branch(seen, way) == [HOLDS]
    assert branch(seen, "builtin") == [HOLDS]


def test_a_program_installed_whose_version_is_unread_still_holds(
        monkeypatch, tmp_path):
    way, ran, seen = _bulk(monkeypatch, tmp_path, lambda p: f"/usr/bin/{p}",
                           version=None)
    assert ran == ["bulk"]
    assert branch(seen, way) == [HOLDS]


def test_a_bulk_way_missing_a_program_is_refused_for_the_copier(
        monkeypatch, tmp_path):
    way, ran, seen = _bulk(monkeypatch, tmp_path,
                           lambda p: "/usr/bin/x" if p == "dumper" else None)
    assert ran == ["copier"]
    assert branch(seen, way) == [REFUSES]
    assert "not installed here: loader" in why(seen, way)
    assert "dumper" not in why(seen, way)
    assert branch(seen, "builtin") == [HOLDS]


def _spanner(monkeypatch, unfit):
    from migkit.config import Endpoint, Hop
    from migkit.engines.spanner import SpannerEngine
    ep = Endpoint(host="example-project", options={"instance": "inst"})
    eng = SpannerEngine(Hop(name="s", engine="spanner", source=ep,
                            target=ep, databases=["app"]))
    ran = []
    eng._restore_unfit = lambda db, tables: unfit
    eng._restored = lambda db, log: ran.append(f"restore {db}")
    eng.copy_by_copier = lambda db, t, log, held: ran.append(f"copy {t}")

    def each(e, db, tables, rungs, facts_for, run, log):
        return {t: run("copier", t) and "copier" for t in tables}
    monkeypatch.setattr(tableway, "each", each)
    with watched() as seen:
        steps = eng.native_bulk("app", ["a", "b"], True, lambda m: None)
    return steps, ran, seen


def test_a_spanner_database_a_backup_restores_is_restored_whole(monkeypatch):
    steps, ran, seen = _spanner(monkeypatch, None)
    assert ran == ["restore app"]
    assert steps == ["app: restored whole from a backup of the source"
                     " taken now"]
    assert branch(seen, "restore") == [HOLDS]
    assert branch(seen, "tables") == [HOLDS]


def test_a_spanner_database_a_backup_cannot_restore_goes_by_its_tables(
        monkeypatch):
    unfit = "the target database is there already"
    steps, ran, seen = _spanner(monkeypatch, unfit)
    assert ran == ["copy a", "copy b"]
    assert all(unfit in s for s in steps)
    assert branch(seen, "restore") == [REFUSES]
    assert why(seen, "restore") == unfit
    assert branch(seen, "tables") == [HOLDS]


def test_a_known_topology_is_held_before_its_ladder_climbs():
    with watched() as seen:
        got = scenarios.topology_case(_merge(_strides(3)))
    assert got.choice.path == "union-apart"
    assert "(many-to-one)" in got.choice.work
    assert branch(seen, "topology-unknown") == [HOLDS]


def test_a_topology_not_known_is_refused_naming_the_known_ones():
    with watched() as seen:
        got = scenarios.topology_case(
            scenarios.topology_facts("ring", ("a", "b"), ("a", "b"), TABLES))
    assert got.choice.rung is None
    assert branch(seen, "topology-unknown") == [REFUSES]
    said = why(seen, "topology-unknown")
    assert "'ring' is not one of" in said
    assert all(k in said for k in scenarios.TOPOLOGIES)
    assert "'ring' is not one of" in got.choice.reason
    # nothing past the kind is climbed for it
    assert {n for n, _, _ in seen} == {"topology-unknown"}


class _Stop(Exception):
    pass


def test_the_relink_spool_is_a_carrier_no_rung_stands_for(monkeypatch,
                                                          tmp_path):
    from migkit import evidence
    built = []

    def exporter(w, carrier, recipients):
        built.append(carrier)
        raise _Stop
    monkeypatch.setattr(ship, "_Exporter", exporter)
    monkeypatch.setattr(evidence, "recipients_of", lambda hop: ())
    w = type("W", (), {"spool": tmp_path, "hop": None,
                       "log": lambda self, m: None})()
    loader = type("L", (), {"w": w})()
    tgt = type("T", (), {"entry": {"table": "orders"}})()
    with watched() as seen, pytest.raises(_Stop):
        ship._Loader.relink(loader, tgt, {"n": 1}, "did not digest alike")
    carrier, = built
    assert carrier.rung is None
    assert carrier.said.startswith("this machine's spool")
    assert str(tmp_path / "relinked") in carrier.said
    assert carrier.reason == "read again"
    assert seen == []
    assert not [k for k in outside() if k[:2] == ("ship", "_Loader")]
