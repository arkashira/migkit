"""The rungs a version upgrade is chosen by and its files carried by,
each reached on its branches on fakes (backlog AUD-Z.rungs-outside): a
copy of the pages only where the hop allows one and says how the target
is upgraded after it, a logical move only into a newer version or a
higher edition; the new version's own upgrade carries the files linked,
cloned or copied - each held, and the first its own check passes taken."""
import types

import pytest

from migkit import decide, physical, upgrade
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("upgrade", "ways", "seeded", "holds"),
    ("upgrade", "ways", "seeded", "refuses"),
    ("upgrade", "ways", "logical", "holds"),
    ("upgrade", "ways", "logical", "refuses"),
    ("upgrade", "pg_in_place", "<r.name>", "holds"),
    ("upgrade", "LINK", "link", "holds"),
    ("upgrade", "CLONE", "clone", "holds"),
    ("upgrade", "COPY", "copy", "holds"),
]

BOTH = {"seed": ("pages", None), "settings": {"bin": "/new/bin"},
        "step": True, "versions": ("15.4", "16.2"), "edition_up": False}


def _ways(facts):
    with watched() as seen:
        got = decide.climb("the upgrade", upgrade.ways(), facts)
    return got, seen


def test_both_ways_hold_where_the_hop_gives_each():
    got, seen = _ways(BOTH)
    assert got.rung.name == "seeded"
    assert branch(seen, "seeded") == [HOLDS]
    assert branch(seen, "logical") == [HOLDS]


def test_a_copy_not_taken_and_a_target_no_newer_are_both_refused():
    got, seen = _ways({"seed": (None, "the source is a managed service"),
                       "settings": {}, "step": False,
                       "versions": ("16.2", "16.2"), "edition_up": False})
    assert got.rung is None
    assert why(seen, "seeded") == ("a copy of the pages is not taken: the"
                                   " source is a managed service")
    assert why(seen, "logical").startswith("the target runs 16.2 and the"
                                           " source 16.2")


def test_a_copy_with_no_upgrade_said_falls_to_the_logical_move():
    got, seen = _ways(dict(BOTH, settings={}))
    assert got.rung.name == "logical"
    assert branch(seen, "seeded") == [REFUSES]
    assert "does not say how the target is upgraded" in why(seen, "seeded")


def test_a_higher_edition_of_the_same_version_takes_a_logical_move():
    got, seen = _ways(dict(BOTH, seed=(None, None),
                           versions=("16.2", "16.2"), edition_up=True))
    assert got.rung.name == "logical"
    assert branch(seen, "logical") == [HOLDS]


class _Eng:
    def _psql(self, side, db, sql):
        if "pg_roles" in sql:
            return "postgres"
        if "server_version" in sql:
            return "16.2\x1f7300000000000000001"
        return "1"


@pytest.fixture
def faked(monkeypatch):
    ran = []
    monkeypatch.setattr(upgrade, "settings", lambda hop: {
        "bin": "/new/bin", "old_bin": "/old/bin", "data": "/new/data",
        "start": "start-new"})
    monkeypatch.setattr(physical, "settings",
                        lambda hop: {"data": "/old/data",
                                     "stop": "stop-old"})
    monkeypatch.setattr(upgrade, "new_version", lambda *a, **k: "16.2")
    monkeypatch.setattr(upgrade, "_pg_initdb", lambda *a: "")
    monkeypatch.setattr(upgrade, "_took_step", lambda *a: 12.0)
    monkeypatch.setattr(upgrade, "after", lambda *a: None)
    monkeypatch.setattr(physical, "_save_seed", lambda hop, rec: None)
    monkeypatch.setattr(physical, "_local", lambda cmd, what: None)
    monkeypatch.setattr(physical, "_wait_for", lambda f, what: f())

    class Costs(dict):
        def __init__(self, hop, prefix):
            super().__init__()

        def saw(self, *a):
            pass
    monkeypatch.setattr(decide, "HopCosts", Costs)

    def on_target(opts, cmd):
        ran.append(cmd)
        refused = "--check --link" in cmd
        return types.SimpleNamespace(
            returncode=1 if refused else 0,
            stdout="cannot link across file systems" if refused else "",
            stderr="")
    monkeypatch.setattr(physical, "_on_target", on_target)
    monkeypatch.setattr(physical, "_tail_of", lambda got, n=20: got.stdout)
    return ran


def test_the_upgrade_files_go_by_the_first_way_its_own_check_passes(faked):
    rec, said = {}, []
    with watched() as seen:
        upgrade.pg_in_place(object(), _Eng(), rec, ["app"], said.append)
    for r in (upgrade.LINK, upgrade.CLONE, upgrade.COPY):
        assert branch(seen, r.name) == [HOLDS], r.name
    assert rec["upgraded"]["files"] == "clone"
    assert rec["upgraded"]["version"] == "16.2"
    # the link held and was asked first; its own check refused it
    assert faked[0].endswith("--check --link")
    assert any("cannot link across file systems" in s for s in said)
    assert " --clone --jobs " in faked[-1] and "--check" not in faked[-1]
