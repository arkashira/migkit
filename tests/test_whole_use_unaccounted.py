"""The catalogue's unaccounted section (backlog 0f): a name no code calls is a
stale entry, a called name is held with its capabilities, a candidate that is
used now is caught, and a called name with no entry is shown as such.

Fakes only: the catalogue and the walk of the package are replaced; the
registry is the one the code holds.
"""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def whole():
    spec = importlib.util.spec_from_file_location(
        "whole_use", ROOT / "tools" / "whole_use.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fake_catalogue(tools=None, unaccounted=None):
    return {"note": "", "import_aliases": {}, "program_aliases": {},
            "system_programs": {}, "tools": tools or {},
            "unaccounted": unaccounted or {}}


def fake_sites(loose_names):
    def sites(cat, names):
        loose = [("library", n, ["migkit/fake.py"]) for n in loose_names]
        return {n: [] for n in names}, loose
    return sites


def cap(**kw):
    return {"id": "c", "what": "w", "candidate": False, **kw}


def test_an_unaccounted_name_no_code_calls_is_a_stale_entry(whole, monkeypatch):
    monkeypatch.setattr(whole, "catalogue", lambda: fake_catalogue(
        unaccounted={"gone-lib": {"release": "x", "capabilities": [cap()]}}))
    monkeypatch.setattr(whole, "sites", fake_sites(["numpy"]))
    facts = whole.build()
    assert any("gone-lib" in p for p in facts["problems"])
    assert "gone-lib" not in facts["unaccounted"]


def test_a_called_unaccounted_name_is_held_with_its_capabilities(
        whole, monkeypatch):
    caps = [cap(id="arrays", what="typed arrays"),
            cap(id="copy", what="copy", candidate=True, rung="bulk ladder")]
    monkeypatch.setattr(whole, "catalogue", lambda: fake_catalogue(
        unaccounted={"numpy": {"release": "numpy 2.5.3", "capabilities": caps}}))
    monkeypatch.setattr(whole, "sites", fake_sites(["numpy"]))
    facts = whole.build()
    assert facts["problems"] == []
    assert facts["unaccounted"]["numpy"] == caps
    assert "`0f.numpy.copy` (no account): copy would strengthen bulk ladder." \
        in whole.backlog_rows(facts)
    assert "arrays" not in whole.backlog_rows(facts)


def test_a_called_name_with_no_entry_is_shown_as_not_yet_catalogued(
        whole, monkeypatch):
    monkeypatch.setattr(whole, "catalogue", lambda: fake_catalogue())
    monkeypatch.setattr(whole, "sites", fake_sites(["pandas"]))
    doc = whole.render(whole.build())
    assert "### pandas (library)\n\nNo capabilities catalogued yet." in doc


def test_a_candidate_whose_option_is_topped_up_now_is_caught(whole, monkeypatch):
    monkeypatch.setattr(whole, "catalogue", lambda: fake_catalogue(tools={
        "dvt": {"release": "data-validation 8.9.3", "capabilities": [
            cap(id="hash", covers=["--hash"], candidate=True,
                rung="verify ladder")]}}))
    monkeypatch.setattr(whole, "sites", fake_sites([]))
    facts = whole.build()
    assert ("dvt hash: marked a candidate and now topped up; clear the mark"
            in facts["problems"])
