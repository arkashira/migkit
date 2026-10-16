"""The cutover's recheck, after the approvals: it leans on what the
preflight measured and measures again only what can change before the
flip - the verdict's freshness, the lag and the open work - through the
same readers, and never says where a file is."""
import json
import os
import time

import pytest

from migkit import cutover, diagnostics, freeze
from migkit.config import Endpoint, Hop


def _hop(tmp_path, name):
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="migkit",
                              password="CHANGE_ME"),
              databases=["app"], options={})
    hop.report_dir = lambda db="": tmp_path
    return hop


def _verdict(tmp_path, bump=0, **kw):
    path = tmp_path / "verdict.json"
    path.write_text(json.dumps({"status": "same", "why": "", **kw}))
    if bump:
        later = time.time() + bump
        os.utime(path, (later, later))


@pytest.fixture
def gate(tmp_path, monkeypatch):
    hop = _hop(tmp_path, "recheck-reuse")
    calls = {"probes": 0, "open": 0}
    leg = cutover.Leg("native", fence=object())

    def probes(h, l, dbs, c):
        calls["probes"] += 1
        return {d: ("0/16", True, 0.1, "") for d in dbs}

    def open_work(*a, **k):
        calls["open"] += 1
        return []
    monkeypatch.setattr(cutover, "_probes", probes)
    monkeypatch.setattr(freeze, "open_work", open_work)
    yield hop, leg, calls
    diagnostics.forget(hop)


def test_the_recheck_passes_on_what_the_preflight_recorded(gate, tmp_path):
    hop, leg, calls = gate
    _verdict(tmp_path)
    conf = dict(cutover.DEFAULTS)
    window = cutover.fresh_window(conf)
    taken = diagnostics.accumulator(hop)
    taken.record("lag", "app", ("0/16", True, 0.1, ""))
    for side in ("src", "dst"):
        taken.record("open-work", f"app:{side}", [])
    assert cutover._open_work_rows(hop, None, "app", conf, "fail",
                                   window) == []
    assert cutover._verify_rows(hop, conf, "fail", window) == [
        ("ok", "verify verdict: the two are the same")]
    assert cutover.recheck(hop, None, ["app"], leg, conf) == []
    # nothing was measured again: the readings were fresh
    assert calls == {"probes": 0, "open": 0}


def test_the_recheck_measures_again_only_what_went_stale(gate, tmp_path):
    hop, leg, calls = gate
    _verdict(tmp_path)
    conf = dict(cutover.DEFAULTS, budget=30)
    assert cutover.recheck(hop, None, ["app"], leg, conf) == []
    # the first time every fact is taken through the same readers
    assert calls["probes"] == 1 and calls["open"] == 2


def test_the_recheck_refuses_a_verdict_that_went_stale_after_preflight(
        gate, tmp_path):
    hop, leg, calls = gate
    conf = dict(cutover.DEFAULTS)
    _verdict(tmp_path)
    assert cutover.recheck(hop, None, ["app"], leg, conf) == []
    _verdict(tmp_path, bump=5, stale=True)
    got = cutover.recheck(hop, None, ["app"], leg, conf)
    assert any("marked stale" in w for w in got), got
    _verdict(tmp_path, bump=10, status="different", why="2 tables differ")
    got = cutover.recheck(hop, None, ["app"], leg, conf)
    assert any("different" in w and "2 tables differ" in w for w in got), got


def test_a_missing_verdict_names_the_hop_and_not_a_path(gate, tmp_path):
    hop, leg, calls = gate
    got = cutover.recheck(hop, None, ["app"], leg, dict(cutover.DEFAULTS))
    text = "; ".join(got)
    assert "no verify verdict yet for recheck-reuse" in text, text
    assert str(tmp_path) not in text and "Errno" not in text, text
    (tmp_path / "verdict.json").write_text("{not json")
    got = "; ".join(cutover.recheck(hop, None, ["app"], leg,
                                    dict(cutover.DEFAULTS)))
    assert "recheck-reuse" in got and str(tmp_path) not in got, got


def test_a_grant_recorded_at_preflight_is_not_asked_again_at_the_flip(
        gate, tmp_path):
    hop, leg, calls = gate
    _verdict(tmp_path)
    conf = dict(cutover.DEFAULTS)
    taken = diagnostics.accumulator(hop)
    asked = []

    def grant():
        asked.append("grant")
        return {"select": True}
    at = diagnostics.where("app", "dst", "migkit")
    taken.probed("grant", at, grant)
    taken.probed("version", diagnostics.where("app", "src"),
                 lambda: asked.append("version") or "16.4")
    assert asked == ["grant", "version"]
    assert cutover.recheck(hop, None, ["app"], leg, conf) == []
    # held until changed: read as the preflight recorded it, never again
    assert asked == ["grant", "version"]
    assert taken.get("grant", at).reads == 1
    # the lag, one read, was measured at the flip
    assert calls["probes"] == 1
    # a grant the preflight found missing stops the cutover by name
    taken.record("grant", at, ok=False, why="no select on orders")
    got = cutover.recheck(hop, None, ["app"], leg, conf)
    assert any(w.startswith("probe grant of app:dst:migkit")
               and "no select on orders" in w for w in got), got
    # another database's probes are not this cutover's
    diagnostics.forget(hop)
    diagnostics.accumulator(hop).record("grant", "other:dst", ok=False,
                                        why="no select")
    assert cutover.recheck(hop, None, ["app"], leg, conf) == []
