"""What the watching loops say, and end with (audit V2, V13, V14).

`watch --verify --delta` skipped a database whose verify raised and told
the hop's receivers "same"; `watch` said SAFE TO CUT OVER from the
servers' row estimates; `watch --verify` wrote a partial cycle over the
check's `report.html` while `verdict.json` described the last whole
check; and both ended 0 whatever they found.
"""
import json

import pytest

from migkit import cli, verdict
from migkit.engines.base import Result


class _Hop:
    name = "w"
    engine = "postgres"
    service = ""
    options = {}
    databases = ["a", "b"]

    def __init__(self, tmp):
        self.tmp = tmp
        from migkit.config import Endpoint
        self.source = Endpoint(host="10.0.0.1", password="p")
        self.target = Endpoint(host="10.0.0.2", password="p")

    def report_dir(self, db=None):
        (self.tmp / (db or "")).mkdir(parents=True, exist_ok=True)
        return self.tmp / (db or "")


class _Eng:
    def __init__(self, fails=(), changes=None):
        self.fails, self.changes = fails, changes or {}

    def databases(self):
        return ["a", "b"]

    def delta_verify(self, d, log=None):
        if d in self.fails:
            raise RuntimeError("could not read the slot")
        return [Result("delta", d, "ok", "0 changed rows differ")]

    def changes_made(self, side, d):
        return self.changes.get(d)


@pytest.fixture
def loop(tmp_path, monkeypatch):
    from migkit import capabilities, notify, tailctl
    hop = _Hop(tmp_path)
    told = []
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "_require_configured", lambda h: None)
    monkeypatch.setattr(capabilities, "require", lambda *a, **k: None)
    monkeypatch.setattr(notify, "verdict",
                        lambda hop, env, say, key="verdict":
                        told.append(env["status"]))
    monkeypatch.setattr(notify, "lags", lambda *a: None)
    monkeypatch.setattr(tailctl, "alive", lambda where: False)
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    said = []
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", **k: said.append(str(m)))

    def run(eng, cycles=1):
        monkeypatch.setattr(cli, "get_engine", lambda h: eng)
        return cli._delta_loop("w", "", 1, cycles, False)
    return run, hop, told, said


def test_a_database_whose_verify_fails_is_an_error_not_left_out(loop):
    run, hop, told, _ = loop
    got = run(_Eng(fails=("b",)))
    assert got.status == "error" and got.exit == 3
    assert told == ["error"], told
    kept = json.loads((hop.report_dir() / "delta-summary.json").read_text())
    assert [r["status"] for r in kept if r["scope"] == "b"] == ["error"]


def test_safe_is_said_only_after_a_verified_quiet_cycle(loop):
    run, _, _, said = loop
    moving = {"a": [10, 5.0], "b": [7, 5.0]}
    eng = _Eng(changes=moving)
    run(eng)
    # one cycle: nothing to hold the source's count to yet
    assert not any("SAFE TO CUT OVER" in s for s in said), said
    said.clear()
    run(eng, cycles=2)
    assert sum("SAFE TO CUT OVER" in s for s in said) == 2, said
    said.clear()

    class Busy(_Eng):
        n = 0

        def changes_made(self, side, d):
            Busy.n += 1
            return [Busy.n, 5.0]
    run(Busy(), cycles=2)
    assert not any("SAFE TO CUT OVER" in s for s in said), said
    assert any("still being written" in s for s in said), said


def test_watch_ends_as_its_last_cycle(tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    (tmp_path / "hops.yaml").write_text("hops: {}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setattr(cli, "_delta_loop",
                        lambda *a: verdict.Decision("different", "", ()))
    got = CliRunner().invoke(cli.main, ["watch", "w", "--verify",
                                        "--delta", "--count", "1"])
    assert got.exit_code == 1, got.output


def test_a_verify_cycle_leaves_the_check_s_report_alone(loop, monkeypatch):
    _, hop, told, _ = loop
    (hop.report_dir() / "report.html").write_text("the last whole check")
    monkeypatch.setattr(cli, "get_engine",
                        lambda h: type("E", (), {"checks": ["counts"],
                                                 "planned_checks":
                                                 lambda self: []})())
    monkeypatch.setattr(cli, "run_checks", lambda *a, **k: (
        [{"check": "counts", "scope": "a", "status": "diff",
          "detail": "src=3 dst=2"}], {}))
    got = cli._monitor("w", "a", 1, "counts", 1)
    assert got.status == "different"
    assert (hop.report_dir() / "report.html").read_text() == \
        "the last whole check"
    assert (hop.report_dir() / cli.WATCH_REPORT).exists()
    assert told == ["different"]
    with pytest.raises(verdict.Usage, match="countz"):
        cli._monitor("w", "a", 1, "countz", 1)
