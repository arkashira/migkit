"""`watch --verify` decides which checks a cycle runs (backlog
AUD-Z.monitor-only): a fixed `--only counts,autoinc` stood as the default,
the same on a database of ten rows and one of ten billion. Now the
operator's `--only` is taken as said; otherwise the first cycle runs every
check and times each, and each cycle after runs the counts and then the
checks the measured times fit in the interval (`decide.monitor_checks`); a
watch whose counts alone outlast the interval stops, saying why. Each
cycle's line is said in the verdicts' words and kept in the run's log, and
its answers are written whole or not at all."""
import pytest

from migkit import cli, decide, durable, runlog, verdict
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("monitor_checks", "asked", "holds"),
    ("monitor_checks", "asked", "refuses"),
    ("monitor_checks", "unmeasured", "holds"),
    ("monitor_checks", "unmeasured", "refuses"),
    ("monitor_checks", "fit", "holds"),
    ("monitor_checks", "fit", "refuses"),
    ("monitor_checks", "refused", "holds"),
]

EVERY = ["schema", "counts", "autoinc", "data"]


def test_the_operators_checks_are_taken_as_named():
    with watched() as seen:
        got, how = decide.monitor_checks({
            "asked": ["data", "data"], "checks": EVERY,
            "took": {"data": 9000}, "interval": 10})
    assert (got, how.path) == (["data"], "asked")
    assert branch(seen, "asked") == [HOLDS]


def test_the_first_cycle_runs_every_check_counts_first():
    with watched() as seen:
        got, how = decide.monitor_checks({
            "asked": [], "checks": EVERY, "took": {}, "interval": 300})
    assert (got, how.path) == (["counts", "schema", "autoinc", "data"],
                               "unmeasured")
    assert branch(seen, "asked") == [REFUSES]
    assert branch(seen, "unmeasured") == [HOLDS]


def test_a_measured_cycle_keeps_the_checks_that_fit_the_interval():
    with watched() as seen:
        got, how = decide.monitor_checks({
            "asked": [], "checks": EVERY, "lanes": 2, "interval": 300,
            # two databases at once: the data's 900s are 450s a cycle,
            # past the 300s the counts (20s), schema and autoinc leave
            "took": {"schema": 40, "counts": 40, "autoinc": 20,
                     "data": 900}})
    assert (got, how.path) == (["counts", "schema", "autoinc"], "fit")
    assert branch(seen, "unmeasured") == [REFUSES]
    assert branch(seen, "fit") == [HOLDS]


def test_counts_that_outlast_the_interval_are_refused_with_why():
    with watched() as seen:
        got, how = decide.monitor_checks({
            "asked": [], "checks": EVERY, "interval": 300,
            "took": {"counts": 420, "schema": 1}})
    assert (got, how.path) == ([], "refused")
    assert branch(seen, "fit") == [REFUSES]
    assert branch(seen, "refused") == [HOLDS]
    assert "420.0s of a 300s interval" in why(seen, "fit")


class _Hop:
    name = "w"
    engine = "postgres"
    options = {}
    workers = 1

    def __init__(self, tmp):
        self.tmp = tmp

    def report_dir(self, db=None):
        (self.tmp / (db or "")).mkdir(parents=True, exist_ok=True)
        return self.tmp / (db or "")


class _Eng:
    checks = ["schema", "counts", "autoinc", "data"]

    def planned_checks(self):
        return []

    def databases(self):
        return ["a"]


@pytest.fixture
def watch(tmp_path, monkeypatch):
    from migkit import notify, report
    hop = _Hop(tmp_path)
    seen = {"ran": [], "said": [], "logged": [], "written": []}
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: _Eng())
    monkeypatch.setattr(cli, "_require_configured", lambda h: None)
    monkeypatch.setattr(cli, "_hop_again",
                        lambda name, h, e, db, dbs: (h, e, dbs, False))
    monkeypatch.setattr(notify, "verdict", lambda *a, **k: None)
    monkeypatch.setattr(notify, "lags", lambda *a: None)
    monkeypatch.setattr(report, "write_report", lambda *a, **k: None)
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", **k: seen["said"].append(str(m)))
    monkeypatch.setattr(runlog, "event", lambda name, msg="", **kw:
                        seen["logged"].append((name, msg, kw)))
    real = durable.write_json

    def write_json(path, value, **kw):
        seen["written"].append(path.name)
        return real(path, value, **kw)
    monkeypatch.setattr(durable, "write_json", write_json)

    def run(took, status="ok"):
        def checks(hop, eng, dbs, wanted, workers=1, whole=True):
            seen["ran"].append(list(wanted))
            return ([{"check": c, "scope": "a", "status": status,
                      "detail": "src=3 dst=2"} for c in wanted],
                    {"a": {c: took.get(c, 1) for c in wanted}})
        monkeypatch.setattr(cli, "run_checks", checks)
    return hop, seen, run


def test_the_watch_times_every_check_then_runs_what_fits(watch):
    hop, seen, run = watch
    run({"schema": 5, "counts": 30, "autoinc": 5, "data": 600,
         "deep": 900, "params": 5})
    got = cli._monitor("w", "a", 300, "", 2)
    # the engine's checks, the deep ones and the settings with them
    assert seen["ran"] == [["counts", "schema", "autoinc", "data", "deep",
                            "params"],
                           ["counts", "schema", "autoinc", "params"]]
    # the second cycle left the data out: narrowed, never `same`
    assert got.status == "incomplete"
    assert seen["written"] == [cli.WATCH_SUMMARY] * 2
    assert (hop.report_dir() / cli.WATCH_SUMMARY).exists()


def test_the_operators_only_is_run_every_cycle(watch):
    _, seen, run = watch
    run({"schema": 5, "counts": 900, "autoinc": 5, "data": 600})
    cli._monitor("w", "a", 300, "counts", 2)
    assert seen["ran"] == [["counts"], ["counts"]]


def test_a_watch_whose_counts_outlast_the_interval_stops_saying_why(watch):
    _, seen, run = watch
    run({"schema": 1, "counts": 400, "autoinc": 1, "data": 1})
    with pytest.raises(SystemExit, match="400.0s of a 300s interval"):
        cli._monitor("w", "a", 300, "", 3)
    assert len(seen["ran"]) == 1
    assert [m for n, m, kw in seen["logged"] if kw.get("level") == "error"]


def test_the_cycle_line_says_the_verdicts_words_and_is_logged(watch):
    _, seen, run = watch
    run({"schema": 1, "counts": 1, "autoinc": 1, "data": 1}, status="diff")
    cli._monitor("w", "a", 300, "counts", 1)
    word = verdict.VERDICTS["different"].word
    line = [s for s in seen["said"] if "cycle 1" in s][0]
    assert f"counts: {word}" in line and "DIFF" not in line
    name, msg, kw = seen["logged"][0]
    assert name == "watch.cycle" and kw["cycle"] == 1
    assert msg == f"cycle 1 a: counts: {word} src=3 dst=2"
    assert "[" not in msg
