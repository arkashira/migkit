"""Every command that writes passes one guard (audit V3, V4, V5, V9, V11,
V15, K3, ST3).

Three write paths skipped what `move` holds: `sync --mode migrate/stream
--go` was a move and a stream of its own (no position before its seed, a
subscription copying again over what the seed loaded); `sync --go` was a
third repair (no approval, the first answer only, exit 0 over a target
still different, its undo in /tmp until the end); `tail` and `replicate`
streamed real values into a hop that masks and wrote to a `goal: verify`
hop. Now one `_guard_write` stands before every first write, the flows are
the commands' own steps, and a dry run asks what `--go` asks.
"""
import json
import sqlite3

import pytest

from migkit import verdict


@pytest.fixture
def lite(tmp_path, monkeypatch):
    """A SQLite hop - no server - whose hop lines `make(extra)` adds to."""
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?, ?)",
                        [(i, f"v{i}") for i in range(3)])
        con.commit()
        con.close()
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    def make(extra=""):
        """`extra`: hop lines, `@ROOT@` standing for the test's directory."""
        conf = tmp_path / "hops.yaml"
        conf.write_text(
            "hops:\n  lite:\n    engine: sqlite\n"
            f"    source: {{host: {tmp_path / 'a.db'}, user: x,"
            " password: x}\n"
            f"    target: {{host: {tmp_path / 'b.db'}, user: x,"
            " password: x}\n"
            "    databases: [main]\n"
            + extra.replace("@ROOT@", str(tmp_path)))
        monkeypatch.setattr(cfg, "CONF", str(conf))
        return tmp_path
    return make


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, got.output


def _target(root):
    con = sqlite3.connect(root / "b.db")
    try:
        return con.execute("select id, v from t order by id").fetchall()
    finally:
        con.close()


def _change_target(root):
    con = sqlite3.connect(root / "b.db")
    con.execute("update t set v = 'changed' where id = 1")
    con.commit()
    con.close()


# ---- the aliases are the move ----------------------------------------------

def test_tail_into_a_hop_that_masks_is_refused(lite):
    root = lite("    options: {mask_on_write: {t.v: null}}\n")
    before = _target(root)
    got, said = _run("tail", "lite", "--db", "main", "--go")
    assert got.exit_code == verdict.EXIT["refused"], said
    assert "masks columns" in said, said
    assert _target(root) == before
    # the plan says it, and does not stop on it
    got, said = _run("tail", "lite", "--db", "main")
    assert "with --go this stops" in said and "masks columns" in said, said


def test_replicate_writes_nothing_into_a_hop_whose_goal_writes_nothing(
        lite):
    root = lite("    goal: verify\n")
    before = _target(root)
    got, said = _run("replicate", "lite", "--go")
    assert got.exit_code == verdict.EXIT["refused"], said
    assert "goal verify" in said, said
    assert _target(root) == before


# ---- sync --go is the one repair --------------------------------------------

def test_sync_go_asks_for_the_repair_s_approval_before_it_writes(lite):
    root = lite("    options: {approvals: {steps: [repair],"
                " signers: '@ROOT@/no-such-allowed_signers'},"
                " state: {mirror: '@ROOT@/mirror'}}\n")
    _change_target(root)
    before = _target(root)
    got, said = _run("sync", "lite", "--go")
    assert got.exit_code == verdict.EXIT["refused"], said
    assert "repair needs approval" in said, said
    assert _target(root) == before
    # and took no restore point for a repair that did not happen
    assert not (root / "reports" / "lite" / "main" / "state").exists()


def test_sync_go_keeps_its_restore_point_first_and_ends_as_the_recheck(
        lite, monkeypatch):
    from migkit import cli
    root = lite("    options: {state: {mirror: '@ROOT@/mirror'}}\n")
    _change_target(root)
    seen = []

    def repair_one(hop, eng, db, kind, do_apply, state=None):
        kept = root / "reports" / "lite" / db / "state" / state
        # committed where the backend keeps it before the repair writes
        seen.append((db, state, kept.is_dir()))
    monkeypatch.setattr(cli, "_repair_one", repair_one)
    got, said = _run("sync", "lite", "--go")
    assert len(seen) == 1 and seen[0][0] == "main" and seen[0][2], seen
    # nothing was repaired, so the check after it still differs - and the
    # command ends as that check does, where it ended 0
    assert got.exit_code == verdict.VERDICTS["different"].exit, said


def test_every_answer_is_read_not_the_first():
    """`check_data(d)[0]` read the first answer: a first `ok` repaired
    nothing and said "data: ok"."""
    from migkit import cli
    answers = [{"check": "data", "scope": "main.a", "status": "ok"},
               {"check": "data", "scope": "main.b", "status": "diff"},
               {"check": "params", "scope": "other", "status": "diff"}]
    assert cli._repair_dbs(answers, "rows") == ["main"]
    assert cli._repair_dbs(answers[:1], "rows") == []


def test_the_hop_s_on_conflict_holds_in_a_repair():
    from migkit import cli
    from migkit.config import Endpoint, Hop

    def hop(**opts):
        return Hop(name="h", engine="postgres",
                   source=Endpoint(host="10.0.0.1"),
                   target=Endpoint(host="10.0.0.2"), options=dict(opts))
    kept = hop(on_conflict="keep-target")
    assert cli._on_conflict(kept) == "keep-target"
    assert kept.options["on_conflict"] == "keep-target"
    # written the other way, it meant source-wins to every reader
    assert cli._on_conflict(hop(on_conflict="keep_target")) == "keep-target"
    assert cli._on_conflict(hop(), None) == "source-wins"
    assert cli._on_conflict(kept, "source-wins") == "source-wins"
    with pytest.raises(verdict.Usage):
        cli._on_conflict(hop(on_conflict="newest"))
    # the command line no longer brings its own default over the hop's
    on = next(p for p in cli.sync.params if p.name == "on_conflict")
    assert on.default is None


# ---- sync's flows are the commands' steps -----------------------------------

def _decision(status):
    return verdict.Decision(status, "", ())


def test_migrate_is_the_move_and_the_check(lite, monkeypatch):
    from migkit import cli
    lite()
    moved, checked = [], []
    monkeypatch.setattr(cli, "_move_run",
                        lambda *a: moved.append(a))
    monkeypatch.setattr(cli, "_caught_up", lambda hop, db: None)
    monkeypatch.setattr(cli, "_check",
                        lambda *a, **k: checked.append(a)
                        or (_decision("different"), []))
    got, said = _run("sync", "lite", "--mode", "migrate", "--go")
    assert [m[3] for m in moved] == ["full+cdc"], moved
    assert moved[0][6] is True
    assert checked, said
    assert got.exit_code == 1, said
    assert not hasattr(cli, "movers_pick_and_run")


def test_seed_plans_without_checking_and_goes_move_check_repair(
        lite, monkeypatch):
    from migkit import cli
    lite()
    moved, checks, repaired = [], [], []
    monkeypatch.setattr(cli, "_move_run", lambda *a: moved.append(a))
    answers = iter([(_decision("different"),
                     [{"check": "data", "scope": "main.t",
                       "status": "diff"}]),
                    (_decision("same"), [])])
    monkeypatch.setattr(cli, "_check", lambda *a, **k: checks.append(a)
                        or next(answers))
    monkeypatch.setattr(cli, "_repair",
                        lambda *a, **k: repaired.append((a, k)))
    # the plan: the move's, and not a check against a target it never
    # loaded (three full checks and a repair plan, then exit 1)
    got, said = _run("sync", "lite", "--mode", "seed")
    assert moved[-1][3] == "full" and moved[-1][6] is False
    assert checks == [] and repaired == [], said
    got, said = _run("sync", "lite", "--mode", "seed", "--go")
    assert len(checks) == 2 and repaired[0][1]["dbs"] == ["main"], repaired
    assert repaired[0][0][3] is True
    assert got.exit_code == 0, said


def test_stream_verifies_what_changed_and_ends_as_its_cycle(lite,
                                                            monkeypatch):
    from migkit import capabilities, cli
    lite()
    moved, loops = [], []
    monkeypatch.setattr(cli, "_move_run", lambda *a: moved.append(a))
    monkeypatch.setattr(capabilities, "unavailable", lambda *a, **k: None)
    monkeypatch.setattr(cli, "_delta_loop", lambda *a: loops.append(a)
                        or _decision("different"))
    got, said = _run("sync", "lite", "--mode", "stream", "--go")
    assert [m[3] for m in moved] == ["cdc"] and loops, (moved, loops)
    assert loops[0][3] == 1           # one cycle without --serve
    assert got.exit_code == 1, said


# ---- a dry run asks what --go asks ------------------------------------------

def test_the_dry_run_reads_the_ways_given_up_and_writes_nothing(tmp_path):
    from migkit import decide
    where = tmp_path / "move.json"
    rungs = (decide.Rung("fast", "the fast way"),
             decide.Rung("slow", "the slow way"))
    ran = []

    def boom(choice):
        ran.append(choice.rung.name)
        if choice.rung.name == "fast":
            raise RuntimeError("the program exited 3")
        return choice.rung.name
    decide.attempt("the copy", rungs, boom, where=where)
    kept = tmp_path / "move-ways.json"
    before = kept.read_text()
    ran.clear()
    got, _ = decide.attempt("the copy", rungs, lambda c: ran.append(
        c.rung.name), where=where, record=False)
    # it named the way --go takes, where it passed `where` over and named
    # the one given up
    assert ran == ["slow"] and got.rung.name == "slow"
    assert kept.read_text() == before


def test_the_bulk_plan_climbs_with_go_s_record(monkeypatch, tmp_path):
    from migkit import cli, decide, movers
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"))
    hop.report_dir = lambda db="": tmp_path
    asked = []
    monkeypatch.setattr(movers, "which", lambda p: None)
    monkeypatch.setattr(decide, "attempt",
                        lambda *a, **k: asked.append(k))
    cli._bulk_or_copier(hop, None, "app", "pgdump", "", 1000, "postgres",
                        False)
    assert asked[0]["record"] is False
    assert asked[0]["where"] == tmp_path / "move.json"


def test_a_guard_says_in_a_plan_what_stops_go(monkeypatch):
    from migkit import cli
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="postgres", goal="verify",
              source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"))
    said = []
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", **k: said.append(str(m)))
    cli._guard_write(hop, None, "a move", None, False)
    assert any("with --go this stops" in m and "goal verify" in m
               for m in said), said
    with pytest.raises(verdict.Refused, match="goal verify"):
        cli._guard_write(hop, None, "a move", None, True)


# ---- flags that do not go together ------------------------------------------

@pytest.mark.parametrize("argv", [
    ["move", "lite", "--mode", "full", "--drop", "--go"],
    ["move", "lite", "--mode", "full+cdc", "--drop"],
    ["move", "lite", "--mode", "cdc", "--table", "t"],
    ["sync", "lite", "--tag", "before"],
    ["sync", "lite", "--interval", "5"],
    ["sync", "lite", "--mode", "verify", "--kind", "rows"],
    ["sync", "lite", "--mode", "seed", "--serve"],
    ["sync", "lite", "--apply", "--go"],
    ["watch", "lite", "--only", "data"],
    ["report", "lite", "--port", "9000"],
])
def test_flags_that_would_be_ignored_are_a_usage_error(lite, argv):
    lite()
    got, said = _run(*argv)
    assert got.exit_code == verdict.EXIT["usage"], said


def test_watch_teardown_alone_drops_the_delta_state(lite, monkeypatch):
    """It started an endless count watch, and the slot it was asked to
    drop kept the source's log growing."""
    from migkit import cli
    lite()
    called = []
    monkeypatch.setattr(cli, "_delta_loop",
                        lambda *a: called.append(a) or None)
    monkeypatch.setattr(cli, "_sample_watch",
                        lambda *a: pytest.fail("a count watch started"))
    got, said = _run("watch", "lite", "--teardown")
    assert got.exit_code == 0 and called and called[0][4] is True, said


# ---- rollback, history ------------------------------------------------------

def test_rollback_holds_the_hop_once_and_gives_the_writes_back_last(
        lite, monkeypatch):
    from migkit import cli, cutover, freeze, repairlog
    lite()
    order, locks = [], []

    class Lease:
        def unlink(self):
            order.append("let go")
    monkeypatch.setattr(cli, "_lock", lambda hop: locks.append(1) or Lease())
    monkeypatch.setattr(cutover, "back_plan", lambda *a: None)
    monkeypatch.setattr(repairlog, "to_reverse", lambda *a: [])
    monkeypatch.setattr(cli, "_restore_target",
                        lambda *a, **k: order.append("restored"))
    monkeypatch.setattr(freeze, "state", lambda hop, db: {"roles": ["app"]})
    monkeypatch.setattr(freeze, "thaw",
                        lambda *a: order.append("writes given back"))
    got, said = _run("rollback", "lite", "--db", "main", "--apply")
    assert got.exit_code == 0, said
    assert order == ["restored", "writes given back", "let go"], order
    assert locks == [1]


def test_history_reads_what_was_kept_not_the_source(lite, monkeypatch):
    from migkit import cli
    root = lite()
    kept = root / "reports" / "lite" / "main" / "state" / "20261001-000000"
    kept.mkdir(parents=True)
    (kept / "meta.json").write_text(json.dumps(
        {"ts": "20261001-000000", "op": "sync", "created": "2026-10-01"}))

    def down(hop):
        raise RuntimeError("could not connect to the source")
    monkeypatch.setattr(cli, "get_engine", down)
    got, said = _run("history", "lite")
    assert got.exit_code == 0, said
    assert "20261001-000000" in said and "main" in said, said


def test_report_refresh_rewrites_no_answer(lite):
    root = lite()
    rep = root / "reports" / "lite"
    rep.mkdir(parents=True)
    rows = [{"check": "data", "scope": "main", "status": "diff",
             "detail": "missing=0 extra=0 changed=0"}]
    (rep / "summary.json").write_text(json.dumps(rows))
    got, said = _run("report", "lite", "--refresh")
    assert json.loads((rep / "summary.json").read_text()) == rows, said
    assert "DIFFERENT" in (rep / "report.html").read_text()


def test_the_guard_s_refusals_are_in_the_troubleshooting_guide():
    """A refusal raised as the verdict's own (`Refused`, `Usage`) is on
    the guide an operator searches, as a `SystemExit` always was."""
    from migkit import troubleshooting
    texts = [t for _, _, t in troubleshooting.refusals()]
    assert any("masks columns on the way, and a change stream" in t
               for t in texts), texts
    assert any(t.startswith("--drop tears the stream down") for t in texts)
    # another module's `Refused` is its own: a converter's reason, not a
    # stop of the command
    assert not any("held no CREATE statement" in t for t in texts)


# ---- the interlock, once a run ----------------------------------------------

def test_the_interlock_asks_each_server_once_a_run(tmp_path, monkeypatch):
    from migkit import clouds, interlock
    from migkit.config import Endpoint, Hop
    monkeypatch.setattr(interlock, "_unreached", lambda hop, side, ep: None)
    monkeypatch.setattr(clouds, "of", lambda eng, side, ep: clouds.Where())
    asked = []

    class Eng:
        CANON_ENGINE = "postgres"

        def server_identity(self, side, db):
            asked.append(side)
            return {"id": {"cluster": side}, "name": side}
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432),
              target=Endpoint(host="10.0.0.2", port=5432))
    hop.report_dir = lambda db=None: tmp_path
    eng = Eng()
    for _ in range(3):
        interlock.before_write(hop, eng, "app", True, lambda m: None)
    assert sorted(asked) == ["dst", "src"], asked
    # another engine - another run's - asks again
    interlock.before_write(hop, Eng(), "app", True, lambda m: None)
    assert len(asked) == 4
