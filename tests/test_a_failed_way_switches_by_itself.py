"""A way that is blocked or fails as it runs is switched from by itself,
and said - never asked (the owner, 2026-09-30).

`decide.attempt` runs the climb's rung; an operational failure undoes
what the rung began, keeps it given up beside the work's position, says
one line and goes on to the next rung that holds everything the work
needs. A verdict is never a failure. No servers here: the rungs' work is
given, and made to fail on purpose.
"""
import subprocess

import pytest

from migkit import decide

A = decide.Rung("a", "the fast way")
B = decide.Rung("b", "the plain way")
C = decide.Rung("c", "the last way")


def _run(fail=(), error=RuntimeError("the program exited 1")):
    ran = []

    def run(choice):
        ran.append(choice.rung.name)
        if choice.rung.name in fail:
            raise error
        return f"done by {choice.rung.name}"
    return run, ran


def test_a_failure_switches_to_the_next_way_and_says_so():
    run, ran = _run(fail=("a",))
    said, undone = [], []
    cleaned = []
    a = decide.Rung("a", "the fast way",
                    cleanup=lambda f, e: cleaned.append(str(e)))
    got, result = decide.attempt(
        "the copy", [a, B], run, log=said.append,
        undo=lambda r, e: undone.append(r.name),
        costs=decide.Costs({"a": 1.0, "b": 3.0}))
    assert ran == ["a", "b"] and result == "done by b"
    assert got.rung.name == "b"
    assert cleaned == ["the program exited 1"] and undone == ["a"]
    assert len(said) == 1, said
    assert "the fast way could not run (the program exited 1)" in said[0]
    assert "switching to the plain way" in said[0]
    assert "3.0 times the time" in said[0], said


@pytest.mark.parametrize("error", [
    OSError("connection refused"), TimeoutError("no answer in 30s"),
    subprocess.CalledProcessError(2, ["x"]), decide.Blocked("refused"),
])
def test_what_counts_as_a_way_that_could_not_run(error):
    run, ran = _run(fail=("a",), error=error)
    decide.attempt("the copy", [A, B], run)
    assert ran == ["a", "b"]


@pytest.mark.parametrize("error", [
    decide.Verdict("3 rows differ"), SystemExit("migkit stopped it"),
    TypeError("a fault in migkit"), KeyError("x"),
])
def test_a_verdict_a_stop_or_a_fault_is_never_switched_from(error):
    """A difference found is the answer; migkit's own stop is said as it
    is; a fault in migkit must be seen, not hidden behind another way."""
    run, ran = _run(fail=("a",), error=error)
    with pytest.raises(type(error)):
        decide.attempt("the check", [A, B], run)
    assert ran == ["a"]


def test_a_way_that_gives_less_than_the_work_needs_is_never_switched_to():
    exact = decide.Rung("a", "the exact way", gives=frozenset({"exact"}))
    loose = decide.Rung("b", "the loose way")
    run, ran = _run(fail=("a",))
    with pytest.raises(decide.Exhausted) as e:
        decide.attempt("the copy", [exact, loose], run, need=("exact",))
    assert ran == ["a"]
    assert "the loose way" in str(e.value) and "no batch may be applied" \
        " twice" in str(e.value), e.value


def test_every_way_failing_stops_with_every_reason():
    run, ran = _run(fail=("a", "b"))
    with pytest.raises(decide.Exhausted) as e:
        decide.attempt("the copy", [A, B], run)
    assert ran == ["a", "b"]
    text = str(e.value)
    assert text.startswith("no way of the copy could run here")
    assert "the fast way: the program exited 1" in text
    assert "the plain way: the program exited 1" in text
    assert isinstance(e.value.__cause__, RuntimeError)
    assert isinstance(e.value, SystemExit)


def test_what_cannot_be_undone_stops_everything():
    def cannot(f, e):
        raise RuntimeError("the target is half replaced")
    a = decide.Rung("a", "the fast way", cleanup=cannot)
    run, ran = _run(fail=("a",))
    with pytest.raises(SystemExit) as e:
        decide.attempt("the seed", [a, B], run)
    assert ran == ["a"]
    assert "could not be undone (the target is half replaced)" in \
        str(e.value) and "no other way is tried" in str(e.value)


def test_only_blocked_switches_where_a_way_cannot_be_put_back():
    run, ran = _run(fail=("a",))
    with pytest.raises(RuntimeError):
        decide.attempt("the stream", [A, B], run,
                       switch=decide.only_blocked)
    run, ran = _run(fail=("a",), error=decide.Blocked("cannot dial"))
    decide.attempt("the stream", [A, B], run, switch=decide.only_blocked)
    assert ran == ["a", "b"]


def test_a_restart_does_not_retry_a_way_blindly(tmp_path):
    where = tmp_path / "move.json"
    where.write_text("{}")
    facts = {"version": "1.0"}
    a = decide.Rung("a", "the fast way", needs=(
        decide.Need("its program", lambda f: f["version"] is not None),))
    run, ran = _run(fail=("a",))
    decide.attempt("the copy", [a, B], run, dict(facts), where=where)
    assert ran == ["a", "b"]
    assert "a" in decide.given_up(where, "the copy")
    # the next run: the fact that stopped it is the same
    run, ran = _run()
    said = []
    got, _ = decide.attempt("the copy", [a, B], run, dict(facts),
                            where=where, log=said.append)
    assert ran == ["b"]
    assert any(r.name == "a" and "(given up on here)" in w
               for r, w in got.passed), got.passed
    # a new copy (its position gone) after the program was upgraded: the
    # fast way is tried again, and that is said
    where.unlink()
    run, ran = _run()
    decide.attempt("the copy", [a, B], run, {"version": "2.0"},
                   where=where, log=said.append)
    assert ran == ["a"]
    assert any("tried again - what stopped it last time has changed" in m
               for m in said), said


def test_where_every_way_was_given_up_the_last_is_tried_again(tmp_path):
    where = tmp_path / "move.json"
    where.write_text("{}")
    run, ran = _run(fail=("a", "b"))
    with pytest.raises(decide.Exhausted):
        decide.attempt("the copy", [A, B], run, {}, where=where)
    run, ran = _run()
    said = []
    decide.attempt("the copy", [A, B], run, {}, where=where,
                   log=said.append)
    # the plain way was given up last: the one the work stood on last
    assert ran == ["b"], ran
    assert any("trying the plain way again" in m for m in said), said


def test_the_way_that_ran_is_kept_for_a_restart(tmp_path):
    where = tmp_path / "move.json"
    where.write_text("{}")
    run, _ = _run(fail=("a",))
    decide.attempt("the copy", [A, B], run, {}, where=where)
    assert decide.remembered(where, "the copy") == "b"


def _change(state, name="plugin"):
    return decide.Change(name, "a plugin", lambda f: decide.Stand(state),
                         make=("INSTALL IT",), undo=("UNINSTALL IT",))


def test_the_offer_is_the_one_question_and_no_switches_by_itself():
    fast = decide.Rung("a", "the fast way",
                       changes=(_change(decide.POSSIBLE),))
    run, ran = _run()
    asked = []

    def answer(offer):
        asked.append((offer.rung.name, [c.name for c in offer.changes],
                      offer.instead.rung.name))
        return "no"
    said = []
    got, _ = decide.attempt("the seed", [fast, B], run, {}, answer=answer,
                            log=said.append)
    assert asked == [("a", ["plugin"], "b")]
    assert ran == ["b"]
    assert "the answer is no" in got.reason, got.reason
    # yes takes the fast way; a change there already asks nothing
    run, ran = _run()
    decide.attempt("the seed", [fast, B], run, {}, answer=lambda o: "yes")
    assert ran == ["a"]
    there = decide.Rung("a", "the fast way",
                        changes=(_change(decide.PRESENT),))
    run, ran = _run()
    decide.attempt("the seed", [there, B], run, {},
                   answer=lambda o: pytest.fail("asked of a change there"))
    assert ran == ["a"]


@pytest.mark.parametrize("state,words", [
    (decide.IMPOSSIBLE, "which this source does not allow"),
    (decide.BY_DBA, "which only a DBA can make here"),
])
def test_a_change_nobody_here_can_make_is_never_asked(state, words):
    fast = decide.Rung("a", "the fast way", changes=(_change(state),))
    run, ran = _run()
    got, _ = decide.attempt(
        "the seed", [fast, B], run, {},
        answer=lambda o: pytest.fail("asked where nothing could be done"))
    assert ran == ["b"] and words in got.reason, got.reason


def test_a_probe_that_fails_is_a_dbas_never_a_yes():
    def broken(f):
        raise RuntimeError("permission denied for pg_authid")
    fast = decide.Rung("a", "the fast way", changes=(
        decide.Change("plugin", "a plugin", broken),))
    got = decide.climb("the seed", [fast, B], {})
    assert got.rung.name == "b"
    assert "could not be read: permission denied" in got.reason


# --- the sites routed through it -------------------------------------------

def test_a_bulk_way_that_fails_falls_to_the_table_copier(monkeypatch,
                                                         tmp_path):
    from migkit import cli, movers
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="app"),
              target=Endpoint(host="10.0.0.2", port=5432, user="app"))
    hop.report_dir = lambda db="": tmp_path
    ran = []

    def bulk(*a):
        ran.append("bulk")
        raise RuntimeError("the program exited 3")
    monkeypatch.setattr(cli, "_bulk_db", bulk)
    monkeypatch.setattr(cli, "_move_full",
                        lambda *a: ran.append("copier"))
    monkeypatch.setattr(cli, "_lock", lambda h: type(
        "L", (), {"unlink": lambda self: None})())
    monkeypatch.setattr(movers, "which", lambda p: None)
    said = []
    monkeypatch.setattr(cli.console, "print", lambda m="", **k:
                        said.append(str(m)))
    cli._bulk_or_copier(hop, None, "app", "pgdump", "", 1000, "postgres")
    assert ran == ["bulk", "copier"]
    assert any("switching to the table copier" in m for m in said), said
    # migkit's own stop in the bulk way is not switched from
    ran.clear()

    def stop(*a):
        ran.append("bulk")
        raise SystemExit("an earlier move's program is still running")
    monkeypatch.setattr(cli, "_bulk_db", stop)
    (tmp_path / "move-ways.json").unlink()
    with pytest.raises(SystemExit, match="still running"):
        cli._bulk_or_copier(hop, None, "app", "pgdump", "", 1000,
                            "postgres")
    assert ran == ["bulk"]


def test_a_target_that_cannot_dial_the_source_falls_to_the_leg_from_here(
        monkeypatch, tmp_path):
    from migkit import cli, movers
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="app"),
              target=Endpoint(host="10.0.0.2", port=5432, user="app"))
    hop.report_dir = lambda db="": tmp_path

    class Eng:
        def maps_columns(self):
            return False

        def replicate_sql(self, *a, **k):
            return {}
    monkeypatch.setattr(movers, "follow_selected", lambda: "")
    ran = []

    def subscribe(*a):
        ran.append("subscription")
        raise decide.Blocked("the target could not reach the source while"
                             " running CREATE SUBSCRIPTION")
    monkeypatch.setattr(cli, "_replicate_released", subscribe)
    monkeypatch.setattr(cli, "_follow", lambda *a: ran.append("follow"))
    said = []
    monkeypatch.setattr(cli.console, "print", lambda m="", **k:
                        said.append(str(m)))
    cli._cdc(hop, Eng(), "app", False, True, "postgres")
    assert ran == ["subscription", "follow"], ran
    assert any("switching to the change leg run from here" in m
               for m in said), said
    # the next run goes to the leg from here at once
    ran.clear()
    cli._cdc(hop, Eng(), "app", False, True, "postgres")
    assert ran == ["follow"], ran
