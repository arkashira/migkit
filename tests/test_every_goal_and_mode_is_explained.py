"""Every goal and mode explained plainly, from one table, wherever the
operator meets it (the owner, 2026-09-30) - and `goal: cutover` does not
turn the migkit-driven cutover on.

What each goal does on the source and on the target and what it
guarantees is written once (`hopkeys.GOALS`, `MODES`, `CUTOVER`) and said
by `init`, `assess`, the dry run and the docs. A hop cut over without
`cutover:` is told every time what it does by hand and what turning it
on adds.
"""
import pathlib

import pytest

from migkit import cutover, hopkeys
from migkit.config import Endpoint, Hop

DOCS = pathlib.Path(__file__).resolve().parents[1] / "docs"


def _hop(goal="cutover", **options):
    hop = Hop(name="shop", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="app"),
              target=Endpoint(host="10.0.0.2", port=5432, user="app"),
              options=dict(options))
    hop.goal = goal
    return hop


@pytest.mark.parametrize("goal", sorted(hopkeys.GOALS))
def test_no_goal_turns_the_cutover_on(goal, monkeypatch):
    from migkit import freeze
    monkeypatch.setattr(freeze, "supported", lambda hop: True)
    hop = _hop(goal)
    assert "cutover" not in hopkeys.GOALS[goal]["options"]
    assert "cutover" not in hopkeys.goal_options(hop, goal)
    hopkeys.apply_goal(hop)
    assert cutover.settings(hop) is None
    assert not hopkeys.cutover_on(hop)


def test_a_hop_turns_it_on_itself_and_a_goal_keeps_it():
    hop = _hop("cutover", cutover=True)
    hopkeys.apply_goal(hop)
    assert cutover.settings(hop) is not None and hopkeys.cutover_on(hop)
    assert not hopkeys.cutover_on(_hop("cutover", cutover=False))


@pytest.mark.parametrize("goal", sorted(hopkeys.GOALS))
def test_every_goal_says_each_side_and_what_it_guarantees(goal):
    g = hopkeys.GOALS[goal]
    for part in ("what", "source", "target", "guarantees"):
        assert isinstance(g[part], str) and len(g[part]) > 8, (goal, part)
    said = hopkeys.explain_goal(_hop(goal), goal)
    assert said[0].startswith(f"goal {goal}: ")
    assert said[1] == f"  on the source: {g['source']}"
    assert said[2] == f"  on the target: {g['target']}"
    assert said[3] == f"  it guarantees: {g['guarantees']}"


def test_verify_and_copy_never_write_the_source():
    for goal in ("verify", "copy"):
        assert "never written" in hopkeys.GOALS[goal]["source"] or \
            hopkeys.GOALS[goal]["source"] == "read only", goal
    assert hopkeys.GOALS["verify"]["target"] == "read only"


def test_a_cutover_without_it_is_told_the_hand_steps_and_what_it_adds():
    text = "\n".join(hopkeys.explain_goal(_hop(reverse="at_cutover")))
    assert "cutover: off - the goal does not turn it on" in text
    for step in hopkeys.CUTOVER["by hand"]:
        assert step.format(hop="shop", back=" and starts the stream"
                           " back") in text, step
    for words in ("a timed path", "the freeze", "the drain",
                  "the fenced check", "the counters", "the stream back",
                  "the undo", "the outage measured"):
        assert words in text, words
    assert "turning it on (options: cutover: true" in text


def test_a_cutover_with_it_says_what_migkit_runs_and_writes():
    text = "\n".join(hopkeys.explain_goal(_hop(cutover={"budget": 20})))
    assert "cutover: on - migkit runs the cutover itself" in text
    assert "by hand" not in text
    assert "written during the cutover only" in text
    assert "migkit rollback shop --db DB --apply" in text
    # a goal that is not a cutover says nothing of it unless it is on
    assert not any("cutover:" in line for line in hopkeys.explain_goal(
        _hop("copy")))


def test_the_cutover_command_says_the_same_steps():
    """`move --mode cdc --drop` without `cutover:` lists the steps from
    the one table, and what turning it on adds."""
    hop = _hop(reverse="at_cutover")
    text = "\n".join(cutover.plan(hop, ["app"], None))
    for i, step in enumerate(hopkeys.CUTOVER["by hand"], 1):
        assert f"  {i}. " + step.format(
            hop="shop", back=" and starts the stream back") in text, step
    for add in hopkeys.CUTOVER["adds"]:
        assert add in text, add
    assert "no goal turns it on" in text


@pytest.mark.parametrize("key", sorted(hopkeys.MODES))
def test_every_mode_is_said(key):
    command, mode = key
    drop = mode == "cdc --drop"
    got = hopkeys.explain_mode(command, "cdc" if drop else mode, drop)
    assert got == f"--mode {mode}: {hopkeys.MODES[key]}"
    assert hopkeys.explain_mode("move", "no such mode") == ""


def test_init_says_what_the_goal_does(tmp_path):
    from migkit import guide
    said = []
    guide.plan(_hop("cutover"), {}, "cutover", said.append)
    text = "\n".join(said)
    assert hopkeys.GOALS["cutover"]["source"] in text
    assert "cutover: off - the goal does not turn it on" in text


def test_assess_says_what_each_side_sees(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from migkit import cli

    hop = _hop("cutover")
    hop.report_dir = lambda db="": tmp_path

    class Eng:
        def assess(self):
            return [{"level": "pass", "scope": "x", "item": "y",
                     "detail": ""}]

        def databases(self):
            return ["app"]
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: Eng())
    monkeypatch.setattr(cli, "_require_configured", lambda h: None)
    monkeypatch.setattr(cutover, "summary",
                        lambda h, e, d: ("warn", "no cutover: said"))
    got = CliRunner().invoke(cli.main, ["assess", "shop"])
    out = " ".join(got.output.split())
    assert "what each side sees" in out, out
    assert "on the source:" in out and "it guarantees:" in out, out
    assert "the operator does this by hand" in out, out
    assert "the outage measured" in out, out


def test_the_docs_say_every_goal_mode_and_what_cutover_adds():
    page = (DOCS / "goals-and-modes.md").read_text()
    for goal in hopkeys.GOALS:
        assert f"`{goal}`" in page, goal
    for command, mode in hopkeys.MODES:
        assert f"{command} --mode {mode}" in page, (command, mode)
    for words in ("a timed path", "the freeze", "the drain",
                  "the fenced check", "the counters", "the stream back",
                  "the undo", "the outage measured"):
        assert words in page, words
    assert "does not turn `cutover:` on" in page
    cut = (DOCS / "cutover.md").read_text()
    assert "goals-and-modes.md" in cut
    assert "does not turn `cutover:` on" in cut
