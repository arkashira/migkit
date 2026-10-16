"""The level a change-reading depth earns is the goal's (W15J-D0,
`decide.change_depth_level`): a goal that reads no changes is not held by
a source whose changes cannot be read (D0, a note), one that follows them
is (D0 fails, with the statement that raises it) - one rung read by
`changedepth.lines` and the readiness check through the goal, never a
second table of levels."""

import pytest

from migkit import changedepth, decide, ready
from tests.conftest import needs_docker
from tests.mysql_pair import my_pair  # noqa: F401 - a fixture

D0 = {"w9c.o": ("D0", "set global binlog_row_metadata = 'FULL'")}


def _row(rows):
    assert len(rows) == 1, rows
    return rows[0]


def test_a_copy_goal_passes_a_d0_source_with_a_note():
    level, item, detail = _row(changedepth.lines(D0, goal="copy",
                                                 follows=False))
    assert level == "pass"
    assert item.startswith("change reading D0")
    assert "a note: goal copy reads no changes" in detail
    # the statement is kept for a later goal that follows changes
    assert detail.endswith("to follow changes later: set global"
                           " binlog_row_metadata = 'FULL'")
    assert "to raise it" not in detail


def test_a_goal_that_follows_changes_fails_on_d0_with_the_raise():
    level, _, detail = _row(changedepth.lines(D0, goal="cutover",
                                              follows=True))
    assert level == "fail"
    assert detail.endswith("to raise it: set global binlog_row_metadata"
                           " = 'FULL'")
    assert "a note" not in detail


def test_a_goal_not_said_is_read_as_following_changes():
    """Where the caller says no goal (`doctor`'s view), the stricter level."""
    assert _row(changedepth.lines(D0))[0] == "fail"


def test_the_choice_names_the_goal_and_the_depth():
    copy = decide.change_depth_level({"depth": "D0", "goal": "copy",
                                      "follows": False})
    held = decide.change_depth_level({"depth": "D0", "goal": "cutover",
                                      "follows": True})
    assert (copy.path, held.path) == ("unread", "none")
    for got, goal in ((copy, "copy"), (held, "cutover")):
        assert "D0" in got.reason and f"goal {goal}" in got.reason, got.reason
    assert "the goal follows the source's changes" in held.reason
    assert {decide.CHANGE_DEPTH_LEVELS[c.path] for c in (copy, held)} == {
        "pass", "fail"}


@pytest.mark.parametrize("depth,follows,level", [
    ("D4", True, "pass"), ("D4", False, "pass"),
    ("D3", True, "warn"), ("D2", True, "warn"), ("D1", True, "warn"),
    ("D3", False, "pass"), ("D1", False, "pass"),
    ("D0", True, "fail"), ("D0", None, "fail"), ("D0", False, "pass")])
def test_every_depth_earns_its_level_from_the_goal(depth, follows, level):
    got = decide.change_depth_level({"depth": depth, "goal": "g",
                                     "follows": follows})
    assert decide.CHANGE_DEPTH_LEVELS[got.path] == level, got.reason


class _Hop:
    name, databases = "t", ["w9c"]

    def __init__(self, goal):
        self.goal = goal


class _Eng:
    """An engine whose own assessment says its depths the way the MySQL
    and PostgreSQL engines do: `changedepth.lines`, no goal given."""

    def assess(self):
        return [{"level": lv, "item": it, "detail": d}
                for lv, it, d in changedepth.lines(D0)]


@pytest.mark.parametrize("goal,mode,level", [
    ("copy", None, "pass"), ("copy", "full", "pass"),
    ("copy", "full+cdc", "fail"), ("cutover", None, "fail"),
    ("keep-in-sync", "full", "pass"), ("", None, "pass")])
def test_the_readiness_check_gives_the_engine_the_goal(goal, mode, level):
    """`ready.depth` asks the engine's assessment under the hop's goal and
    the step's mode (a `migrate` copy's move is `full`): the D0 row's level
    is the goal's, and a copy's gate is not held by it."""
    quick, rows, _ = ready.depth(_Hop(goal), _Eng(), mode)
    assert not quick
    assert _row([r for r in rows if "change reading" in r["item"]])[
        "level"] == level
    # outside the readiness check the engine's own rows say no goal
    assert _row(_Eng().assess())["level"] == "fail"


def _copy_on_d0(tmp_path):
    from tests import mysql_pair
    from tests.test_autonomous_scenarios import GO, Side, _conf, _run
    side = Side("mysql", mysql_pair._side("src"), mysql_pair._side("dst"))
    assert side.sql("src", "select @@binlog_row_metadata").upper() != "FULL"
    conf = _conf(tmp_path, side)
    side.seed(100)
    return side, conf, _run, _run(conf, side, *GO)


@needs_docker
def test_a_copy_on_a_source_without_row_metadata_is_not_held(
        my_pair, tmp_path):  # noqa: F811
    """`migrate --goal copy` on the default MySQL profile, whose source
    keeps no row metadata in its log (D0): the assessment does not hold
    the database back - its copy starts - and `assess` says the depth as
    a note, not a stop."""
    side, conf, run_, run = _copy_on_d0(tmp_path)
    out = run["r"].stdout
    assert "not started" not in out, out[-3000:]
    assert "copy of w9c" in out or "w9c bulk copy" in out, out[-3000:]
    said = run_(conf, side, "assess", "t")["r"].stdout
    line = next((ln for ln in said.splitlines() if "change reading D0" in ln),
                said[-3000:])
    assert "a note: goal copy reads no changes" in said, said[-3000:]
    assert "fail" not in line.lower(), line


@needs_docker
@pytest.mark.xfail(strict=True, reason=(
    "W15J-MYBULK (W16L): past the assessment, the MySQL bulk copy's one"
    " trial on the default profile stops with 'a failure no class here"
    " describes' and the copy is held"))
def test_a_copy_on_a_source_without_row_metadata_completes(
        my_pair, tmp_path):  # noqa: F811
    """The same copy completes, every row equal on both sides."""
    side, _, _, run = _copy_on_d0(tmp_path)
    assert run["r"].returncode == 0, run["r"].stdout[-3000:]
    assert side.digest("dst") == side.digest("src")
