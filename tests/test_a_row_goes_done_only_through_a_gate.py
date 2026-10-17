"""A backlog row becomes DONE only with a PASS line in docs/agents/GATES.md."""
import importlib.util
import pathlib

SPEC = importlib.util.spec_from_file_location(
    "check_merge_gate",
    pathlib.Path(__file__).resolve().parent.parent / "tools" / "check_merge_gate.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

BEFORE = "| 7 | a | x | OPEN | L | y |\n| 8 | b | x | PARTLY | L | y |\n| 9 | c | x | DONE | L | y |\n"
AFTER = "| 7 | a | x | DONE 2026-10-17 | L | y |\n| 8 | b | x | DONE | L | y |\n| 9 | c | x | DONE | L | y |\n"


def test_a_row_set_done_without_a_pass_line_is_named():
    assert gate.ungated(BEFORE, AFTER, "") == ["7", "8"]


def test_a_row_with_its_pass_line_goes_through():
    gates = "| 7 | 2026-10-17 | PASS | critic | abc123 | - |\n"
    assert gate.ungated(BEFORE, AFTER, gates) == ["8"]


def test_a_failed_review_does_not_count_as_a_gate():
    gates = "| 7 | 2026-10-17 | REVISE | critic | abc123 | facade |\n"
    assert gate.ungated(BEFORE, AFTER, gates) == ["7", "8"]


def test_a_row_already_done_or_set_back_to_review_needs_nothing():
    after = "| 7 | a | x | REVIEW 2026-10-17 | L | y |\n| 9 | c | x | DONE | L | y |\n"
    assert gate.ungated(BEFORE, after, "") == []


def test_the_hook_runs_the_gate():
    hook = pathlib.Path(__file__).resolve().parent.parent / ".githooks" / "pre-commit"
    assert "check_merge_gate.py --staged" in hook.read_text()
