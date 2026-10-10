"""Every task migkit starts is named in `decide.TASKS`: routed through the
engine, a declared gap, or a stated no-choice (P0-DECISION-ALL.coverage)."""
import ast
from pathlib import Path

from migkit import decide

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = []


def _commands():
    tree = ast.parse((Path(decide.__file__).with_name("cli.py")).read_text())
    names = []
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        for d in fn.decorator_list:
            if isinstance(d, ast.Call) and ast.unparse(d.func) == "main.command":
                given = [a.value for a in d.args if isinstance(a, ast.Constant)]
                names.append(given[0] if given else fn.name)
    return names


def test_every_command_is_in_the_task_table():
    assert sorted(set(_commands()) - set(decide.TASKS)) == []


def test_no_task_is_named_that_does_not_exist():
    assert sorted(set(decide.TASKS) - set(_commands())) == []


def test_a_routed_task_names_modules_that_use_the_engine():
    bad = [t for t, why in decide.task_gaps() if "does not use" in why]
    assert bad == []


def test_a_no_choice_states_its_reason():
    for task, (_f, got) in decide.TASKS.items():
        assert got[0] in ("routed", "gap", "no choice"), task
        if got[0] != "routed":
            assert len(got[1]) > 10, task
