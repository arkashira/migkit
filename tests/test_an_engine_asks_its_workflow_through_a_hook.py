"""An engine adapter asks the workflow above it through `engines/hooks.py`.

The engines rank below the workflows (`docs/agents/LAYERS.md`), so an
engine that needs a step only a workflow does - the schema comparison, the
change follower, the grants a hop carries - asks `hooks` for the piece by
the workflow's name. The workflow fills it when it is imported. These prove
the three edges of that: the piece is there once the workflow is imported,
a piece asked before that is never answered with nothing, and nothing but
what the contract declares can be asked or filled.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from migkit.engines import hooks

ROOT = Path(__file__).resolve().parent.parent


def _fresh(code):
    got = subprocess.run([sys.executable, "-c", code], cwd=ROOT,
                         env={**os.environ, "PYTHONPATH": str(ROOT)},
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 0, got.stderr[-1500:]
    return got.stdout.strip()


def test_the_hook_is_filled_when_its_workflow_is_imported():
    out = _fresh(
        "from migkit.engines import hooks\n"
        "before = hooks.filled('verifiers')\n"
        "import migkit.verifiers as v\n"
        "print(before, hooks.filled('verifiers'),"
        " hooks.ask('verifiers', 'compare_schema') is v.compare_schema)")
    assert out == "False True True"


def test_an_engine_asking_first_gets_its_workflow_loaded_and_the_piece():
    """A copier or check that never started the command line still does the
    step: asking loads the workflow, which fills the hook."""
    out = _fresh(
        "import sys\n"
        "from migkit.engines import hooks\n"
        "assert 'migkit.drift' not in sys.modules\n"
        "got = hooks.workflow('drift').transient_among\n"
        "import migkit.drift as d\n"
        "print(got is d.transient_among)")
    assert out == "True"


def test_every_declared_piece_is_held_by_its_workflow_and_asked_by_an_engine():
    import importlib
    engines = "\n".join(p.read_text() for p in
                        (ROOT / "migkit" / "engines").glob("*.py")
                        if p.name != "hooks.py")
    for workflow, names in hooks.HOOKS.items():
        mod = importlib.import_module(f"migkit.{workflow}")
        assert hooks.filled(workflow)
        for name in names:
            assert hasattr(mod, name), (workflow, name)
            assert hooks.ask(workflow, name) is getattr(mod, name)
            assert re.search(rf"\b{re.escape(name)}\b", engines), (
                f"{workflow}.{name} is declared and no engine asks it")


def test_a_piece_asked_of_a_workflow_that_cannot_be_loaded_is_refused_by_name(
        monkeypatch):
    monkeypatch.setitem(hooks.HOOKS, "nowhere_at_all", ("thing",))
    with pytest.raises(hooks.HookNotFilled) as got:
        hooks.ask("nowhere_at_all", "thing")
    assert "`nowhere_at_all`" in str(got.value) and "thing" in str(got.value)
    with pytest.raises(hooks.HookNotFilled):
        hooks.workflow("nowhere_at_all").thing


def test_a_workflow_that_has_not_filled_its_hooks_is_refused_by_name(
        monkeypatch):
    """`hopkeys` imports fine and never fills: the answer is the refusal
    naming it, not None."""
    monkeypatch.setitem(hooks.HOOKS, "hopkeys", ("HOP_KEYS",))
    with pytest.raises(hooks.HookNotFilled) as got:
        hooks.ask("hopkeys", "HOP_KEYS")
    assert "`hopkeys`" in str(got.value)
    assert not hooks.filled("hopkeys")


def test_only_what_is_declared_can_be_asked_or_filled():
    with pytest.raises(KeyError, match="not a declared hook"):
        hooks.ask("verifiers", "not_a_piece")
    with pytest.raises(KeyError, match="not a declared hook"):
        hooks.ask("not_a_workflow", "x")
    with pytest.raises(KeyError, match="no engine hook is declared"):
        hooks.workflow("not_a_workflow")
    with pytest.raises(KeyError, match="no engine hook is declared"):
        hooks.fill("not_a_workflow", {})
    with pytest.raises(KeyError, match="compare_schema"):
        hooks.fill("verifiers", {})


def test_a_failed_fill_leaves_the_earlier_one_in_place():
    import migkit.verifiers as v
    with pytest.raises(KeyError):
        hooks.fill("verifiers", {})
    assert hooks.ask("verifiers", "compare_schema") is v.compare_schema
