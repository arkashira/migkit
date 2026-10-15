"""Every module under migkit/ has an importer inside migkit: a module only
its own test imports is logic the tool never runs (CRIT0 2)."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "migkit"

#: entry points: started from outside, never imported by migkit
ENTRY = {
    "migkit", "migkit.cli",
    # imported by the team's scheduler (Airflow, Dagster, a CI step)
    "migkit.integrations",
    # started as a process of its own on the reading machine
    "migkit.runners", "migkit.runners.second_reader",
    # run as a program by `cloudapi.Huawei` in the SDK's own environment
    "migkit.huawei_sdk",
    # read by `tools/gen_docs.py` into the manual, and run with -m
    "migkit.troubleshooting",
}

#: unreached today, each owned by a ticket that wires or deletes it
TODO = {
    # TODO SCEN: the decided cases are wired into plan by that ticket
    "migkit.scenarios",
}


def _name(path):
    rel = path.relative_to(ROOT.parent).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imported(path, me):
    """The modules `path` imports, absolute, relative and by
    importlib.import_module("migkit.x") alike."""
    pkg = me if path.name == "__init__.py" else me.rpartition(".")[0]
    got = set()
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(node, ast.Import):
            got.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                up = pkg.split(".")
                up = up[:len(up) - (node.level - 1)]
                base = ".".join(up + ([base] if base else []))
            got.add(base)
            got.update(f"{base}.{a.name}" for a in node.names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and node.value.startswith("migkit."):
            got.add(node.value)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and path.name == "__init__.py" and node.value.isidentifier():
            # a package's registry naming its modules, loaded by name
            # (`engines`: kind -> (module, class))
            got.add(f"{pkg}.{node.value}")
    return got


def _graph():
    mods = {_name(p): p for p in ROOT.rglob("*.py")}
    reached = {}
    for me, path in mods.items():
        for other in _imported(path, me):
            while other:
                if other in mods and other != me:
                    reached.setdefault(other, set()).add(me)
                other = other.rpartition(".")[0]
    return mods, reached


def test_every_module_has_an_importer_inside_migkit():
    mods, reached = _graph()
    lonely = sorted(m for m in mods
                    if m not in reached and m not in ENTRY | TODO)
    assert not lonely, ("modules nothing in migkit imports (wire them into"
                        f" their owner, or delete them): {lonely}")


def test_the_allowlists_name_only_modules_that_are_there_and_unreached():
    mods, reached = _graph()
    assert not (ENTRY | TODO) - set(mods), "allowlisted but gone"
    stale = sorted(m for m in TODO if m in reached)
    assert not stale, f"reached now, take them off TODO: {stale}"


def test_the_lint_sees_a_lonely_module(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "a.py").write_text("from . import b\n")
    (tmp_path / "pkg" / "b.py").write_text("")
    (tmp_path / "pkg" / "lonely.py").write_text("")
    a = _imported(tmp_path / "pkg" / "a.py", "pkg.a")
    assert "pkg.b" in a and "pkg.lonely" not in a
