"""Every rung of the decision engine is reached by a test on each branch
it has (backlog AUD-Z.rungs): a rung no test holds and refuses can carry a
need that never fails or one that never holds, and nothing says so.

The way chosen is a declaration over a recorder, not a tracer over the
suite. A tracer (`sys.settrace`, coverage of the need lambdas) proves a
need was evaluated only by running every decide-touching test inside this
one - minutes, docker lanes and a guard whose answer depends on what ran.
A declaration is read from the source in milliseconds and answers alike
everywhere; it is kept honest by what may declare: a test module names a
rung in `PROVES_RUNGS` as `(ladder, rung, branch)` only where it imports
`tests.rung_branches` and opens `watched()`, which records each rung
`decide.prune` was handed as held, refused by one of its own needs, or
lacking a capability before any need was asked - so the module's asserts
stand on needs seen evaluated, and a refusal by capability is not a
refusal by need.

A rung with needs (or changes on the source) is reached only where both
`holds` and `refuses` are declared: one proved only on facts that make
every need hold is unreached on its other branch. A rung with no need has
only `holds`. Rungs are named `(ladder, rung)`: the top-level function or
constant in `migkit/decide.py` that builds them, and the rung's name (an
expression in angle brackets where the name is computed). A `climb` over
rungs its caller passes is counted where those rungs are built; the rungs
other modules build are read the same way, named `(module, ladder,
rung)` - the module's path under `migkit/` dotted - and declared as
`(module, ladder, rung, branch)` (backlog AUD-Z.rungs-outside). A
constructor is `Rung` or a subclass of it `decide.py` keeps; no other
module keeps a `Rung` class of its own, so none escapes the reading.

What no test reaches yet is `OWED`, each with the test module that should
reach it (one that already names the ladder, or `new:` where none does).
The rungs built outside `decide.py` no test reaches yet are
`OWED_OUTSIDE`, owned by backlog row AUD-Z.rungs-outside-owed.
The list only shrinks: an entry reached fails until it is taken out, and a
rung added unreached fails until a test reaches it."""
import ast
import functools
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
MIGKIT = HERE.parent / "migkit"
DECIDE = MIGKIT / "decide.py"
HOLDS, REFUSES = "holds", "refuses"
HELPER = "tests.rung_branches"

#: (ladder, rung) -> the test module that should reach it on every branch
#: it has; owned by backlog row AUD-Z.rungs-owed
OWED = {}


#: (module, ladder, rung) built outside decide.py -> the test module that
#: should reach it on every branch it has; owned by backlog row
#: AUD-Z.rungs-outside-owed. It only shrinks.
OWED_OUTSIDE = {}


def _ladder(stmt):
    if isinstance(stmt, (ast.FunctionDef, ast.ClassDef)):
        return stmt.name
    if isinstance(stmt, ast.Assign):
        return ast.unparse(stmt.targets[0])
    if isinstance(stmt, ast.AnnAssign):
        return ast.unparse(stmt.target)
    return "<module>"


def _some(node):
    """A needs/changes argument that may hold something."""
    return not (isinstance(node, (ast.Tuple, ast.List)) and not node.elts)


def _named(node):
    return getattr(node, "id", getattr(node, "attr", None))


def _decide_makers(tree):
    """`Rung` and the classes `decide.py` builds on it."""
    got, more = {"Rung"}, True
    while more:
        more = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)
                and {_named(b) for b in n.bases} & got} - got
        got |= more
    return frozenset(got)


def _calls(tree):
    """[(ladder, call)] of every call `tree` makes, by the top-level
    function or constant it is made in."""
    return [(_ladder(stmt), node) for stmt in tree.body
            for node in ast.walk(stmt) if isinstance(node, ast.Call)]


def _built_in(calls, makes):
    """{(ladder, rung): the branches it has} of every rung built among
    `calls` ([(ladder, call)]), a rung being a call whose callee
    `makes(func)` says builds one."""
    got = {}
    for ladder, node in calls:
        if not makes(node.func):
            continue
        first = node.args[0] if node.args else next(
            (k.value for k in node.keywords if k.arg == "name"), None)
        name = first.value if isinstance(first, ast.Constant) \
            else f"<{ast.unparse(first)}>"
        asks = len(node.args) > 3 and _some(node.args[3]) or any(
            k.arg in ("needs", "changes") and _some(k.value)
            for k in node.keywords)
        had = got.get((ladder, name), (HOLDS,))
        got[(ladder, name)] = (HOLDS, REFUSES) if asks \
            or REFUSES in had else (HOLDS,)
    return got


def rungs(source=None):
    """{(ladder, rung): the branches it has} of every `Rung(` built."""
    tree = ast.parse(DECIDE.read_text() if source is None else source)
    makers = _decide_makers(tree)
    return _built_in(_calls(tree), lambda func: _named(func) in makers)


def module_of(path, root=MIGKIT):
    """`migkit/engines/pg.py` -> `engines.pg`."""
    return ".".join(path.relative_to(root).with_suffix("").parts)


def _sources(sources=None):
    if sources is not None:
        return sources
    return {module_of(p): p.read_text()
            for p in sorted(MIGKIT.rglob("*.py")) if p != DECIDE}


def _trees(sources=None):
    """{module: its tree}, `decide` among them; the package's read once."""
    if sources is None:
        return _package_trees()
    trees = {m: ast.parse(t) for m, t in sources.items()}
    trees.setdefault("decide", ast.parse(DECIDE.read_text()))
    return trees


@functools.lru_cache(maxsize=None)
def _package_trees():
    """The modules that name a rung class, or a name built on one: a
    module naming none builds none, and is not parsed (the package is a
    third of a million lines)."""
    texts = _sources()
    trees = {"decide": ast.parse(DECIDE.read_text())}
    names = set(_decide_makers(trees["decide"]))
    while True:
        said = re.compile(r"\b(%s)\b" % "|".join(map(re.escape, names)))
        new = {m: ast.parse(t) for m, t in texts.items()
               if m not in trees and said.search(t)}
        if not new:
            return trees
        trees.update(new)
        names |= {n.name if isinstance(n, ast.ClassDef) else n.targets[0].id
                  for tree in new.values() for n in tree.body
                  if isinstance(n, ast.ClassDef) and _bases_say(n, said)
                  or isinstance(n, ast.Assign) and len(n.targets) == 1
                  and isinstance(n.targets[0], ast.Name)
                  and said.search(ast.unparse(n.value))}


def _bases_say(cls, said):
    return any(said.search(ast.unparse(b)) for b in cls.bases)


def _absolute(module, level, name):
    """The module a relative import in `module` names, under migkit."""
    if not level:
        parts = (name or "").split(".")
        return ".".join(parts[1:]) if parts[0] == "migkit" else None
    base = module.split(".")[:-level]
    return ".".join(base + ([name] if name else []))


class _Names:
    """What each module's names stand for: a module of migkit's, or a
    class of one (`(module, class)`), read from its imports and its own
    top-level classes and assignments."""

    def __init__(self, trees):
        self.trees, self.bound, self.calls = trees, {}, {}
        for module, tree in trees.items():
            names = self.bound.setdefault(module, {})
            calls = self.calls.setdefault(module, [])
            for ladder, n in ((_ladder(stmt), n) for stmt in tree.body
                              for n in ast.walk(stmt)):
                if isinstance(n, ast.Call):
                    calls.append((ladder, n))
                elif isinstance(n, ast.ImportFrom):
                    src = _absolute(module, n.level, n.module)
                    for a in n.names if src is not None else ():
                        whole = f"{src}.{a.name}" if src else a.name
                        names[a.asname or a.name] = ("module", whole) \
                            if whole in trees else (src, a.name)
                elif isinstance(n, ast.Import):
                    for a in n.names:
                        sub = _absolute(module, 0, a.name)
                        if a.asname and sub in trees:
                            names[a.asname] = ("module", sub)
            for n in tree.body:
                if isinstance(n, ast.ClassDef):
                    names[n.name] = (module, n.name)

    def means(self, module, node):
        """(module, class) or ("module", name) `node` stands for, or None."""
        if isinstance(node, ast.Name):
            return self.bound.get(module, {}).get(node.id)
        if isinstance(node, ast.Attribute):
            got = self.means(module, node.value)
            if got and got[0] == "module":
                return self.bound.get(got[1], {}).get(node.attr) \
                    or (got[1], node.attr)
        return None


def _rung_classes(names):
    """Every (module, class) built on `decide.Rung`, and every module
    name bound to one (`Rung = decide.ServerRung`)."""
    trees = names.trees
    got = {("decide", n) for n in _decide_makers(trees["decide"])}
    more = True
    while more:
        more = set()
        for module, tree in trees.items():
            for n in tree.body:
                if isinstance(n, ast.ClassDef) and (module, n.name) \
                        not in got and any(names.means(module, b) in got
                                           for b in n.bases):
                    more.add((module, n.name))
                elif isinstance(n, ast.Assign) and len(n.targets) == 1 \
                        and isinstance(n.targets[0], ast.Name) \
                        and (module, n.targets[0].id) not in got \
                        and names.means(module, n.value) in got:
                    more.add((module, n.targets[0].id))
                    names.bound[module][n.targets[0].id] = \
                        (module, n.targets[0].id)
        got |= more
    return got


def outside(sources=None):
    """{(module, ladder, rung): the branches it has} of every rung built
    outside `decide.py` - a call of `decide.Rung`, of a class built on it
    or of a name bound to one, wherever it is kept; `sources`: {module:
    source}, else the package."""
    trees = _trees(sources)
    names = _Names(trees)
    classes = _rung_classes(names)
    got = {}
    for module in trees:
        if module == "decide":
            continue
        for (ladder, rung), branches in _built_in(
                names.calls[module], lambda f, m=module: names.means(m, f)
                in classes).items():
            got[(module, ladder, rung)] = branches
    return got


def own_rung_classes(sources=None):
    """[(module, class)] a module outside `decide.py` keeps under a name
    the engine's rungs have - its own `Rung` - built on decide's or not."""
    trees = _trees(sources)
    taken = _decide_makers(trees["decide"])
    return sorted((module, n.name)
                  for module, tree in trees.items() if module != "decide"
                  for n in tree.body
                  if isinstance(n, ast.ClassDef) and n.name in taken)


def _declaration(tree):
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and any(
                getattr(t, "id", None) == "PROVES_RUNGS"
                for t in stmt.targets):
            return ast.literal_eval(stmt.value)
    return None


def _watches(tree):
    imports = any(isinstance(n, ast.ImportFrom) and n.module == HELPER
                  or isinstance(n, ast.Import)
                  and any(a.name == HELPER for a in n.names)
                  for n in ast.walk(tree))
    opens = any(isinstance(n, ast.Call) and getattr(
        n.func, "id", getattr(n.func, "attr", None)) == "watched"
        for n in ast.walk(tree))
    return imports and opens


def declared(where=HERE):
    """{test module: ([(ladder, rung, branch)], it watches the needs)}."""
    got = {}
    for path in sorted(where.glob("test_*.py")):
        text = path.read_text()
        if "PROVES_RUNGS" not in text:
            continue
        tree = ast.parse(text, str(path))
        entries = _declaration(tree)
        if entries is not None:
            got[f"tests/{path.name}"] = ([tuple(e) for e in entries],
                                         _watches(tree))
    return got


def reached(built, declarations):
    """The rungs every branch of which some watching module declares."""
    seen = {}
    for entries, watches in declarations.values():
        for *key, branch in entries if watches else ():
            seen.setdefault(tuple(key), set()).add(branch)
    return {k for k, branches in built.items()
            if set(branches) <= seen.get(k, set())}


@functools.lru_cache(maxsize=None)
def _built():
    return rungs()


@functools.lru_cache(maxsize=None)
def _outside():
    return outside()


@functools.lru_cache(maxsize=None)
def _declared():
    return declared()


def test_every_rung_is_reached_on_each_branch_or_owed():
    built = _built()
    left = set(built) - reached(built, _declared()) - set(OWED)
    assert not left, ("rungs no test holds and refuses: reach them in a"
                      f" watching test, or owe them: {sorted(left)}")


def unreached_outside(built, declarations, owed):
    """The rungs built outside `decide.py` that no watching module
    reaches on every branch and that are not owed."""
    return set(built) - reached(built, declarations) - set(owed)


def test_every_rung_built_outside_is_reached_or_owed():
    left = unreached_outside(_outside(), _declared(), OWED_OUTSIDE)
    assert not left, ("rungs built outside decide.py no test holds and"
                      " refuses: reach them in a watching test naming"
                      f" (module, ladder, rung, branch): {sorted(left)}")


def test_an_outside_rung_once_reached_leaves_the_owed_list():
    done = reached(_outside(), _declared()) & set(OWED_OUTSIDE)
    assert not done, ("reached now, take them out of OWED_OUTSIDE:"
                      f" {sorted(done)}")


def test_an_outside_owed_rung_is_one_still_built():
    gone = set(OWED_OUTSIDE) - set(_outside())
    assert not gone, f"owed rungs no module builds now: {sorted(gone)}"
    for key, module in OWED_OUTSIDE.items():
        assert module.startswith("new: tests/test_") or (
            HERE.parent / module).is_file(), (key, module)


def test_no_module_keeps_a_rung_class_of_its_own():
    assert not own_rung_classes(), (
        "a rung class kept outside decide.py: build on decide.Rung there"
        f" (decide.ServerRung) - {own_rung_classes()}")


OUTSIDE_SOURCES = {
    "m": "from . import decide\n"
         "L = (decide.Rung('a', 'a', needs=(n,)),)\n",
    "sub.n": "from ..decide import Rung as R\n"
             "class Mine(R):\n    pass\n"
             "Alias = Mine\n"
             "def ladder():\n    return (Alias('b', 'b'),)\n",
    "o": "from .sub import n\n"
         "class Way:\n    pass\n"
         "W = (n.Mine('c', 'c', changes=(c,)), Way('not a rung'))\n",
}


def test_a_new_rung_built_outside_without_a_test_is_refused():
    built = outside(OUTSIDE_SOURCES)
    assert built == {("m", "L", "a"): (HOLDS, REFUSES),
                     ("sub.n", "ladder", "b"): (HOLDS,),
                     ("o", "W", "c"): (HOLDS, REFUSES)}
    # nothing declares them: each is refused until a test or the owed
    # list names it
    assert unreached_outside(built, {}, {}) == set(built)
    half = {"t": ([("m", "L", "a", HOLDS), ("sub.n", "ladder", "b", HOLDS)],
                  True)}
    assert unreached_outside(built, half, {}) == {("m", "L", "a"),
                                                  ("o", "W", "c")}
    whole = {"t": (half["t"][0] + [("m", "L", "a", REFUSES),
                                   ("o", "W", "c", HOLDS),
                                   ("o", "W", "c", REFUSES)], True)}
    assert unreached_outside(built, whole, {}) == set()
    # a declaration with no recorder behind it reaches nothing
    assert unreached_outside(built, {"t": (whole["t"][0], False)},
                             {("o", "W", "c"): "x"}) == {
        ("m", "L", "a"), ("sub.n", "ladder", "b")}


def test_a_rung_class_of_a_modules_own_is_refused():
    assert own_rung_classes({"m": "class Rung:\n    pass\n",
                             "n": "class Way:\n    pass\n"}) == [
        ("m", "Rung")]


def test_an_owed_rung_once_reached_leaves_the_list():
    built = _built()
    done = reached(built, _declared()) & set(OWED)
    assert not done, f"reached now, take them out of OWED: {sorted(done)}"


def test_an_owed_rung_is_one_the_engine_still_builds():
    gone = set(OWED) - set(_built())
    assert not gone, f"owed rungs decide.py no longer builds: {sorted(gone)}"


def test_owed_names_the_module_that_should_reach_each():
    for key, module in OWED.items():
        assert module.startswith("new: tests/test_") or (
            HERE.parent / module).is_file(), (key, module)


def test_a_declaration_names_a_rung_and_a_branch_it_has():
    built = {**_built(), **_outside()}
    for module, (entries, _) in _declared().items():
        for *key, branch in entries:
            assert tuple(key) in built, (module, key)
            assert branch in built[tuple(key)], (module, key, branch)


def test_a_declaring_module_watches_the_needs_it_names():
    blind = [m for m, (_, watches) in _declared().items() if not watches]
    assert not blind, ("PROVES_RUNGS without tests.rung_branches.watched():"
                       f" {blind}")


SOURCE = '''
LADDER = (Rung("asks", "a", needs=(Need("x", bool),)),
          Rung("plain", "p"), Rung("none", "n", needs=()))
def ladder(f):
    return climb("w", (Rung(f.name, "s", changes=(c,)),), f)
'''


def test_a_rung_proved_only_where_it_holds_is_still_unreached():
    built = rungs(SOURCE)
    assert built == {("LADDER", "asks"): (HOLDS, REFUSES),
                     ("LADDER", "plain"): (HOLDS,),
                     ("LADDER", "none"): (HOLDS,),
                     ("ladder", "<f.name>"): (HOLDS, REFUSES)}
    half = {"m": ([("LADDER", "asks", HOLDS), ("LADDER", "plain", HOLDS)],
                  True)}
    assert reached(built, half) == {("LADDER", "plain")}
    whole = {"m": ([("LADDER", "asks", HOLDS), ("LADDER", "asks", REFUSES)],
                   True)}
    assert reached(built, whole) == {("LADDER", "asks")}
    # a declaration with no recorder behind it reaches nothing
    assert reached(built, {"m": (whole["m"][0], False)}) == set()


def test_the_recorder_tells_a_refused_need_from_a_lacking_capability():
    from migkit import decide
    from tests.rung_branches import LACKS, REFUSES as NO, watched
    ladder = (decide.Rung("a", "a", gives=frozenset({"x"}), needs=(
                  decide.Need("y", lambda f: f.get("y")),)),
              decide.Rung("b", "b"))
    with watched() as seen:
        decide.climb("w", ladder, decide.Facts({"y": False}), prove=False)
        decide.climb("w", ladder, decide.Facts({"y": True}), need=("z",),
                     prove=False)
    assert ("a", NO, "a needs y") in seen
    assert [b for n, b, _ in seen if n == "a"][-1] == LACKS
    assert decide.prune.__module__ == "migkit.decide"


def test_proof_lazy_facts_are_recorded_in_choice_asked():
    from migkit import decide

    def prove_check(facts):
        _ = facts["lazy_check_fact"]
        return None

    facts = decide.Facts({"initial_fact": True}, lazy_check_fact=lambda: "valid")
    _ = facts["initial_fact"]
    rung = decide.Rung("proving", "proving way", prove=prove_check)
    choice = decide.climb("test_work", (rung,), facts=facts, prove=True)
    assert choice.rung.name == "proving"
    assert "initial_fact" in choice.asked
    assert "lazy_check_fact" in choice.asked

