"""The troubleshooting guide, read off the code (backlog 42).

Every refusal migkit can stop with, found where it is raised, so the guide
cannot fall behind the code: `python -m migkit.troubleshooting` writes
`docs/troubleshooting.md`, and a test fails when the two differ. Each
refusal already says what to do in its own words; the guide adds where it
comes from, so the text an operator saw can be searched for.

A part of a message filled in at run time is shown as `<...>`. A refusal
whose words are built elsewhere (`capabilities.unavailable`, for one) is
listed under the function that builds them.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TITLE = "# Troubleshooting: every refusal, where it comes from"

#: the part of migkit a refusal comes from, in the words a person looks
#: for it by: a module's path said nothing to an operator, and named a
#: program for some (`redisshake.py`). An engine's own modules are that
#: engine's (`engines.TITLES`); a module named nowhere is "elsewhere".
AREAS = (
    ("Addresses, `migkit init` and the hop's configuration",
     ("addresses", "config", "hopkeys", "guide", "imports")),
    ("The commands", ("cli",)),
    ("Planning: the ways, the machine and the limits",
     ("decide", "jointplan", "machine", "model", "costmodel", "sizing",
      "planner", "tableway", "tuning")),
    ("Moving the data",
     ("movers", "crossmove", "legs", "physical", "cloudseed", "backupseed",
      "redisshake", "mirror", "fromsnapshot", "bulkways", "dsbulk",
      "ddbexport", "loadtext")),
    ("Following changes, and two ways at once",
     ("changetopic", "tailctl", "tailtables", "streamout", "marks",
      "twoway", "logddl", "drainmark")),
    ("Checking and proving",
     ("second_reader", "report", "leaves", "kept", "verifiers", "digests",
      "export", "repairlog", "repairways")),
    ("Values kept at rest", ("evidence",)),
    ("The cutover, and what guards a write",
     ("cutover", "freeze", "interlock", "ready", "lease", "approvals",
      "sourcechange")),
    ("Goals: upgrade, subset and rehearsal",
     ("upgrade", "subset", "rehearsal")),
    ("Stored code carried to another engine", ("storedcode",)),
    ("Users and grants", ("users", "dba")),
    ("Schedules, the run's state and tunnels",
     ("schedule", "state", "tunnel", "session")),
    ("Help from a model", ("assist",)),
)
#: what the engine modules shared by every engine are
EVERY_ENGINE = "Every engine"
ELSEWHERE = "Elsewhere in migkit"


def _engine_modules():
    """{an engine module's name: [the engines it is]}, read off the
    engine classes, so a new engine's module is placed without a word
    here."""
    from .engines import NAMES, _class_for
    out = {}
    for name in NAMES:
        cls = _class_for(name)
        if cls is not None:
            out.setdefault(cls.__module__.rsplit(".", 1)[-1], []).append(name)
    # the wire two engines share, and the passes of the two SQL engines
    out.setdefault("pgwire", ["cockroachdb", "dsql"])
    out.setdefault("pg_pass", ["postgres"])
    out.setdefault("my_pass", ["mysql"])
    return out


def area(module):
    """The heading a refusal raised in `module` (a path under the
    package, `migkit/engines/redis.py`) is listed under."""
    from .engines import TITLES
    parts = Path(module).with_suffix("").parts[1:]
    if not parts:
        return ELSEWHERE
    if parts[0] == "engines" and len(parts) > 1:
        stem = parts[1]
        mods = _engine_modules()
        names = mods.get(stem) or next(
            (mods[m] for m in sorted(mods, key=len, reverse=True)
             if stem.startswith(m + "_") or stem.startswith(m)), None)
        if not names:
            return EVERY_ENGINE
        said = [TITLES.get(n, n) for n in names]
        return said[0] if len(said) == 1 else \
            ", ".join(said[:-1]) + " and " + said[-1]
    for words, stems in AREAS:
        if parts[0] in stems:
            return words
    return ELSEWHERE


def by_area():
    """[(area, [message])] in the guide's order: migkit's parts as
    `AREAS` lists them, then each engine as `engines.NAMES` does, then
    what every engine shares, then the rest."""
    from .engines import NAMES, TITLES
    got = {}
    for module, _fn, text in refusals():
        said = got.setdefault(area(module), [])
        if text not in said:
            said.append(text)
    order = [w for w, _ in AREAS]
    engines = [TITLES.get(n, n) for n in NAMES]
    rank = {w: i for i, w in enumerate(order)}

    def key(heading):
        if heading in rank:
            return (0, rank[heading], "")
        if heading == EVERY_ENGINE:
            return (2, 0, "")
        if heading == ELSEWHERE:
            return (3, 0, "")
        first = next((i for i, t in enumerate(engines)
                      if heading.startswith(t)), len(engines))
        return (1, first, heading)
    return [(h, got[h]) for h in sorted(got, key=key)]


def _text(node):
    """The message as a template, or None where it is not written here."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        out = []
        for part in node.values:
            if isinstance(part, ast.Constant):
                out.append(str(part.value))
            else:
                out.append("<...>")
        return "".join(out)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _text(node.left), _text(node.right)
        if left is None and right is None:
            return None
        return (left if left is not None else "<...>") + \
            (right if right is not None else "<...>")
    if isinstance(node, ast.IfExp):
        a, b = _text(node.body), _text(node.orelse)
        if a is None and b is None:
            return None
        return (a or "<...>") if a == b else \
            f"{a or '<...>'} | {b or '<...>'}"
    return None


#: what a refusal is raised as besides `SystemExit`: the two of it that
#: say which end the command has (`verdict.Refused`, exit 4;
#: `verdict.Usage`, exit 2) - by that module's name only, another
#: module's `Refused` being its own
RAISED_AS = ("Refused", "Usage")


def _raised_as(tree):
    """The names a module raises a refusal by: `SystemExit`, and the
    verdict's own as it imports them."""
    names = {"SystemExit"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(
                ".")[-1] == "verdict":
            names |= {a.asname or a.name for a in node.names
                      if a.name in RAISED_AS}
    return names


def _raises_a_refusal(exc, names):
    f = exc.func
    if isinstance(f, ast.Name):
        return f.id in names
    return (isinstance(f, ast.Attribute) and f.attr in RAISED_AS
            and getattr(f.value, "id", "") in ("verdict", "_verdict"))


def refusals():
    """[(module, function, template)] for every `raise SystemExit(...)`
    whose words are written where it is raised, in file order."""
    out = []
    for path in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        module = str(path.relative_to(ROOT.parent))
        names = _raised_as(tree)
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if not (isinstance(node, ast.Raise)
                        and isinstance(node.exc, ast.Call)
                        and _raises_a_refusal(node.exc, names)
                        and node.exc.args):
                    continue
                text = _text(node.exc.args[0])
                if text is None or not text.strip("<.> "):
                    continue
                out.append((module, fn.name, " ".join(text.split())))
    seen, unique = set(), []
    for item in out:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def render(groups=None):
    """The guide; `groups` is `by_area()` where the caller has it already
    (the site's page is written from the same reading)."""
    lines = [TITLE, "",
             "Generated by `python -m migkit.troubleshooting` from the"
             " code; do not edit.",
             "Each entry is the message as migkit prints it, with the parts"
             " filled in at run time shown as `<...>`, under the part of"
             " migkit that raises it. The message says what to do.", ""]
    for heading, texts in (by_area() if groups is None else groups):
        lines += [f"## {heading}", ""]
        # neither the function's name nor the module's is given: some are
        # named for the program they drive, and a program's name is not
        # for the reader
        for text in texts:
            lines += [f"* {text}", ""]
    return "\n".join(lines).rstrip() + "\n"


def main():
    target = ROOT.parent / "docs" / "troubleshooting.md"
    target.write_text(render())
    print(f"{target}: {len(refusals())} refusals")


if __name__ == "__main__":
    main()
