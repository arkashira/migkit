#!/usr/bin/env python3
"""Where everything in migkit is, written from the code.

An agent (or a person) new to a session does not remember a codebase of
two hundred modules; it searched again every time, and twice it wrote
over or beside something that already existed - a module of the same
name, a method of the same name in the same class. This writes the map
that is read first instead:

* `docs/agents/CODEMAP.md` (kept in the repo, checked by the pre-commit
  hook as the manual is): every module in one line - what it is for, its
  main classes - by package, and the registries every change touches;
* `docs/agents/STATE.md` (kept in the repo and checked as the map is):
  what migkit can do engine by engine and what is left, from the
  capability table, the decision engine's coverage, the depth matrix and
  the backlog;
* `.codemap/CALLERS.tsv` and `.codemap/LOCAL.md` (this machine only):
  who calls what, and what this machine last proved - the full suite,
  work not pushed, agents' branches kept;
* `.codemap/SYMBOLS.tsv` (this machine only, written on every commit):
  every class, function and method with `file:line`, its qualified name
  and the first line of what it says about itself - one grep answers
  "where is X" and "is this name taken".

    python tools/gen_codemap.py            write both
    python tools/gen_codemap.py --check    exit 3 where CODEMAP.md is behind
    python tools/gen_codemap.py --symbols  write .codemap/ only (SYMBOLS,
                                           CALLERS, LOCAL)
    python tools/gen_codemap.py --since <ref>
                                           the modules, classes and
                                           functions added, removed or
                                           changed since a commit
"""
import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PKG = ROOT / "migkit"
MAP = ROOT / "docs" / "agents" / "CODEMAP.md"
SYMBOLS = ROOT / ".codemap" / "SYMBOLS.tsv"

#: the places a change usually has to touch, and what each holds
HUBS = (
    ("migkit/hopkeys.py", "HOP_KEYS, HOP_OPTIONS: every key a hop may say,"
     " and the hop file's validation (`check`)"),
    ("migkit/envkeys.py", "every MIGKIT_* environment variable"),
    ("migkit/config.py", "Hop: the scope (`excluded`, `included`,"
     " `takes_object`, `carries`, `row_filter`, `target_table_in`,"
     " `scope_fingerprint`), `build_hop`, `get_hop`"),
    ("migkit/engines/base.py", "Engine: the contract every engine fills"
     " (`in_scope`, `takes`, checks, repairs, the pre-flight items)"),
    ("migkit/engines/__init__.py", "the engine names and aliases,"
     " `engine_named`, `get_engine`"),
    ("migkit/decide.py", "the decision engine: rungs, `climb`, `attempt`,"
     " `coverage()` - what each engine can do, cell by cell"),
    ("migkit/capabilities.py", "`matrix()`: the capability table the"
     " docs show; `depth()`: every engine against every depth item, read"
     " off the code and proved by the tests' `PROVES`"),
    ("migkit/verdict.py", "`decide`: the one verdict rule; VERDICTS and"
     " exit codes; `hop_scope`; the envelope (`summarize`)"),
    ("migkit/cli.py", "every command and its options"),
    ("migkit/movers.py", "the bulk ways and the stream ways (`stream_ways`)"),
    ("migkit/tailtables.py", "a running tail's per-table states and the"
     " DDL policy"),
    ("migkit/imports.py", "another tool's task read as a hop (DMS, DTS,"
     " DRS, Google, Azure, connectors, LOAD files)"),
    ("tools/gen_docs.py", "the manual and the site, written from the code"),
    ("tools/final_suite.py", "the full suite, run once at the end"),
)


def _modules():
    return sorted(p for p in PKG.rglob("*.py") if "__pycache__" not in p.parts)


def _first_line(doc):
    if not doc:
        return ""
    line = doc.strip().split("\n\n")[0].replace("\n", " ").strip()
    return " ".join(line.split())[:140]


def _walk(tree, rel):
    """(qualified name, kind, line, first doc line) of every class,
    function and method of one module."""
    out = []

    def visit(node, prefix):
        for child in getattr(node, "body", []):
            if isinstance(child, ast.ClassDef):
                q = f"{prefix}{child.name}"
                out.append((q, "class", child.lineno,
                            _first_line(ast.get_docstring(child))))
                visit(child, q + ".")
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                q = f"{prefix}{child.name}"
                kind = "method" if prefix and prefix[0].isupper() else \
                    "function"
                out.append((q, kind, child.lineno,
                            _first_line(ast.get_docstring(child))))
    visit(tree, "")
    return out


def _parse(path):
    try:
        return ast.parse(path.read_text())
    except SyntaxError:
        return None


def codemap():
    lines = ["# migkit code map", "",
             "Written by `tools/gen_codemap.py` from the code; the pre-commit"
             " hook stops a commit that leaves it behind. Read it before"
             " looking for anything; every symbol with its `file:line` is in"
             " `.codemap/SYMBOLS.tsv` (this machine only).", "",
             "## Hubs: what most changes touch", ""]
    lines += [f"- `{f}` - {what}" for f, what in HUBS]
    by_pkg = {}
    for p in _modules():
        rel = p.relative_to(ROOT).as_posix()
        by_pkg.setdefault(p.parent.relative_to(ROOT).as_posix(), []).append(
            (rel, p))
    for pkg, mods in sorted(by_pkg.items()):
        lines += ["", f"## `{pkg}/` ({len(mods)} modules)", ""]
        for rel, p in mods:
            tree = _parse(p)
            doc = _first_line(ast.get_docstring(tree)) if tree else \
                "(does not parse)"
            classes = [n.name for n in (tree.body if tree else [])
                       if isinstance(n, ast.ClassDef)
                       and not n.name.startswith("_")][:6]
            extra = f" Classes: {', '.join(classes)}." if classes else ""
            lines.append(f"- `{rel}` - {doc or '(no docstring)'}{extra}")
    return "\n".join(lines) + "\n"


def symbols():
    rows = ["name\tkind\twhere\tqualified\tsays"]
    for p in _modules():
        tree = _parse(p)
        if tree is None:
            continue
        rel = p.relative_to(ROOT).as_posix()
        for q, kind, line, doc in _walk(tree, rel):
            rows.append(f"{q.rsplit('.', 1)[-1]}\t{kind}\t{rel}:{line}\t{q}"
                        f"\t{doc.replace(chr(9), ' ')}")
    return "\n".join(rows) + "\n"


STATE = ROOT / "docs" / "agents" / "STATE.md"
CALLERS = ROOT / ".codemap" / "CALLERS.tsv"
LOCAL = ROOT / ".codemap" / "LOCAL.md"
INDEX = ROOT / "docs" / "backlog-index.md"
STATUSES = ("DONE", "PARTLY", "IN PROGRESS", "OPEN", "BLOCKED",
            "NOT PURSUED")


def _backlog():
    """[(id, status, area)] of the backlog index's rows."""
    rows = []
    for line in INDEX.read_text().splitlines() if INDEX.exists() else []:
        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 8 or not cells[1].isdigit():
            continue
        status = next((s for s in STATUSES if cells[4].startswith(s)), "?")
        rows.append((cells[2], status, cells[6]))
    return rows


def state():
    """What migkit can do and what is left, from the code and the
    backlog: the capability table and the decision engine's coverage for
    every engine, every cell not yet done named, the depth matrix, the
    backlog by status and area. The same on every machine; a session
    reads it to know the tool's strength and its gaps without asking the
    code again."""
    sys.path.insert(0, str(ROOT))
    from migkit import capabilities, decide
    caps = capabilities.matrix()
    cov = decide.coverage()
    names = list(capabilities.CAPABILITIES)
    shapes = sorted({k for cells in cov.values() for k in cells})
    lines = ["# migkit state", "",
             "Written by `tools/gen_codemap.py` from `capabilities.matrix()`,"
             " `decide.coverage()`, `capabilities.depth()` and"
             " `docs/backlog-index.md`; the"
             " pre-commit hook keeps it current. What this machine last"
             " proved (the full suite, unpushed work) is in"
             " `.codemap/LOCAL.md`.", ""]

    def gaps(cells):
        return [k for k, v in cells.items()
                if str(v).lower() not in ("yes", "not applicable", "n/a",
                                          "instead") and not str(
                    v).lower().startswith(("yes", "not applicable",
                                           "instead"))]
    lines += ["## Capabilities by engine", "",
              f"{len(names)} capabilities (`capabilities.CAPABILITIES`) and"
              f" {len(shapes)} shapes (`decide.coverage()`); a row lists"
              " what is not yet there.", "",
              "| engine | capabilities | shapes | not yet |",
              "|---|---|---|---|"]
    for eng in sorted(set(caps) | set(cov)):
        cg = gaps(caps.get(eng, {})) if eng in caps else ["(no row)"]
        sg = gaps(cov.get(eng, {})) if eng in cov else ["(no row)"]
        lines.append(f"| {eng} | {len(names) - len(cg)}/{len(names)} |"
                     f" {len(shapes) - len(sg)}/{len(shapes)} |"
                     f" {', '.join(cg + sg) or '-'} |")
    lines += _depth(capabilities)
    rows = _backlog()
    by = {s: 0 for s in STATUSES}
    areas = {}
    for _, st, area in rows:
        by[st] = by.get(st, 0) + 1
        areas.setdefault(area, {}).setdefault(st, 0)
        areas[area][st] += 1
    lines += ["", f"## Backlog ({len(rows)} items, `docs/backlog-index.md`)",
              "", " - ".join(f"{s} {n}" for s, n in by.items() if n), "",
              "| area | " + " | ".join(STATUSES) + " |",
              "|---|" + "---|" * len(STATUSES)]
    for area, c in sorted(areas.items()):
        lines.append(f"| {area} | " + " | ".join(str(c.get(s, 0))
                                                for s in STATUSES) + " |")
    left = [i for i, st, _ in rows if st in ("OPEN", "IN PROGRESS",
                                             "PARTLY", "BLOCKED")]
    lines += ["", f"Not done ({len(left)}): " + ", ".join(left), ""]
    return "\n".join(lines) + "\n"


def _depth(capabilities):
    """The depth matrix (`capabilities.depth`, backlog P0-PARITY): per
    engine, each area's ways a test proves of the items that apply to it;
    per item, the engines in each state - the largest group said as the
    rest, so the table stays short."""
    caps = capabilities
    got = caps.depth()
    owed = {(i, n) for i, engines in caps.UNPROVED.items()
            for n in engines.split()}

    def state(n, i):
        s = got[n][i]
        if s == caps.NOT_YET and (i, n) in owed:
            return "test owed"
        return {"yes": "yes", caps.NOT_APPLICABLE: "n/a",
                caps.NOT_YET: "not yet"}.get(s, s)
    areas = list(caps.AREAS)
    by_area = {a: [i for i in caps.DEPTH if i.split(".", 1)[0] == a]
               for a in areas}
    total = {}
    for n in got:
        for i in caps.DEPTH:
            total[state(n, i)] = total.get(state(n, i), 0) + 1
    out = ["", "## Depth by engine", "",
           f"{len(caps.DEPTH)} items in {len(areas)} areas"
           " (`capabilities.depth()`, backlog P0-PARITY), each read off the"
           " code an engine runs; a way counts once a test proves it on"
           " that engine (`PROVES` in the test module). Cells: "
           + ", ".join(f"{k} {v}" for k, v in sorted(total.items()))
           + ". A cell below is the ways proved of the items that apply.",
           "", "| engine | " + " | ".join(areas) + " | all |",
           "|---|" + "---|" * (len(areas) + 1)]
    for n in sorted(got):
        yes = {a: sum(1 for i in by_area[a] if state(n, i) == "yes")
               for a in areas}
        apply = {a: sum(1 for i in by_area[a] if state(n, i) != "n/a")
                 for a in areas}
        out.append(f"| {n} | " + " | ".join(
            f"{yes[a]}/{apply[a]}" if apply[a] else "-" for a in areas)
            + f" | {sum(yes.values())}/{sum(apply.values())} |")
    out += ["", "Per item: the engines in each state - \"test owed\" is a way"
            " in the code whose test on that engine is not written"
            " (`UNPROVED`).", "",
            "| item | yes | test owed | not yet | n/a |",
            "|---|---|---|---|---|"]
    kinds = ("yes", "test owed", "not yet", "n/a")
    for i in caps.DEPTH:
        groups = {k: sorted(n for n in got if state(n, i) == k)
                  for k in kinds}
        biggest = max(kinds, key=lambda k: len(groups[k]))
        cells = []
        for k in kinds:
            if not groups[k]:
                cells.append("-")
            elif k == biggest and len(groups[k]) > 6:
                cells.append(f"the rest ({len(groups[k])})")
            else:
                cells.append(" ".join(groups[k]))
        out.append(f"| {i} | " + " | ".join(cells) + " |")
    return out


def callers():
    """Who calls what: every call by name, with the function it is made
    in and its file:line. By name, not by type - a method called on an
    object is listed under its attribute name - which is what a search
    for the places a change reaches needs."""
    rows = ["callee\tcaller\twhere"]
    for p in _modules():
        tree = _parse(p)
        if tree is None:
            continue
        rel = p.relative_to(ROOT).as_posix()

        def visit(node, owner):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                    visit(child, f"{owner}.{child.name}" if owner
                          else child.name)
                    continue
                if isinstance(child, ast.Call):
                    f = child.func
                    name = f.id if isinstance(f, ast.Name) else \
                        f.attr if isinstance(f, ast.Attribute) else None
                    if name:
                        rows.append(f"{name}\t{owner or '<module>'}\t"
                                    f"{rel}:{child.lineno}")
                visit(child, owner)
        visit(tree, "")
    return "\n".join(rows) + "\n"


def local():
    """What this machine knows that the repo does not: the last full
    suite (`tools/final_suite.py`), work not pushed, agents' branches
    kept."""
    import json
    lines = ["# migkit on this machine", ""]
    runs = sorted((ROOT / "reports" / "final-suite").glob("*/report.json"))
    if runs:
        rec = json.loads(runs[-1].read_text())
        counts = {}
        for t in rec.get("tests", []):
            counts[t.get("outcome")] = counts.get(t.get("outcome"), 0) + 1
        lines.append(f"- Last full suite: {runs[-1].parent.name} at commit"
                     f" {rec.get('commit')}: {counts}")
    else:
        lines.append("- Last full suite: none recorded on this machine")

    def git(*argv):
        return subprocess.run(["git", *argv], capture_output=True, text=True,
                              cwd=ROOT).stdout.strip()
    ahead = git("rev-list", "--count", "origin/master..HEAD") or "?"
    lines.append(f"- HEAD {git('rev-parse', '--short', 'HEAD')}: {ahead}"
                 " commits not pushed")
    kept = [b for b in git("branch", "--format=%(refname:short)").split()
            if b.startswith("worktree-agent-")]
    lines.append(f"- Agents' branches kept: {len(kept)}"
                 + (f" ({', '.join(kept)})" if kept else ""))
    lines.append("- Open review findings and the resume order:"
                 " `docs/research/WAVE-STATE-2026-09-28.md` (latest section)")
    return "\n".join(lines) + "\n"


def _write_local():
    SYMBOLS.parent.mkdir(parents=True, exist_ok=True)
    SYMBOLS.write_text(symbols())
    CALLERS.write_text(callers())
    LOCAL.write_text(local())


def since(ref):
    """What was added, removed or changed since `ref`, module by module,
    symbol by symbol (a symbol whose body changed counts as changed)."""
    files = subprocess.run(["git", "diff", "--name-status", f"{ref}...HEAD",
                            "--", "migkit"], capture_output=True, text=True,
                           cwd=ROOT).stdout.split("\n")
    out = []
    for line in filter(None, files):
        status, _, path = line.partition("\t")
        if not path.endswith(".py"):
            continue
        old = subprocess.run(["git", "show", f"{ref}:{path}"],
                             capture_output=True, text=True, cwd=ROOT).stdout
        new_p = ROOT / path
        new = new_p.read_text() if new_p.exists() else ""

        def defs(text):
            try:
                tree = ast.parse(text)
            except SyntaxError:
                return {}
            got = {}
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                     ast.ClassDef)):
                    got[node.name + f"@{node.lineno}"] = ast.dump(node)
            return {k.split("@")[0]: v for k, v in got.items()}
        a, b = defs(old), defs(new)
        added = sorted(set(b) - set(a))
        removed = sorted(set(a) - set(b))
        changed = sorted(k for k in set(a) & set(b) if a[k] != b[k])
        out.append(f"{status} {path}: +{len(added)} -{len(removed)}"
                   f" ~{len(changed)}"
                   + (f"\n  added: {', '.join(added[:20])}" if added else "")
                   + (f"\n  removed: {', '.join(removed[:20])}"
                      if removed else ""))
    return "\n".join(out)


def main(argv):
    if argv[:1] == ["--since"] and len(argv) > 1:
        print(since(argv[1]))
        return 0
    if argv[:1] == ["--symbols"]:
        _write_local()
        return 0
    text, st = codemap(), state()
    if argv[:1] == ["--check"]:
        current = MAP.exists() and MAP.read_text() == text and \
            STATE.exists() and STATE.read_text() == st
        return 0 if current else 3
    MAP.parent.mkdir(parents=True, exist_ok=True)
    MAP.write_text(text)
    STATE.write_text(st)
    _write_local()
    print(f"wrote {MAP.relative_to(ROOT)}, {STATE.relative_to(ROOT)} and"
          " .codemap/ (SYMBOLS.tsv, CALLERS.tsv, LOCAL.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
