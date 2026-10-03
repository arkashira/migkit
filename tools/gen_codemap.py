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
* `.codemap/SYMBOLS.tsv` (this machine only, written on every commit):
  every class, function and method with `file:line`, its qualified name
  and the first line of what it says about itself - one grep answers
  "where is X" and "is this name taken".

    python tools/gen_codemap.py            write both
    python tools/gen_codemap.py --check    exit 3 where CODEMAP.md is behind
    python tools/gen_codemap.py --symbols  write SYMBOLS.tsv only
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
     " docs show"),
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
        SYMBOLS.parent.mkdir(parents=True, exist_ok=True)
        SYMBOLS.write_text(symbols())
        return 0
    text = codemap()
    if argv[:1] == ["--check"]:
        return 0 if MAP.exists() and MAP.read_text() == text else 3
    MAP.parent.mkdir(parents=True, exist_ok=True)
    MAP.write_text(text)
    SYMBOLS.parent.mkdir(parents=True, exist_ok=True)
    SYMBOLS.write_text(symbols())
    print(f"wrote {MAP.relative_to(ROOT)} and {SYMBOLS.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
