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
* `docs/agents/INVENTORY.md` and `docs/agents/LAYERS.md` (kept in the
  repo, checked as the map is; backlog P1-SOURCE-HIERARCHY): every file
  under `migkit/` and `tools/` with its responsibility package, owner
  domain, importers, wrapped programs and tests; and the layering rule
  `tests/test_the_layers_import_downward.py` holds every import to, with
  the upward imports still allowed;
* `.codemap/CALLERS.tsv` and `.codemap/LOCAL.md` (this machine only):
  who calls what, and what this machine last proved - the full suite,
  work not pushed, agents' branches kept;
* `.codemap/SYMBOLS.tsv` (this machine only, written on every commit):
  every class, function and method with `file:line`, its qualified name
  and the first line of what it says about itself - one grep answers
  "where is X" and "is this name taken".

    python tools/gen_codemap.py            write all four docs and .codemap/
    python tools/gen_codemap.py --check    exit 3 where a doc is behind
    python tools/gen_codemap.py --symbols  write .codemap/ only (SYMBOLS,
                                           CALLERS, LOCAL)
    python tools/gen_codemap.py --since <ref>
                                           the modules, classes and
                                           functions added, removed or
                                           changed since a commit
"""
import ast
import json
import re
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
    ("tools/whole_use.py", "every wrapped program and library used, unused or topped up, written from the tool-use registry"),
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


_PARSED = {}


def _parse(path):
    """The tree of a file, parsed once per process while the file is as it
    was (the inventory reads every file of the tree several times)."""
    key = (str(path), path.stat().st_mtime_ns)
    if key not in _PARSED:
        try:
            _PARSED[key] = ast.parse(path.read_text())
        except SyntaxError:
            _PARSED[key] = None
    return _PARSED[key]


def codemap():
    lines = ["# migkit code map", "",
             "Written by `tools/gen_codemap.py` from the code; the pre-commit"
             " hook stops a commit that leaves it behind. Read it before"
             " looking for anything; every symbol with its `file:line` is in"
             " `.codemap/SYMBOLS.tsv` (this machine only).", "",
             "## Hubs: what most changes touch", ""]
    lines += [f"- `{f}` - {what}" for f, what in HUBS]
    lines += ["- [`docs/agents/INVENTORY.md`](INVENTORY.md) - every file of"
              " `migkit/` and `tools/`: its responsibility package, owner"
              " domain, size, importers, the packages it reaches, the"
              " programs it wraps and the tests that reach it",
              "- [`docs/agents/LAYERS.md`](LAYERS.md) - the layering rule"
              " (a package imports its own rank and below) and every upward"
              " import still listed, with its reason"]
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


def _pair_gaps(lacking):
    """The line that says what the pairs still lack: a pair lacks what its
    two engines lack (`autonomous.gaps`), so the count of lacks by kind is
    summed per pair from `decide.still_lacking()`, and the line names the
    pair with the fewest and the most."""
    kinds = ("shape", "capability", "depth")
    per = {e: {k: sum(1 for t, *_ in rows if t == k) for k in kinds}
           for e, rows in lacking.items()}
    pairs = sorted(((sum(per[a].values()) + sum(per[b].values()), a, b)
                    for a in per for b in per if a != b))
    if not pairs:
        return "Gaps per pair (`autonomous.gaps`): none."
    low, high = pairs[0], pairs[-1]
    total = {k: sum(v[k] for v in per.values()) for k in kinds}
    return (f"Gaps per pair (`autonomous.gaps`: a pair lacks what its two"
            f" engines lack): {len(pairs)} ordered pairs of {len(per)}"
            " engines; "
            + ", ".join(f"{total[k]} {k} lacks" for k in kinds)
            + f" in all; fewest {low[1]} -> {low[2]} ({low[0]}), most"
              f" {high[1]} -> {high[2]} ({high[0]}).")


def _registry_pairs():
    """The pairs the engine registry carries a way for, read off the code:
    every engine of `engines.NAMES` to itself, and every pair one of the
    cross-engine ways joins (`crossmove._joined`)."""
    from migkit import crossmove
    from migkit.engines import NAMES
    return sorted({(n, n) for n in NAMES} | set(crossmove._joined()))


def _pair_inventory(lacking):
    """The section that says, per pair the registry carries, what it lacks
    by kind (`autonomous.gaps`: a pair lacks what its two engines lack, an
    engine to itself once), read off `decide.still_lacking()`."""
    kinds = ("shape", "capability", "depth")
    pairs = _registry_pairs()

    def count(eng, kind):
        return sum(1 for t, *_ in lacking.get(eng, ()) if t == kind)
    rows = []
    for src, dst in pairs:
        per = [count(src, k) + (count(dst, k) if dst != src else 0)
               for k in kinds]
        rows.append(f"| {src} -> {dst} | " + " | ".join(map(str, per))
                    + f" | {sum(per)} |")
    total = sum(int(r.rsplit("|", 2)[1]) for r in rows)
    return ["", "## Gaps by pair", "",
            f"{len(pairs)} pairs the registry carries (every engine of"
            " `engines.NAMES` to itself, and every pair a cross-engine way"
            " joins, `crossmove._joined`), each with what"
            " `autonomous.gaps(src, dst)` says it lacks by kind; "
            f"{total} lacks in all (backlog P0-DECISION-ALL.gaps).", "",
            "| pair | shape | capability | depth | in all |",
            "|---|---|---|---|---|"] + rows


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
             " `decide.coverage()`, `decide.still_lacking()`,"
             " `capabilities.depth()` and"
             " `docs/backlog-index.md`; the"
             " pre-commit hook keeps it current. What this machine last"
             " proved (the full suite, unpushed work) is in"
             " `.codemap/LOCAL.md`.", ""]

    lacking = decide.still_lacking()

    def gaps(eng, table):
        return [name for t, name, *_ in lacking.get(eng, ()) if t == table]
    lines += ["## Capabilities by engine", "",
              f"{len(names)} capabilities (`capabilities.CAPABILITIES`) and"
              f" {len(shapes)} shapes (`decide.coverage()`); a row lists"
              " what is not yet there and counts the depth items owed, as"
              " each engine says it still lacks them"
              " (`decide.still_lacking()`).", "",
              "| engine | capabilities | shapes | depth owed | not yet |",
              "|---|---|---|---|---|"]
    for eng in sorted(set(caps) | set(cov)):
        cg = gaps(eng, "capability") if eng in caps else ["(no row)"]
        sg = gaps(eng, "shape") if eng in cov else ["(no row)"]
        lines.append(f"| {eng} | {len(names) - len(cg)}/{len(names)} |"
                     f" {len(shapes) - len(sg)}/{len(shapes)} |"
                     f" {len(gaps(eng, 'depth'))} |"
                     f" {', '.join(cg + sg) or '-'} |")
    lines += _depth(capabilities)
    up = upward_imports()
    lines += ["", "## Source layout", "",
              f"{len(up)} upward imports still listed"
              " (`docs/agents/LAYERS.md`; the list only shrinks);"
              f" {sum(1 for f in _files() if package_of(f) is None)} files"
              " no package claims (`docs/agents/INVENTORY.md`)."]
    lines += ["", _pair_gaps(lacking)]
    lines += _pair_inventory(lacking)
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
            return "built, unproved" if n in caps.UNPROVED_WHY \
                else "test owed"
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
    out += ["", "Built, unproved - a way the code has on an engine no sandbox"
            " here can run; the engine's module says which server it was"
            " not run on (`UNPROVED_ON`):", ""]
    for n in sorted(caps.UNPROVED_WHY):
        cells = sum(1 for i in caps.DEPTH if state(n, i) == "built, unproved")
        out.append(f"- {n}: {caps.UNPROVED_WHY[n]} ({cells} cells)")
    # what serves no engine - a provider, a brand - declared beside them
    for n, (why, cells) in sorted(caps.UNPROVED_ELSEWHERE.items()):
        out.append(f"- {n}: {why} ({', '.join(cells) or 'no cells'})")
    out += ["", "Per item: the engines in each state - \"test owed\" is a way"
            " in the code whose test on that engine is not written"
            " (`UNPROVED`); \"built, unproved\" is one whose engine module"
            " declares it was not run on a real server.", "",
            "| item | yes | test owed | built, unproved | not yet | n/a |",
            "|---|---|---|---|---|---|"]
    kinds = ("yes", "test owed", "built, unproved", "not yet", "n/a")
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


# ---- the source inventory and the layers (backlog P1-SOURCE-HIERARCHY) -------

INVENTORY = ROOT / "docs" / "agents" / "INVENTORY.md"
LAYERS_DOC = ROOT / "docs" / "agents" / "LAYERS.md"
TESTS = ROOT / "tests"

#: the layers, lowest first: a package may import its own rank and any
#: rank below it, and nothing above. A rank of two is two packages
#: beside each other. `P1-SOURCE-HIERARCHY.layout`; the plan is
#: `docs/research/source-hierarchy-2026-10-06.md`
LAYERS = (
    (("configuration", "what a hop says: its keys, the environment, the"
      " endpoints and the addresses it is built from"),),
    (("scope", "which objects and rows a hop carries"),),
    (("persistence", "state that outlives a process: checkpoints, the"
      " audit record, the run's own files, the undo"),),
    (("safety", "who may write, what must not leak, what keeps a mover"
      " from writing twice"),),
    (("execution", "the running of work: processes, ranges, retries,"
      " pacing, stalls, sizing of what runs at once"),),
    (("conversion", "a value or an object as another engine takes it:"
      " canonical text, transforms, types, stored code"),),
    (("reporting", "what the operator reads: the verdict, the result"
      " shape, the log, the metrics, the notices"),),
    (("decision", "choosing: the rungs, the cost model, the joint plan,"
      " what each engine can do"),),
    (("engines", "the contract every engine fills and each engine's own"
      " adapter"),
     ("integrations", "the programs, libraries and provider APIs migkit"
      " wraps, and the other tools it meets")),
    (("workflows", "a move's phases and what they do around the move:"
      " copy, follow, prove, repair, cut over, recover"),),
    (("cli", "the command line and the guide that asks for two"
      " addresses"),
     ("web", "the web view and its API")),
)
#: developer tooling and shipped resources: listed in the inventory,
#: outside the layering rule
OUTSIDE = (("tools", "the generators, the full suite and the checks run"
            " by the maintainers; they import migkit and migkit never"
            " imports them"),
           ("data", "the files migkit ships as resources"))

#: top-level modules of migkit/ by package; a module is in one package
_BY_NAME = {
    "configuration": "hopkeys envkeys config addresses engineat clouds"
                     " variants managed",
    "scope": "rowfilter case result_scope",
    "persistence": "state checkpoint durable repairlog revert audit digests"
                   " evidence session runlog kept anchor setaside claims"
                   " shared",
    "safety": "redact egress tls approvals interlock fencing ownership"
              " masking freeze leftovers lease",
    "execution": "runner schedule throttle stall attempts ranges util"
                 " failpoint tunnel locks",
    "conversion": "canon rowtext render transforms nanotime intervals geo"
                  " vectors bsontype charsets commercial zones copybook"
                  " pieces ddl ddl_shield merkle schema schema_drift loadtext schemaformats described tally",
    "reporting": "verdict report wording diagnostics export"
                 " troubleshooting telemetry traces notify servicecheck query_guard",
    "decision": "decide costmodel jointplan factbook guarantees scanplan"
                " tuning tune capabilities planner tableway repairways userways"
                " scenarios legs sizing changedepth lacks sourcechange"
                " bulkways ready cloud_readiness dba handwork rehearsal"
                " faults model"
                " machine topology waysaid workload advisors",
    "engines": "catalog pgslot binlog_marks binlog_payload logddl standby"
               " tailwhere",
    "integrations": "tools tooluse toolversion cloudapi cloudseed"
                    " huawei_sdk imports integrations workflow_exporter"
                    " assist mirror redisshake dsbulk ddbexport"
                    " fromsnapshot kafkaclient registry avrostream"
                    " seedtools carriers ship backupseed blockseed",
    "workflows": "movers cutover flight_simulator tailtables topicstates tailctl verifiers"
                 " autonomous upgrade indexes appversions drift crossmove"
                 " physical ha jobs users split consolidate reshard subset"
                 " archive editions tableengine twoway barriers marks"
                 " drainmark applyway replicasum rules leaves unchanged"
                 " streamout changetopic fleet second_reader proxy ring_buffer"
                 " dag_scheduler reverse autopilot",
    "cli": "cli guide ui estate",
}
#: a subpackage, whole
_BY_PREFIX = {"engines": "engines", "storedcode": "conversion",
              "web": "web", "runners": "execution"}

#: the owners CLAUDE.md names ("One place") and the decision engine's
#: rules: the file that holds the one reading, for the inventory
OWNERS = {
    "migkit/config.py": "scope: Hop.excluded, Hop.carries,"
                        " Hop.target_table_in",
    "migkit/engines/base.py": "scope: Engine.in_scope, Engine.takes",
    "migkit/verdict.py": "verdict: verdict.decide",
    "migkit/rowfilter.py": "row filter",
    "migkit/transforms.py": "column transform",
    "migkit/hopkeys.py": "hop keys: HOP_KEYS, HOP_OPTIONS",
    "migkit/envkeys.py": "environment variables",
    "migkit/decide.py": "decision: every choice is a rung",
    "migkit/ranges.py": "ranges",
    "migkit/diagnostics.py": "probes: ProbeAccumulator",
}

_RANK = {p: i for i, rank in enumerate(LAYERS) for p, _ in rank}
_TOP_OF = {m: p for p, names in _BY_NAME.items() for m in names.split()}
_COUNTED = sum(len(v.split()) for v in _BY_NAME.values())
assert _COUNTED == len(_TOP_OF), "a module is named in two packages"


def package_of(rel):
    """The responsibility package of a file under migkit/ or tools/, or
    None where the table does not claim it (the inventory lists those as
    unassigned and a test fails on one)."""
    parts = Path(rel).with_suffix("").parts
    if parts[0] == "tools":
        return "tools"
    if parts[0] != "migkit":
        return None
    if len(parts) == 1:
        return None
    if Path(rel).suffix != ".py":
        return "data" if parts[1] == "data" else \
            _BY_PREFIX.get(parts[1])
    if len(parts) == 2:
        return "configuration" if parts[1] == "__init__" else \
            _TOP_OF.get(parts[1])
    return _BY_PREFIX.get(parts[1])


def _files(root=ROOT):
    out = []
    for top in ("migkit", "tools"):
        for p in (root / top).rglob("*"):
            if p.is_file() and "__pycache__" not in p.parts \
                    and p.suffix != ".pyc":
                out.append(p.relative_to(root).as_posix())
    return sorted(out)


def _modname(rel):
    parts = list(Path(rel).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _known(files):
    return {_modname(f) for f in files if f.endswith(".py")}


_DOTTED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")


def _imports(path, name, known):
    """[(module, line, lazy)] every module of `known` the file imports:
    an import statement, at the top or inside a function (lazy), and a
    module named whole by a string (`import_module("migkit.x")`)."""
    tree = _parse(path)
    if tree is None:
        return []
    pkg = name if path.name == "__init__.py" else name.rpartition(".")[0]
    tools = {k.split(".", 1)[1] for k in known if k.startswith("tools.")}
    out = []

    def keep(target, line, lazy):
        while target and target not in known:
            target = target.rpartition(".")[0]
        if target and target != name:
            out.append((target, line, lazy))

    def visit(node, lazy):
        for c in ast.iter_child_nodes(node):
            if not isinstance(c, (ast.stmt, ast.ExceptHandler,
                                  ast.match_case)):
                continue
            inner = lazy or isinstance(c, (ast.FunctionDef,
                                           ast.AsyncFunctionDef))
            if isinstance(c, ast.Import):
                for a in c.names:
                    first = a.name.split(".")[0]
                    keep("tools." + a.name if first in tools and
                         "." not in a.name else a.name, c.lineno, inner)
            elif isinstance(c, ast.ImportFrom):
                base = c.module or ""
                if c.level:
                    up = pkg.split(".")
                    up = up[:len(up) - (c.level - 1)]
                    base = ".".join(up + ([base] if base else []))
                elif base in tools:
                    base = "tools." + base
                for a in c.names:
                    full = f"{base}.{a.name}"
                    keep(full if full in known else base, c.lineno, inner)
            visit(c, inner)
    visit(tree, False)
    for c in ast.walk(tree):
        if isinstance(c, ast.Constant) and isinstance(c.value, str) \
                and c.value in known:
            keep(c.value, c.lineno, True)
    return out


_GRAPHS = {}


def import_graph(root=ROOT, files=None):
    """{module: [(imported module, line, lazy)]} of every Python file of
    migkit/ and tools/ under `root`."""
    files = _files(root) if files is None else files
    key = (str(root), tuple(files), tuple(
        (root / f).stat().st_mtime_ns for f in files))
    if key not in _GRAPHS:
        known = _known(files)
        _GRAPHS[key] = {_modname(f): _imports(root / f, _modname(f), known)
                        for f in files if f.endswith(".py")}
    return _GRAPHS[key]


def upward_imports(root=ROOT, files=None):
    """[(file, line, imported module, from package, to package)] of every
    import in migkit/ that reaches a package of a higher rank than its
    own's."""
    files = _files(root) if files is None else files
    by_mod = {_modname(f): f for f in files if f.endswith(".py")}
    out = []
    for mod, edges in import_graph(root, files).items():
        rel = by_mod[mod]
        a = package_of(rel)
        if a not in _RANK:
            continue
        seen = set()
        for target, line, _lazy in sorted(edges):
            b = package_of(by_mod[target])
            if b in _RANK and _RANK[b] > _RANK[a] and target not in seen:
                seen.add(target)
                out.append((rel, line, target, a, b))
    return sorted(out)


def _test_reach(known):
    """{module: {test file}} the tests import, by import statement or by
    a module's name as a string."""
    out = {}
    for t in sorted(TESTS.rglob("*.py")):
        tree = _parse(t)
        if tree is None:
            continue
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module \
                    and not node.level:
                names = [node.module] + [f"{node.module}.{a.name}"
                                         for a in node.names]
            elif isinstance(node, ast.Constant) \
                    and isinstance(node.value, str) \
                    and node.value.startswith("migkit.") \
                    and _DOTTED.match(node.value):
                names = [node.value]
            for n in names:
                while n and n not in known:
                    n = n.rpartition(".")[0]
                if n:
                    out.setdefault(n, set()).add(t.name)
    return out


def _wrapped():
    """{file: [tool]} the wrapped programs and libraries each file
    registers with `tooluse`, and the files whose code a tool's uses are
    read off (`reads`) - asked in a process of its own, so what the
    registry holds does not depend on what this one imported."""
    code = (
        "import json, sys\n"
        "sys.path.insert(0, %r)\n"
        "from migkit import tooluse\n"
        "got = {}\n"
        "orig = tooluse.register\n"
        "def spy(name, **kw):\n"
        "    f = sys._getframe(1).f_code.co_filename\n"
        "    got.setdefault(f, set()).add(name)\n"
        "    return orig(name, **kw)\n"
        "tooluse.register = spy\n"
        "tooluse._load()\n"
        "for t in tooluse.REGISTRY.values():\n"
        "    for r in (t.reads[0] if t.reads else ()):\n"
        "        got.setdefault('migkit/' + r.partition('::')[0], set())"
        ".add(t.name)\n"
        "print(json.dumps({k: sorted(v) for k, v in got.items()}))\n"
    ) % str(ROOT)
    run = subprocess.run([sys.executable, "-c", code], capture_output=True,
                         text=True, cwd=ROOT)
    if run.returncode:
        raise RuntimeError("the wrapped-tool registry could not be read: "
                           + run.stderr.strip().splitlines()[-1])
    out = {}
    for f, names in json.loads(run.stdout.strip().splitlines()[-1]).items():
        try:
            rel = Path(f).relative_to(ROOT).as_posix()
        except ValueError:
            rel = f
        out.setdefault(rel, set()).update(names)
    return {k: sorted(v) for k, v in out.items()}


def _entry_modules():
    """The modules started from outside migkit (`tests/test_every_module_
    is_reached.py`, the one list)."""
    sys.path.insert(0, str(ROOT))
    try:
        import importlib
        return importlib.import_module(
            "tests.test_every_module_is_reached").ENTRY
    finally:
        sys.path.pop(0)


def _short(mod):
    return mod[len("migkit."):] if mod.startswith("migkit.") else mod


def _cap(items, n=5):
    items = list(items)
    return ", ".join(items[:n]) + (f" (+{len(items) - n})"
                                   if len(items) > n else "")


def reached_by_tests(files=None):
    """{file: (kind, detail)} how a test reaches each Python file of
    migkit/: "direct" (a test imports it; detail the tests), "through"
    (a module a test imports imports it; detail those modules) or "none"."""
    files = _files() if files is None else files
    known = _known(files)
    graph = import_graph(ROOT, files)
    direct = _test_reach(known)
    through = {}
    for m in direct:
        for t, _l, _z in graph.get(m, ()):
            through.setdefault(t, set()).add(m)
    out = {}
    for f in files:
        if not (f.startswith("migkit/") and f.endswith(".py")):
            continue
        m = _modname(f)
        out[f] = ("direct", sorted(direct[m])) if m in direct else \
            ("through", sorted(through[m])) if m in through else \
            ("none", [])
    return out


def inventory():
    """Every file of migkit/ and tools/: its package, its owner, its
    size, who imports it and what it reaches, the programs it wraps and
    the tests that reach it."""
    files = _files()
    known = _known(files)
    by_mod = {_modname(f): f for f in files if f.endswith(".py")}
    graph = import_graph(ROOT, files)
    importers = {}
    for m, edges in graph.items():
        for t, _l, _z in edges:
            importers.setdefault(t, set()).add(m)
    wrapped = _wrapped()
    tests = reached_by_tests(files)
    entry = _entry_modules()
    unassigned = [f for f in files if package_of(f) is None]
    pkgs = [p for rank in LAYERS for p, _ in rank] + [p for p, _ in OUTSIDE]
    said = dict(p for rank in LAYERS for p in rank) | dict(OUTSIDE)
    lines = [
        "# migkit source inventory", "",
        "Written by `tools/gen_codemap.py` from the code; the pre-commit"
        " hook stops a commit that leaves it behind. Every file under"
        " `migkit/` and `tools/` with the responsibility package a table"
        " in the generator gives it (`_BY_NAME`, `_BY_PREFIX`), the owner"
        " domain where it holds one of the one-place readings (`OWNERS`),"
        " its size, the modules that import it and the packages it reaches"
        " (module-level and inside functions alike), the wrapped programs"
        " and libraries it registers or is read for, and the tests that"
        " reach it. The layering rule these packages are held to is"
        " [LAYERS.md](LAYERS.md); where things are by module is"
        " [CODEMAP.md](CODEMAP.md).", "",
        "## Unassigned", ""]
    lines += [f"- `{f}`" for f in unassigned] or ["None."]
    lines += ["", "## Packages", "",
              "| package | rank | files | lines | responsibility |",
              "|---|---|---|---|---|"]
    count = {}
    for f in files:
        p = package_of(f)
        n, ln = count.get(p, (0, 0))
        count[p] = (n + 1, ln + len(
            (ROOT / f).read_text(errors="replace").splitlines()))
    for p in pkgs:
        n, ln = count.get(p, (0, 0))
        lines.append(f"| {p} | {_RANK[p] + 1 if p in _RANK else '-'} |"
                     f" {n} | {ln} | {said[p]} |")
    for p in pkgs:
        rows = [f for f in files if package_of(f) == p]
        if not rows:
            continue
        lines += ["", f"## {p} ({len(rows)} files)", "",
                  "| file | owner | lines | imported by | reaches |"
                  " wraps | tests |", "|---|---|---|---|---|---|---|"]
        for f in rows:
            mod = _modname(f) if f.endswith(".py") else None
            n = len((ROOT / f).read_text(errors="replace").splitlines())
            inn = sorted(_short(i) for i in importers.get(mod, ()))
            reach = {}
            for t, _l, _z in graph.get(mod, ()):
                q = package_of(by_mod[t]) or "unassigned"
                reach[q] = reach.get(q, 0) + 1
            out = ", ".join(f"{q} {c}" for q, c in sorted(reach.items())
                            ) or "-"
            if mod is None:
                tcell = "-"
            else:
                kind, detail = tests.get(f, ("none", []))
                names = [d[len("test_"):-3] if d.startswith("test_")
                         else d for d in detail]
                tcell = f"{len(detail)}: {_cap(names, 3)}" \
                    if kind == "direct" else \
                    f"through {_cap([_short(d) for d in detail], 2)}" \
                    if kind == "through" else \
                    "entry point" if mod in entry else "none"
            imp = f"{len(inn)}: {_cap(inn)}" if inn else "0"
            lines.append(f"| `{f}` | {OWNERS.get(f, '-')} | {n} | {imp} |"
                         f" {out} | {', '.join(wrapped.get(f, ())) or '-'} |"
                         f" {tcell} |")
    return "\n".join(lines) + "\n"


def layers():
    """The layering rule as a page: the ranks, and every upward import
    still allowed, each with its reason."""
    found = upward_imports()
    listed = {(f, m): r for r, pairs in KNOWN_UPWARD.items()
              for f, m in (p.split(" ") for p in pairs)}
    lines = [
        "# migkit layers", "",
        "Written by `tools/gen_codemap.py` from `LAYERS` and `KNOWN_UPWARD`"
        " in it; the pre-commit hook stops a commit that leaves it behind."
        " `tests/test_the_layers_import_downward.py` holds every import in"
        " `migkit/` to it. The packages are the plan's"
        " (`docs/research/source-hierarchy-2026-10-06.md`); which file is"
        " in which is [INVENTORY.md](INVENTORY.md).", "",
        "## The rule", "",
        "A package imports its own rank and the ranks below it, never one"
        " above. A rank of two lists packages beside each other. An import"
        " inside a function counts as one at the top of the module. A"
        " module that must reach a higher package is a defect to move, and"
        " until it is moved it is listed under `KNOWN_UPWARD` with its"
        " reason: the list only shrinks (the test fails on an import added"
        " and on one listed that is gone).", "",
        "## Ranks, lowest first", "",
        "| rank | package | responsibility |", "|---|---|---|"]
    for i, rank in enumerate(LAYERS, 1):
        for p, what in rank:
            lines.append(f"| {i} | {p} | {what} |")
    lines += ["", "Outside the rule: " + "; ".join(
        f"`{p}` ({what})" for p, what in OUTSIDE) + ".", "",
        f"## Upward imports still listed: {len(found)}", ""]
    by = {}
    for f, line, m, a, b in found:
        by.setdefault(listed.get((f, m), "(not listed)"), []).append(
            f"`{f}:{line}` {a} -> {b} (`{_short(m)}`)")
    for reason, items in sorted(by.items()):
        lines += [f"### {reason}", ""] + [f"- {i}" for i in items] + [""]
    return "\n".join(lines)


#: every import that reaches a higher package than the importer's own, each
#: under the reason it is there and what ends it: `file module`. The list
#: only shrinks (`tests/test_the_layers_import_downward.py`)
KNOWN_UPWARD = {
    ("a conversion asks the decision engine (`decide`) which way to "
     "take; the choice is handed in as a fact"): (
        "migkit/charsets.py migkit.decide",
        "migkit/pieces.py migkit.decide",
        "migkit/storedcode/collations.py migkit.decide",
        "migkit/storedcode/rungs.py migkit.decide",
    ),
    ("a safety module asks the decision engine (`decide.lift_hold`) "
     "whether a hold a run left may be lifted; the holds and their "
     "ages are to be handed up as facts by the command"): (
        "migkit/leftovers.py migkit.decide",
    ),
    ("a conversion asks the decision engine (`tuning`) which way to "
     "take; the choice is handed in as a fact"): (
        "migkit/canon.py migkit.tuning",
    ),
    ("a conversion asks the decision engine (`workload`) which way to "
     "take; the choice is handed in as a fact"): (
        "migkit/storedcode/statements.py migkit.workload",
    ),
    ("a conversion calls `changetopic`, the workflow it serves; the "
     "call is inverted into a hook"): (
        "migkit/described.py migkit.changetopic",
    ),
    ("a conversion calls `consolidate`, the workflow it serves; the "
     "call is inverted into a hook"): (
        "migkit/charsets.py migkit.consolidate",
    ),
    ("a conversion calls `cutover`, the workflow it serves; the call "
     "is inverted into a hook"): (
        "migkit/storedcode/reference.py migkit.cutover",
    ),
    ("a conversion calls `movers`, the workflow it serves; the call is "
     "inverted into a hook"): (
        "migkit/storedcode/statements.py migkit.movers",
    ),
    ("a conversion calls `physical`, the workflow it serves; the call "
     "is inverted into a hook"): (
        "migkit/charsets.py migkit.physical",
    ),
    ("a conversion calls `streamout`, the workflow it serves; the call "
     "is inverted into a hook"): (
        "migkit/schemaformats.py migkit.streamout",
    ),
    ("a conversion drives a wrapped tool or provider (`assist`); the "
     "call goes through an integration contract below conversion"): (
        "migkit/storedcode/convert.py migkit.assist",
        "migkit/storedcode/model.py migkit.assist",
    ),
    ("a conversion drives a wrapped tool or provider (`avrostream`); "
     "the call goes through an integration contract below conversion"): (
        "migkit/schemaformats.py migkit.avrostream",
    ),
    ("a conversion drives a wrapped tool or provider (`registry`); the "
     "call goes through an integration contract below conversion"): (
        "migkit/schemaformats.py migkit.registry",
    ),
    ("a conversion drives a wrapped tool or provider (`tools`); the "
     "call goes through an integration contract below conversion"): (
        "migkit/storedcode/programs.py migkit.tools",
    ),
    ("a conversion drives a wrapped tool or provider (`tooluse`); the "
     "call goes through an integration contract below conversion"): (
        "migkit/storedcode/appscan.py migkit.tooluse",
        "migkit/storedcode/indexadvice.py migkit.tooluse",
        "migkit/storedcode/programs.py migkit.tooluse",
    ),
    ("a conversion reads an engine (`engines.base`) for its catalogue "
     "or connection; the engine contract moves below conversion in the "
     "engine split"): (
        "migkit/charsets.py migkit.engines.base",
        "migkit/pieces.py migkit.engines.base",
        "migkit/schemaformats.py migkit.engines.base",
        "migkit/storedcode/builtins.py migkit.engines.base",
        "migkit/storedcode/collations.py migkit.engines.base",
        "migkit/storedcode/constraints.py migkit.engines.base",
        "migkit/storedcode/convert.py migkit.engines.base",
        "migkit/storedcode/partitions.py migkit.engines.base",
        "migkit/storedcode/prove.py migkit.engines.base",
        "migkit/storedcode/reference.py migkit.engines.base",
        "migkit/storedcode/sizing.py migkit.engines.base",
        "migkit/storedcode/statements.py migkit.engines.base",
        "migkit/zones.py migkit.engines.base",
    ),
    ("a conversion reads an engine (`engines.dbapi`) for its catalogue "
     "or connection; the engine contract moves below conversion in the "
     "engine split"): (
        "migkit/pieces.py migkit.engines.dbapi",
        "migkit/storedcode/constraints.py migkit.engines.dbapi",
        "migkit/storedcode/corpus.py migkit.engines.dbapi",
        "migkit/storedcode/history.py migkit.engines.dbapi",
        "migkit/storedcode/packages.py migkit.engines.dbapi",
        "migkit/storedcode/partitions.py migkit.engines.dbapi",
        "migkit/storedcode/sandbox.py migkit.engines.dbapi",
    ),
    ("a conversion reads an engine (`engines.hetero`) for its "
     "catalogue or connection; the engine contract moves below "
     "conversion in the engine split"): (
        "migkit/charsets.py migkit.engines.hetero",
        "migkit/storedcode/sandbox.py migkit.engines.hetero",
    ),
    ("a conversion reads an engine (`engines.mssql`) for its catalogue "
     "or connection; the engine contract moves below conversion in the "
     "engine split"): (
        "migkit/storedcode/corpus.py migkit.engines.mssql",
    ),
    ("a conversion reads an engine (`engines.oracle`) for its "
     "catalogue or connection; the engine contract moves below "
     "conversion in the engine split"): (
        "migkit/storedcode/constraints.py migkit.engines.oracle",
        "migkit/storedcode/packages.py migkit.engines.oracle",
    ),
    ("a conversion reads an engine (`engines`) for its catalogue or "
     "connection; the engine contract moves below conversion in the "
     "engine split"): (
        "migkit/canon.py migkit.engines",
        "migkit/charsets.py migkit.engines",
        "migkit/commercial.py migkit.engines",
        "migkit/storedcode/sandbox.py migkit.engines",
    ),
    ("a conversion reads an engine (`logddl`) for its catalogue or "
     "connection; the engine contract moves below conversion in the "
     "engine split"): (
        "migkit/ddl.py migkit.logddl",
    ),
    ("a durable record uses the executor's `attempts`; the shared "
     "piece moves below persistence"): (
        "migkit/claims.py migkit.attempts",
    ),
    ("a durable record uses the executor's `failpoint`; the shared "
     "piece moves below persistence"): (
        "migkit/claims.py migkit.failpoint",
    ),
    ("a durable record uses the executor's `locks`; the shared piece "
     "moves below persistence"): (
        "migkit/revert.py migkit.locks",
    ),
    ("a durable record uses the executor's `ranges`; the shared piece "
     "moves below persistence"): (
        "migkit/checkpoint.py migkit.ranges",
        "migkit/claims.py migkit.ranges",
    ),
    ("a durable record uses the executor's `util`; the shared piece "
     "moves below persistence"): (
        "migkit/digests.py migkit.util",
        "migkit/runlog.py migkit.util",
    ),
    ("a durable write goes through safety's `egress`; the order of the "
     "ranks is to be settled with the safety package"): (
        "migkit/anchor.py migkit.egress",
        "migkit/runlog.py migkit.egress",
    ),
    ("a durable write goes through safety's `fencing`; the order of "
     "the ranks is to be settled with the safety package"): (
        "migkit/claims.py migkit.fencing",
    ),
    ("a durable write goes through safety's `lease`; the order of the "
     "ranks is to be settled with the safety package"): (
        "migkit/claims.py migkit.lease",
        "migkit/setaside.py migkit.lease",
    ),
    ("a durable write goes through safety's `redact`; the order of the "
     "ranks is to be settled with the safety package"): (
        "migkit/anchor.py migkit.redact",
    ),
    ("a hop is checked against the engines (`engines.base`) it names; "
     "ends when the engine registry is read through a contract"): (
        "migkit/hopkeys.py migkit.engines.base",
    ),
    ("a hop is checked against the engines (`engines.saas`) it names; "
     "ends when the engine registry is read through a contract"): (
        "migkit/addresses.py migkit.engines.saas",
    ),
    ("a hop is checked against the engines (`engines`) it names; ends "
     "when the engine registry is read through a contract"): (
        "migkit/addresses.py migkit.engines",
        "migkit/config.py migkit.engines",
        "migkit/engineat.py migkit.engines",
        "migkit/hopkeys.py migkit.engines",
        "migkit/managed.py migkit.engines",
    ),
    ("a hop is checked by safety's `egress`; the check moves to the "
     "key table"): (
        "migkit/hopkeys.py migkit.egress",
    ),
    ("a hop is checked by safety's `freeze`; the check moves to the "
     "key table"): (
        "migkit/hopkeys.py migkit.freeze",
        "migkit/managed.py migkit.freeze",
    ),
    ("a hop is checked by safety's `interlock`; the check moves to the "
     "key table"): (
        "migkit/hopkeys.py migkit.interlock",
    ),
    ("a hop is checked by safety's `leftovers`; the check moves to the "
     "key table"): (
        "migkit/config.py migkit.leftovers",
    ),
    ("a hop is checked by safety's `tls`; the check moves to the key "
     "table"): (
        "migkit/config.py migkit.tls",
    ),
    ("a hop key's check or default asks `archive` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.archive",
    ),
    ("a hop key's check or default asks `consolidate` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.consolidate",
    ),
    ("a hop key's check or default asks `cutover` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/managed.py migkit.cutover",
    ),
    ("a hop key's check or default asks `physical` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.physical",
    ),
    ("a hop key's check or default asks `reshard` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.reshard",
    ),
    ("a hop key's check or default asks `split` what it accepts; ends "
     "when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.split",
    ),
    ("a hop key's check or default asks `subset` what it accepts; ends "
     "when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.subset",
    ),
    ("a hop key's check or default asks `tableengine` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.tableengine",
    ),
    ("a hop key's check or default asks `tailtables` what it accepts; "
     "ends when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.tailtables",
    ),
    ("a hop key's check or default asks `twoway` what it accepts; ends "
     "when the key table is read from the owner"): (
        "migkit/hopkeys.py migkit.twoway",
    ),
    ("a hop option is explained by the decision engine (`dba`); the "
     "explanation moves to the key table"): (
        "migkit/managed.py migkit.dba",
    ),
    ("a hop option is explained by the decision engine (`decide`); the "
     "explanation moves to the key table"): (
        "migkit/engineat.py migkit.decide",
        "migkit/managed.py migkit.decide",
    ),
    ("a hop option is explained by the decision engine (`machine`); "
     "the explanation moves to the key table"): (
        "migkit/hopkeys.py migkit.machine",
    ),
    ("a hop option is explained by the decision engine (`ready`); the "
     "explanation moves to the key table"): (
        "migkit/managed.py migkit.ready",
    ),
    ("a hop option is explained by the decision engine (`scenarios`); "
     "the explanation moves to the key table"): (
        "migkit/hopkeys.py migkit.scenarios",
    ),
    ("a hop option is explained by the decision engine "
     "(`sourcechange`); the explanation moves to the key table"): (
        "migkit/hopkeys.py migkit.sourcechange",
    ),
    ("a record is rendered by conversion's `canon`; the rendering "
     "moves below persistence"): (
        "migkit/digests.py migkit.canon",
        "migkit/kept.py migkit.canon",
        "migkit/repairlog.py migkit.canon",
    ),
    ("a record is rendered by conversion's `render`; the rendering "
     "moves below persistence"): (
        "migkit/kept.py migkit.render",
    ),
    ("a record is rendered by conversion's `rowtext`; the rendering "
     "moves below persistence"): (
        "migkit/digests.py migkit.rowtext",
        "migkit/kept.py migkit.rowtext",
    ),
    ("a record is written in the shape `fleet` gives it; the shape "
     "moves below persistence"): (
        "migkit/claims.py migkit.fleet",
    ),
    ("a record is written in the shape `ha` gives it; the shape moves "
     "below persistence"): (
        "migkit/claims.py migkit.ha",
        "migkit/state.py migkit.ha",
    ),
    ("a record is written in the shape `indexes` gives it; the shape "
     "moves below persistence"): (
        "migkit/setaside.py migkit.indexes",
    ),
    ("a record is written in the shape `leaves` gives it; the shape "
     "moves below persistence"): (
        "migkit/kept.py migkit.leaves",
    ),
    ("a record is written in the shape `tailctl` gives it; the shape "
     "moves below persistence"): (
        "migkit/kept.py migkit.tailctl",
        "migkit/setaside.py migkit.tailctl",
        "migkit/state.py migkit.tailctl",
    ),
    ("a record is written in the shape `twoway` gives it; the shape "
     "moves below persistence"): (
        "migkit/kept.py migkit.twoway",
    ),
    ("a record keeps what the decision engine (`decide`) measured; the "
     "fact is handed down"): (
        "migkit/session.py migkit.decide",
        "migkit/shared.py migkit.decide",
    ),
    ("a record keeps what the decision engine (`model`) measured; the "
     "fact is handed down"): (
        "migkit/claims.py migkit.model",
    ),
    ("a record keeps what the decision engine (`sizing`) measured; the "
     "fact is handed down"): (
        "migkit/claims.py migkit.sizing",
    ),
    ("a record notifies or exports through `export`; the call is "
     "inverted into an event"): (
        "migkit/anchor.py migkit.export",
        "migkit/session.py migkit.export",
    ),
    ("a record notifies or exports through `notify`; the call is "
     "inverted into an event"): (
        "migkit/session.py migkit.notify",
    ),
    ("a record notifies or exports through `telemetry`; the call is "
     "inverted into an event"): (
        "migkit/session.py migkit.telemetry",
    ),
    ("a record notifies or exports through `traces`; the call is "
     "inverted into an event"): (
        "migkit/session.py migkit.traces",
    ),
    ("a report asks an engine (`engines.base`) for a name or a fact; "
     "the engine contract moves below reporting"): (
        "migkit/telemetry.py migkit.engines.base",
        "migkit/verdict.py migkit.engines.base",
    ),
    ("a report asks an engine (`engines`) for a name or a fact; the "
     "engine contract moves below reporting"): (
        "migkit/export.py migkit.engines",
        "migkit/report.py migkit.engines",
        "migkit/telemetry.py migkit.engines",
        "migkit/troubleshooting.py migkit.engines",
    ),
    ("a report names a wrapped tool's account (`tools`); the account "
     "is read through a contract below reporting"): (
        "migkit/export.py migkit.tools",
    ),
    ("a report names a wrapped tool's account (`tooluse`); the account "
     "is read through a contract below reporting"): (
        "migkit/export.py migkit.tooluse",
        "migkit/notify.py migkit.tooluse",
        "migkit/telemetry.py migkit.tooluse",
        "migkit/traces.py migkit.tooluse",
    ),
    ("the hop's sign-in and the readiness audit ask the cloud provider's "
     "own API (`cloudapi`); the provider calls move to a contract below "
     "configuration and decision"): (
        "migkit/cloud_readiness.py migkit.cloudapi",
        "migkit/config.py migkit.cloudapi",
    ),
    ("a forwarder asks the decision engine (`decide.cloud_forwarder`) "
     "which way in to take; the choice is handed in as a fact"): (
        "migkit/tunnel.py migkit.decide",
    ),
    ("every provider call waits on the egress gate (`cloudapi.gate`, "
     "`egress.decide`) before its client is made; the gate moves to a "
     "contract below persistence"): (
        "migkit/anchor.py migkit.cloudapi",
        "migkit/dba.py migkit.cloudapi",
        "migkit/freeze.py migkit.cloudapi",
        "migkit/machine.py migkit.cloudapi",
        "migkit/state.py migkit.cloudapi",
        "migkit/variants.py migkit.cloudapi",
        "migkit/variants.py migkit.egress",
    ),
    ("the hop check refuses prefixes that meet through `topology.overlaps`; "
     "the key spaces move below configuration"): (
        "migkit/hopkeys.py migkit.topology",
    ),
    ("the deep check compares Agent jobs through `jobs.shapes`; the job "
     "shapes move below engines"): (
        "migkit/engines/mssql.py migkit.jobs",
    ),
    ("a column is hidden by the way `decide.hide_column` picks from its "
     "facts; the rung moves below conversion"): (
        "migkit/transforms.py migkit.decide",
    ),
    ("the fence's way and place are rungs (`decide.fence_way`, "
     "`decide.fence_place`) on the account's rights the engine reads; a "
     "finished range is named a ledger unit through the freeze's writer; "
     "a broker's transaction start is bounded by the lease's own term"): (
        "migkit/fencing.py migkit.decide",
        "migkit/claims.py migkit.freeze",
        "migkit/engines/kafka.py migkit.ha",
    ),
    ("the anchor's interval is a rung (`decide.anchor_interval`) on the "
     "measured entry rate; the anchor hands the rate in as a fact"): (
        "migkit/anchor.py migkit.decide",
    ),
    ("a sampled check folds the drawn rows of both sides through the one "
     "folder (`canon.fold_rows`), so the sample compares as the full "
     "check does"): (
        "migkit/ranges.py migkit.canon",
    ),
    ("a brand's confidence is a rung (`decide.brand_confidence`) the "
     "configuration reads once the server's signs are in; the rung moves "
     "below configuration"): (
        "migkit/variants.py migkit.decide",
    ),
    ("a report reads `autonomous` for the state of the running "
     "workflow; the state moves below reporting"): (
        "migkit/telemetry.py migkit.autonomous",
    ),
    ("a report reads `ha` for the state of the running workflow; the "
     "state moves below reporting"): (
        "migkit/export.py migkit.ha",
    ),
    ("a report reads `leaves` for the state of the running workflow; "
     "the state moves below reporting"): (
        "migkit/export.py migkit.leaves",
    ),
    ("a report reads `movers` for the state of the running workflow; "
     "the state moves below reporting"): (
        "migkit/export.py migkit.movers",
        "migkit/report.py migkit.movers",
    ),
    ("a report reads `physical` for the state of the running workflow; "
     "the state moves below reporting"): (
        "migkit/export.py migkit.physical",
    ),
    ("a report reads `tailctl` for the state of the running workflow; "
     "the state moves below reporting"): (
        "migkit/notify.py migkit.tailctl",
    ),
    ("a report reads `twoway` for the state of the running workflow; "
     "the state moves below reporting"): (
        "migkit/export.py migkit.twoway",
    ),
    ("a report states what the decision engine (`capabilities`) chose; "
     "the choice is handed down as a fact"): (
        "migkit/diagnostics.py migkit.capabilities",
        "migkit/report.py migkit.capabilities",
    ),
    ("a report states what the decision engine (`decide`) chose; the "
     "choice is handed down as a fact"): (
        "migkit/report.py migkit.decide",
        "migkit/telemetry.py migkit.decide",
    ),
    ("a report states what the decision engine (`faults`) chose; the "
     "choice is handed down as a fact"): (
        "migkit/telemetry.py migkit.faults",
    ),
    ("a report states what the decision engine (`guarantees`) chose; "
     "the choice is handed down as a fact"): (
        "migkit/telemetry.py migkit.guarantees",
    ),
    ("a report states what the decision engine (`sizing`) chose; the "
     "choice is handed down as a fact"): (
        "migkit/report.py migkit.sizing",
    ),
    ("a report states what the decision engine (`waysaid`) chose; the "
     "choice is handed down as a fact"): (
        "migkit/report.py migkit.waysaid",
    ),
    ("a report states what the decision engine (`workload`) chose; the "
     "choice is handed down as a fact"): (
        "migkit/verdict.py migkit.workload",
    ),
    ("a rung or plan reads `consolidate` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/scenarios.py migkit.consolidate",
    ),
    ("a rung or plan reads `cutover` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/planner.py migkit.cutover",
        "migkit/ready.py migkit.cutover",
        "migkit/repairways.py migkit.cutover",
        "migkit/workload.py migkit.cutover",
    ),
    ("a rung or plan reads `drainmark` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/capabilities.py migkit.drainmark",
    ),
    ("a rung or plan reads `drift` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/jointplan.py migkit.drift",
    ),
    ("a rung or plan reads `jobs` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/capabilities.py migkit.jobs",
    ),
    ("a rung or plan reads `leaves` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/repairways.py migkit.leaves",
    ),
    ("a rung or plan reads `movers` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/bulkways.py migkit.movers",
        "migkit/capabilities.py migkit.movers",
        "migkit/legs.py migkit.movers",
        "migkit/machine.py migkit.movers",
        "migkit/planner.py migkit.movers",
        "migkit/repairways.py migkit.movers",
        "migkit/waysaid.py migkit.movers",
    ),
    ("a rung or plan reads `physical` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/costmodel.py migkit.physical",
    ),
    ("a rung or plan reads `reshard` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/scenarios.py migkit.reshard",
        "migkit/topology.py migkit.reshard",
    ),
    ("a rung or plan reads `subset` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/ready.py migkit.subset",
    ),
    ("a rung or plan reads `tableengine` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/scenarios.py migkit.tableengine",
    ),
    ("a rung or plan reads `topicstates` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/capabilities.py migkit.topicstates",
    ),
    ("a rung or plan reads `unchanged` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/scanplan.py migkit.unchanged",
    ),
    ("a rung or plan reads `upgrade` for what the workflow can do; "
     "ends when the workflow publishes its abilities below decision"): (
        "migkit/lacks.py migkit.upgrade",
        "migkit/ready.py migkit.upgrade",
        "migkit/workload.py migkit.upgrade",
    ),
    ("a rung or plan reads `users` for what the workflow can do; ends "
     "when the workflow publishes its abilities below decision"): (
        "migkit/capabilities.py migkit.users",
    ),
    ("a rung reads a wrapped tool's surface (`mirror`); the surface is "
     "a fact handed down"): (
        "migkit/bulkways.py migkit.mirror",
    ),
    ("a rung reads a wrapped tool's surface (`redisshake`); the "
     "surface is a fact handed down"): (
        "migkit/bulkways.py migkit.redisshake",
    ),
    ("a rung reads a wrapped tool's surface (`ship`); the surface is a "
     "fact handed down"): (
        "migkit/machine.py migkit.ship",
    ),
    ("a safety check reads `barriers` for the step it guards; the step "
     "reports its own state down"): (
        "migkit/freeze.py migkit.barriers",
    ),
    ("an engine calls `barriers` for the origin mark its atomic unit "
     "carries; the engine split gives the mark a contract below the "
     "engines"): (
        "migkit/engines/dynamodb.py migkit.barriers",
        "migkit/engines/kafka.py migkit.barriers",
        "migkit/engines/mongodb.py migkit.barriers",
        "migkit/engines/redis.py migkit.barriers",
    ),
    ("a safety check reads `cutover` for the step it guards; the step "
     "reports its own state down"): (
        "migkit/approvals.py migkit.cutover",
        "migkit/freeze.py migkit.cutover",
    ),
    ("a safety check reads `fleet` for the step it guards; the step "
     "reports its own state down"): (
        "migkit/lease.py migkit.fleet",
    ),
    ("a safety check reads `tailctl` for the step it guards; the step "
     "reports its own state down"): (
        "migkit/approvals.py migkit.tailctl",
        "migkit/freeze.py migkit.tailctl",
    ),
    ("a safety check reads `users` for the step it guards; the step "
     "reports its own state down"): (
        "migkit/freeze.py migkit.users",
    ),
    ("a safety refusal is said by reporting's `export`; the wording "
     "moves below safety"): (
        "migkit/egress.py migkit.export",
    ),
    ("a safety refusal is said by reporting's `notify`; the wording "
     "moves below safety"): (
        "migkit/approvals.py migkit.notify",
    ),
    ("a safety refusal is said by reporting's `verdict`; the wording "
     "moves below safety"): (
        "migkit/approvals.py migkit.verdict",
    ),
    ("a safety refusal is said by reporting's `wording`; the wording "
     "moves below safety"): (
        "migkit/interlock.py migkit.wording",
    ),
    ("a scope check asks `editions` what the workflow takes; ends when "
     "the workflow publishes it below scope"): (
        "migkit/case.py migkit.editions",
    ),
    ("a scope check asks `physical` what the workflow takes; ends when "
     "the workflow publishes it below scope"): (
        "migkit/case.py migkit.physical",
    ),
    ("a scope check asks `upgrade` what the workflow takes; ends when "
     "the workflow publishes it below scope"): (
        "migkit/case.py migkit.upgrade",
    ),
    ("a wrapper reports through `crossmove`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/tools.py migkit.crossmove",
    ),
    ("a wrapper reports through `editions`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/backupseed.py migkit.editions",
    ),
    ("a wrapper reports through `leaves`, the workflow that drives it; "
     "the shared piece moves below both"): (
        "migkit/ship.py migkit.leaves",
    ),
    ("a wrapper reports through `movers`, the workflow that drives it; "
     "the shared piece moves below both"): (
        "migkit/redisshake.py migkit.movers",
        "migkit/ship.py migkit.movers",
        "migkit/tooluse.py migkit.movers",
    ),
    ("a wrapper reports through `physical`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/backupseed.py migkit.physical",
        "migkit/cloudseed.py migkit.physical",
    ),
    ("a wrapper reports through `replicasum`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/tooluse.py migkit.replicasum",
    ),
    ("a wrapper reports through `second_reader`, the workflow that "
     "drives it; the shared piece moves below both"): (
        "migkit/tools.py migkit.second_reader",
    ),
    ("a wrapper reports through `tailtables`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/imports.py migkit.tailtables",
    ),
    ("a wrapper reports through `upgrade`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/cloudseed.py migkit.upgrade",
        "migkit/tooluse.py migkit.upgrade",
    ),
    ("a wrapper reports through `verifiers`, the workflow that drives "
     "it; the shared piece moves below both"): (
        "migkit/tooluse.py migkit.verifiers",
    ),
    ("an engine calls `applyway` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.applyway",
    ),
    ("an engine calls `appversions` for a step it cannot do itself; "
     "the engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.appversions",
        "migkit/engines/mysql.py migkit.appversions",
        "migkit/engines/postgres.py migkit.appversions",
    ),
    ("an engine calls `changetopic` for a step it cannot do itself; "
     "the engine split gives that step a contract below the engines"): (
        "migkit/engines/firestore.py migkit.changetopic",
    ),
    ("an engine calls `crossmove` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/hetero.py migkit.crossmove",
    ),
    ("an engine calls `drift` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/hetero.py migkit.drift",
        "migkit/engines/mssql.py migkit.drift",
        "migkit/engines/mysql.py migkit.drift",
        "migkit/engines/postgres.py migkit.drift",
    ),
    ("an engine calls `indexes` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/postgres.py migkit.indexes",
    ),
    ("an engine calls `leaves` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.leaves",
        "migkit/engines/hetero.py migkit.leaves",
        "migkit/engines/mongodb.py migkit.leaves",
    ),
    ("an engine calls `marks` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/cassandra.py migkit.marks",
        "migkit/engines/dynamodb.py migkit.marks",
        "migkit/engines/hetero.py migkit.marks",
        "migkit/engines/kafka.py migkit.marks",
        "migkit/engines/mongodb.py migkit.marks",
        "migkit/engines/mysql.py migkit.marks",
        "migkit/engines/postgres.py migkit.marks",
        "migkit/engines/redis.py migkit.marks",
    ),
    ("an engine calls `movers` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.movers",
        "migkit/engines/cassandra.py migkit.movers",
        "migkit/engines/generic.py migkit.movers",
        "migkit/engines/hetero.py migkit.movers",
        "migkit/engines/mongodb.py migkit.movers",
        "migkit/engines/mysql.py migkit.movers",
        "migkit/engines/pg_pass.py migkit.movers",
        "migkit/engines/postgres.py migkit.movers",
    ),
    ("an engine calls `physical` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/postgres.py migkit.physical",
    ),
    ("an engine calls `replicasum` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/mysql.py migkit.replicasum",
    ),
    ("an engine calls `rules` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.rules",
    ),
    ("an engine calls `second_reader` for a step it cannot do itself; "
     "the engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.second_reader",
        "migkit/engines/hetero.py migkit.second_reader",
        "migkit/engines/mysql.py migkit.second_reader",
        "migkit/engines/postgres.py migkit.second_reader",
    ),
    ("an engine calls `streamout` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/kafka.py migkit.streamout",
    ),
    ("an engine calls `tableengine` for a step it cannot do itself; "
     "the engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.tableengine",
        "migkit/engines/mysql.py migkit.tableengine",
        "migkit/engines/postgres.py migkit.tableengine",
    ),
    ("an engine calls `tailctl` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.tailctl",
        "migkit/engines/cassandra.py migkit.tailctl",
        "migkit/engines/generic.py migkit.tailctl",
        "migkit/engines/hetero.py migkit.tailctl",
        "migkit/engines/kafka.py migkit.tailctl",
        "migkit/engines/mongodb.py migkit.tailctl",
        "migkit/engines/mssql.py migkit.tailctl",
        "migkit/engines/mysql.py migkit.tailctl",
        "migkit/engines/neo4j.py migkit.tailctl",
        "migkit/engines/oracle.py migkit.tailctl",
        "migkit/engines/postgres.py migkit.tailctl",
        "migkit/engines/redis.py migkit.tailctl",
    ),
    ("an engine calls `tailtables` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/hetero.py migkit.tailtables",
        "migkit/engines/mongodb.py migkit.tailtables",
        "migkit/engines/postgres.py migkit.tailtables",
    ),
    ("an engine calls `topicstates` for a step it cannot do itself; "
     "the engine split gives that step a contract below the engines"): (
        "migkit/engines/kafka.py migkit.topicstates",
    ),
    ("an engine calls `twoway` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/base.py migkit.twoway",
        "migkit/engines/dynamodb.py migkit.twoway",
        "migkit/engines/hetero.py migkit.twoway",
        "migkit/engines/mysql.py migkit.twoway",
        "migkit/engines/postgres.py migkit.twoway",
    ),
    ("an engine calls `unchanged` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/mssql.py migkit.unchanged",
        "migkit/engines/mysql.py migkit.unchanged",
        "migkit/engines/postgres.py migkit.unchanged",
    ),
    ("an engine calls `upgrade` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/mysql.py migkit.upgrade",
        "migkit/engines/postgres.py migkit.upgrade",
    ),
    ("an engine calls `users` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/cassandra.py migkit.users",
        "migkit/engines/kafka.py migkit.users",
        "migkit/engines/mongodb.py migkit.users",
        "migkit/engines/mssql.py migkit.users",
        "migkit/engines/mysql.py migkit.users",
        "migkit/engines/postgres.py migkit.users",
        "migkit/engines/redis.py migkit.users",
    ),
    ("an engine calls `verifiers` for a step it cannot do itself; the "
     "engine split gives that step a contract below the engines"): (
        "migkit/engines/cassandra.py migkit.verifiers",
        "migkit/engines/distmysql.py migkit.verifiers",
        "migkit/engines/dynamodb.py migkit.verifiers",
        "migkit/engines/firestore.py migkit.verifiers",
        "migkit/engines/generic.py migkit.verifiers",
        "migkit/engines/hetero.py migkit.verifiers",
        "migkit/engines/kafka.py migkit.verifiers",
        "migkit/engines/mysql.py migkit.verifiers",
        "migkit/engines/oceanbase.py migkit.verifiers",
    ),
    ("an executor asks the decision engine (`decide`) how many or how "
     "fast; the call becomes a fact handed down"): (
        "migkit/attempts.py migkit.decide",
        "migkit/ranges.py migkit.decide",
        "migkit/runner.py migkit.decide",
        "migkit/util.py migkit.decide",
    ),
    ("an executor asks the decision engine (`faults`) how many or how "
     "fast; the call becomes a fact handed down"): (
        "migkit/failpoint.py migkit.faults",
        "migkit/util.py migkit.faults",
    ),
    ("an executor asks the decision engine (`sizing`) how many or how "
     "fast; the call becomes a fact handed down"): (
        "migkit/ranges.py migkit.sizing",
    ),
    ("an executor asks the decision engine (`tuning`) how many or how "
     "fast; the call becomes a fact handed down"): (
        "migkit/ranges.py migkit.tuning",
        "migkit/throttle.py migkit.tuning",
    ),
    ("an executor calls `fleet` for a workflow step; the step is "
     "passed in as a hook"): (
        "migkit/attempts.py migkit.fleet",
        "migkit/ranges.py migkit.fleet",
    ),
    ("an executor calls `leaves` for a workflow step; the step is "
     "passed in as a hook"): (
        "migkit/ranges.py migkit.leaves",
    ),
    ("an executor calls `unchanged` for a workflow step; the step is "
     "passed in as a hook"): (
        "migkit/ranges.py migkit.unchanged",
    ),
    ("an executor says its progress through `notify`; the notice goes "
     "through an event below execution"): (
        "migkit/schedule.py migkit.notify",
    ),
    ("an executor says its progress through `verdict`; the notice goes "
     "through an event below execution"): (
        "migkit/schedule.py migkit.verdict",
    ),
    ("an executor says its progress through `wording`; the notice goes "
     "through an event below execution"): (
        "migkit/runner.py migkit.wording",
        "migkit/util.py migkit.wording",
    ),
    ("configuration reaches `audit` (persistence); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/managed.py migkit.audit",
    ),
    ("configuration reaches `carriers` (integrations); the shared "
     "piece moves below both, or the call is inverted"): (
        "migkit/hopkeys.py migkit.carriers",
    ),
    ("configuration reaches `charsets` (conversion); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/hopkeys.py migkit.charsets",
    ),
    ("configuration reaches `cloudapi` (integrations); the shared "
     "piece moves below both, or the call is inverted"): (
        "migkit/managed.py migkit.cloudapi",
    ),
    ("configuration reaches `durable` (persistence); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/config.py migkit.durable",
    ),
    ("configuration reaches `evidence` (persistence); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/config.py migkit.evidence",
    ),
    ("configuration reaches `guide` (cli); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/managed.py migkit.guide",
    ),
    ("configuration reaches `rowfilter` (scope); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/hopkeys.py migkit.rowfilter",
    ),
    ("configuration reaches `transforms` (conversion); the shared "
     "piece moves below both, or the call is inverted"): (
        "migkit/config.py migkit.transforms",
        "migkit/hopkeys.py migkit.transforms",
    ),
    ("configuration reaches `util` (execution); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/config.py migkit.util",
    ),
    ("configuration reaches `verdict` (reporting); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/config.py migkit.verdict",
    ),
    ("configuration reaches `web.roles` (web); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/hopkeys.py migkit.web.roles",
    ),
    ("engines reaches `cli` (cli); the shared piece moves below both, "
     "or the call is inverted"): (
        "migkit/engines/base.py migkit.cli",
        "migkit/engines/oracle_pump.py migkit.cli",
    ),
    ("execution reaches `engines` (engines); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/attempts.py migkit.engines",
        "migkit/util.py migkit.engines",
    ),
    ("execution reaches `tools` (integrations); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/runner.py migkit.tools",
    ),
    ("execution reaches `toolversion` (integrations); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/runner.py migkit.toolversion",
    ),
    ("integrations reaches `cli` (cli); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/integrations.py migkit.cli",
        "migkit/ship.py migkit.cli",
    ),
    ("integrations reaches `web.uses` (web); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/tooluse.py migkit.web.uses",
    ),
    ("persistence reaches `engines.base` (engines); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/kept.py migkit.engines.base",
    ),
    ("persistence reaches `engines` (engines); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/shared.py migkit.engines",
    ),
    ("persistence reaches `tooluse` (integrations); the shared piece "
     "moves below both, or the call is inverted"): (
        "migkit/anchor.py migkit.tooluse",
        "migkit/shared.py migkit.tooluse",
    ),
    ("reporting reaches `cli` (cli); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/telemetry.py migkit.cli",
    ),
    ("safety reaches `decide` (decision); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/freeze.py migkit.decide",
    ),
    ("safety reaches `engines.dbapi` (engines); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/freeze.py migkit.engines.dbapi",
    ),
    ("safety reaches `engines` (engines); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/freeze.py migkit.engines",
    ),
    ("safety reaches `sourcechange` (decision); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/freeze.py migkit.sourcechange",
    ),
    ("safety reaches `web.approve` (web); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/approvals.py migkit.web.approve",
    ),
    ("scope reaches `interlock` (safety); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/case.py migkit.interlock",
    ),
    ("scope reaches `lacks` (decision); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/case.py migkit.lacks",
    ),
    ("scope reaches `leftovers` (safety); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/result_scope.py migkit.leftovers",
    ),
    ("scope reaches `util` (execution); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/rowfilter.py migkit.util",
    ),
    ("scope reaches `verdict` (reporting); the shared piece moves "
     "below both, or the call is inverted"): (
        "migkit/result_scope.py migkit.verdict",
    ),
    ("the decision engine asks an engine (`engines.base`) what it can "
     "do; ends when the engine contract is a package below decision"): (
        "migkit/capabilities.py migkit.engines.base",
        "migkit/decide.py migkit.engines.base",
        "migkit/model.py migkit.engines.base",
        "migkit/scenarios.py migkit.engines.base",
        "migkit/workload.py migkit.engines.base",
    ),
    ("the decision engine asks an engine (`engines.dbapi`) what it can "
     "do; ends when the engine contract is a package below decision"): (
        "migkit/dba.py migkit.engines.dbapi",
        "migkit/lacks.py migkit.engines.dbapi",
    ),
    ("the decision engine asks an engine (`engines.hetero`) what it "
     "can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/capabilities.py migkit.engines.hetero",
    ),
    ("the decision engine asks an engine (`engines.mysql`) what it can "
     "do; ends when the engine contract is a package below decision"): (
        "migkit/costmodel.py migkit.engines.mysql",
        "migkit/dba.py migkit.engines.mysql",
    ),
    ("the decision engine asks an engine (`engines.oracle_logminer`) "
     "what it can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/dba.py migkit.engines.oracle_logminer",
    ),
    ("the decision engine asks an engine (`engines.oracle_redo`) what "
     "it can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/dba.py migkit.engines.oracle_redo",
    ),
    ("the decision engine asks an engine (`engines.pg_pass`) what it "
     "can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/model.py migkit.engines.pg_pass",
    ),
    ("the decision engine asks an engine (`engines.postgres`) what it "
     "can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/legs.py migkit.engines.postgres",
    ),
    ("the decision engine asks an engine (`engines.sapodp`) what it "
     "can do; ends when the engine contract is a package below "
     "decision"): (
        "migkit/ready.py migkit.engines.sapodp",
    ),
    ("the decision engine asks an engine (`engines`) what it can do; "
     "ends when the engine contract is a package below decision"): (
        "migkit/capabilities.py migkit.engines",
        "migkit/dba.py migkit.engines",
        "migkit/decide.py migkit.engines",
        "migkit/machine.py migkit.engines",
        "migkit/tableway.py migkit.engines",
        "migkit/waysaid.py migkit.engines",
    ),
    ("workflows reaches `cli` (cli); the shared piece moves below "
     "both, or the call is inverted"): (
        "migkit/ha.py migkit.cli",
        "migkit/tailtables.py migkit.cli",
    ),
}


def main(argv):
    if argv[:1] == ["--since"] and len(argv) > 1:
        print(since(argv[1]))
        return 0
    if argv[:1] == ["--symbols"]:
        _write_local()
        return 0
    written = {MAP: codemap(), STATE: state(), INVENTORY: inventory(),
               LAYERS_DOC: layers()}
    if argv[:1] == ["--check"]:
        current = all(p.exists() and p.read_text() == t
                      for p, t in written.items())
        return 0 if current else 3
    MAP.parent.mkdir(parents=True, exist_ok=True)
    for p, t in written.items():
        p.write_text(t)
    _write_local()
    print(f"wrote {MAP.relative_to(ROOT)}, {STATE.relative_to(ROOT)},"
          f" {INVENTORY.relative_to(ROOT)}, {LAYERS_DOC.relative_to(ROOT)}"
          " and .codemap/ (SYMBOLS.tsv, CALLERS.tsv, LOCAL.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
