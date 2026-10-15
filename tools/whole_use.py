#!/usr/bin/env python3
"""Every wrapped program and library, used whole (backlog 0f), as a registry
written from the code.

`migkit/tooluse.py` holds, per tool, what migkit uses (read off the code that
drives it), what it does not and why, and what it does itself better. This
walks the package for every call out to a program or import of a library,
sets the tool-use registry against `tools/whole_use_catalogue.json` (what each
tool can do, by capability), and writes:

* `docs/research/whole-use-2026-10-16.md`: per tool the options used, unused
  and topped up, per capability its state, and the programs and libraries
  migkit calls that have no account yet;
* the 0f audit block in `docs/backlog.md`: one row per unused capability that
  would deepen a rung, and one per call with no account.

A capability's state is computed from the registry, never written: used,
topped up, partly used or unused. A catalogue key the registry does not hold,
or a candidate that is now used, is an error: the catalogue is behind.

    python tools/whole_use.py            write both
    python tools/whole_use.py --check    exit 3 where either is behind
"""
import ast
import collections
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from migkit import tooluse  # noqa: E402

PKG = ROOT / "migkit"
CATALOGUE = ROOT / "tools" / "whole_use_catalogue.json"
DOC = ROOT / "docs" / "research" / "whole-use-2026-10-16.md"
BACKLOG = ROOT / "docs" / "backlog.md"
BEGIN = "<!-- whole-use:begin (written by tools/whole_use.py) -->"
END = "<!-- whole-use:end -->"
#: the process calls that start a program
_RUNNERS = {"run", "Popen", "check_output", "check_call", "call", "_run", "sh"}
PLURAL = {"library": "libraries", "program": "programs"}
STATE_ORDER = ("used", "topped up", "partly used", "unused")


def catalogue():
    return json.loads(CATALOGUE.read_text())


def _norm(name):
    return name.lower().replace("-", "_").replace(".", "_")


def _matches(imp, tool):
    a, b = _norm(imp), _norm(tool)
    return a == b or b.startswith(a + "_") or a.startswith(b + "_")


def walk():
    """({import: files}, {program: files}, {tool: registering file}) over the
    package: the third-party libraries imported, the programs started, and
    the module whose `register(...)` gives each tool its account."""
    std = sys.stdlib_module_names
    imports = collections.defaultdict(set)
    programs = collections.defaultdict(set)
    registered = {}
    for path in sorted(PKG.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        tree = ast.parse(path.read_text())
        consts = {t.id: n.value.value for n in tree.body
                  if isinstance(n, ast.Assign)
                  and isinstance(n.value, ast.Constant)
                  and isinstance(n.value.value, str)
                  for t in n.targets if isinstance(t, ast.Name)}
        for node in ast.walk(tree):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                mods = [node.module or ""]
            for m in mods:
                top = m.split(".")[0]
                if top and top not in std and top != "migkit":
                    imports[top].add(rel)
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(
                fn, "id", "")
            first = node.args[0] if node.args else None
            if name == "register" and first is not None:
                label = (first.value if isinstance(first, ast.Constant)
                         else consts.get(getattr(first, "id", "")))
                if isinstance(label, str):
                    registered.setdefault(label, rel)
            elif name == "which" and isinstance(first, ast.Constant) \
                    and isinstance(first.value, str):
                programs[first.value].add(rel)
            elif name in _RUNNERS and isinstance(first, (ast.List, ast.Tuple)) \
                    and first.elts and isinstance(first.elts[0], ast.Constant) \
                    and isinstance(first.elts[0].value, str):
                programs[first.elts[0].value].add(rel)
    return imports, programs, registered


def _tool_for_import(imp, cat, names):
    alias = cat["import_aliases"].get(imp)
    if alias in names:
        return alias
    return next((n for n in names if _matches(imp, n)), None)


def sites(cat, names):
    """({tool: files that call it}, [(kind, name, files)] called with no
    account and not a system program)."""
    imports, programs, registered = walk()
    out = {n: set() for n in names}
    for n, rel in registered.items():
        if n in out:
            out[n].add(rel)
    for n in names:
        reads = tooluse.REGISTRY[n].reads
        for rel in (reads[0] if reads else ()):
            out[n].add("migkit/" + rel.partition("::")[0])
    loose = []
    for imp, files in sorted(imports.items()):
        tool = _tool_for_import(imp, cat, names)
        if tool:
            out[tool] |= files
        else:
            loose.append(("library", imp, sorted(files)))
    for prog, files in sorted(programs.items()):
        tool = cat["program_aliases"].get(prog, prog)
        if tool in out:
            out[tool] |= files
        elif prog not in cat["system_programs"]:
            loose.append(("program", prog, sorted(files)))
    return {n: sorted(f) for n, f in out.items()}, loose


def capability_state(states):
    """used / topped up / partly used / unused from the states of the
    options a capability covers."""
    got = set(states)
    if got == {"unused"}:
        return "unused"
    if "unused" in got:
        return "partly used"
    return "topped up" if "topped up" in got else "used"


def build():
    """The facts everything is written from: the registry, the catalogue
    held against it, the walk."""
    cat = catalogue()
    registry = tooluse.tools_used()
    names = sorted(registry)
    problems = []
    flat = {n: {**r["used"], **r["unused"], **r["topped up"]}
            for n, r in registry.items()}
    state_of = {n: {k: s for s in ("used", "unused", "topped up")
                    for k in registry[n][s]} for n in names}
    caps = {}
    for tool, entry in cat["tools"].items():
        if tool not in registry:
            problems.append(f"catalogue names {tool}, which the registry"
                            " does not hold")
            continue
        rows = []
        for c in entry["capabilities"]:
            missing = [k for k in c["covers"] if k not in flat[tool]]
            if missing:
                problems.append(f"{tool} {c['id']}: the registry holds no"
                                f" {', '.join(missing)}")
                continue
            state = capability_state(state_of[tool][k] for k in c["covers"])
            if c.get("candidate") and state in ("used", "topped up"):
                problems.append(f"{tool} {c['id']}: marked a candidate and"
                                f" now {state}; clear the mark")
            if c.get("candidate") and not c.get("rung"):
                problems.append(f"{tool} {c['id']}: a candidate names no"
                                " rung")
            why = flat[tool][c["covers"][0]]
            rows.append({**c, "state": state, "why": why})
        caps[tool] = rows
    where, loose = sites(cat, names)
    return {"cat": cat, "registry": registry, "caps": caps, "where": where,
            "loose": loose, "problems": problems}


def _cell(text):
    return str(text).replace("|", "/").replace("\n", " ")


def render(facts):
    reg, caps, where = facts["registry"], facts["caps"], facts["where"]
    out = ["# Every wrapped program and library: used, unused, topped up",
           "",
           "Written by `tools/whole_use.py` from the tool-use registry"
           " (`migkit/tooluse.py`), `tools/whole_use_catalogue.json` and a"
           " walk of the package. Do not edit; run the tool. Internal: the"
           " tools are named here and nowhere an operator reads.",
           "",
           "## Per tool",
           "",
           "| tool | licence | used | topped up | unused | capabilities"
           " (used / topped / partly / unused) | called from |",
           "|---|---|---|---|---|---|---|"]
    for n, r in reg.items():
        c = collections.Counter(x["state"] for x in caps.get(n, ()))
        cc = ("-" if n not in caps else
              " / ".join(str(c[s]) for s in STATE_ORDER))
        files = where.get(n, [])
        shown = ", ".join(f"`{f}`" for f in files[:3])
        if len(files) > 3:
            shown += f" +{len(files) - 3}"
        out.append(f"| {n} | {_cell(r['licence'])} | {len(r['used'])} |"
                   f" {len(r['topped up'])} | {len(r['unused'])} | {cc} |"
                   f" {shown or '-'} |")
    out += ["", f"{len(reg)} tools in the registry; {len(caps)} also have a"
            " capability catalogue. The rest are accounted option by option"
            " in the registry and have no capability view yet.", ""]
    out.append("## Capabilities, tool by tool")
    for n, rows in caps.items():
        out += ["", f"### {n}", "",
                f"Catalogued against {facts['cat']['tools'][n]['release']}.",
                "", "| capability | state | covers | why (registry) | rung it"
                " would strengthen |", "|---|---|---|---|---|"]
        for x in sorted(rows, key=lambda x: STATE_ORDER.index(x["state"])):
            cov = ", ".join(f"`{k}`" for k in x["covers"])
            rung = x.get("rung", "") if x.get("candidate") else ""
            out.append(f"| {_cell(x['what'])} | {x['state']} | {cov} |"
                       f" {_cell(x['why'])} | {_cell(rung) or '-'} |")
    out += ["", "## Called, with no account in the registry", "",
            "A library imported or a program started that no tool-use entry"
            " accounts for. Each is a row under 0f: give it an entry, option"
            " by option, as the others have.", "",
            "| kind | name | files |", "|---|---|---|"]
    for kind, name, files in facts["loose"]:
        shown = ", ".join(f"`{f}`" for f in files[:3])
        if len(files) > 3:
            shown += f" +{len(files) - 3}"
        out.append(f"| {kind} | {name} | {shown} |")
    out += ["", "System programs left out (no account wanted): "
            + ", ".join(f"`{p}`" for p in sorted(
                facts["cat"]["system_programs"])) + ".", ""]
    return "\n".join(out)


def backlog_rows(facts):
    """The 0f audit block: a row per unused or partly used candidate
    capability, and one per call with no account."""
    lines = [BEGIN, "",
             "*0f whole-use audit (2026-10-16, written by"
             " `tools/whole_use.py`; the full table is"
             " `docs/research/whole-use-2026-10-16.md`).*", ""]
    for n, rows in facts["caps"].items():
        for x in rows:
            if x.get("candidate") and x["state"] in ("unused", "partly used"):
                lines.append(f"* `0f.{n}.{x['id']}` ({x['state']}): {x['what']}"
                             f" would strengthen {x['rung']}.")
    for kind in ("library", "program"):
        names = [n for k, n, _ in facts["loose"] if k == kind]
        if names:
            lines.append(f"* `0f.account.{kind}`: {len(names)} {PLURAL[kind]} are"
                         " called with no tool-use account (each written"
                         " option by option, as the others are):"
                         f" {', '.join(f'`{n}`' for n in names)}.")
    lines += ["", END]
    return "\n".join(lines)


def _backlog_with(block):
    text = BACKLOG.read_text()
    if BEGIN in text:
        head, _, rest = text.partition(BEGIN)
        _, _, tail = rest.partition(END)
        return head + block + tail
    anchor = "\n**0f. "
    start = text.index(anchor) + 1
    nxt = text.find("\n**", start + 5)
    nxt = len(text) if nxt < 0 else nxt
    return text[:nxt].rstrip("\n") + "\n\n" + block + "\n" + text[nxt:]


def main(argv):
    facts = build()
    if facts["problems"]:
        print("the catalogue is behind the registry:", *facts["problems"],
              sep="\n  ", file=sys.stderr)
        return 2
    doc = render(facts) + "\n"
    backlog = _backlog_with(backlog_rows(facts))
    if "--check" in argv:
        behind = [p.name for p, new in ((DOC, doc), (BACKLOG, backlog))
                  if not p.exists() or p.read_text() != new]
        if behind:
            print(f"{', '.join(behind)} behind the code: run python"
                  " tools/whole_use.py", file=sys.stderr)
            return 3
        return 0
    DOC.write_text(doc)
    BACKLOG.write_text(backlog)
    print(f"wrote {DOC.relative_to(ROOT)} and the 0f block of"
          f" {BACKLOG.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
