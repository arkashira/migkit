import html
import json
import time
from pathlib import Path

CHECK_ORDER = ["schema", "counts", "autoinc", "data", "deep", "delta"]
CHECK_LABEL = {"schema": "schema", "counts": "rows", "autoinc": "autoinc",
               "data": "data", "deep": "deep", "delta": "delta"}

CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; margin: 0; }
body {
  font: 14px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif;
  background: #f4f4f2; color: #0b0b0b; padding: 32px 16px;
}
.wrap { max-width: 960px; margin: 0 auto; }
.card {
  background: #fcfcfb; border: 1px solid #e4e3df; border-radius: 6px;
  padding: 20px 24px; margin-bottom: 16px;
}
h1 { font-size: 18px; font-weight: 600; }
h2 { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
.sub { color: #52514e; font-size: 13px; margin-top: 4px; }
.route { font-family: ui-monospace, Menlo, monospace; font-size: 12.5px;
  color: #52514e; margin-top: 8px; word-break: break-all; }
.banner { display: flex; align-items: center; gap: 12px; padding: 14px 20px;
  border-radius: 6px; margin-bottom: 16px; font-weight: 650; font-size: 15px; }
.banner.pass { background: #e7f6e7; border: 1px solid #0ca30c; }
.banner.fail { background: #fdeeee; border: 1px solid #d03b3b; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit,
  minmax(140px, 1fr)); gap: 12px; margin-bottom: 16px; }
.tile { background: #fcfcfb; border: 1px solid #e4e3df; border-radius: 6px;
  padding: 14px 16px; }
.tile .n { font-size: 24px; font-weight: 600; }
.tile .l { color: #52514e; font-size: 12.5px; margin-top: 2px; }
.dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%;
  margin-right: 7px; flex: none; }
.ok .dot, .dot.ok { background: #0ca30c; }
.diff .dot, .dot.diff { background: #fab219; }
.error .dot, .dot.error { background: #d03b3b; }
.skip .dot, .dot.skip { background: #c3c2b7; }
.warn .dot, .dot.warn { background: #e07b00; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #eceae6;
  font-size: 13.5px; white-space: nowrap; }
th { color: #52514e; font-weight: 600; font-size: 12px; }
td.db { font-family: ui-monospace, Menlo, monospace; font-size: 12.5px; }
.chip { display: inline-flex; align-items: center; font-weight: 600;
  font-size: 12px; }
.finding { border: 1px solid #e4e3df; border-left: 4px solid #fab219;
  border-radius: 6px; padding: 12px 16px; margin-bottom: 10px;
  background: #fcfcfb; }
.finding.error { border-left-color: #d03b3b; }
.finding .head { display: flex; align-items: center; gap: 8px;
  font-weight: 600; margin-bottom: 4px; }
.finding .what { color: #52514e; font-size: 13px; overflow-wrap: anywhere; }
.fix { font-family: ui-monospace, Menlo, monospace; font-size: 12px;
  background: #f1f0ec; border-radius: 6px; padding: 6px 10px; margin-top: 8px;
  display: inline-block; }
details { margin-top: 8px; }
summary { cursor: pointer; color: #52514e; font-size: 12.5px; }
pre { background: #f1f0ec; border-radius: 6px; padding: 10px 12px;
  font-size: 11.5px; overflow-x: auto; margin-top: 6px; line-height: 1.45; }
footer { color: #52514e; font-size: 12px; text-align: center; margin-top: 24px; }
@media (prefers-color-scheme: dark) {
  body { background: #111110; color: #ffffff; }
  .card, .tile, .finding { background: #1a1a19; border-color: #33322f; }
  .sub, .route, .tile .l, th, summary, .finding .what, footer { color: #c3c2b7; }
  th, td { border-color: #2a2927; }
  .banner.pass { background: #14240f; }
  .banner.fail { background: #2b1414; }
  .fix, pre { background: #242422; }
}
"""


def _chip(status, label=None):
    label = label or {"ok": "OK", "diff": "DIFF", "error": "ERROR",
                      "warn": "WARN", "skip": "SKIP"}.get(status,
                                                          str(status).upper())
    return (f'<span class="chip {status}"><span class="dot {status}"></span>'
            f'{label}</span>')


def _excerpt(detail, report_path):
    for token in detail.split():
        if "/" in token and Path(token).is_file():
            report_path = token
            break
    if not report_path or not Path(report_path).is_file():
        return ""
    lines = Path(report_path).read_text(errors="replace").splitlines()[:40]
    if not lines:
        return ""
    body = html.escape("\n".join(lines))
    name = html.escape(Path(report_path).name)
    return f"<details><summary>{name}</summary><pre>{body}</pre></details>"


OBJ_TYPES = ["table", "view", "matview", "sequence", "index", "pk", "fk",
             "unique", "check", "trigger", "function", "procedure",
             "extension", "event", "collection", "invalid-index"]
OBJ_LABEL = {"table": "tables", "view": "views", "matview": "mviews",
             "sequence": "seqs", "index": "indexes", "pk": "pk", "fk": "fk",
             "unique": "uniq", "check": "check", "trigger": "trig",
             "function": "func", "procedure": "proc", "extension": "ext",
             "invalid-index": "invalid idx (src)", "event": "events",
             "collection": "colls"}


def _objects_section(hop, dbs):
    data = {}
    for d in dbs:
        rdir = hop.report_dir(d) if hasattr(hop, "report_dir") and callable(hop.report_dir) else None
        if not rdir:
            continue
        p = Path(rdir) / "objects.json"
        if p.exists():
            data[d] = json.loads(p.read_text())
    if not data:
        return ""
    types = [t for t in OBJ_TYPES
             if any(t in inv for inv in data.values())]
    head = "".join(f"<th>{OBJ_LABEL[t]}</th>" for t in types)
    rows = []
    for d, inv in data.items():
        cells = []
        for t in types:
            v = inv.get(t)
            if not v:
                cells.append("<td>-</td>")
            elif v["src"] == v["dst"] and not v["missing"] and not v["extra"]:
                cells.append(f'<td><span class="dot ok"></span>{v["src"]}</td>')
            else:
                names = ", ".join((v["missing"] + v["extra"])[:5])
                cells.append(
                    f'<td title="{html.escape(names)}">'
                    f'<span class="dot diff"></span>{v["src"]}/{v["dst"]}</td>')
        rows.append(f'<tr><td class="db">{html.escape(d)}</td>'
                    f'{"".join(cells)}</tr>')
    return (f'<div class="card"><h2>objects (source count, src/dst when'
            f' they differ)</h2><table><thead><tr><th>db</th>{head}</tr>'
            f'</thead><tbody>{"".join(rows)}</tbody></table></div>')


def _scope_section(hop, taken=None):
    """What the hop takes, under the verdict it qualifies: a `same` is
    about these and nothing else (`verdict.hop_scope`). Nothing for a hop
    that takes everything whole.

    `taken` is the verdict file of answers taken earlier (`from_summary`):
    the scope said is the one they were taken under, as it was recorded,
    and where the hop's is another now, that is said beside it. The
    report rendered today's scope over yesterday's answers, so a `same`
    read as covering what the hop takes now. A verdict file that kept no
    scope predates this, and the hop's is said as it always was."""
    from . import verdict
    if taken is not None and not taken.get("scope_fingerprint"):
        taken = None
    scope = verdict.hop_scope(hop) if taken is None else \
        (taken.get("scope") or {})
    changed = taken is not None and hasattr(hop, "scope_changed") \
        and hop.scope_changed(taken)
    if not scope and not changed:
        return ""

    def rows_of(said):
        return "".join(
            f'<tr><td class="db">{html.escape(what)}</td>'
            f'<td>{html.escape(", ".join(v[:40]))}'
            f'{" ..." if len(v) > 40 else ""}</td></tr>'
            for what, v in verdict.scope_said(said))
    rows = rows_of(scope) or ('<tr><td class="db">took</td><td>everything,'
                              ' whole</td></tr>')
    if changed:
        now = verdict.hop_scope(hop)
        rows += ('<tr><td class="db">the hop now</td><td>takes another'
                 ' scope than these answers were taken under: they are'
                 ' about the scope above, and `migkit check` takes them'
                 ' under the one below</td></tr>'
                 + (rows_of(now) or '<tr><td class="db">takes</td>'
                    '<td>everything, whole</td></tr>'))
    return (f'<div class="card"><h2>scope (what the verdict covers)</h2>'
            f'<table><tbody>{rows}</tbody></table></div>')


def _service_section(hop, results):
    """Another tool's own check of this move, table by table beside
    migkit's answers (`servicecheck`): what it missed first."""
    from . import servicecheck
    got = servicecheck.of(hop, results)
    if not got:
        return ""
    words = {"missed": "it called the same; migkit found them different",
             "not found": "it called different; migkit found the same",
             "unproved": "it gave its word; migkit's answer was neither"
                         " the same nor different",
             "skipped": "it passed over", "unchecked": "it never checked",
             "beyond": "migkit found beyond any table check",
             "not in the hop": "it checked; the hop does not take",
             "agreed": "agreed"}
    rows = "".join(
        f'<tr><td class="db">{html.escape(words[k])}'
        f' ({len(got[k])})</td><td>{html.escape("; ".join(got[k][:30]))}'
        f'{" ..." if len(got[k]) > 30 else ""}</td></tr>'
        for k in words if got.get(k))
    return (f'<div class="card"><h2>{html.escape(servicecheck.said(got))}'
            f'</h2><table><tbody>{rows}</tbody></table></div>')


#: a cell's worst answer, and what it is shown as
_WORST = ("error", "diff", "warn", "skip", "ok")


def _time_left_section(hop):
    """What is left of each task and when it ends, as its last estimate
    said (`util.EstimateHistory`): the time with its range and confidence,
    or why none is said, how old the estimate is and what to do next."""
    import time

    from .util import STALE_SECONDS, EstimateHistory, human_secs
    try:
        latest = EstimateHistory(
            hop.report_dir() / "eta-history.jsonl").latest()
    except Exception:  # noqa: BLE001 - no estimate kept, no section
        return ""
    rows = []
    try:
        held = EstimateHistory(
            hop.report_dir() / "eta-history.jsonl").calibrations()
    except Exception:  # noqa: BLE001 - no record, no calibration
        held = {}
    for scope, e in sorted(latest.items()):
        age = time.time() - float(e.get("at") or 0.0)
        old = (f" (said {human_secs(age)} ago: stale, measured again on the"
               " next run)" if age > STALE_SECONDS else "")
        rows.append(f"<tr><td>{html.escape(scope)}</td>"
                    f"<td>{html.escape(str(e.get('formatted', '')))}{old}"
                    f"</td><td>{html.escape(str(e.get('next_step', '')))}"
                    "</td></tr>")
    if not rows:
        return ""
    return ('<div class="card"><h2>time left</h2><table><thead><tr>'
            "<th>task</th><th>estimate</th><th>next</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
            f"{_calibration_said(held)}</div>")


def _calibration_said(held):
    """How the estimates of each kind of task held against when the task
    really ended (`util.EstimateHistory.calibration`): how many ended
    inside their range against the confidence they stated, and their
    median error - so the reader knows how far to trust the times above."""
    from .util import CALIBRATION_LEAST, TASK_KINDS
    said = []
    for kind, cal in sorted(held.items()):
        if not cal:
            continue
        trusted = ("" if cal["n"] < CALIBRATION_LEAST
                   or cal["coverage"] >= cal["stated"] else
                   ": below what they said, so new estimates say less")
        said.append(
            f"<li>{html.escape(TASK_KINDS.get(kind, kind))}: {cal['inside']}"
            f" of the last {cal['n']} estimates ended inside their range"
            f" (they said {cal['stated']:.0%} confidence), off by"
            f" {cal['error']:.0%} at the median{trusted}</li>")
    if not said:
        return ("<p>calibration: no task has ended since its estimates"
                " were kept, so none is held to how it ended yet</p>")
    return f"<p>calibration</p><ul>{''.join(said)}</ul>"


def _target_writes_section(hop, dbs):
    """The target's writes: kept from the application, or each of
    migkit's named by table, kind and unit (`freeze.attributed`), a write
    not accounted for an issue, never left out."""
    from . import freeze
    if not (getattr(hop, "options", None) or {}).get("protect_target"):
        return ""
    rows, issues = [], []
    for db in dbs:
        got = freeze.attributed(hop, None, db)
        rows.append(f"<tr><td>{html.escape(db)}</td><td>"
                    f"{html.escape(got['said'])}</td></tr>")
        for n in got["named"]:
            rows.append(f"<tr><td></td><td>{html.escape(n['table'] or '-')}"
                        f" {html.escape(n['kind'])}: {n['rows']} by"
                        f" {html.escape(', '.join(n['units']))}</td></tr>")
        issues += [f"<li>{html.escape(db)}: {html.escape(f)}</li>"
                   for f in got["findings"]]
    return ("<div class=\"card\"><h2>target writes</h2><table><tbody>"
            + "".join(rows) + "</tbody></table>"
            + (f"<ul>{''.join(issues)}</ul>" if issues else "") + "</div>")


def render(hop, results, generated=None, decided=None, taken=None):
    """The report of a check's answers. It reads them and changes nothing:
    the verdict is `verdict.decide`'s - the one the check's exit code and
    `verdict.json` hold - and a cell shows its worst answer, a skip as a
    skip. It once rewrote a `diff` whose detail said no row differed into
    an `ok`, in the very rows the check then counted its exit code from:
    the console said different, then "all green", and exited 0. `taken`:
    the verdict file the answers were kept with, where they were taken
    earlier - their scope is the one it recorded (`_scope_section`)."""
    from . import verdict
    generated = generated or time.strftime("%Y-%m-%d %H:%M:%S")
    results = [dict(r) for r in results]
    decided = decided or verdict.decide(results, hop)
    dbs = []
    matrix = {}
    for r in results:
        db = verdict.db_of(r.get("scope", ""))
        if db not in dbs:
            dbs.append(db)
        cell = matrix.setdefault((db, r.get("check", "")), [])
        cell.append(r)

    def cell_status(db, check):
        rs = matrix.get((db, check))
        if not rs:
            return "skip"
        seen = {r.get("status") if r.get("status") in _WORST else "error"
                for r in rs}
        return next(s for s in _WORST if s in seen)

    ran = [c for c in CHECK_ORDER
           if any(matrix.get((d, c)) for d in dbs)]
    ran += sorted({c for _, c in matrix} - set(ran))
    tiles = []
    for c in ran:
        total = sum(1 for d in dbs if matrix.get((d, c)))
        passed = sum(1 for d in dbs if cell_status(d, c) == "ok"
                     and matrix.get((d, c)))
        worst = [cell_status(d, c) for d in dbs if matrix.get((d, c))]
        status = next((s for s in _WORST if s in worst), "skip")
        tiles.append(
            f'<div class="tile"><div class="n">{passed}/{total}</div>'
            f'<div class="l"><span class="dot {status}"></span>'
            f'{html.escape(CHECK_LABEL.get(c, c).lower())}</div></div>')

    rows = []
    for d in dbs:
        cells = "".join(
            f"<td>{_chip(cell_status(d, c)) if matrix.get((d, c)) else '-'}</td>"
            for c in ran)
        rows.append(f'<tr><td class="db">{html.escape(d)}</td>{cells}</tr>')

    findings = [r for r, sev in zip(results, decided.severity)
                if sev not in ("same", "not compared") or r.get("stale")]
    cards = []
    for r in findings:
        st = str(r.get("status", ""))
        sev = "error" if st not in ("diff", "warn", "skip") else ""
        fix = r.get("fix_hint", "")
        fix_html = f'<div class="fix">{html.escape(fix)}</div>' if fix else ""
        origin = r.get("artifact_origin")
        origin_badge = (f' <span class="fix" style="margin:0;padding:2px 6px;font-size:11px;">'
                        f'{html.escape(origin)} artifact</span>') if origin else ""
        cards.append(
            f'<div class="finding {sev}"><div class="head">'
            # rows the hop keeps by the changes it does not carry
            f'{_chip(st, "KEPT" if r.get("kept_by_hop") else None)}'
            f'<span>{html.escape(str(r.get("check", "")))} &middot; '
            f'{html.escape(str(r.get("scope", "")))}</span>{origin_badge}</div>'
            f'<div class="what">{html.escape(str(r.get("detail", "")))}</div>'
            f'{fix_html}{_excerpt(str(r.get("detail", "")), r.get("report", ""))}</div>')
    if not cards:
        cards = ['<div class="sub">none</div>']

    n_all = len(results)
    said = verdict.VERDICTS[decided.status].word
    why = f" &middot; {html.escape(decided.why)}" if decided.why else ""
    banner = (
        f'<div class="banner pass"><span class="dot ok"></span>'
        f'SAME &mdash; {n_all} checks{why}</div>'
        if decided.status == "same" else
        f'<div class="banner fail"><span class="dot'
        f' {"diff" if decided.status != "error" else "error"}"></span>'
        f'{html.escape(said.upper())} &mdash; {len(findings)} of {n_all}'
        f' checks{why}</div>')

    src = f"{hop.source.user}@{hop.source.host}:{hop.source.port}"
    dst = f"{hop.target.user}@{hop.target.host}:{hop.target.port}" \
        if hop.target.configured() else "(not configured)"

    objects_html = _objects_section(hop, dbs)
    scope_html = _scope_section(hop, taken) + _service_section(hop,
                                                                results)
    from . import result_scope
    envelope = result_scope.build_grouped_summary(results, hop)
    db_summary_html = result_scope.format_html(envelope)

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit report - {html.escape(hop.name)}</title>
<style>{CSS}</style></head><body><div class="wrap">
<div class="card"><h1>{html.escape(hop.name)}</h1>
<div class="sub">migkit check / {hop.engine} / {html.escape(hop.service or "no service")} / {generated}</div>
<div class="route">{html.escape(src)} &rarr; {html.escape(dst)}</div></div>
{banner}
{scope_html}
{db_summary_html}
<div class="tiles">{"".join(tiles)}</div>
<div class="card"><h2>status</h2>
<table><thead><tr><th>db</th>
{"".join(f"<th>{html.escape(CHECK_LABEL.get(c, c).lower())}</th>" for c in ran)}
</tr></thead><tbody>{"".join(rows)}</tbody></table></div>
{objects_html}
{_target_writes_section(hop, dbs)}
{_time_left_section(hop)}
<div class="card"><h2>issues</h2>{"".join(cards)}</div>
</div></body></html>"""


def write_report(hop, results, out_path=None, generated=None, decided=None,
                 taken=None):
    out = Path(out_path) if out_path else hop.report_dir() / "report.html"
    out.write_text(render(hop, results, generated, decided, taken))
    return out


def from_summary(hop):
    """The report of the last check, from what it left on disk - its
    answers, what it was narrowed to and the scope it was taken under -
    never changing them."""
    from . import verdict
    p = hop.report_dir() / "summary.json"
    if not p.exists():
        raise SystemExit(f"no summary at {p}, run migkit check first")
    generated = time.strftime("%Y-%m-%d %H:%M:%S",
                              time.localtime(p.stat().st_mtime))
    results = json.loads(p.read_text())
    try:
        taken = json.loads((hop.report_dir() / "verdict.json").read_text())
    except (OSError, ValueError):
        taken = None
    taken = taken if isinstance(taken, dict) else None
    coverage = (taken or {}).get("coverage")
    return write_report(hop, results, None, generated,
                        verdict.decide(results, hop, coverage),
                        taken), generated


# The organisation's own change sheet (R25.3)
#
# The plan's measured phases as steps a team's task sheet maps onto: each
# step's id a hash of the hop, the phase and its table, so the same step
# of a later plan keeps its id and a sheet's rows stay mapped; what the
# run then proves is written back per step - its status, its time and the
# proof it stands on. It holds nothing beyond what the plan shows.

SHEET_FIELDS = ("id", "hop", "db", "phase", "table", "p50", "p90", "binds",
                "truth", "status", "seconds", "proof", "at")
#: what a step's phase writes into the target, as the kind it is named by
WRITES_KIND = {"copy": "copy", "index": "index", "indexes": "index",
               "catch-up": "change", "schema": "schema"}


def step_id(hop, phase, table=""):
    """A step's id: stable across plans of the same hop, phase and table."""
    import hashlib
    key = "|".join((str(getattr(hop, "name", hop)), phase, table or ""))
    return "mk-" + hashlib.sha256(key.encode()).hexdigest()[:12]


def _phase_of(p):
    name = getattr(p, "name", None) or (p.get("name") if isinstance(p, dict)
                                        else str(p))
    table = getattr(p, "table", None) or (p.get("table") if isinstance(
        p, dict) else None)
    head, _, rest = str(name).partition(" ")
    return (head, table or rest.strip()), p


def _get(p, k):
    return p.get(k) if isinstance(p, dict) else getattr(p, k, None)


def change_sheet(hop, db, phases):
    """[step] of the plan's phases (`planner.Phase`, or their dicts) of
    `db`, in the plan's order; a phase the plan did not estimate keeps
    its empty times - never a figure made up."""
    out, seen = [], set()
    for p in phases:
        (phase, table), raw = _phase_of(p)
        sid = step_id(hop, f"{db}/{phase}", table)
        if sid in seen:
            continue
        seen.add(sid)
        out.append({"id": sid, "hop": str(hop.name), "db": db,
                    "phase": phase, "table": table,
                    "p50": _get(raw, "p50"), "p90": _get(raw, "p90"),
                    "binds": _get(raw, "binds") or "",
                    "truth": _get(raw, "truth") or "",
                    "status": "planned", "seconds": None, "proof": "",
                    "at": ""})
    return out


def _sheet_paths(hop):
    d = hop.report_dir()
    return d / "change-sheet.json", d / "change-sheet.csv"


def write_change_sheet(hop, steps):
    """The sheet kept as JSON and CSV beside the hop's reports; a step
    already there keeps the evidence written back to it."""
    import csv
    js, cs = _sheet_paths(hop)
    before = {s["id"]: s for s in read_change_sheet(hop)}
    merged = []
    for s in steps:
        was = before.pop(s["id"], None)
        if was and was.get("status") != "planned":
            s = {**s, **{k: was.get(k) for k in
                         ("status", "seconds", "proof", "at")}}
        merged.append(s)
    merged += list(before.values())
    js.write_text(json.dumps(merged, indent=1))
    with open(cs, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SHEET_FIELDS, extrasaction="ignore")
        w.writeheader()
        for s in merged:
            w.writerow({k: "" if s.get(k) is None else s.get(k)
                        for k in SHEET_FIELDS})
    return js, cs


def read_change_sheet(hop):
    try:
        got = json.loads(_sheet_paths(hop)[0].read_text())
    except (OSError, ValueError):
        return []
    return [s for s in got if isinstance(s, dict) and s.get("id")]


def write_back(hop, sid, status, seconds, proof, rows=None, unit=None):
    """The run's evidence written back to step `sid`: its status (done,
    failed, ...), the seconds it took and the proof it stands on. A step
    whose phase wrote into the target names that write (`freeze.name_write`)
    under the step's id as its unit. Raises where no step has that id - an
    evidence line that maps onto nothing is a fault, not a row dropped."""
    from . import freeze
    steps = read_change_sheet(hop)
    step = next((s for s in steps if s["id"] == sid), None)
    if step is None:
        raise SystemExit(f"{hop.name}: no step {sid} in the change sheet;"
                         " export it from the plan first")
    step.update(status=str(status), seconds=None if seconds is None
                else round(float(seconds), 3), proof=str(proof or ""),
                at=time.strftime("%Y-%m-%d %H:%M:%S"))
    kind = WRITES_KIND.get(step["phase"])
    if kind and status == "done":
        freeze.name_write(hop, step["db"], step["table"], kind,
                          unit or sid, rows or 1)
    write_change_sheet(hop, steps)
    return step


def step_ended(hop, db, phase, status, seconds, proof, rows=None):
    """A run's phase of `db` ended at its boundary: every step of the
    hop's change sheet that is that phase (`copy`, `index`, `catch-up`,
    `check`, `cutover`) gets the evidence written back (`write_back`).
    A step with a table takes no seconds of its own - the phase's time
    is not split by guess - and says so in its proof. Nothing where the
    hop keeps no sheet or the sheet has no such step; a step already
    done is left as it is. Returns the ids written."""
    out = []
    for s in read_change_sheet(hop):
        if s.get("db") != db or s.get("phase") != phase \
                or s.get("status") == "done":
            continue
        alone = not s.get("table")
        write_back(hop, s["id"], status, seconds if alone else None,
                   str(proof) if alone else
                   f"{proof} (the phase's {seconds:.1f}s is not split by"
                   " table)" if seconds is not None else str(proof),
                   rows=rows if alone else None)
        out.append(s["id"])
    return out


def check_ended(hop, results, severity, took, coverage=None):
    """The check's end written back to the change sheet's check steps, one
    database at a time (`step_ended`): `done` where every answer of that
    database held, `failed` where one did not, and `partial` where the run
    was narrowed (`coverage`) so that the whole check was not taken. The
    proof says which, and the one next step. Returns the ids written; a
    sheet that cannot be written is said by the caller."""
    from . import verdict
    held = ("same", "note")
    by_db = {}
    for r, sev in zip(results, severity):
        by_db.setdefault(verdict.db_of(r.get("scope", "")), []).append(sev)
    out = []
    for d, sevs in sorted(by_db.items()):
        bad = sum(1 for x in sevs if x not in held)
        if bad:
            status = "failed"
            proof = (f"{bad} of {len(sevs)} answers did not hold or were not compared; open the"
                     " check report and fix what it lists, then check again")
        elif coverage:
            status = "partial"
            proof = ("every answer taken held, but the run was narrowed to "
                     + ", ".join(sorted(str(k) for k in coverage))
                     + "; check again without narrowing to prove the whole"
                     " hop")
        else:
            status = "done"
            proof = f"all {len(sevs)} answers held"
        out += step_ended(hop, d, "check", status, took, proof)
    return out


# What an operator is told where a command stops or finds a fault: what
# happened, what it means and the one next step. Said here once, so that
# every site of the CLI words it the same way.

def _t(x):
    return str(x).strip().rstrip(".")


def restore_unreachable(ts, backend, hop):
    return (f"state {ts} not retrievable from {backend}: the saved state"
            " could not be fetched, so nothing was put back. Check access"
            " to that backend and that the saved files are still there,"
            f" then run `migkit rollback {hop} --state {ts}` again; do not"
            " apply another state by guess")


def record_changed(entry, hop):
    return (f"the record has been changed: {_t(entry)}. Keep this record and"
            " the saved run logs as they are and look into the change"
            f" (`migkit history {hop}`) before relying on this history for"
            " a rollback or an audit; do not rewrite the record to clear"
            " this warning")


def no_saved_states(hop):
    return ("no saved states: there is no saved state of the counters to"
            f" put back. Run `migkit history {hop}` to see what recovery"
            " evidence there is; if none fits, stop this rollback and get"
            " an approved recovery plan, and save a state with"
            f" `migkit sync {hop} --go` before the next repair")


def schema_failed(count, whole, failed, hop):
    return (f"{count} could not be created on the target"
            + ("; nothing else was either - the schema is created in one"
               " transaction, whole or not at all" if whole else
               "; what was created stays on the target")
            + ":\n  " + "\n  ".join(failed)
            + f"\nRead the statements above and the target's permissions,"
            " put right what they name, then run"
            f" `migkit schema {hop} --convert` again to review what is"
            " left before applying it")


def mask_refused(hop, db, refused):
    return (f"{hop}: {db}: the hop asks to mask "
            + "; ".join(f"{n} ({_t(why)})" for n, why in refused)
            + ". Nothing has been started. Correct the mapping or column"
            f" types of the named columns, then run `migkit move {hop}`"
            " again without --go to review the plan; the move stays"
            " blocked until every masked column is supported")


def two_way_refused(why, hop):
    return (f"topology: two_way - {_t(why)}. Nothing was set up. Put right"
            " what that names so changes cannot loop between the sides,"
            f" then run `migkit doctor` and `migkit move {hop}` without"
            " --go to review the plan before replication is authorised")


def replication_refused(said, hop):
    return (f"{_t(said)}. Nothing was set up. Put right what that names, then"
            f" run `migkit doctor` and run `migkit move {hop}` again"
            " without --go to review the plan before it is carried out")


def copy_not_started(db, why, hop):
    return (f"{db}: not started - {_t(why)}. Nothing was copied. Put right"
            f" what that names, run `migkit doctor`, then run"
            f" `migkit move {hop}` again without --go to review the copy"
            " plan")


def repair_stopped(db, said, hop):
    return (f"{db}: {_t(said)}. The repair was stopped before any statement"
            " was run. Put right what that names, then run"
            f" `migkit sync {hop}` again without --apply to review the"
            " plan; compare the target with the saved repair journal"
            " first if an earlier try may have written part of it")


def scope_empty(hop):
    return ("the source lists no database this hop covers, so nothing is"
            " compared - the user migkit signs in as may not see them."
            " Check the database names and the include and exclude rules,"
            " ask the database owner to confirm that this login can list"
            f" and read them, run `migkit doctor`, then `migkit check {hop}`")


def config_unreadable(path, err):
    return (f"{path} is not YAML migkit can read: {_t(err)}. Correct the"
            " place it names without removing the hops already in the"
            " file, then run `migkit doctor` again")


def probe_unasked(hop, side, what, now):
    return (f"{hop} {side}: {what} - could not be asked ({now}). Nothing"
            " is known about it yet: ask the database owner to put right"
            " the error or the missing permission, then run"
            " `migkit doctor` again to ask it afresh")


def transport_failed(side, state, detail):
    what = "verified" if state == "unverified" else "encrypted"
    return (f"connection NOT {what}: {_t(detail)}. Data on this connection"
            f" can be read or changed on its way. Set up TLS with"
            f" certificate checking for the {side}'s endpoint (or the"
            " protected tunnel the hop supports), then run"
            " `migkit doctor` again and move no data until this passes")


def port_silent(side, tcp):
    return (f"link: the {side}'s port does not answer ({_t(tcp)}). Nothing"
            " can be read or written there. Check the host and port in"
            " the hop, the network route, the firewall and that the"
            " database is listening, then run `migkit doctor` again")


def hop_not_reread(err, hop):
    return (f"the hop could not be read again ({_t(err)}); this cycle goes by"
            " the one read before, so a scope or setting changed since is"
            " not seen. Put right the file or its read access; do not"
            " approve a scope change or a cutover on this cycle, and"
            f" confirm that the next cycle of `{hop}` reads it again")


def address_unreadable(side):
    return (f"no {side} address migkit could read. Give it as"
            f" MIGKIT_{side.upper()}=<engine>://<user>@<host>:<port>/<db>"
            " (for example postgres://app@db1:5432/shop), or run"
            " `migkit init` in a terminal and enter it again")


def source_unreachable(err):
    return (f"could not reach the source: {_t(err)}. Nothing about the source"
            " is verified yet. Check its address, the network route, the"
            " login and its read permission, then run `migkit init` again")


#: what a way's `leaves` words name on a server, and which side it is on
#: ("" is the side(s) the way loads)
SERVER_OBJECTS = (
    ("replication slot", "src"), ("slot", "src"), ("publication", "src"),
    ("trigger", "src"), ("user mapping", "dst"), ("subscription", "dst"),
    ("extension", "dst"), ("server", "dst"), ("schema", ""), ("table", ""),
    ("role", ""), ("user", ""))
_OBJECT_RE = None
#: the words of a way that keeps files on this machine
_FILES_HERE = ("file", "directory", "dump")
_SIDE_NAME = {"src": "source", "dst": "target"}


def _objects_re():
    global _OBJECT_RE
    if _OBJECT_RE is None:
        import re
        names = "|".join(re.escape(n) for n, _ in SERVER_OBJECTS)
        _OBJECT_RE = re.compile(rf"\b({names})(s?)\b")
    return _OBJECT_RE


def server_objects(leaves):
    """[(object, side, plural)] a way's `leaves` words name on the
    servers: nothing where it says nothing is left there, and None where
    it does not say."""
    low = (leaves or "").lower()
    if not low:
        return None
    if "nothing on the servers" in low:
        return []
    found, side_of = [], dict(SERVER_OBJECTS)
    for kind, plural in _objects_re().findall(low):
        row = (kind, side_of[kind], bool(plural))
        if row not in found:
            found.append(row)
    return found


def _files_here(leaves):
    low = (leaves or "").lower()
    return "on this machine" in low and any(w in low for w in _FILES_HERE)


def ways_of(hop, eng, stream_climb):
    """[(leg, rung or None)] the hop's whole-database move and the follow
    of its changes stand on, read from the decision engine's own climbs
    (`movers`, `stream_climb`) read-only, as `waysaid` reads them."""
    from . import capabilities, decide, movers, waysaid
    from .engines import ALIASES
    out = []
    try:
        engine = ALIASES.get(hop.engine, hop.engine)
        base, _ = movers.for_hop(hop, engine)
        via, _ = movers.fitted(hop, engine, base)
        out.append(("the whole-database move", waysaid.rung_named(via)))
    except (Exception, SystemExit):  # noqa: BLE001 - said as not chosen
        out.append(("the whole-database move", None))
    if capabilities.unavailable(hop.engine, "stream"):
        return out
    try:
        if eng.maps_columns():
            rung = next((r for r in movers.stream_ways()
                         if r.name == "tail"), None)
        else:
            facts, rungs, _, work = stream_climb(hop, eng, None, False)
            rung = decide.climb(work, rungs, facts, costs=facts["costs"],
                                prove=False).rung
    except (Exception, SystemExit):  # noqa: BLE001 - said as not chosen
        rung = None
    out.append(("following its changes", rung))
    return out


def footprint_lines(hop, databases, plan, sides, worker_memory, carried,
                    ways, here=None, spool_free=None):
    """The numbers a hop's move takes, a line per side and leg, from the
    owners' own figures and none made here: the connections each server
    gets to open (`sizing.Plan`, `sizing.CONNECTIONS_PER_WORKER`, the
    server's free ones), the memory this machine holds (the workers the
    plan runs at `sizing.Plan` times what one holds, `model.Model`), the
    disk the ways keeping files here take (the bytes the move carries),
    and the objects each way leaves on the servers (`decide.Rung.leaves`,
    a number a database)."""
    from . import sizing
    from .movers import DRIVEN
    from .wording import human_bytes, without_programs
    out = []
    per = sizing.CONNECTIONS_PER_WORKER
    for side in ("src", "dst"):
        c = sides.get(side) or {}
        free = c.get("free_connections")
        out.append(
            f"{side}: opens {plan.start * per} connections at the start,"
            f" up to {plan.most * per} as it goes ({per} a worker, "
            + (f"{plan.start} to {plan.most} workers)" if plan.most
               > plan.start else f"{plan.start} workers)")
            + (f"; the server has {free} free" if free is not None
               else "; how many the server has free could not be read"))
    each = int(worker_memory or sizing.WORKER_MEMORY)
    have = (here or {}).get("memory")
    out.append(
        f"this machine: memory {human_bytes(plan.start * each)} at the"
        f" start, up to {human_bytes(plan.most * each)} as it goes"
        f" ({human_bytes(each)} a worker)"
        + (f"; {human_bytes(have)} free here" if have else
           "; the free memory here could not be read"))
    held = [(leg, r) for leg, r in ways if r is not None
            and _files_here(r.leaves)]
    for leg, r in held:
        if carried:
            line = (f"this machine: disk spool up to"
                    f" {human_bytes(carried)} for {leg}, the bytes the"
                    " move carries before any compression")
            if spool_free is not None and carried > spool_free:
                line += (f" - NOT ENOUGH, {human_bytes(spool_free)} free"
                         " where the reports are kept")
            elif spool_free is not None:
                line += f"; {human_bytes(spool_free)} free"
        else:
            line = (f"this machine: {leg} keeps files here; the bytes it"
                    " carries are not known yet, so its disk spool is not")
        out.append(line)
    if not held:
        out.append("this machine: no disk spool - no way chosen keeps files"
                   " here")
    n = max(1, len(databases or []))
    for leg, r in ways:
        if r is None:
            out.append(f"{leg}: what it creates on the servers is said when"
                       " its way is chosen, as the move runs")
            continue
        objs = server_objects(r.leaves)
        if objs is None:
            out.append(f"{leg}: what it creates on the servers is not said")
        elif not objs:
            out.append(f"{leg}: creates nothing on the servers")
        else:
            by = {}
            for kind, side, plural in objs:
                where = side or "src and dst"
                by.setdefault(where, []).append(
                    f"{kind}{'s' if plural else ''} "
                    + ("(several a database)" if plural else
                       f"x{n} (one a database, {n} database"
                       f"{'s' if n != 1 else ''})"))
            out.append(f"{leg}: creates " + "; ".join(
                f"on {w}: {', '.join(v)}" for w, v in by.items()))
    return [without_programs(line, DRIVEN) for line in out]
