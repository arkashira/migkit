import html
import json
import time
from pathlib import Path

CHECK_ORDER = ["schema", "counts", "autoinc", "data"]
CHECK_LABEL = {"schema": "schema", "counts": "rows",
               "autoinc": "autoinc", "data": "data"}

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
.tiles { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px;
  margin-bottom: 16px; }
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


def _chip(status):
    label = {"ok": "OK", "diff": "DIFF", "error": "ERROR",
             "skip": "SKIP"}.get(status, status.upper())
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
        p = hop.report_dir(d) / "objects.json"
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


def _reclassify(results):
    """A data row whose drilldown found no differing rows is a settled
    checksum flicker (in-flight replication), not a real diff."""
    for r in results:
        if (r.get("check") == "data" and r.get("status") == "diff"
                and "missing=0 extra=0 changed=0" in r.get("detail", "")):
            r["status"] = "ok"
            r["detail"] = "checksum flicker settled, 0 rows differ"
    return results


def render(hop, results, generated=None):
    generated = generated or time.strftime("%Y-%m-%d %H:%M:%S")
    results = _reclassify(results)
    dbs = []
    matrix = {}
    for r in results:
        db = r["scope"].split()[0].split(".")[0].replace("(migra)", "").strip()
        if db not in dbs:
            dbs.append(db)
        cell = matrix.setdefault((db, r["check"]), [])
        cell.append(r)

    def cell_status(db, check):
        rs = matrix.get((db, check))
        if not rs:
            return "skip"
        worst = "ok"
        for r in rs:
            if r["status"] == "error":
                return "error"
            if r["status"] == "diff":
                worst = "diff"
        return worst

    tiles = []
    for c in CHECK_ORDER:
        total = sum(1 for d in dbs if matrix.get((d, c)))
        passed = sum(1 for d in dbs if cell_status(d, c) == "ok"
                     and matrix.get((d, c)))
        status = "ok" if passed == total and total else "diff"
        if any(cell_status(d, c) == "error" for d in dbs):
            status = "error"
        tiles.append(
            f'<div class="tile"><div class="n">{passed}/{total}</div>'
            f'<div class="l"><span class="dot {status}"></span>'
            f'{CHECK_LABEL[c].lower()}</div></div>')

    rows = []
    for d in dbs:
        cells = "".join(
            f"<td>{_chip(cell_status(d, c)) if matrix.get((d, c)) else '-'}</td>"
            for c in CHECK_ORDER)
        rows.append(f'<tr><td class="db">{html.escape(d)}</td>{cells}</tr>')

    findings = [r for r in results if r["status"] not in ("ok", "skip")]
    cards = []
    for r in findings:
        sev = "error" if r["status"] == "error" else ""
        fix = r.get("fix_hint", "")
        fix_html = f'<div class="fix">{html.escape(fix)}</div>' if fix else ""
        cards.append(
            f'<div class="finding {sev}"><div class="head">{_chip(r["status"])}'
            f'<span>{html.escape(r["check"])} &middot; '
            f'{html.escape(r["scope"])}</span></div>'
            f'<div class="what">{html.escape(r.get("detail", ""))}</div>'
            f'{fix_html}{_excerpt(r.get("detail", ""), r.get("report", ""))}</div>')
    if not cards:
        cards = ['<div class="sub">none</div>']

    n_bad = len(findings)
    n_all = len(results)
    banner = (
        f'<div class="banner pass"><span class="dot ok"></span>'
        f'PASS &mdash; {n_all}/{n_all} checks</div>'
        if n_bad == 0 else
        f'<div class="banner fail"><span class="dot error"></span>'
        f'FAIL &mdash; {n_bad} of {n_all} checks</div>')

    src = f"{hop.source.user}@{hop.source.host}:{hop.source.port}"
    dst = f"{hop.target.user}@{hop.target.host}:{hop.target.port}" \
        if hop.target.configured() else "(not configured)"

    objects_html = _objects_section(hop, dbs)

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit report - {html.escape(hop.name)}</title>
<style>{CSS}</style></head><body><div class="wrap">
<div class="card"><h1>{html.escape(hop.name)}</h1>
<div class="sub">migkit check / {hop.engine} / {html.escape(hop.service or "no service")} / {generated}</div>
<div class="route">{html.escape(src)} &rarr; {html.escape(dst)}</div></div>
{banner}
<div class="tiles">{"".join(tiles)}</div>
<div class="card"><h2>status</h2>
<table><thead><tr><th>db</th>
{"".join(f"<th>{CHECK_LABEL[c].lower()}</th>" for c in CHECK_ORDER)}
</tr></thead><tbody>{"".join(rows)}</tbody></table></div>
{objects_html}
<div class="card"><h2>issues</h2>{"".join(cards)}</div>
</div></body></html>"""


def write_report(hop, results, out_path=None, generated=None):
    out = Path(out_path) if out_path else hop.report_dir() / "report.html"
    out.write_text(render(hop, results, generated))
    return out


def from_summary(hop):
    p = hop.report_dir() / "summary.json"
    if not p.exists():
        raise SystemExit(f"no summary at {p}, run migkit check first")
    generated = time.strftime("%Y-%m-%d %H:%M:%S",
                              time.localtime(p.stat().st_mtime))
    return write_report(hop, json.loads(p.read_text()),
                        None, generated), generated
