"""The dashboard and `/metrics` (`migkit report --serve`).

What a hop's last check ended as is its verdict (`verdict.json`, decided
by `verdict.decide`), shown and exported in the words of the one table
of verdicts (`verdict.VERDICTS`). Measured before: the page and the
metrics counted every `skip` as a difference - a clean two-database MySQL
check, whose settings are compared on the first database only, paged
`MigkitHopDifferent` while its verdict said `same` - and a check narrowed
to one table showed `ok` where the verdict said `incomplete`. A check's
tile and a database's row are the same rule over their own answers.

`/metrics` is served from a snapshot one thread takes every
`Collector.EVERY` seconds, with its age and the readings that failed
(`migkit_collect_age_seconds`, `migkit_collect_errors_total`): each
scrape used to load the hops, read every record whole, start a program
and open connections - serially, past Prometheus's 10 s - and a broken
hops file answered nothing at all. Every sample is written by one helper
(`_sample`) that escapes every label value, grouped by family with its
help and type (`FAMILIES`): counters end in `_total`, no label is
`quantile`, labels hold keys, not sentences. The families renamed keep
their old names beside the new until 1.0, their help saying so.

Who may see what: the holder of the address this start printed, once;
a person a signing-in proxy names in its header, only with the secret
the proxy sends (`MIGKIT_UI_PROXY_SECRET`) - before it, any local process
sending that header was signed in; `/metrics` for a scraper with the
bearer `MIGKIT_METRICS_TOKEN`, or anyone signed in, each person seeing
only the hops their `access` gives them. The page runs no inline code
and is served with a policy allowing only its own script and style.
"""
import base64
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import REPORTS, load_hops

#: a verdict's colour, as the page draws it
_SHADE = {"green": "var(--ok)", "yellow": "var(--diff)", "red": "var(--err)"}
#: what a hop never checked is called, beside the verdicts
NONE = "none"
NONE_WORD = "not checked"


def _verdicts():
    """{status: `verdict.Verdict`}: the one table every output renders a
    verdict from."""
    from .verdict import VERDICTS
    return VERDICTS


def _css():
    shades = "\n".join(
        f".pill.{k} {{ color: {_SHADE.get(v.colour, 'var(--na)')}; }}"
        f" .dot.{k} {{ background: {_SHADE.get(v.colour, 'var(--na)')}; }}"
        f" .tile.{k}, .verdict.{k} {{ background: var(--{_BG.get(v.colour, 'card')}); }}"
        for k, v in _verdicts().items())
    return _BASE_CSS + shades + "\n.dot.error { animation: pulse 1.6s infinite; }\n"


_BG = {"green": "ok-bg", "yellow": "diff-bg", "red": "err-bg"}

_BASE_CSS = """
:root {
  color-scheme: light dark;
  --bg: #f6f6f4; --card: #ffffff; --line: #e6e4df; --ink: #171614;
  --mut: #6d6a63; --mono: ui-monospace, "SF Mono", Menlo, monospace;
  --ok: #16a34a; --diff: #d97706; --err: #dc2626; --na: #a8a29e;
  --ok-bg: #f0fdf4; --diff-bg: #fffbeb; --err-bg: #fef2f2;
  --shadow: 0 1px 2px rgba(23,22,20,.04), 0 4px 16px rgba(23,22,20,.05);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #121210; --card: #1c1b19; --line: #33312d; --ink: #f4f3f0;
    --mut: #a5a199; --ok-bg: #12210f; --diff-bg: #271d0a; --err-bg: #2a1212;
    --shadow: 0 1px 2px rgba(0,0,0,.4);
  }
}
* { box-sizing: border-box; margin: 0; }
body { font: 14px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif;
  background: var(--bg); color: var(--ink); padding: 28px 18px 40px; }
.wrap { max-width: 1180px; margin: 0 auto; }
header { display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap;
  margin-bottom: 18px; }
h1 { font-size: 20px; font-weight: 700; letter-spacing: -.02em; }
h1 .k { color: var(--mut); font-weight: 500; }
.sum { display: flex; gap: 8px; align-items: center; margin-left: auto;
  font-size: 12.5px; color: var(--mut); flex-wrap: wrap; }
.pill { display: inline-flex; align-items: center; gap: 6px;
  border: 1px solid var(--line); background: var(--card);
  border-radius: 999px; padding: 3px 10px; font-weight: 600;
  font-size: 12px; color: var(--na); }
.grid { display: grid;
  grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 14px; }
.card { background: var(--card); border: 1px solid var(--line);
  border-radius: 12px; padding: 16px 18px; box-shadow: var(--shadow); }
.card h2 { font-size: 14.5px; font-weight: 650; display: flex;
  align-items: center; gap: 8px; }
.card h2 a { font-size: 11.5px; font-weight: 500; color: var(--mut);
  margin-left: auto; text-decoration: none; border: 1px solid var(--line);
  border-radius: 6px; padding: 2px 8px; }
.card h2 a:hover { color: var(--ink); border-color: var(--mut); }
.badge { font-size: 10.5px; font-weight: 600; color: var(--mut);
  border: 1px solid var(--line); border-radius: 5px; padding: 1px 6px;
  text-transform: uppercase; letter-spacing: .04em; }
.route { font-family: var(--mono); font-size: 11px; color: var(--mut);
  margin: 6px 0 8px; word-break: break-all; }
.verdict { font-size: 12px; border-radius: 8px; padding: 4px 8px;
  margin-bottom: 10px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit,
  minmax(64px, 1fr)); gap: 8px; margin-bottom: 12px; }
.tile { border: 1px solid var(--line); border-radius: 9px;
  padding: 7px 6px 6px; text-align: center; }
.tile b { font-size: 15px; display: block; font-variant-numeric: tabular-nums; }
.tile span { font-size: 10px; color: var(--mut); text-transform: uppercase;
  letter-spacing: .05em; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
  flex: none; background: var(--na); }
@keyframes pulse { 50% { opacity: .35; } }
.rows { font-size: 12.5px; }
.rows div { display: flex; align-items: center; gap: 7px; padding: 4px 0;
  border-top: 1px solid var(--line); }
.rows .db { font-family: var(--mono); font-size: 11.5px; }
.rows .note { color: var(--mut); font-size: 11px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
.meta { font-size: 11px; color: var(--mut); margin-top: 10px; }
.feed { margin-top: 16px; }
.feed h3 { font-size: 12px; color: var(--mut); font-weight: 600;
  text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.feed div { display: flex; gap: 8px; align-items: baseline; padding: 4px 0;
  border-top: 1px solid var(--line); font-size: 12px; }
.feed .t { font-family: var(--mono); font-size: 10.5px; color: var(--mut);
  flex: none; }
.feed .op { font-weight: 600; }
.feed .hop { color: var(--mut); margin-left: auto; font-size: 11px; }
footer { color: var(--mut); font-size: 11px; text-align: center;
  margin-top: 26px; }
"""

#: the page's one script: everything it shows is put in as text, by DOM
#: calls, and the buttons are given their handlers - no markup built from
#: data and no inline handler, so a name holding a quote or a tag is a
#: name (before, a database named `x');import('//e/a.js');//` ran its
#: script from a button's `onclick`)
SCRIPT = """
'use strict';
let csrf = '';
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = String(text);
  return e;
}
function dot(status) { return el('span', 'dot ' + status); }
async function act(what, hop, db) {
  await fetch('/api/' + what, {method: 'POST', headers: {
    'Content-Type': 'application/json', 'X-Migkit-Csrf': csrf},
    body: JSON.stringify({hop, db})});
  load();
}
async function signin() {
  // the code migkit printed rides in the address's fragment, which a
  // browser never sends anywhere: handed over once, in a header, for the
  // cookie, and taken out of the address bar and its history
  const code = location.hash.slice(1);
  if (!code) return;
  history.replaceState(null, '', location.pathname);
  await fetch('/api/login', {method: 'POST', headers: {
    'X-Migkit-Login': code}});
}
function card(h) {
  const c = el('div', 'card');
  const title = el('h2');
  title.append(dot(h.status), h.name, el('span', 'badge', h.engine));
  if (h.service) title.append(el('span', 'badge', h.service));
  if (h.has_report) {
    const a = el('a', '', 'report');
    a.href = '/report/' + encodeURIComponent(h.name);
    a.target = '_blank';
    a.rel = 'noopener';
    title.append(a);
  }
  c.append(title, el('div', 'route', h.route),
    el('div', 'verdict ' + h.status, h.word + (h.why ? ': ' + h.why : '')));
  const tiles = el('div', 'tiles');
  for (const [name, st] of Object.entries(h.checks)) {
    const t = el('div', 'tile ' + st.status);
    t.append(el('b', '', st.same + '/' + st.answers), el('span', '', name));
    tiles.append(t);
  }
  if (tiles.childElementCount) c.append(tiles);
  const rows = el('div', 'rows');
  for (const d of h.dbs) {
    const row = el('div');
    row.append(dot(d.status), el('span', 'db', d.name));
    if (d.note) row.append(el('span', 'note', d.note));
    rows.append(row);
  }
  for (const t of (h.tails || [])) {
    if (!t.running) continue;
    const held = t.paused || t.asked;
    const row = el('div');
    row.append(el('span', 'db', 'tail ' + t.db), el('span', 'note',
      t.paused ? 'held' : t.asked ? 'holding' : 'running'));
    if (h.role === 'operator') {
      const b = el('button', '', held ? 'resume' : 'hold');
      b.type = 'button';
      b.addEventListener('click',
        () => act(held ? 'resume' : 'hold', h.name, t.db));
      row.append(b);
    }
    rows.append(row);
  }
  c.append(rows, el('div', 'meta', h.meta));
  return c;
}
async function load() {
  const r = await fetch('/api/data');
  const sum = document.getElementById('sum');
  if (r.status === 403) {
    sum.textContent = 'open the address migkit printed when it started -' +
      ' it opens the view once; start it again for another browser';
    return;
  }
  const data = await r.json();
  csrf = data.csrf;
  const count = {};
  for (const h of data.hops) count[h.status] = (count[h.status] || 0) + 1;
  sum.replaceChildren();
  for (const [status, word] of Object.entries(data.verdicts)) {
    const p = el('span', 'pill ' + status);
    p.append(dot(status), (count[status] || 0) + ' ' + word);
    sum.append(p);
  }
  sum.append(el('span', '', 'updated ' + data.now));
  const g = document.getElementById('grid');
  g.replaceChildren(...data.hops.map(card));
  const rows = document.getElementById('feedrows');
  rows.replaceChildren();
  for (const a of (data.activity || [])) {
    const row = el('div');
    row.append(el('span', 't', a.at), el('span', 'op', a.op),
      el('span', '', (a.db || '') + ' ' + (a.detail || a.note || '')),
      el('span', 'hop', a.hop));
    rows.append(row);
  }
  document.getElementById('feed').hidden = !rows.childElementCount;
}
signin().then(() => { load(); setInterval(load, 10000); });
"""


def _hash(text):
    return "'sha256-" + base64.b64encode(
        hashlib.sha256(text.encode()).digest()).decode() + "'"


def _page():
    css = _css()
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit</title>
<style>{css}</style></head><body><div class="wrap">
<header>
  <h1>migkit <span class="k">dashboard</span></h1>
  <div class="sum" id="sum">loading&hellip;</div>
</header>
<div class="grid" id="grid"></div>
<div class="card feed" id="feed" hidden>
  <h3>recent writes</h3><div id="feedrows"></div>
</div>
<footer>auto-refresh 10s &middot; a tail can be held and let go on</footer>
</div>
<script>{SCRIPT}</script></body></html>"""
    policy = ("default-src 'none'; script-src " + _hash(SCRIPT)
              + "; style-src " + _hash(css) + "; connect-src 'self';"
              " img-src 'self'; base-uri 'none'; form-action 'none';"
              " frame-ancestors 'none'")
    return page, policy


_BUILT = None


def page():
    """(the page, its content security policy): the policy names the
    page's own script and style by their digests, and nothing else runs."""
    global _BUILT
    if _BUILT is None:
        _BUILT = _page()
    return _BUILT


def __getattr__(name):
    # `PAGE` is built from the verdicts' table on first use
    if name == "PAGE":
        return page()[0]
    raise AttributeError(name)


#: a report's page runs no script: its policy allows none
REPORT_POLICY = ("default-src 'none'; style-src 'unsafe-inline';"
                 " img-src data:; base-uri 'none'; form-action 'none';"
                 " frame-ancestors 'none'")


def _tails(name, hop):
    """[{db, running, paused}] for every tail the hop's reports know of."""
    from . import tailctl
    out = []
    root = REPORTS / name
    for marker in sorted(root.glob(f"*/{tailctl.PID}")):
        where = marker.parent
        out.append({"db": where.name, "running": tailctl.alive(where),
                    "paused": (where / tailctl.PAUSED).exists(),
                    "asked": (where / tailctl.PAUSE).exists()})
    return out


def _verdict_at(where):
    """(status, why, when) of the hop's last verdict, or (NONE, "", None)
    where it was never checked - or its file says a status the table has
    no word for, which is read as nothing, never as a pass."""
    got = _json_at(where / "verdict.json")
    status = got.get("status") if isinstance(got, dict) else None
    if status not in _verdicts():
        return NONE, "", None
    try:
        when = (where / "verdict.json").stat().st_mtime
    except OSError:
        when = None
    return status, str(got.get("why") or ""), when


def _answers(where):
    """The last check's answers (`summary.json`), [] where there are
    none."""
    got = _json_at(where / "summary.json", [])
    return [r for r in got if isinstance(r, dict)] \
        if isinstance(got, list) else []


def _grouped(rows, key):
    out = {}
    for r in rows:
        out.setdefault(key(r), []).append(r)
    return out


#: record entries that are not writes: an export's anchor, a rotation's
#: carried head
_NOT_WRITES = ("evidence", "rotated")


def _writes(where, n):
    """The last `n` entries of the hop's record that are writes, read from
    its end."""
    from . import runlog
    out = []
    for line in runlog.last_lines(where / "changelog.jsonl", 4 * n + 8):
        try:
            e = json.loads(line)
        except ValueError:
            continue
        if isinstance(e, dict) and e.get("op") not in _NOT_WRITES:
            out.append(e)
    return out[-n:]


def _hop_data(name, hop):
    from .verdict import db_of, decide
    verdicts = _verdicts()
    rpt = REPORTS / name
    status, why, when = _verdict_at(rpt)
    out = {"name": name, "engine": hop.engine, "service": hop.service or "",
           "status": status,
           "word": verdicts[status].word if status in verdicts
           else NONE_WORD, "why": why, "checks": {}, "dbs": [],
           "meta": "no checks run yet", "has_report": False,
           "route": f"{hop.source.host or '?'} -> "
                    f"{hop.target.host or hop.target.options.get('hosts', '?')}"}
    out["has_report"] = (rpt / "report.html").exists()
    out["tails"] = _tails(name, hop)
    rows = _answers(rpt)
    for check, got in sorted(_grouped(rows, lambda r: r.get("check", "?"))
                             .items()):
        out["checks"][check] = {
            "same": sum(1 for r in got if r.get("status") == "ok"),
            "answers": len(got), "status": decide(got, hop).status}
    for db, got in sorted(_grouped(rows, lambda r: db_of(r.get("scope")))
                          .items()):
        st = decide(got, hop).status
        note = next((str(r.get("detail") or "")[:60] for r in got
                     if r.get("status") in ("diff", "error")), "")
        out["dbs"].append({"name": db, "status": st,
                           "note": note if st in ("different", "error")
                           else ""})
    if when is not None:
        age = time.time() - when
        out["meta"] = (f"last check {int(age // 60)}m ago"
                       if age < 86400 else
                       f"last check {int(age // 3600)}h ago")
    last = _writes(rpt, 1)
    if last:
        e = last[-1]
        out["meta"] += (f" | last write: {e.get('op')}"
                        f" {e.get('db', '')} at {e.get('at')}")
    return out


# ---- the metrics ----------------------------------------------------------------

#: (family, type, help, the family it is replaced by - None where it is
#: current). The order is the scrape's.
FAMILIES_LIST = [
    ("migkit_verdict", "gauge", "The hop's last verdict as a state set:"
     " one sample a verdict, 1 for the one its last check ended as"
     " (none: never checked)", None),
    ("migkit_hop_status", "gauge", "The hop's last verdict (0 same,"
     " 1 different, 2 could not verify, 3 not checked, 4 incomplete)",
     "migkit_verdict"),
    ("migkit_check_answers", "gauge", "Answers the last check gave, by"
     " check and by each answer's status", None),
    ("migkit_check_pass", "gauge", "Answers of a check that were the same",
     "migkit_check_answers{status=\"ok\"}"),
    ("migkit_check_total", "gauge", "Answers of a check",
     "migkit_check_answers"),
    ("migkit_last_check_age_seconds", "gauge", "Age of the last verdict",
     None),
    ("migkit_tail_running", "gauge", "A change tail is marked running here"
     " (0: its process is gone and it did not stop)", None),
    ("migkit_tail_paused", "gauge", "The tail is holding still for a"
     " repair", None),
    ("migkit_tail_heartbeat_age_seconds", "gauge", "Since the tail last"
     " went round its loop", None),
    ("migkit_tail_behind_seconds", "gauge", "Since the tail last read to"
     " the end of the source's log", None),
    ("migkit_tail_changes_applied_total", "counter", "Changes this tail has"
     " applied since it started", None),
    ("migkit_tail_changes_applied", "gauge", "Changes this tail has applied"
     " since it started", "migkit_tail_changes_applied_total"),
    ("migkit_tail_stopped", "gauge", "The tail stopped on an error and has"
     " not been started again (1), or not (0)", None),
    ("migkit_tail_retention_margin_seconds", "gauge", "How much longer the"
     " source keeps what the tail has not read (MySQL, MongoDB)", None),
    ("migkit_tail_retention_margin_bytes", "gauge", "What the source can"
     " still write before the tail's slot loses WAL (PostgreSQL)", None),
    ("migkit_tail_source_held_bytes", "gauge", "Log the source holds for"
     " the tail", None),
    ("migkit_tail_commit_lag_seconds", "gauge", "From the source's commit"
     " of the last change applied to the target's (the two clocks'"
     " difference included)", None),
    ("migkit_tail_applied_through_age_seconds", "gauge", "Since the source"
     " committed the last change the tail applied", None),
    ("migkit_tail_tables", "gauge", "Tables of the tail in each state:"
     " live, copying (added, copied beside the stream), parked (a DDL the"
     " target does not follow yet), removed", None),
    ("migkit_tail_set_aside_bytes", "gauge", "Changes the tail holds on"
     " this machine for tables being copied or parked", None),
    ("migkit_follow_applying", "gauge", "The change leg run from here is"
     " applying (0: it waits to be told to)", None),
    ("migkit_follow_behind_bytes", "gauge", "Source log the change leg run"
     " from here has not received yet", None),
    ("migkit_follow_held_bytes", "gauge", "Log the change leg has received"
     " and not applied, by its own account (the target's origin is the"
     " proof)", None),
    ("migkit_move_rate_rows_per_second", "gauge", "A running move's recent"
     " rate (a moving average of its last windows)", None),
    ("migkit_move_workers", "gauge", "How many a running move carries at"
     " once", None),
    ("migkit_move_rows_done", "gauge", "Rows a running move has copied",
     None),
    ("migkit_move_time_left_seconds", "gauge", "Time left of each phase of"
     " a move, by estimate: p50 and p90 as planned, live from the recent"
     " rate", None),
    ("migkit_move_eta_seconds", "gauge", "Time left of each phase of a"
     " move", "migkit_move_time_left_seconds"),
    ("migkit_move_phase_never", "gauge", "A phase that cannot finish at the"
     " rates measured (1: a catch-up slower than the changes arriving),"
     " or can (0)", None),
    ("migkit_move_resource_bytes_per_second", "gauge", "What each resource"
     " of a move allows, by its key and where the number came from", None),
    ("migkit_move_resource_binding", "gauge", "The resource that sets a"
     " move's rate, by its key", None),
    ("migkit_move_resource_info", "gauge", "A resource's key, and what an"
     " operator reads for it", None),
    ("migkit_resource_bytes_per_second", "gauge", "What each resource of a"
     " move allows", "migkit_move_resource_bytes_per_second"),
    ("migkit_resource_binding", "gauge", "The resource that sets a move's"
     " rate", "migkit_move_resource_binding"),
    ("migkit_worker_memory_bytes", "gauge", "What one worker of a move"
     " holds for its widest rows", None),
    ("migkit_move_rows", "gauge", "Rows the copy has carried into a table",
     None),
    ("migkit_move_tables", "gauge", "Tables the copy of a database holds a"
     " place for", None),
    ("migkit_move_tables_done", "gauge", "Tables the copy of a database"
     " finished", None),
    ("migkit_copy_way_rows_per_second", "gauge", "Rows a second the last"
     " copy by a way went at, by the way's key", None),
    ("migkit_copy_way_seconds", "gauge", "Seconds the last copy by a way"
     " took, by the way's key", None),
    ("migkit_way_info", "gauge", "A way's key, and the words an operator"
     " reads for it", None),
    ("migkit_copy_rows_per_second", "gauge", "Rows a second the last copy"
     " by a way went at", "migkit_copy_way_rows_per_second"),
    ("migkit_copy_seconds", "gauge", "Seconds the last copy by a way took",
     "migkit_copy_way_seconds"),
    ("migkit_check_seconds", "gauge", "Seconds a check of a database took"
     " in the last run", None),
    ("migkit_verify_ranges_done", "gauge", "Ranges of a table the verify in"
     " progress has proved equal", None),
    ("migkit_verify_rows_done", "gauge", "Rows in those ranges", None),
    ("migkit_table_rows", "gauge", "Rows a table held on a side at its"
     " last digest", None),
    ("migkit_table_same", "gauge", "A table's rows and digest equal on both"
     " sides as one run took them (1) or not (0); a table one run took"
     " one side of has none", None),
    ("migkit_cutover_outage_seconds", "gauge", "Write outage the last"
     " cutover measured", None),
    ("migkit_cutover_phase_seconds", "gauge", "Seconds a phase of the last"
     " cutover took", None),
    ("migkit_cutover_running", "gauge", "A cutover is under way (1), or"
     " not (0)", None),
    ("migkit_two_way_conflicts_total", "counter", "Rows a two-way tail"
     " found both sides had changed, by table, kind and what was decided,"
     " since its reports began", None),
    ("migkit_two_way_conflicts", "gauge", "Rows a two-way tail found both"
     " sides had changed, by table, kind and what was decided, since its"
     " reports began", "migkit_two_way_conflicts_total"),
    ("migkit_two_way_rows_parked", "gauge", "Rows a two-way conflict set"
     " aside until they are decided on both sides", None),
    ("migkit_stream_behind_source_seconds", "gauge", "How far the hop's"
     " change stream's reader is behind the source, by its own account",
     None),
    ("migkit_stream_events_seen_total", "counter", "Changes the hop's"
     " stream reader has read since it started", None),
    ("migkit_stream_events_seen", "gauge", "Changes the stream's reader"
     " has read since it started", "migkit_stream_events_seen_total"),
    ("migkit_stream_queue_remaining", "gauge", "Room left in the stream"
     " reader's queue", None),
    ("migkit_stream_connected", "gauge", "The stream's reader is connected"
     " to the source (1)", None),
    ("migkit_stream_snapshot_tables_remaining", "gauge", "Tables the"
     " stream's first read has still to read", None),
    ("migkit_stream_running", "gauge", "A part of the change stream is"
     " running (1) or not (0)", None),
    ("migkit_collect_age_seconds", "gauge", "Since the snapshot these"
     " metrics are read from was taken", None),
    ("migkit_collect_seconds", "gauge", "Seconds the last snapshot took to"
     " take", None),
    ("migkit_collect_errors_total", "counter", "Readings that failed while"
     " a snapshot was taken, by what was being read", None),
]
FAMILIES = {f: (kind, said, instead) for f, kind, said, instead
            in FAMILIES_LIST}
#: the families renamed, kept beside their new names until 1.0
DEPRECATED = {f: instead for f, (_, _, instead) in FAMILIES.items()
              if instead}


def _lv(value):
    """A label's value as the text format writes one."""
    return (str(value).replace("\\", "\\\\").replace('"', '\\"')
            .replace("\n", "\\n"))


def _labels(**labels):
    """Labels as a sample carries them, each value escaped."""
    return ",".join(f'{k}="{_lv(v)}"' for k, v in labels.items()
                    if v is not None)


def _sample(family, base, value, **labels):
    """One sample: the family's name - one of `FAMILIES` - the labels
    `base` already written (`_labels`) and `labels`, each value escaped,
    and the value, which must read as a number."""
    if family not in FAMILIES:
        raise KeyError(f"{family} is not a family migkit exports")
    float(value)
    lbl = ",".join(x for x in (base, _labels(**labels)) if x)
    return f"{family}{{{lbl}}} {value}" if lbl else f"{family} {value}"


def _family(line):
    return line.split("{", 1)[0].split(" ", 1)[0]


def render(per_hop, visible=None, extra=()):
    """The scrape: each family's help and type, then every sample of it -
    the hops' in `visible` (None: every one) and `extra` - grouped as the
    text format wants them."""
    by = {}
    for name, lines in per_hop.items():
        if visible is not None and name not in visible:
            continue
        for line in lines:
            by.setdefault(_family(line), []).append(line)
    for line in extra:
        by.setdefault(_family(line), []).append(line)
    out = []
    for family, (kind, said, instead) in FAMILIES.items():
        help_ = said + (f" (deprecated until 1.0: {instead})" if instead
                        else "")
        out += [f"# HELP {family} {help_}", f"# TYPE {family} {kind}"]
        out += by.pop(family, [])
    return "\n".join(out) + "\n"


def prometheus(hops, errors=None):
    """Prometheus text-format metrics of every hop in `hops`, read now -
    what `migkit report --metrics` prints and each snapshot takes. Alert
    on `migkit_verdict{status="different"} == 1`: a hop never checked is
    `status="none"`, not a failure. `errors` counts the readings that
    failed, by what was being read."""
    return render(collect(hops, errors))


def collect(hops, errors=None):
    """{hop: its samples}: every reading of every hop, one reading that
    fails counted in `errors` and the rest read."""
    errors = {} if errors is None else errors
    return {name: _hop_lines(name, hop, errors)
            for name, hop in hops.items()}


def _verdict_lines(name, hop, where, lbl):
    status, _, when = _verdict_at(where)
    verdicts = _verdicts()
    out = [_sample("migkit_verdict", lbl, int(status == st), status=st)
           for st in list(verdicts) + [NONE]]
    out.append(_sample("migkit_hop_status", lbl,
                       verdicts[status].metric if status in verdicts
                       else 3))
    rows = _answers(where)
    for check, got in sorted(_grouped(rows, lambda r: r.get("check", "?"))
                             .items()):
        by = _grouped(got, lambda r: str(r.get("status", "?")))
        for st, n in sorted(by.items()):
            out.append(_sample("migkit_check_answers", lbl, len(n),
                               check=check, status=st))
        out.append(_sample("migkit_check_pass", lbl, len(by.get("ok", [])),
                           check=check))
        out.append(_sample("migkit_check_total", lbl, len(got),
                           check=check))
    if when is None:
        try:
            when = (where / "summary.json").stat().st_mtime
        except OSError:
            when = None
    if when is not None:
        out.append(_sample("migkit_last_check_age_seconds", lbl,
                           int(time.time() - when)))
    return out


#: what each reading of a hop is, for the errors a snapshot counts
PARTS = (
    ("verdict", lambda n, h, w, lbl: _verdict_lines(n, h, w, lbl)),
    ("tail", lambda n, h, w, lbl: _tail_metrics(w, lbl)),
    ("follow", lambda n, h, w, lbl: _follow_metrics(h, w, lbl)),
    ("move", lambda n, h, w, lbl: _move_metrics(w, lbl)),
    ("moved", lambda n, h, w, lbl: _tables_moved_metrics(w, lbl)),
    ("copy", lambda n, h, w, lbl: _copy_metrics(w, lbl)),
    ("check", lambda n, h, w, lbl: _check_metrics(w, lbl)),
    ("verify", lambda n, h, w, lbl: _verify_metrics(w, lbl)),
    ("table", lambda n, h, w, lbl: _table_metrics(w, lbl)),
    ("cutover", lambda n, h, w, lbl: _cutover_metrics(w, lbl)),
    ("conflict", lambda n, h, w, lbl: _conflict_metrics(w, lbl)),
    ("stream", lambda n, h, w, lbl: _stream_metrics(w, lbl)),
)


def _hop_lines(name, hop, errors):
    where = REPORTS / name
    lbl = _labels(hop=name, engine=hop.engine)
    out = []
    for part, read in PARTS:
        try:
            out += read(name, hop, where, lbl)
        except Exception:  # noqa: BLE001 - counted; the rest is read
            errors[part] = errors.get(part, 0) + 1
    return out


def _json_at(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def _db_dirs(where):
    try:
        return sorted(p for p in where.iterdir() if p.is_dir())
    except OSError:
        return []


def _tables_moved_metrics(where, lbl):
    """The copy's own checkpoint: rows per table, tables finished. Never
    where it stands in a table - that is a key, which is data."""
    out = []
    for d in _db_dirs(where):
        ck = _json_at(d / "move.json", {}) or {}
        tables = {k: v for k, v in ck.items() if isinstance(v, dict)
                  and ("moved" in v or "done" in v)}
        if not tables:
            continue
        dl = _labels(db=d.name)
        base = ",".join(x for x in (lbl, dl) if x)
        out.append(_sample("migkit_move_tables", base, len(tables)))
        out.append(_sample("migkit_move_tables_done", base,
                           sum(1 for v in tables.values() if v.get("done"))))
        for t, v in sorted(tables.items()):
            out.append(_sample("migkit_move_rows", base,
                               int(v.get("moved") or 0), table=t))
    return out


def way_key(label):
    """A way's key in the metrics: stable whatever its words, and never a
    program's name (the code labels some ways by the program that takes
    them) - a digest of the label."""
    return "w" + hashlib.blake2b(str(label).encode(),
                                 digest_size=4).hexdigest()


def _copy_metrics(where, lbl):
    """The last copy by each way the hop took, as its rates keep it: by
    the way's key (`way_key`), its words beside it (`migkit_way_info`) -
    the engine's words (`export.way`), never a program's name."""
    from .export import way
    out = []
    for label, runs in sorted((_json_at(where / "throughput.json", {})
                               or {}).items()):
        if not isinstance(runs, list) or not runs:
            continue
        last = runs[-1]
        rows, secs = last.get("rows"), last.get("seconds")
        if not rows or not secs:
            continue
        said, key = way(label), way_key(label)
        rate = round(rows / secs, 1)
        out.append(_sample("migkit_way_info", lbl, 1, way_id=key, way=said))
        out.append(_sample("migkit_copy_way_rows_per_second", lbl, rate,
                           way_id=key))
        out.append(_sample("migkit_copy_way_seconds", lbl, secs,
                           way_id=key))
        out.append(_sample("migkit_copy_rows_per_second", lbl, rate,
                           way=said))
        out.append(_sample("migkit_copy_seconds", lbl, secs, way=said))
    return out


def _check_metrics(where, lbl):
    got = _json_at(where / "check-timings.json", {}) or {}
    out = []
    for d, checks in sorted((got.get("databases") or {}).items()):
        for c, secs in sorted(checks.items()):
            out.append(_sample("migkit_check_seconds", lbl, secs, db=d,
                               check=c))
    return out


def _verify_metrics(where, lbl):
    """The verify in progress, from the ranges it has proved and kept
    (`checkpoint`); a table finished is cleared from it."""
    out = []
    for d in _db_dirs(where):
        got = _json_at(d / "checkpoint.json", {}) or {}
        for t, entry in sorted((got.get("tables") or {}).items()):
            done = (entry or {}).get("done") or {}
            if not done:
                continue
            out.append(_sample("migkit_verify_ranges_done", lbl, len(done),
                               db=d.name, table=t))
            out.append(_sample("migkit_verify_rows_done", lbl,
                               sum(int(v[0]) for v in done.values()),
                               db=d.name, table=t))
    return out


def _table_metrics(where, lbl):
    """Every table's rows on each side and whether the two agree, as one
    run took them (`digests.read_in`): a table whose last run took one
    side only has no `migkit_table_same`."""
    from . import digests
    out = []
    for (db, table, scope), sides in sorted(digests.read_in(where).items()):
        if scope:
            continue
        for side, line in sorted(sides.items()):
            if line.get("rows") is not None:
                out.append(_sample("migkit_table_rows", lbl,
                                   int(line["rows"]), db=db, table=table,
                                   side=side))
        src, dst = sides.get("src"), sides.get("dst")
        if src and dst:
            same = (src.get("rows") == dst.get("rows")
                    and str(src.get("digest")) == str(dst.get("digest")))
            out.append(_sample("migkit_table_same", lbl, int(same), db=db,
                               table=table))
    return out


def _cutover_metrics(where, lbl):
    from . import runlog
    out = []
    running = _json_at(where / "cutover.json")
    if isinstance(running, dict):
        out.append(_sample("migkit_cutover_running", lbl,
                           int(running.get("state") == "running")))
    last = None
    for line in reversed(runlog.last_lines(where / "cutover-runs.jsonl",
                                           8)):
        try:
            last = json.loads(line)
            break
        except ValueError:
            continue
    if isinstance(last, dict):
        if last.get("outage") is not None:
            out.append(_sample("migkit_cutover_outage_seconds", lbl,
                               last["outage"]))
        for phase, secs in sorted((last.get("took") or {}).items()):
            out.append(_sample("migkit_cutover_phase_seconds", lbl, secs,
                               phase=phase))
    return out


def _conflict_metrics(where, lbl):
    """The rows a two-way tail found both sides had changed, by table,
    kind and what was decided, and the rows parked now - from its counts
    (`twoway.COUNTS`), which hold names and numbers and never a value."""
    from . import twoway
    out = []
    for d in _db_dirs(where):
        got = _json_at(d / twoway.COUNTS, {}) or {}
        for line, n in sorted((got.get("counts") or {}).items()):
            table, kind, decision = (str(line).split("\t") + ["", ""])[:3]
            for family in ("migkit_two_way_conflicts_total",
                           "migkit_two_way_conflicts"):
                out.append(_sample(family, lbl, int(n), db=d.name,
                                   table=table, kind=kind,
                                   decision=decision))
        for table, n in sorted((got.get("parked") or {}).items()):
            out.append(_sample("migkit_two_way_rows_parked", lbl, int(n),
                               db=d.name, table=table))
    return out


#: where the change stream's pipeline answers for its parts, where its
#: reader answers its own readings, and the readings carried out: {its
#: attribute: migkit's metrics}
STREAM_API = 8083
STREAM_READINGS = 8778
STREAM_METRICS = {
    "MilliSecondsBehindSource": ("migkit_stream_behind_source_seconds",),
    "TotalNumberOfEventsSeen": ("migkit_stream_events_seen_total",
                                "migkit_stream_events_seen"),
    "QueueRemainingCapacity": ("migkit_stream_queue_remaining",),
    "Connected": ("migkit_stream_connected",),
    "RemainingTableCount": ("migkit_stream_snapshot_tables_remaining",),
}


def _stream_metrics(where, lbl):
    """The hop's change stream, where it runs one: each part running or
    not (its connectors and their tasks, as the pipeline's own API says),
    and - while its reader runs - the reader's own readings, of this
    hop's connector only (`movers.stream_readings`, `server=`): how far
    behind the source, how many changes read, the room in its queue,
    whether it is connected, the tables its first read has left. Before,
    every hop with a stream directory was given the readings of whichever
    connector the machine ran, a stopped one's too."""
    from . import movers
    if not (where / "stream" / "docker-compose.yml").exists():
        return []
    out = []
    parts = movers.stream_parts(where.name, STREAM_API)
    for part, running in parts:
        out.append(_sample("migkit_stream_running", lbl, int(running),
                           part=part))
    if not any(running for part, running in parts if part == "source"):
        return out
    for attr, value in movers.stream_readings(
            tuple(STREAM_METRICS), STREAM_READINGS,
            server=f"migkit-{where.name}").items():
        if attr == "MilliSecondsBehindSource":
            value = value / 1000.0
        for family in STREAM_METRICS[attr]:
            out.append(_sample(family, lbl, f"{float(value):g}"))
    return out


#: seconds after which a move's progress is no longer a running move's
LIVE = 600

#: where a resource's number came from, as a key (`model.TRUTHS`)
_TRUTH_KEY = {"measured": "measured", "measured on an earlier run": "earlier",
              "measured on its rehearsal": "rehearsed",
              "catalogued": "catalogued", "assumed": "assumed"}


def _move_metrics(where, lbl):
    """A move's resources and phases (`model.json`) and, while it runs, its
    rate and time left (`progress.json`), per database."""
    from . import model as resource_model
    from . import planner
    out = []
    if not where.is_dir():
        return out
    for d in sorted(p for p in where.iterdir() if p.is_dir()):
        ml = ",".join(x for x in (lbl, _labels(db=d.name)) if x)
        m = resource_model.load(d)
        if m is not None:
            for s in m.stages.values():
                if s.bound():
                    bound = f"{s.bound():.0f}"
                    truth = _TRUTH_KEY.get(s.truth, "assumed")
                    out.append(_sample("migkit_move_resource_info", ml, 1,
                                       resource=s.name, said=s.said))
                    out.append(_sample(
                        "migkit_move_resource_bytes_per_second", ml, bound,
                        resource=s.name, truth=truth))
                    out.append(_sample("migkit_resource_bytes_per_second",
                                       ml, bound, resource=s.said,
                                       truth=s.truth))
            b = m.binding() if m.measured else None
            if b is not None:
                out.append(_sample("migkit_move_resource_binding", ml, 1,
                                   resource=b.name))
                out.append(_sample("migkit_resource_binding", ml, 1,
                                   resource=b.said))
            if m.worker_memory:
                out.append(_sample("migkit_worker_memory_bytes", ml,
                                   int(m.worker_memory)))
            for ph in m.planned:
                out.append(_sample("migkit_move_phase_never", ml,
                                   int(bool(ph.never)), phase=ph.name))
                if ph.never:
                    continue
                for q, v in (("0.5", ph.p50), ("0.9", ph.p90)):
                    if v is not None:
                        out.append(_sample(
                            "migkit_move_time_left_seconds", ml, f"{v:.0f}",
                            phase=ph.name,
                            estimate="p50" if q == "0.5" else "p90"))
                        out.append(_sample("migkit_move_eta_seconds", ml,
                                           f"{v:.0f}", phase=ph.name,
                                           quantile=q))
        p = planner.read_progress(d)
        if p and time.time() - float(p.get("at") or 0) < LIVE:
            if p.get("rate") is not None:
                out.append(_sample("migkit_move_rate_rows_per_second", ml,
                                   f"{float(p['rate']):.1f}"))
            out.append(_sample("migkit_move_workers", ml, int(p["workers"])))
            out.append(_sample("migkit_move_rows_done", ml,
                               int(p["rows_done"])))
            if p.get("eta_copy") is not None:
                left = f"{float(p['eta_copy']):.0f}"
                out.append(_sample("migkit_move_time_left_seconds", ml, left,
                                   phase="copy", estimate="live"))
                out.append(_sample("migkit_move_eta_seconds", ml, left,
                                   phase="copy", quantile="live"))
    return out


ROOM = {"seconds": "migkit_tail_retention_margin_seconds",
        "bytes": "migkit_tail_retention_margin_bytes",
        "held_bytes": "migkit_tail_source_held_bytes"}


def _follow_metrics(hop, where, lbl):
    """The change leg run from here, per database, as its own sentinel
    reads it (`movers.follow_lag`); nothing where none runs."""
    from . import movers
    out = []
    if not where.is_dir():
        return out
    for d in sorted(p for p in where.iterdir() if (p / "follow").is_dir()):
        try:
            lag = movers.follow_lag(hop, d.name)
        except Exception:  # noqa: BLE001 - a scrape never fails on one leg
            lag = None
        if not lag:
            continue
        out.append(_sample("migkit_follow_applying", lbl, int(lag["apply"]),
                           db=d.name))
        for key in ("behind", "held"):
            if lag.get(key) is not None:
                out.append(_sample(f"migkit_follow_{key}_bytes", lbl,
                                   int(lag[key]), db=d.name))
    return out


def _tail_metrics(where, lbl):
    from . import tailctl
    out = []
    if not where.is_dir():
        return out
    for d in sorted(p for p in where.iterdir() if p.is_dir()):
        st = tailctl.state(d)
        if st is None:
            continue
        tl = ",".join(x for x in (lbl, _labels(db=d.name)) if x)
        if st["marked"]:
            out.append(_sample("migkit_tail_running", tl,
                               int(st["running"])))
            out.append(_sample("migkit_tail_paused", tl, int(st["paused"])))
        if st["beat_age"] is not None:
            out.append(_sample("migkit_tail_heartbeat_age_seconds", tl,
                               int(st["beat_age"])))
            out.append(_sample("migkit_tail_behind_seconds", tl,
                               int(st["behind"])))
            for family in ("migkit_tail_changes_applied_total",
                           "migkit_tail_changes_applied"):
                out.append(_sample(family, tl, int(st["changes"] or 0)))
            for key, value in (st["room"] or {}).items():
                if key in ROOM and value is not None:
                    out.append(_sample(ROOM[key], tl, int(value)))
            if st.get("lag") is not None:
                out.append(_sample("migkit_tail_commit_lag_seconds", tl,
                                   f"{float(st['lag']):.3f}"))
            if st.get("through") is not None:
                out.append(_sample(
                    "migkit_tail_applied_through_age_seconds", tl,
                    f"{max(time.time() - float(st['through']), 0):.3f}"))
            for state, n in sorted((st.get("tables") or {}).items()):
                if state == "set_aside_bytes":
                    out.append(_sample("migkit_tail_set_aside_bytes", tl,
                                       int(n)))
                else:
                    out.append(_sample("migkit_tail_tables", tl, int(n),
                                       state=state))
        out.append(_sample("migkit_tail_stopped", tl,
                           int(bool(st["stopped"]))))
    return out


class Collector:
    """The metrics of every hop, read by one thread every `EVERY` seconds
    into a snapshot every scrape is served from: a scrape reads nothing
    and waits on nothing, whatever the hops' programs and servers do. A
    hops file that cannot be read, or a reading that fails, is counted
    (`migkit_collect_errors_total`) and the last snapshot served, its age
    said (`migkit_collect_age_seconds`)."""

    EVERY = 15.0

    def __init__(self, loader=None, every=None):
        self.loader = loader or (lambda: load_hops())
        self.every = every or self.EVERY
        self.per_hop, self.hops = {}, {}
        self.at = self.took = None
        self.errors = {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.halt = threading.Event()
        self.thread = None

    def once(self):
        began = time.time()
        failed = {}
        try:
            hops = self.loader()
        except (Exception, SystemExit):  # noqa: BLE001 - counted
            failed["hops"] = 1
            hops = None
        got = collect(hops, failed) if hops is not None else None
        with self.lock:
            for part, n in failed.items():
                self.errors[part] = self.errors.get(part, 0) + n
            if got is not None:
                self.per_hop, self.hops = got, hops
                self.at, self.took = time.time(), time.time() - began
        self.ready.set()

    def run(self):
        while not self.halt.is_set():
            self.once()
            self.halt.wait(self.every)

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True,
                                       name="migkit-metrics")
        self.thread.start()
        return self

    def stop(self):
        self.halt.set()

    def visible(self, user):
        """The hops `user` may see (None: every one)."""
        if user is None:
            return None
        with self.lock:
            return {n for n, h in self.hops.items()
                    if role(h, user) is not None}

    def text(self, user=None, wait=8.0):
        if self.thread is None and not self.ready.is_set():
            self.once()
        self.ready.wait(wait)
        visible = self.visible(user)
        with self.lock:
            extra = []
            if self.at is not None:
                extra.append(_sample("migkit_collect_age_seconds", "",
                                     f"{time.time() - self.at:.3f}"))
                extra.append(_sample("migkit_collect_seconds", "",
                                     f"{self.took:.3f}"))
            for part, n in sorted(self.errors.items()):
                extra.append(_sample("migkit_collect_errors_total", "", n,
                                     family=part))
            return render(self.per_hop, visible, extra)


def role(hop, user):
    """What `user` may do with a hop in the view: "operator" (hold a tail,
    let it go on), "viewer", or None (the hop is not shown). The holder of
    the printed address operates every hop; so does anyone the proxy lets
    in where the hop names no `access`. Where it does:

        options:
          access:
            operator: [ann@example.com]
            viewer: ["*@example.com"]      # shell patterns
    """
    from fnmatch import fnmatch
    if user is None:
        return "operator"
    access = (hop.options or {}).get("access")
    if not access:
        return "operator"
    for name in ("operator", "viewer"):
        if any(fnmatch(user, str(p)) for p in access.get(name) or []):
            return name
    return None


def _activity(hops):
    entries = []
    for name in hops:
        for e in _writes(REPORTS / name, 10):
            e["hop"] = name
            entries.append(e)
    entries.sort(key=lambda e: e.get("at", ""), reverse=True)
    return entries[:12]


def _same(given, wanted):
    """Whether two secrets are the same, compared in constant time."""
    import hmac
    return bool(wanted) and hmac.compare_digest(
        str(given or "").encode(), str(wanted).encode())


#: the header a signing-in proxy carries its secret in
PROXY_HEADER = "X-Migkit-Proxy"


class Handler(BaseHTTPRequestHandler):
    """The dashboard, and the few things it may do: hold a tail between
    batches and let it go on. A request is served only for this machine's
    own names and those `MIGKIT_UI_HOSTS` adds (a page elsewhere cannot
    reach it through a name it controls), data and actions only with the
    token kept in a cookie no script and no other site can read - set
    once, for the code this start printed - or for the person a proxy
    names with its secret, and an action only with the header only this
    page sends. Every action is written to the hop's record.

    The token itself was printed, in the address's query: it sat in the
    browser's history and the terminal's scrollback and opened the view
    for as long as the process ran, to anyone who read either. What is
    printed now is a code in the address's fragment - which a browser
    sends nowhere - good for one sign-in, after which that address opens
    nothing."""

    #: set by `serve`: the token the cookie holds, never printed; the code
    #: printed, good once; the port; and the metrics' snapshot
    token = ""
    once = ""
    port = 0
    collector = None
    _signing = None

    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body, headers=()):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes)
                         else body.encode())

    def _host_ok(self):
        from . import envkeys
        extra = envkeys.items("MIGKIT_UI_HOSTS")
        return self.headers.get("Host", "") in (
            f"127.0.0.1:{self.port}", f"localhost:{self.port}", *extra)

    def _user(self):
        """Who is asking, where the view is shared behind a proxy that
        signs people in, names them in a header of its own
        (`MIGKIT_UI_USER_HEADER`, e.g. `X-Auth-Request-Email`) and proves
        it is the proxy with the secret it sends (`MIGKIT_UI_PROXY_SECRET`
        in `X-Migkit-Proxy`); None for anyone else - the holder of the
        token this start printed included."""
        from . import envkeys
        header = envkeys.text("MIGKIT_UI_USER_HEADER")
        if not header or not _same(self.headers.get(PROXY_HEADER, ""),
                                   envkeys.text("MIGKIT_UI_PROXY_SECRET")):
            return None
        got = self.headers.get(header, "").strip()
        return got or None

    def _authed(self):
        from http.cookies import SimpleCookie
        if self._user():
            # named by the proxy in front, which proved it is the proxy
            return True
        jar = SimpleCookie(self.headers.get("Cookie", ""))
        got = jar.get("migkit_ui")
        return got is not None and _same(got.value, self.token)

    def _scraper(self):
        """Whether a scrape carries the bearer `MIGKIT_METRICS_TOKEN`."""
        from . import envkeys
        got = self.headers.get("Authorization", "")
        return got.startswith("Bearer ") and _same(
            got[len("Bearer "):].strip(),
            envkeys.text("MIGKIT_METRICS_TOKEN"))

    def _sign_in(self):
        """The cookie, for the code this start printed - once. The first
        to hand it over is signed in and the code is spent, so the
        printed address, wherever it was kept, opens nothing after."""
        given = self.headers.get("X-Migkit-Login", "")
        lock = Handler._signing or threading.Lock()
        Handler._signing = lock
        with lock:
            good = bool(Handler.once and self.token) and _same(
                given, Handler.once)
            if good:
                Handler.once = ""
        if not good:
            return self._send(403, "text/plain", "that address has been"
                                                 " used, or was never this"
                                                 " one's")
        return self._send(204, "text/plain", "", headers=(
            ("Set-Cookie", f"migkit_ui={self.token}; HttpOnly;"
                           " SameSite=Strict; Path=/"),))

    def _csrf(self):
        import hmac
        return hmac.new(self.token.encode(), b"csrf",
                        hashlib.sha256).hexdigest()

    def _metrics(self):
        if self._scraper():
            user = None
        elif self._authed():
            user = self._user()
        else:
            return self._send(401, "text/plain", "a scrape carries the"
                                                 " bearer MIGKIT_METRICS_TOKEN",
                              headers=(("WWW-Authenticate", "Bearer"),))
        collector = Handler.collector or Collector()
        return self._send(200, "text/plain; version=0.0.4",
                          collector.text(user))

    def do_GET(self):
        from urllib.parse import urlparse
        if not self._host_ok():
            return self._send(403, "text/plain", "not this host")
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            body, policy = page()
            return self._send(200, "text/html; charset=utf-8", body,
                              headers=(("Content-Security-Policy", policy),))
        if url.path == "/metrics":
            return self._metrics()
        if not self._authed():
            return self._send(403, "text/plain", "open the address migkit"
                                                 " printed when it started")
        if url.path == "/api/data":
            user = self._user()
            hops = {n: h for n, h in load_hops().items()
                    if role(h, user) is not None}
            verdicts = {k: v.word for k, v in _verdicts().items()}
            data = {"now": time.strftime("%H:%M:%S"), "user": user,
                    "verdicts": {**verdicts, NONE: NONE_WORD},
                    "hops": [dict(_hop_data(n, h), role=role(h, user))
                             for n, h in hops.items()],
                    "activity": _activity(hops), "csrf": self._csrf()}
            return self._send(200, "application/json", json.dumps(data))
        if self.path.startswith("/report/"):
            name = self.path[len("/report/"):].split("/")[0].split("?")[0]
            hop = load_hops().get(name)
            if hop is None or role(hop, self._user()) is None:
                return self._send(404, "text/plain", "not found")
            f = REPORTS / name / "report.html"
            if f.exists() and Path(f).resolve().parent == (
                    REPORTS / name).resolve():
                return self._send(200, "text/html; charset=utf-8",
                                  f.read_bytes(), headers=((
                                      "Content-Security-Policy",
                                      REPORT_POLICY),))
        return self._send(404, "text/plain", "not found")

    def do_POST(self):
        from urllib.parse import urlparse
        # the body read before anything is answered: refused unread, the
        # connection closed under a client still sending it
        try:
            size = min(int(self.headers.get("Content-Length", 0)), 4096)
        except ValueError:
            size = 0
        raw = self.rfile.read(size) if size else b""
        if not self._host_ok():
            return self._send(403, "text/plain", "not this host")
        if urlparse(self.path).path == "/api/login":
            return self._sign_in()
        if not self._authed() or not _same(
                self.headers.get("X-Migkit-Csrf", ""), self._csrf()):
            return self._send(403, "text/plain", "not from this page")
        url = urlparse(self.path)
        if url.path not in ("/api/hold", "/api/resume"):
            return self._send(404, "text/plain", "not found")
        try:
            asked = json.loads(raw or b"{}")
            name, db = str(asked["hop"]), str(asked["db"])
        except (ValueError, KeyError):
            return self._send(400, "text/plain", "hop and db, as JSON")
        hops = load_hops()
        user = self._user()
        if name not in hops or role(hops[name], user) is None:
            return self._send(404, "text/plain", "no such hop")
        if role(hops[name], user) != "operator":
            return self._send(403, "text/plain", "you view this hop and do"
                                                 " not operate it")
        # a database of this hop, and only of this one: `../other/app`
        # resolves inside the reports but is another hop's, whose tail the
        # role asked for here does not reach
        where = REPORTS / name / db
        if where.resolve().parent != (REPORTS / name).resolve() \
                or not where.is_dir():
            return self._send(404, "text/plain", "no such database")
        from . import audit, tailctl
        if url.path == "/api/hold":
            (where / tailctl.PAUSE).write_text(str(time.time()))
        else:
            tailctl.resume(where)
        audit.append(REPORTS / name / "changelog.jsonl",
                     {"op": "tail-" + url.path.rsplit("/", 1)[1],
                      "db": db, "through": "dashboard",
                      "by": user or "the holder of the printed address"})
        return self._send(200, "application/json", json.dumps({"ok": True}))


def serve(port):
    """The dashboard on `port`, at the address `MIGKIT_UI_BIND` names
    (127.0.0.1 unless a container's own network must reach it), with the
    metrics' snapshot taken beside it and the hops' own schedules fired
    from here."""
    import secrets

    from . import envkeys, schedule
    from .config import load_hops
    if envkeys.text("MIGKIT_UI_USER_HEADER") and \
            not envkeys.text("MIGKIT_UI_PROXY_SECRET"):
        raise SystemExit(
            "MIGKIT_UI_USER_HEADER names the header a signing-in proxy puts"
            " who it let in into, and anything that reaches the view can"
            " send that header: set MIGKIT_UI_PROXY_SECRET too - the value"
            f" the proxy sends in {PROXY_HEADER} - and the view believes"
            " only the proxy")
    bind = envkeys.text("MIGKIT_UI_BIND")
    Handler.token, Handler.port = secrets.token_urlsafe(24), port
    Handler.once = secrets.token_urlsafe(24)
    srv = ThreadingHTTPServer((bind, port), Handler)
    collector = Handler.collector = Collector().start()
    print(f"migkit ui: http://127.0.0.1:{port}/#{Handler.once}"
          " (opens the view once; ctrl-c to stop)")
    # the hops that run on a schedule of their own are fired from here,
    # the one process of migkit's that runs anyway (`migkit.schedule`)
    stop = threading.Event()
    threading.Thread(target=schedule.loop, args=(load_hops, stop),
                     daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        collector.stop()
        Handler.collector = None
