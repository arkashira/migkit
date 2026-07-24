import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import REPORTS, load_hops

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>migkit</title>
<style>
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
  font-size: 12px; }
.pill.ok { color: var(--ok); } .pill.diff { color: var(--diff); }
.pill.error { color: var(--err); } .pill.na { color: var(--na); }
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
  margin: 6px 0 12px; word-break: break-all; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit,
  minmax(64px, 1fr)); gap: 8px; margin-bottom: 12px; }
.tile { border: 1px solid var(--line); border-radius: 9px;
  padding: 7px 6px 6px; text-align: center; }
.tile.ok { background: var(--ok-bg); } .tile.diff { background: var(--diff-bg); }
.tile.error { background: var(--err-bg); }
.tile b { font-size: 15px; display: block; font-variant-numeric: tabular-nums; }
.tile span { font-size: 10px; color: var(--mut); text-transform: uppercase;
  letter-spacing: .05em; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
  flex: none; }
.dot.ok { background: var(--ok); } .dot.diff { background: var(--diff); }
.dot.error { background: var(--err); animation: pulse 1.6s infinite; }
.dot.na { background: var(--na); }
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
</style></head><body><div class="wrap">
<header>
  <h1>migkit <span class="k">dashboard</span></h1>
  <div class="sum" id="sum">loading&hellip;</div>
</header>
<div class="grid" id="grid"></div>
<div class="card feed" id="feed" style="display:none">
  <h3>recent writes</h3><div id="feedrows"></div>
</div>
<footer>auto-refresh 10s &middot; read-only view &middot; 127.0.0.1 only</footer>
</div>
<script>
const esc = s => String(s ?? '').replace(/[&<>"]/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function load() {
  const r = await fetch('/api/data');
  const data = await r.json();
  const worst = {ok: 0, diff: 0, error: 0, na: 0};
  for (const h of data.hops) worst[h.status] = (worst[h.status] || 0) + 1;
  document.getElementById('sum').innerHTML =
    `<span class="pill ok"><span class="dot ok"></span>${worst.ok} ok</span>` +
    `<span class="pill diff"><span class="dot diff"></span>${worst.diff} diff</span>` +
    `<span class="pill error"><span class="dot error"></span>${worst.error} error</span>` +
    `<span class="pill na"><span class="dot na"></span>${worst.na} idle</span>` +
    `<span>updated ${data.now}</span>`;
  const g = document.getElementById('grid');
  g.innerHTML = '';
  for (const h of data.hops) {
    const el = document.createElement('div');
    el.className = 'card';
    let tiles = '';
    for (const [name, st] of Object.entries(h.checks)) {
      tiles += `<div class="tile ${st.status}"><b>${st.pass}/${st.total}</b>` +
        `<span>${esc(name)}</span></div>`;
    }
    let rows = '';
    for (const d of h.dbs) {
      rows += `<div><span class="dot ${d.status}"></span>` +
        `<span class="db">${esc(d.name)}</span>` +
        (d.note ? `<span class="note">${esc(d.note)}</span>` : '') + '</div>';
    }
    el.innerHTML =
      `<h2><span class="dot ${h.status}"></span>${esc(h.name)}` +
      `<span class="badge">${esc(h.engine)}</span>` +
      (h.service ? `<span class="badge">${esc(h.service)}</span>` : '') +
      (h.has_report ? `<a href="/report/${esc(h.name)}" target="_blank">report</a>` : '') +
      `</h2>` +
      `<div class="route">${esc(h.route)}</div>` +
      (tiles ? `<div class="tiles">${tiles}</div>` : '') +
      `<div class="rows">${rows}</div>` +
      `<div class="meta">${esc(h.meta)}</div>`;
    g.appendChild(el);
  }
  const feed = document.getElementById('feed');
  if (data.activity && data.activity.length) {
    feed.style.display = '';
    document.getElementById('feedrows').innerHTML = data.activity.map(a =>
      `<div><span class="t">${esc(a.at)}</span>` +
      `<span class="op">${esc(a.op)}</span>` +
      `<span>${esc(a.db || '')} ${esc(a.detail || a.note || '')}</span>` +
      `<span class="hop">${esc(a.hop)}</span></div>`).join('');
  }
}
load();
setInterval(load, 10000);
</script></body></html>"""


def _hop_data(name, hop):
    out = {"name": name, "engine": hop.engine, "service": hop.service or "",
           "status": "na", "checks": {}, "dbs": [],
           "meta": "no checks run yet", "has_report": False,
           "route": f"{hop.source.host or '?'} -> "
                    f"{hop.target.host or hop.target.options.get('hosts', '?')}"}
    rpt = REPORTS / name
    summary = rpt / "summary.json"
    out["has_report"] = (rpt / "report.html").exists()
    if summary.exists():
        try:
            results = json.loads(summary.read_text())
        except ValueError:
            results = []
        per_check = {}
        per_db = {}
        for r in results:
            c = r.get("check", "?")
            db = r.get("scope", "?").split()[0].split(".")[0]
            st = r.get("status", "?")
            pc = per_check.setdefault(c, {"pass": 0, "total": 0,
                                          "status": "ok"})
            pc["total"] += 1
            if st == "ok":
                pc["pass"] += 1
            elif st == "error":
                pc["status"] = "error"
            elif pc["status"] != "error":
                pc["status"] = "diff"
            cur = per_db.setdefault(db, {"status": "ok", "note": ""})
            if st == "error":
                cur["status"] = "error"
            elif st == "diff" and cur["status"] != "error":
                cur["status"] = "diff"
                cur["note"] = (r.get("detail") or "")[:60]
        out["checks"] = per_check
        out["dbs"] = [{"name": k, **v} for k, v in sorted(per_db.items())]
        worst = "ok"
        for pc in per_check.values():
            if pc["status"] == "error":
                worst = "error"
                break
            if pc["status"] == "diff":
                worst = "diff"
        out["status"] = worst
        age = time.time() - summary.stat().st_mtime
        out["meta"] = (f"last check {int(age // 60)}m ago"
                       if age < 86400 else
                       f"last check {int(age // 3600)}h ago")
    cl = rpt / "changelog.jsonl"
    if cl.exists():
        last = cl.read_text().splitlines()[-1:]
        if last:
            try:
                e = json.loads(last[0])
                out["meta"] += (f" | last write: {e.get('op')}"
                                f" {e.get('db', '')} at {e.get('at')}")
            except ValueError:
                pass
    return out


def _activity(hops):
    entries = []
    for name in hops:
        cl = REPORTS / name / "changelog.jsonl"
        if not cl.exists():
            continue
        for line in cl.read_text().splitlines()[-10:]:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            e["hop"] = name
            entries.append(e)
    entries.sort(key=lambda e: e.get("at", ""), reverse=True)
    return entries[:12]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes)
                         else body.encode())

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            return self._send(200, "text/html; charset=utf-8", PAGE)
        if self.path == "/api/data":
            hops = load_hops()
            data = {"now": time.strftime("%H:%M:%S"),
                    "hops": [_hop_data(n, h) for n, h in hops.items()],
                    "activity": _activity(hops)}
            return self._send(200, "application/json", json.dumps(data))
        if self.path.startswith("/report/"):
            name = self.path[len("/report/"):].split("/")[0].split("?")[0]
            f = REPORTS / name / "report.html"
            if f.exists() and Path(f).resolve().is_relative_to(
                    REPORTS.resolve()):
                return self._send(200, "text/html; charset=utf-8",
                                  f.read_bytes())
        return self._send(404, "text/plain", "not found")


def serve(port):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"migkit ui: http://127.0.0.1:{port} (ctrl-c to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
